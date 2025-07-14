import os
import pytest
from unittest.mock import Mock, patch
from datetime import datetime

@pytest.fixture
def gmail_config():
    return {
        'credentials_json': {
            'client_id': 'test-client',
            'client_secret': 'test-secret',
            'refresh_token': 'test-refresh'
        }
    }

@pytest.fixture
def mock_gmail_list():
    return {
        'messages': [
            {'id': 'msg1', 'threadId': 'thread1'},
            {'id': 'msg2', 'threadId': 'thread2'}
        ]
    }

@pytest.fixture
def mock_gmail_message():
    return {
        'id': 'msg1',
        'threadId': 'thread1',
        'snippet': 'Test email content',
        'payload': {
            'headers': [
                {'name': 'From', 'value': 'sender@example.com'},
                {'name': 'Subject', 'value': 'Test Subject'},
                {'name': 'Date', 'value': 'Thu, 13 Jul 2023 10:00:00 GMT'}
            ],
            'body': {'data': 'VGVzdCBlbWFpbCBjb250ZW50'} # Base64 "Test email content"
        }
    }

def test_smoke_gmail(gmail_config, mock_gmail_list, mock_gmail_message):
    """Smoke test for Gmail connector."""
    with patch('googleapiclient.discovery.build') as mock_build, \
         patch('google.oauth2.credentials.Credentials') as mock_creds:
        
        # Mock credentials
        mock_creds.return_value.refresh.return_value = None
        
        # Mock Gmail service
        mock_service = Mock()
        mock_build.return_value = mock_service
        
        # Mock messages API
        mock_messages = Mock()
        mock_messages.list().execute.return_value = mock_gmail_list
        mock_messages.get().execute.return_value = mock_gmail_message
        
        # Mock users().messages() chain
        mock_users = Mock()
        mock_users.messages.return_value = mock_messages
        mock_service.users.return_value = mock_users

        from sources.gmail_source import GmailSource
        source = GmailSource(gmail_config)
        
        # Should list messages
        messages = list(source.list_entities())
        assert len(messages) == 2
        assert messages[0] == 'msg1'
        
        # Should get message content
        chunks = list(source.iter_content('msg1'))
        assert len(chunks) == 1
        chunk = chunks[0]
        assert 'Test email content' in chunk.content
        assert chunk.metadata['source'] == 'gmail'
        assert chunk.metadata['id'] == 'msg1'
        assert chunk.metadata['subject'] == 'Test Subject'
