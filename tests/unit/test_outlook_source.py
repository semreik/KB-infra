import os
import pytest
from unittest.mock import Mock, patch
from datetime import datetime

@pytest.fixture
def outlook_config():
    return {
        'client_id': 'test-client',
        'client_secret': 'test-secret',
        'tenant_id': 'test-tenant'
    }

@pytest.fixture
def mock_outlook_list():
    return {
        'value': [
            {
                'id': 'msg1',
                'subject': 'Test Email 1',
                'receivedDateTime': '2023-07-13T10:00:00Z'
            },
            {
                'id': 'msg2',
                'subject': 'Test Email 2',
                'receivedDateTime': '2023-07-13T11:00:00Z'
            }
        ]
    }

@pytest.fixture
def mock_outlook_message():
    return {
        'id': 'msg1',
        'subject': 'Test Email 1',
        'body': {'content': 'Test email content'},
        'from': {'emailAddress': {'address': 'sender@example.com'}},
        'receivedDateTime': '2023-07-13T10:00:00Z'
    }

def test_smoke_outlook(outlook_config, mock_outlook_list, mock_outlook_message):
    """Smoke test for Outlook connector."""
    with patch('msal.ConfidentialClientApplication') as mock_msal:
        mock_msal.return_value.acquire_token_silent.return_value = {'access_token': 'test-token'}
        
        with patch('requests.get') as mock_get:
            # Mock list messages
            mock_get.return_value.json.side_effect = [mock_outlook_list, mock_outlook_message]
            mock_get.return_value.ok = True

            from sources.outlook_source import OutlookSource
            source = OutlookSource(outlook_config)
            
            # Should list messages
            messages = list(source.list_entities())
            assert len(messages) == 2
            assert messages[0] == 'msg1'
            
            # Should get message content
            chunks = list(source.iter_content('msg1'))
            assert len(chunks) == 1
            chunk = chunks[0]
            assert 'Test email content' in chunk.content
            assert chunk.metadata['source'] == 'outlook'
            assert chunk.metadata['id'] == 'msg1'
            assert chunk.metadata['subject'] == 'Test Email 1'
