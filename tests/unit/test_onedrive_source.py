import os
import pytest
from unittest.mock import Mock, patch
from datetime import datetime

@pytest.fixture
def onedrive_config():
    return {
        'client_id': 'test-client',
        'client_secret': 'test-secret',
        'tenant_id': 'test-tenant'
    }

@pytest.fixture
def mock_onedrive_list():
    return {
        'value': [
            {
                'id': 'file1',
                'name': 'Test Doc 1.docx',
                'lastModifiedDateTime': '2023-07-13T10:00:00Z',
                'file': {'mimeType': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'}
            },
            {
                'id': 'file2',
                'name': 'Test Sheet 1.xlsx',
                'lastModifiedDateTime': '2023-07-13T11:00:00Z',
                'file': {'mimeType': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'}
            }
        ]
    }

@pytest.fixture
def mock_onedrive_content():
    return b'Test document content'

def test_smoke_onedrive(onedrive_config, mock_onedrive_list, mock_onedrive_content):
    """Smoke test for OneDrive connector."""
    with patch('msal.ConfidentialClientApplication') as mock_msal:
        # Mock MSAL client
        mock_client = Mock()
        mock_msal.return_value = mock_client
        mock_client.acquire_token_silent.return_value = {'access_token': 'test-token'}
        
        with patch('requests.get') as mock_get:
            # Mock list files response
            mock_list_response = Mock()
            mock_list_response.ok = True
            mock_list_response.json.return_value = {
                'value': [
                    {'id': 'file1', 'name': 'Test Doc 1.docx'},
                    {'id': 'file2', 'name': 'Test Doc 2.docx'}
                ]
            }
            
            # Mock file metadata response
            mock_file_response = Mock()
            mock_file_response.json.return_value = {
                'file': {
                    'mimeType': 'text/plain'
                }
            }
            mock_file_response.ok = True
            
            # Mock file metadata response
            mock_file_response = Mock()
            mock_file_response.json.return_value = {
                'file': {
                    'mimeType': 'text/plain'
                },
                'name': 'Test Doc 1.docx',
                'id': 'file1'
            }
            mock_file_response.ok = True
            
            # Mock content response
            mock_content_response = Mock()
            mock_content_response.content = b'Test document content'
            mock_content_response.ok = True
            
            # Configure mock to return different responses
            mock_get.side_effect = [mock_list_response, mock_file_response, mock_content_response]

            from sources.onedrive_source import OneDriveSource
            source = OneDriveSource(onedrive_config)
            
            # Should list files
            files = list(source.list_entities())
            assert len(files) == 2
            assert files[0] == 'file1'
            
            # Should get file content
            chunks = list(source.iter_content('file1'))
            assert len(chunks) == 1
            chunk = chunks[0]
            assert 'Test document content' in chunk.content
            assert chunk.metadata['source'] == 'onedrive'
            assert chunk.metadata['id'] == 'file1'
            assert chunk.metadata['name'] == 'Test Doc 1.docx'
