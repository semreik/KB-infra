import os
import pytest
from unittest.mock import Mock, patch
from datetime import datetime

@pytest.fixture
def gdrive_config():
    return {
        'credentials_json': {
            'client_id': 'test-client',
            'client_secret': 'test-secret',
            'refresh_token': 'test-refresh'
        }
    }

@pytest.fixture
def mock_gdrive_list():
    return {
        'files': [
            {
                'id': 'file1',
                'name': 'Test Doc 1',
                'mimeType': 'application/vnd.google-apps.document',
                'modifiedTime': '2023-07-13T10:00:00.000Z'
            },
            {
                'id': 'file2',
                'name': 'Test Sheet 1',
                'mimeType': 'application/vnd.google-apps.spreadsheet',
                'modifiedTime': '2023-07-13T11:00:00.000Z'
            }
        ]
    }

@pytest.fixture
def mock_gdrive_content():
    return {
        'body': 'Test document content'
    }

def test_smoke_gdrive(gdrive_config, mock_gdrive_list, mock_gdrive_content):
    """Smoke test for Google Drive connector."""
    with patch('googleapiclient.discovery.build') as mock_build, \
         patch('google.oauth2.credentials.Credentials') as mock_creds:
        
        # Mock credentials
        mock_creds.return_value.refresh.return_value = None
        
        # Mock Drive service
        mock_service = Mock()
        mock_build.return_value = mock_service
        
        # Mock files API
        mock_files = Mock()
        mock_service.files.return_value = mock_files
        
        # Mock list method
        mock_list = Mock()
        mock_list.execute.return_value = {
            'files': [
                {'id': 'file1', 'name': 'Test Doc 1.docx', 'mimeType': 'application/vnd.google-apps.document'},
                {'id': 'file2', 'name': 'Test Doc 2.docx', 'mimeType': 'application/vnd.google-apps.document'}
            ]
        }
        mock_files.list.return_value = mock_list
        
        # Mock get method
        mock_get_file = Mock()
        mock_get_file.execute.return_value = {
            'id': 'file1',
            'name': 'Test Doc 1.docx',
            'mimeType': 'application/vnd.google-apps.document'
        }
        mock_files.get.return_value = mock_get_file
        
        # Mock get_media method
        mock_get_media = Mock()
        mock_get_media.execute.return_value = b'Test document content'
        mock_files.get_media.return_value = mock_get_media

        from sources.gdrive_source import GDriveSource
        source = GDriveSource(gdrive_config)
        
        # Should list files
        files = list(source.list_entities())
        assert len(files) == 2
        assert files[0] == 'file1'
        
        # Should get file content
        chunks = list(source.iter_content('file1'))
        assert len(chunks) == 1
        chunk = chunks[0]
        assert 'Test document content' in chunk.content
        assert chunk.metadata['source'] == 'gdrive'
        assert chunk.metadata['id'] == 'file1'
        assert chunk.metadata['name'] == 'Test Doc 1'
