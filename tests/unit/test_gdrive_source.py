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
                {
                    'id': 'doc1',
                    'name': 'Test Document.gdoc',
                    'mimeType': 'application/vnd.google-apps.document',
                    'modifiedTime': '2023-07-14T10:00:00Z'
                },
                {
                    'id': 'file1', 
                    'name': 'Test File.txt',
                    'mimeType': 'text/plain',
                    'modifiedTime': '2023-07-14T11:00:00Z'
                }
            ]
        }
        mock_files.list.return_value = mock_list
        
        # Mock get method for Google Doc
        mock_get_doc = Mock()
        mock_get_doc.execute.return_value = {
            'id': 'doc1',
            'name': 'Test Document.gdoc',
            'mimeType': 'application/vnd.google-apps.document',
            'modifiedTime': '2023-07-14T10:00:00Z'
        }
        
        # Mock get method for regular file
        mock_get_file = Mock()
        mock_get_file.execute.return_value = {
            'id': 'file1',
            'name': 'Test File.txt',
            'mimeType': 'text/plain',
            'modifiedTime': '2023-07-14T11:00:00Z'
        }
        
        # Configure get method to return different responses based on file ID
        def mock_get(**kwargs):
            file_id = kwargs.get('fileId')
            if file_id == 'doc1':
                return mock_get_doc
            return mock_get_file
        mock_files.get.side_effect = mock_get
        
        # Mock export method for Google Doc
        mock_export = Mock()
        mock_export.execute.return_value = b'Test document content'
        mock_files.export.return_value = mock_export
        
        # Mock get_media method for regular file
        mock_get_media = Mock()
        mock_get_media.execute.return_value = b'Test file content'
        mock_files.get_media.return_value = mock_get_media

        from sources.gdrive_source import GDriveSource
        source = GDriveSource(gdrive_config)
        
        # Should list files
        files = list(source.list_entities())
        assert len(files) == 2
        assert 'doc1' in files
        assert 'file1' in files
        
        # Should get Google Doc content
        doc_chunks = list(source.iter_content('doc1'))
        assert len(doc_chunks) == 1
        doc_chunk = doc_chunks[0]
        assert 'Test document content' in doc_chunk.content
        assert doc_chunk.metadata['source'] == 'gdrive'
        assert doc_chunk.metadata['id'] == 'doc1'
        assert doc_chunk.metadata['name'] == 'Test Document.gdoc'
        assert doc_chunk.metadata['mime_type'] == 'application/vnd.google-apps.document'
        
        # Should get regular file content
        file_chunks = list(source.iter_content('file1'))
        assert len(file_chunks) == 1
        file_chunk = file_chunks[0]
        assert 'Test file content' in file_chunk.content
        assert file_chunk.metadata['source'] == 'gdrive'
        assert file_chunk.metadata['id'] == 'file1'
        assert file_chunk.metadata['name'] == 'Test File.txt'
        assert file_chunk.metadata['mime_type'] == 'text/plain'
