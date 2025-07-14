import os
import pytest
from unittest.mock import patch, Mock

@pytest.fixture
def mock_google_services():
    """Mock Google API services."""
    # Gmail service
    mock_gmail_service = Mock()
    mock_gmail_messages = Mock()
    mock_gmail_messages.list().execute.return_value = {'messages': [{'id': 'msg1'}]}
    mock_gmail_service.users().messages = mock_gmail_messages
    
    # Drive service
    mock_drive_service = Mock()
    mock_drive_files = Mock()
    mock_drive_files.list().execute.return_value = {'files': [{'id': 'file1'}]}
    mock_drive_service.files = mock_drive_files
    
    return mock_gmail_service, mock_drive_service

@pytest.fixture
def mock_configs():
    """Test configs with mock credentials."""
    return {
        'sap': {
            'base_url': 'https://sap-test.example.com',
            'client_id': 'test-client',
            'client_secret': 'test-secret'
        },
        'gmail': {
            'credentials_json': {
                'client_id': 'test-client',
                'client_secret': 'test-secret',
                'refresh_token': 'test-refresh'
            }
        },
        'outlook': {
            'client_id': 'test-client',
            'client_secret': 'test-secret',
            'tenant_id': 'test-tenant'
        },
        'gdrive': {
            'credentials_json': {
                'client_id': 'test-client',
                'client_secret': 'test-secret',
                'refresh_token': 'test-refresh'
            }
        },
        'onedrive': {
            'client_id': 'test-client',
            'client_secret': 'test-secret',
            'tenant_id': 'test-tenant'
        },
        'postgres': {
            'host': 'localhost',
            'port': 5432,
            'database': 'test_db',
            'user': 'test_user',
            'password': 'test_pass',
            'tables': ['users', 'orders']
        }
    }

@pytest.fixture
def mock_responses():
    """Mock API responses."""
    return {
        'sap': {'d': {'results': [{'PurchaseOrder': '4500000001'}]}},
        'gmail': {'messages': [{'id': 'msg1', 'threadId': 'thread1'}]},
        'outlook': {'value': [{'id': 'msg1', 'subject': 'Test Email 1'}]},
        'gdrive': {'files': [{'id': 'file1', 'name': 'Test Doc 1'}]},
        'onedrive': {'value': [{'id': 'file1', 'name': 'Test Doc 1.docx'}]},
        'postgres': [('users',), ('orders',)]
    }

def test_all_connectors(mock_google_services, mock_configs, mock_responses):
    """Smoke test all connectors to verify they can list and retrieve content."""
    
    # Import all source modules
    from sources.sap_source import SAPSource
    from sources.gmail_source import GmailSource
    from sources.outlook_source import OutlookSource
    from sources.gdrive_source import GDriveSource
    from sources.onedrive_source import OneDriveSource
    from sources.postgres_source import PostgresSource
    
    mock_gmail_service, mock_drive_service = mock_google_services
    
    # Test each source
    with patch.dict(os.environ, {'SAP_MOCK': 'false'}):
        with patch('requests.get') as mock_get, \
             patch('requests.post') as mock_post, \
             patch('googleapiclient.discovery.build') as mock_build, \
             patch('msal.ConfidentialClientApplication') as mock_msal, \
             patch('psycopg2.connect') as mock_pg, \
             patch('google.oauth2.credentials.Credentials') as mock_creds:
            
            # Configure mocks
            mock_get.return_value.json.return_value = mock_responses['sap']
            mock_get.return_value.ok = True
            mock_post.return_value.json.return_value = {'access_token': 'test-token', 'expires_in': 3600}
            mock_post.return_value.ok = True
            
            # Mock Google API
            mock_build.return_value = mock_gmail_service
            mock_creds.return_value.refresh.return_value = None
            
            # Mock MSAL
            mock_msal.return_value.acquire_token_silent.return_value = {'access_token': 'test-token'}
            
            # Mock Postgres cursor
            mock_cursor = Mock()
            mock_cursor.fetchall.return_value = mock_responses['postgres']
            mock_pg.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = mock_cursor
            
            # Test SAP
            source = SAPSource(mock_configs['sap'])
            entities = list(source.list_entities())
            assert len(entities) > 0
            
            # Test Gmail 
            source = GmailSource(mock_configs['gmail'])
            entities = list(source.list_entities())
            assert len(entities) > 0
            
            # Test Outlook
            source = OutlookSource(mock_configs['outlook'])
            entities = list(source.list_entities())
            assert len(entities) > 0
            
            # Test Google Drive
            source = GDriveSource(mock_configs['gdrive'])
            entities = list(source.list_entities())
            assert len(entities) > 0
            
            # Test OneDrive
            source = OneDriveSource(mock_configs['onedrive'])
            entities = list(source.list_entities())
            assert len(entities) > 0
            
            # Test Postgres
            source = PostgresSource(mock_configs['postgres'])
            entities = list(source.list_entities())
            assert len(entities) > 0
