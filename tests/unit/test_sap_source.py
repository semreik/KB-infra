import os
import pytest
from unittest.mock import Mock, patch
from datetime import datetime, timedelta

from sources.sap_source import SAPSource

@pytest.fixture
def sap_config():
    return {
        'base_url': 'https://sap-test.example.com',
        'client_id': 'test-client',
        'client_secret': 'test-secret',
        'service_path': '/sap/opu/odata/sap/ZKB_PO_SRV'
    }

@pytest.fixture
def mock_token_response():
    return {
        'access_token': 'test-token',
        'expires_in': 3600,
        'token_type': 'Bearer'
    }

@pytest.fixture
def mock_po_list_response():
    return {
        'd': {
            'results': [
                {
                    'PurchaseOrder': '4500000001',
                    'CompanyCode': '1000',
                    'DocumentType': 'NB',
                    'CreatedAt': '/Date(1689254400000)/',
                    'Supplier': '100602',
                    'Status': 'Open'
                }
            ]
        }
    }

@pytest.fixture
def mock_po_detail_response():
    return {
        'd': {
            'PurchaseOrder': '4500000001',
            'CompanyCode': '1000',
            'DocumentType': 'NB',
            'CreatedAt': '/Date(1689254400000)/',
            'Supplier': '100602',
            'Status': 'Open',
            'Items': {
                'results': [
                    {
                        'ItemNumber': '00010',
                        'Material': 'MAT001',
                        'Description': 'Test Material',
                        'Quantity': '100.000',
                        'Unit': 'EA',
                        'DeliveryDate': '/Date(1689340800000)/'
                    }
                ]
            }
        }
    }

def test_default_config():
    """Test source initialization with default config."""
    source = SAPSource({})
    assert source.base_url == 'https://sap.example.com'
    assert source.client_id == 'demo'
    assert source.client_secret == 'demo'
    assert source.use_mock == True

def test_mock_mode_enabled(sap_config):
    """Test that mock mode works correctly."""
    with patch.dict(os.environ, {'SAP_MOCK': 'true'}):
        source = SAPSource(sap_config)
        assert source.use_mock == True
        
        # Should return mock token without making HTTP call
        token = source._get_auth_token()
        assert token == 'mock-token'
        
        # Should return mock PO list without making HTTP call
        pos = list(source.list_entities())
        assert len(pos) == 1
        assert pos[0] == '4500000001'

def test_auth_token_retrieval(sap_config, mock_token_response):
    """Test OAuth2 token retrieval and caching."""
    with patch.dict(os.environ, {'SAP_MOCK': 'false'}):
        with patch('requests.post') as mock_post:
            mock_post.return_value.json.return_value = mock_token_response
            mock_post.return_value.ok = True
            
            source = SAPSource(sap_config)
            token = source._get_auth_token()
            
            assert token == 'test-token'
            mock_post.assert_called_once_with(
                'https://sap-test.example.com/oauth/token',
                data={
                    'grant_type': 'client_credentials',
                    'client_id': 'test-client',
                    'client_secret': 'test-secret'
                },
                headers={'Accept': 'application/json'}
            )

def test_token_caching():
    """Test that tokens are cached and reused."""
    with patch.dict(os.environ, {'SAP_MOCK': 'false'}):
        with patch('requests.post') as mock_post:
            mock_response = Mock()
            mock_response.json.return_value = {'access_token': 'test-token', 'expires_in': 3600}
            mock_response.ok = True
            mock_post.return_value = mock_response
            
            source = SAPSource({
                'base_url': 'https://sap-test.example.com',
                'client_id': 'test-client',
                'client_secret': 'test-secret'
            })
            
            # Reset mock call count
            mock_post.reset_mock()
            
            # First call should get token
            token1 = source._get_auth_token()
            assert token1 == 'test-token'
            assert mock_post.call_count == 1
            
            # Second call should reuse cached token
            token2 = source._get_auth_token()
            assert token2 == 'test-token'
            assert mock_post.call_count == 1

def test_list_entities(sap_config, mock_po_list_response):
    """Test listing available purchase orders."""
    with patch('requests.post') as mock_post, patch('requests.get') as mock_get:
        # Mock token response
        mock_token_resp = Mock()
        mock_token_resp.json.return_value = {'access_token': 'test-token', 'expires_in': 3600}
        mock_token_resp.ok = True
        mock_post.return_value = mock_token_resp
        
        # Mock list response
        mock_list_resp = Mock()
        mock_list_resp.json.return_value = {
            'd': {
                'results': [
                    {
                        'PurchaseOrder': '4500000001',
                        'CompanyCode': '1000',
                        'DocumentType': 'NB',
                        'CreatedAt': '/Date(1689254400000)/',
                        'Supplier': '100602',
                        'Status': 'Open'
                    }
                ]
            }
        }
        mock_list_resp.ok = True
        mock_get.return_value = mock_list_resp
        
        source = SAPSource(sap_config)
        entities = list(source.list_entities())
        
        assert len(entities) == 1
        assert entities[0] == '4500000001'

def test_iter_content(sap_config, mock_po_detail_response):
    """Test retrieving purchase order content."""
    with patch('requests.post') as mock_post, patch('requests.get') as mock_get:
        # Mock token response
        mock_token_resp = Mock()
        mock_token_resp.json.return_value = {'access_token': 'test-token', 'expires_in': 3600}
        mock_token_resp.ok = True
        mock_post.return_value = mock_token_resp
        
        # Mock content response
        mock_content_resp = Mock()
        mock_content_resp.json.return_value = {
            'd': {
                'PurchaseOrder': '4500000001',
                'CompanyCode': '1000',
                'DocumentType': 'NB',
                'CreatedAt': '/Date(1689254400000)/',
                'Supplier': '100602',
                'Status': 'Open',
                'Items': {
                    'results': [
                        {
                            'ItemNumber': '00010',
                            'Material': 'MAT001',
                            'Description': 'Test Material',
                            'Quantity': '100.000',
                            'Unit': 'EA',
                            'DeliveryDate': '/Date(1689340800000)/'
                        }
                    ]
                }
            }
        }
        mock_content_resp.ok = True
        mock_get.return_value = mock_content_resp
        
        source = SAPSource(sap_config)
        chunks = list(source.iter_content('4500000001'))
        
        assert len(chunks) == 1
        chunk = chunks[0]
        
        # Check content formatting
        assert '4500000001' in chunk.content
        assert 'Company: 1000' in chunk.content
        assert 'Supplier: 100602' in chunk.content
        assert 'Item 00010: 100.000 EA of Test Material' in chunk.content
        
        # Check metadata
        assert chunk.metadata['source'] == 'sap_s4hana'
        assert chunk.metadata['id'] == '4500000001'
        assert chunk.metadata['supplier'] == '100602'
        assert chunk.metadata['status'] == 'Open'

@pytest.mark.skipif(not os.getenv('SAP_TEST'), reason='SAP integration tests require SAP_TEST=1')
def test_integration_real_sap():
    """Integration test with real SAP instance.
    
    To run this test:
    1. Set SAP_TEST=1
    2. Configure these env vars:
       - SAP_BASE_URL
       - SAP_CLIENT_ID
       - SAP_CLIENT_SECRET
    """
    config = {
        'base_url': os.getenv('SAP_BASE_URL'),
        'client_id': os.getenv('SAP_CLIENT_ID'),
        'client_secret': os.getenv('SAP_CLIENT_SECRET')
    }
    
    source = SAPSource(config)
    
    # Should be able to get auth token
    token = source._get_auth_token()
    assert token
    
    # Should list at least one PO
    pos = list(source.list_entities())
    assert len(pos) > 0
    
    # Should get PO details
    po_id = pos[0]
    chunks = list(source.iter_content(po_id))
    assert len(chunks) == 1
    
    chunk = chunks[0]
    assert chunk.content
    assert chunk.metadata['id'] == po_id
