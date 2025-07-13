import pytest
from unittest.mock import Mock, patch
from datetime import datetime, timedelta

from sources.sap_source import SAPSource

@pytest.fixture
def sap_config():
    return {
        "base_url": "https://sap-test.example.com",
        "client_id": "test-client",
        "client_secret": "test-secret",
        "service_path": "/sap/opu/odata/sap/ZKB_PO_SRV"
    }

@pytest.fixture
def mock_token_response():
    return {
        "access_token": "test-token",
        "expires_in": 3600,
        "token_type": "Bearer"
    }

@pytest.fixture
def mock_po_list_response():
    return {
        "d": {
            "results": [
                {
                    "PurchaseOrder": "4500000001",
                    "CompanyCode": "1000",
                    "DocumentType": "NB",
                    "CreatedAt": "/Date(1689254400000)/",
                    "Supplier": "100602",
                    "Status": "Open"
                }
            ]
        }
    }

@pytest.fixture
def mock_po_detail_response():
    return {
        "d": {
            "PurchaseOrder": "4500000001",
            "CompanyCode": "1000",
            "DocumentType": "NB",
            "CreatedAt": "/Date(1689254400000)/",
            "Supplier": "100602",
            "Status": "Open",
            "Items": {
                "results": [
                    {
                        "ItemNumber": "00010",
                        "Material": "MAT001",
                        "Description": "Test Material",
                        "Quantity": "100.000",
                        "Unit": "EA",
                        "DeliveryDate": "/Date(1689340800000)/"
                    }
                ]
            }
        }
    }

def test_auth_token_retrieval(sap_config, mock_token_response):
    """Test OAuth2 token retrieval and caching."""
    with patch("requests.post") as mock_post:
        mock_post.return_value.json.return_value = mock_token_response
        mock_post.return_value.ok = True
        
        source = SAPSource(sap_config)
        token = source._get_auth_token()
        
        assert token == "test-token"
        mock_post.assert_called_once()

def test_list_entities(sap_config, mock_po_list_response):
    """Test listing available purchase orders."""
    with patch("requests.get") as mock_get:
        mock_get.return_value.json.return_value = mock_po_list_response
        mock_get.return_value.ok = True
        
        source = SAPSource(sap_config)
        entities = list(source.list_entities())
        
        assert len(entities) == 1
        assert entities[0] == "4500000001"

def test_iter_content(sap_config, mock_po_detail_response):
    """Test iterating over PO content and converting to chunks."""
    with patch("requests.get") as mock_get:
        mock_get.return_value.json.return_value = mock_po_detail_response
        mock_get.return_value.ok = True
        
        source = SAPSource(sap_config)
        chunks = list(source.iter_content("4500000001"))
        
        assert len(chunks) == 1
        chunk = chunks[0]
        assert "4500000001" in chunk.content
        assert "Test Material" in chunk.content
        assert chunk.metadata["supplier"] == "100602"
