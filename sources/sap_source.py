from typing import Iterator, Dict, Any, Optional
import os
import json
from datetime import datetime, timedelta
import requests
from airweave.sources import BaseSource
from airweave.types import TextChunk

class SAPSource(BaseSource):
    """Custom source for SAP S/4HANA OData service.
    
    This connector fetches Purchase Orders from SAP via OData.
    For development/testing, it can use a mock response when SAP_MOCK=true.
    """
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.base_url = config.get('base_url', 'https://sap.example.com')
        self.client_id = config.get('client_id', 'demo')
        self.client_secret = config.get('client_secret', 'demo')
        self.service_path = config.get('service_path', '/sap/opu/odata/sap/ZKB_PO_SRV')
        self._access_token = None
        self._token_expires_at = None
        
        # For development/testing
        self.use_mock = os.getenv('SAP_MOCK', 'true').lower() == 'true'

    def _get_mock_data(self, path: str) -> Dict[str, Any]:
        """Return mock data for development/testing."""
        if path == '/PurchaseOrders':
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
        elif path.startswith('/PurchaseOrders('):
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
        return {'d': {'results': []}}

    def _get_auth_token(self) -> str:
        """Get OAuth2 access token for SAP API."""
        if self.use_mock:
            return 'mock-token'
            
        token_url = f"{self.base_url}/oauth/token"
        response = requests.post(
            token_url,
            data={
                'grant_type': 'client_credentials',
                'client_id': self.client_id,
                'client_secret': self.client_secret
            },
            headers={'Accept': 'application/json'}
        )
        response.raise_for_status()
        
        token_data = response.json()
        self._token_expires_at = datetime.now() + timedelta(seconds=token_data['expires_in'])
        return token_data['access_token']

    def _make_request(self, path: str) -> Dict[str, Any]:
        """Make authenticated request to SAP API."""
        if self.use_mock:
            return self._get_mock_data(path)
            
        if not self._access_token or datetime.now() >= self._token_expires_at:
            self._access_token = self._get_auth_token()
            
        headers = {
            'Authorization': f'Bearer {self._access_token}',
            'Accept': 'application/json'
        }
        
        response = requests.get(
            f"{self.base_url}{self.service_path}{path}",
            headers=headers
        )
        response.raise_for_status()
        return response.json()

    def _format_po_as_text(self, po_data: Dict[str, Any]) -> str:
        """Format PO data as searchable text."""
        items = po_data.get('Items', {}).get('results', [])
        items_text = '\n'.join(
            f"Item {item['ItemNumber']}: {item['Quantity']} {item['Unit']} of {item['Description']} "
            f"(Material: {item['Material']}) - Delivery: {item['DeliveryDate']}"
            for item in items
        )
        
        return f"""Purchase Order {po_data['PurchaseOrder']}
Company: {po_data['CompanyCode']}
Type: {po_data['DocumentType']}
Created: {po_data['CreatedAt']}
Supplier: {po_data['Supplier']}
Status: {po_data['Status']}

Items:
{items_text}
"""

    def list_entities(self) -> Iterator[str]:
        """List available purchase orders."""
        response = self._make_request('/PurchaseOrders')
        for po in response['d']['results']:
            yield po['PurchaseOrder']

    def iter_content(self, entity: str) -> Iterator[TextChunk]:
        """Yield purchase order items as TextChunks."""
        response = self._make_request(f'/PurchaseOrders({entity})')
        po_data = response['d']
        
        yield TextChunk(
            content=self._format_po_as_text(po_data),
            metadata={
                'source': 'sap_s4hana',
                'id': po_data['PurchaseOrder'],
                'supplier': po_data['Supplier'],
                'status': po_data['Status'],
                'company_code': po_data['CompanyCode'],
                'created_at': po_data['CreatedAt']
            }
        )

    def get_metadata(self) -> Dict[str, Any]:
        """Return source metadata."""
        return {
            'name': 'sap_s4hana',
            'type': 'erp',
            'description': 'SAP S/4HANA Purchase Orders via OData'
        }
