from typing import Dict, Iterator
import msal
import requests
from .base_source import BaseSource, Chunk

class OneDriveSource(BaseSource):
    """OneDrive connector for Airweave."""
    
    def __init__(self, config: Dict):
        super().__init__(config)
        self.client_id = config['client_id']
        self.client_secret = config['client_secret']
        self.tenant_id = config['tenant_id']
        self.authority = f'https://login.microsoftonline.com/{self.tenant_id}'
        self.scopes = ['https://graph.microsoft.com/.default']
        
        self.app = msal.ConfidentialClientApplication(
            self.client_id,
            authority=self.authority,
            client_credential=self.client_secret
        )
        self._access_token = None
    
    def _get_token(self) -> str:
        """Get Microsoft Graph access token."""
        if not self._access_token:
            result = self.app.acquire_token_silent(self.scopes, account=None)
            if not result:
                result = self.app.acquire_token_for_client(scopes=self.scopes)
            self._access_token = result['access_token']
        return self._access_token
    
    def list_entities(self) -> Iterator[str]:
        """List all file IDs."""
        headers = {
            'Authorization': f'Bearer {self._get_token()}',
            'Accept': 'application/json'
        }
        response = requests.get(
            'https://graph.microsoft.com/v1.0/me/drive/root/children',
            headers=headers
        )
        response.raise_for_status()
        files = response.json().get('value', [])
        for file in files:
            yield file['id']
    
    def iter_content(self, file_id: str) -> Iterator[Chunk]:
        """Get content of a specific file."""
        headers = {
            'Authorization': f'Bearer {self._get_token()}',
            'Accept': 'application/json'
        }
        
        # Get file metadata
        response = requests.get(
            f'https://graph.microsoft.com/v1.0/me/drive/items/{file_id}',
            headers=headers
        )
        response.raise_for_status()
        file = response.json()
        
        # For text files, get content
        if 'file' in file and file['file'].get('mimeType', '').startswith('text/'):
            content_response = requests.get(
                f'https://graph.microsoft.com/v1.0/me/drive/items/{file_id}/content',
                headers=headers
            )
            content = content_response.text
        else:
            content = f"[File: {file['name']}]"
        
        yield Chunk(
            content=content,
            metadata={
                'source': 'onedrive',
                'id': file_id,
                'name': file.get('name'),
                'mime_type': file.get('file', {}).get('mimeType'),
                'modified': file.get('lastModifiedDateTime')
            }
        )
