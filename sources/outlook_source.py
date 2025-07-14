from typing import Dict, Iterator
import msal
import requests
from .base_source import BaseSource, Chunk

class OutlookSource(BaseSource):
    """Outlook/Microsoft Graph connector for Airweave."""
    
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
        """List all message IDs."""
        headers = {
            'Authorization': f'Bearer {self._get_token()}',
            'Accept': 'application/json'
        }
        response = requests.get(
            'https://graph.microsoft.com/v1.0/me/messages',
            headers=headers
        )
        response.raise_for_status()
        messages = response.json().get('value', [])
        for message in messages:
            yield message['id']
    
    def iter_content(self, message_id: str) -> Iterator[Chunk]:
        """Get content of a specific message."""
        headers = {
            'Authorization': f'Bearer {self._get_token()}',
            'Accept': 'application/json'
        }
        response = requests.get(
            f'https://graph.microsoft.com/v1.0/me/messages/{message_id}',
            headers=headers
        )
        response.raise_for_status()
        message = response.json()
        
        content = f"""
From: {message.get('from', {}).get('emailAddress', {}).get('address', 'Unknown')}
Subject: {message.get('subject', 'No Subject')}
Date: {message.get('receivedDateTime', 'Unknown')}

{message.get('body', {}).get('content', '')}
"""
        
        yield Chunk(
            content=content.strip(),
            metadata={
                'source': 'outlook',
                'id': message_id,
                'subject': message.get('subject'),
                'from': message.get('from', {}).get('emailAddress', {}).get('address'),
                'date': message.get('receivedDateTime')
            }
        )
