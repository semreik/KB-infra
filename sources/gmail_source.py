import base64
from typing import Dict, Iterator, List
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from .base_source import BaseSource, Chunk

class GmailSource(BaseSource):
    """Gmail connector for Airweave."""
    
    def __init__(self, config: Dict):
        super().__init__(config)
        self.credentials = Credentials(
            token=None,
            refresh_token=config['credentials_json']['refresh_token'],
            client_id=config['credentials_json']['client_id'],
            client_secret=config['credentials_json']['client_secret'],
            token_uri='https://oauth2.googleapis.com/token'
        )
        self.service = build('gmail', 'v1', credentials=self.credentials)
    
    def list_entities(self) -> Iterator[str]:
        """List all message IDs."""
        print("Listing Gmail messages...")
        try:
            results = self.service.users().messages().list(userId='me').execute()
            messages = results.get('messages', [])
            print(f"Found {len(messages)} messages")
            for message in messages:
                yield message['id']
        except Exception as e:
            print(f"Error listing messages: {str(e)}")
            raise
    
    def _get_parts(self, message_part, parts=None):
        """Recursively get all parts from a message."""
        if parts is None:
            parts = []
            
        if 'parts' in message_part:
            for part in message_part['parts']:
                self._get_parts(part, parts)
        else:
            parts.append(message_part)
            
        return parts

    def iter_content(self, message_id: str) -> Iterator[Chunk]:
        """Get content of a specific message including attachments."""
        message = self.service.users().messages().get(userId='me', id=message_id).execute()
        
        # Extract headers
        headers = {}
        for header in message['payload']['headers']:
            name = header['name'].lower()
            if name in ['from', 'to', 'subject', 'date']:
                headers[name] = header['value']
        
        # Get all message parts
        parts = self._get_parts(message['payload'])
        
        # First yield the main message content
        main_content = None
        for part in parts:
            if part.get('mimeType', '').startswith('text/plain'):
                if 'data' in part['body']:
                    main_content = base64.urlsafe_b64decode(
                        part['body']['data'].encode('ASCII')
                    ).decode('utf-8')
                    break
        
        if not main_content:
            main_content = message.get('snippet', '')
        
        # Format content
        content = f"""
From: {headers.get('from', 'Unknown')}
To: {headers.get('to', 'Unknown')}
Subject: {headers.get('subject', 'No Subject')}
Date: {headers.get('date', 'Unknown')}

{main_content}
"""
        
        yield Chunk(
            content=content.strip(),
            metadata={
                'source': 'gmail',
                'id': message_id,
                'subject': headers.get('subject'),
                'from': headers.get('from'),
                'date': headers.get('date'),
                'is_attachment': False
            }
        )
        
        # Then yield any attachments
        for part in parts:
            if 'filename' in part and part['filename']:
                if 'body' in part and 'attachmentId' in part['body']:
                    attachment = self.service.users().messages().attachments().get(
                        userId='me',
                        messageId=message_id,
                        id=part['body']['attachmentId']
                    ).execute()
                    
                    file_data = base64.urlsafe_b64decode(attachment['data'].encode('ASCII'))
                    
                    yield Chunk(
                        content=file_data,
                        metadata={
                            'source': 'gmail',
                            'id': message_id,
                            'is_attachment': True,
                            'filename': part['filename'],
                            'mime_type': part.get('mimeType', 'application/octet-stream')
                        }
                    )
