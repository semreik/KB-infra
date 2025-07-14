from typing import Dict, Iterator
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from .base_source import BaseSource, Chunk

class GDriveSource(BaseSource):
    """Google Drive connector for Airweave."""
    
    def __init__(self, config: Dict):
        super().__init__(config)
        self.credentials = Credentials(
            token=None,
            refresh_token=config['credentials_json']['refresh_token'],
            client_id=config['credentials_json']['client_id'],
            client_secret=config['credentials_json']['client_secret'],
            token_uri='https://oauth2.googleapis.com/token'
        )
        self.service = build('drive', 'v3', credentials=self.credentials)
    
    def list_entities(self) -> Iterator[str]:
        """List all file IDs."""
        results = self.service.files().list(
            pageSize=100,
            fields="files(id, name, mimeType, modifiedTime)"
        ).execute()
        files = results.get('files', [])
        for file in files:
            yield file['id']
    
    def iter_content(self, file_id: str) -> Iterator[Chunk]:
        """Get content of a specific file.
        
        For Google Workspace files (Docs, Sheets, etc), exports as text.
        For regular files, downloads the binary content.
        
        Args:
            file_id: The ID of the file to retrieve
            
        Returns:
            Iterator of chunks containing file content and metadata
        """
        # Get file metadata
        file = self.service.files().get(fileId=file_id, fields='id,name,mimeType,modifiedTime').execute()
        mime_type = file.get('mimeType', '')
        
        # Handle different file types
        if mime_type.startswith('application/vnd.google-apps'):
            # Google Workspace files need to be exported
            export_mime_type = None
            if 'document' in mime_type:
                export_mime_type = 'text/plain'
            elif 'spreadsheet' in mime_type:
                export_mime_type = 'text/csv'
            elif 'presentation' in mime_type:
                export_mime_type = 'text/plain'
            
            if export_mime_type:
                content = self.service.files().export(
                    fileId=file_id,
                    mimeType=export_mime_type
                ).execute()
                if isinstance(content, bytes):
                    content = content.decode('utf-8')
            else:
                content = f"[{file['name']} - Google {mime_type.split('.')[-1].title()}]"
        else:
            # Regular files can be downloaded directly
            request = self.service.files().get_media(fileId=file_id)
            content = request.execute()
            if isinstance(content, bytes):
                content = content.decode('utf-8', errors='replace')
        
        yield Chunk(
            content=content,
            metadata={
                'source': 'gdrive',
                'id': file_id,
                'name': file.get('name'),
                'mime_type': mime_type,
                'modified': file.get('modifiedTime')
            }
        )
