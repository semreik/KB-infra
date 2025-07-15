"""Hourly job to load new documents from various sources."""
import os
from datetime import datetime, timedelta, timezone
from typing import Dict, List

from dagster import job, op, schedule
from airweave import AirweaveSDK
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from sqlalchemy import create_engine, text

# Connectors
@op
def fetch_gmail_docs(context) -> List[Dict]:
    """Fetch new emails from Gmail."""
    creds = Credentials.from_authorized_user_info({
        'client_id': os.getenv('GMAIL_CLIENT_ID'),
        'client_secret': os.getenv('GMAIL_CLIENT_SECRET'),
        'refresh_token': os.getenv('GMAIL_REFRESH_TOKEN')
    })
    
    service = build('gmail', 'v1', credentials=creds)
    
    # Get emails from last hour
    query = f'after:{int((datetime.now() - timedelta(hours=1)).timestamp())}'
    results = service.users().messages().list(userId='me', q=query).execute()
    messages = results.get('messages', [])
    
    docs = []
    for msg in messages:
        email = service.users().messages().get(userId='me', id=msg['id']).execute()
        
        # Extract relevant fields
        headers = email['payload']['headers']
        subject = next(h['value'] for h in headers if h['name'] == 'Subject')
        from_email = next(h['value'] for h in headers if h['name'] == 'From')
        
        docs.append({
            'content': f"Subject: {subject}\nFrom: {from_email}\n\n{email['snippet']}",
            'metadata': {
                'collection': 'emails',
                'source': 'gmail',
                'email_id': email['id'],
                'timestamp': email['internalDate']
            }
        })
    
    context.log.info(f"Fetched {len(docs)} new emails")
    return docs

@op
def fetch_gdrive_docs(context) -> List[Dict]:
    """Fetch new documents from Google Drive."""
    creds = Credentials.from_authorized_user_info({
        'client_id': os.getenv('GDRIVE_CLIENT_ID'),
        'client_secret': os.getenv('GDRIVE_CLIENT_SECRET'),
        'refresh_token': os.getenv('GDRIVE_REFRESH_TOKEN')
    })
    
    service = build('drive', 'v3', credentials=creds)
    
    # Get files modified in last hour from specific folder
    folder_id = os.getenv('GDRIVE_FOLDER_ID')
    query = f"'{folder_id}' in parents and modifiedTime > '{(datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()}'"
    
    results = service.files().list(
        q=query,
        spaces='drive',
        fields='files(id, name, mimeType, modifiedTime)'
    ).execute()
    
    files = results.get('files', [])
    docs = []
    
    for file in files:
        if file['mimeType'] == 'application/pdf':
            # Download and extract text from PDF
            content = service.files().get_media(fileId=file['id']).execute()
            docs.append({
                'content': content.decode('utf-8'),
                'metadata': {
                    'collection': 'documents',
                    'source': 'gdrive',
                    'file_id': file['id'],
                    'file_name': file['name'],
                    'timestamp': file['modifiedTime']
                }
            })
    
    context.log.info(f"Fetched {len(docs)} new documents")
    return docs

@op
def store_docs_in_airweave(gmail_docs: List[Dict], gdrive_docs: List[Dict]):
    """Store documents in Airweave."""
    docs = gmail_docs + gdrive_docs
    if not docs:
        context.log.info("No new documents to store")
        return
        
    try:
        client = AirweaveSDK(
            api_key=os.getenv('AIRWEAVE_API_KEY'),
            organization_id=os.getenv('AIRWEAVE_ORG_ID')
        )
        
        for doc in docs:
            client.add_document(
                collection=doc['metadata']['collection'],
                content=doc['content'],
                metadata=doc['metadata']
            )
        
        context.log.info(f"Stored {len(docs)} documents in Airweave")
        
    except Exception as e:
        context.log.error(f"Error storing documents in Airweave: {str(e)}")
        raise

@job
def load_new_docs():
    """Job to load new documents from various sources."""
    gmail_docs = fetch_gmail_docs()
    gdrive_docs = fetch_gdrive_docs()
    store_docs_in_airweave(gmail_docs, gdrive_docs)

@schedule(
    job=load_new_docs,
    cron_schedule="0 * * * *",  # Run every hour
    execution_timezone="UTC"
)
def hourly_doc_loader_schedule(context):
    """Schedule for hourly document loading."""
    return {}
