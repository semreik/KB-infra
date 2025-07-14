"""Live integration tests for Gmail connector.

These tests require real Gmail credentials and will access a real Gmail account.
They are skipped by default and only run when GMAIL_TEST=1 environment variable is set.

Required environment variables:
    GOOGLE_CLIENT_ID: OAuth2 client ID
    GOOGLE_CLIENT_SECRET: OAuth2 client secret
    GOOGLE_REFRESH_TOKEN: OAuth2 refresh token
    TEST_EMAIL: Email address to test with
    GMAIL_TEST: Set to 1 to enable these tests
"""
import os
import pytest
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()
from datetime import datetime, timedelta
from sources.gmail_source import GmailSource

pytestmark = pytest.mark.skipif(
    not os.getenv('GMAIL_TEST'), 
    reason='Gmail integration tests require GMAIL_TEST=1'
)

def test_gmail_list_messages(gmail_credentials, test_email):
    """Test listing messages from real Gmail account."""
    source = GmailSource(gmail_credentials)
    
    # Get message IDs
    message_ids = list(source.list_entities())
    
    # Basic validation
    assert len(message_ids) > 0, "No messages found in Gmail account"
    assert all(isinstance(id, str) for id in message_ids), "Invalid message ID format"
    
    print("\n=== Gmail Messages ===")
    print(f"Total messages found: {len(message_ids)}")
    
    # Get content for first few messages to show subjects
    for message_id in message_ids[:5]:  # Show up to 5 messages
        chunks = list(source.iter_content(message_id))
        if chunks:
            chunk = chunks[0]
            print(f"\nMessage: {chunk.metadata.get('subject', '[No subject]')}")
            print(f"Date: {chunk.metadata.get('date')}")
            print(f"From: {chunk.metadata.get('from', '[Unknown]')}")

def test_gmail_read_message(gmail_credentials, test_email):
    """Test reading message content from real Gmail account."""
    source = GmailSource(gmail_credentials)
    
    # Get message IDs
    message_ids = list(source.list_entities())
    assert len(message_ids) > 0, "No messages found in Gmail account"
    
    # Check each message for attachments
    for message_id in message_ids:
        chunks = list(source.iter_content(message_id))
        assert len(chunks) > 0, "No content returned for message"
        
        print(f"\n{'='*20} Message {'='*20}")
        print(f"Subject: {chunks[0].metadata.get('subject', '[No subject]')}")
        print(f"From: {chunks[0].metadata.get('from', '[Unknown]')}")
        print(f"Date: {chunks[0].metadata.get('date')}")
        
        # Show attachments if any
        attachments = [chunk for chunk in chunks if chunk.metadata.get('is_attachment', False)]
        if attachments:
            print(f"\nAttachments found: {len(attachments)}")
            for idx, attachment in enumerate(attachments, 1):
                print(f"\nAttachment {idx}:")
                print(f"Name: {attachment.metadata.get('filename', '[Unknown]')}")
                print(f"Type: {attachment.metadata.get('mime_type', '[Unknown]')}")
                print(f"Size: {len(attachment.content)} bytes")
                
                # If it's a PDF, try to show some content
                if attachment.metadata.get('mime_type') == 'application/pdf':
                    print("\nPDF Content Preview:")
                    print("-" * 50)
                    # Note: PDF content might be binary, we'll need to handle it properly
                    content_preview = attachment.content[:500]
                    if isinstance(content_preview, bytes):
                        try:
                            content_preview = content_preview.decode('utf-8', errors='replace')
                        except:
                            content_preview = '[Binary PDF content]'
                    print(content_preview + '...' if len(attachment.content) > 500 else content_preview)
                    print("-" * 50)
        
        # Show email body
        main_content = next((chunk for chunk in chunks if not chunk.metadata.get('is_attachment', False)), None)
        if main_content:
            print("\nEmail Content:")
            print("-" * 50)
            content_preview = main_content.content[:200]
            print(content_preview + '...' if len(main_content.content) > 200 else main_content.content)
            print("-" * 50)
        
        print("\n" + "="*50)
