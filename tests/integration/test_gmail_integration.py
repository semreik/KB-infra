import os
import pytest
from datetime import datetime

@pytest.mark.integration
@pytest.mark.skipif(not os.getenv('GMAIL_TEST'), reason='GMAIL_TEST not set')
def test_gmail_integration():
    """Integration test for Gmail connector with live API."""
    from sources.gmail_source import GmailSource
    
    config = {
        'credentials_json': {
            'client_id': os.getenv('GMAIL_CLIENT_ID'),
            'client_secret': os.getenv('GMAIL_CLIENT_SECRET'),
            'refresh_token': os.getenv('GMAIL_REFRESH_TOKEN')
        }
    }
    
    source = GmailSource(config)
    
    # Should list messages
    messages = list(source.list_entities())
    assert len(messages) > 0
    
    # Should get message content
    msg_id = messages[0]
    chunks = list(source.iter_content(msg_id))
    assert len(chunks) > 0
    chunk = chunks[0]
    assert chunk.content  # Should have content
    assert chunk.metadata['source'] == 'gmail'
    assert chunk.metadata['id'] == msg_id
    assert 'subject' in chunk.metadata  # Should have subject
