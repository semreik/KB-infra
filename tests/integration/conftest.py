"""Integration test configuration and fixtures."""
import os
import pytest
from pathlib import Path
from typing import Dict, Optional

def load_credentials(service: str) -> Optional[Dict]:
    """
    Safely load credentials for a service from environment variables.
    Never logs or prints credential values.
    
    Args:
        service: Service name (gmail, gdrive)
        
    Returns:
        Dict of credentials if available, None if missing required credentials
    """
    if service == 'gmail':
        required_vars = [
            'GOOGLE_CLIENT_ID',
            'GOOGLE_CLIENT_SECRET',
            'GOOGLE_REFRESH_TOKEN',
            'TEST_EMAIL'
        ]
    elif service == 'gdrive':
        required_vars = [
            'GOOGLE_CLIENT_ID',
            'GOOGLE_CLIENT_SECRET', 
            'GOOGLE_REFRESH_TOKEN',
            'TEST_FOLDER_ID'
        ]
    else:
        raise ValueError(f"Unknown service: {service}")
    
    # Check all required variables exist
    missing = [var for var in required_vars if not os.getenv(var)]
    if missing:
        pytest.skip(f"Missing required environment variables for {service}: {', '.join(missing)}")
        return None
        
    if service in ['gmail', 'gdrive']:
        return {
            'credentials_json': {
                'client_id': os.getenv('GOOGLE_CLIENT_ID'),
                'client_secret': os.getenv('GOOGLE_CLIENT_SECRET'),
                'refresh_token': os.getenv('GOOGLE_REFRESH_TOKEN')
            }
        }
    
    return None

@pytest.fixture
def gmail_credentials():
    """Gmail credentials fixture. Skips test if credentials not available."""
    return load_credentials('gmail')

@pytest.fixture
def gdrive_credentials():
    """GDrive credentials fixture. Skips test if credentials not available."""
    return load_credentials('gdrive')

@pytest.fixture
def test_email():
    """Test email address to use for Gmail tests."""
    email = os.getenv('TEST_EMAIL')
    if not email:
        pytest.skip("TEST_EMAIL environment variable not set")
    return email

@pytest.fixture
def test_folder_id():
    """Test folder ID to use for GDrive tests."""
    folder_id = os.getenv('TEST_FOLDER_ID')
    if not folder_id:
        pytest.skip("TEST_FOLDER_ID environment variable not set")
    return folder_id
