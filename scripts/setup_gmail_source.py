"""Set up Gmail source connection in Airweave."""

import os
import requests
from services.airweave_client import AirweaveClient
from datetime import datetime, timedelta
import json

def main():
    """Set up Gmail source connection."""
    # Initialize Airweave client
    client = AirweaveClient()
    
    # Get credentials from environment variables
    credentials = {
        "client_id": os.getenv("GMAIL_CLIENT_ID"),
        "client_secret": os.getenv("GMAIL_CLIENT_SECRET"),
        "refresh_token": os.getenv("GMAIL_REFRESH_TOKEN")
    }
    
    # Create source connection
    source_connection = {
        "name": "Supplier Risk Gmail",
        "type": "gmail",
        "credentials": credentials,
        "config": {
            "email_address": "airwavetest66@gmail.com",
            "query": "after:2025/01/01"  # Only process emails from 2025 onwards
        },
        "collection_id": "emails"  # Use the collection ID we created earlier
    }
    
    # Create source connection
    url = f"{client.api_url}/source-connections"
    response = requests.post(
        url,
        headers=client.headers,
        json=source_connection
    )
    
    if response.status_code == 200:
        print("Successfully created Gmail source connection!")
        print("Starting sync...")
        
        # Start sync
        sync_url = f"{client.api_url}/source-connections/{response.json()['id']}/sync"
        sync_response = requests.post(
            sync_url,
            headers=client.headers
        )
        
        if sync_response.status_code == 200:
            print("Sync initiated successfully!")
        else:
            print(f"Error starting sync: {sync_response.text}")
    else:
        print(f"Error creating source connection: {response.text}")

if __name__ == "__main__":
    main()
