"""Set up news source connection in Airweave."""

import os
import requests
from services.airweave_client import AirweaveClient

def main():
    """Set up news source connection."""
    # Initialize Airweave client
    client = AirweaveClient()
    
    # Create news source connection
    news_source = {
        "name": "Supplier Risk News",
        "type": "news",
        "config": {
            "query": ""  # Will be updated dynamically based on company name
        },
        "collection_id": "news"  # Use the collection ID we created earlier
    }
    
    # Create source connection
    url = f"{client.api_url}/source-connections"
    response = requests.post(
        url,
        headers=client.headers,
        json=news_source
    )
    
    if response.status_code == 200:
        print("Successfully created news source connection!")
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
