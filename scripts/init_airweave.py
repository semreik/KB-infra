"""Initialize Airweave collections for supplier risk assessment."""

from services.airweave_client import AirweaveClient

def main():
    """Initialize Airweave collections."""
    # Initialize Airweave client
    client = AirweaveClient()
    
    # Create necessary collections
    collections = [
        {"name": "emails", "dimensions": 1536},
        {"name": "news", "dimensions": 1536},
        {"name": "documents", "dimensions": 1536},
        {"name": "risk_profiles", "dimensions": 1536}
    ]
    
    for collection in collections:
        try:
            result = client.create_collection(
                name=collection["name"],
                dimensions=collection["dimensions"]
            )
            print(f"Created collection {collection['name']}: {result}")
        except Exception as e:
            print(f"Error creating collection {collection['name']}: {e}")

if __name__ == "__main__":
    main()
