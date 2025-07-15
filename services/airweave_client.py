"""Airweave API client."""

import requests
from typing import Dict, Optional, Any
from utils.env import get_env_var
from qdrant_client import QdrantClient

class AirweaveClient:
    """Client for interacting with the Airweave API."""

    def __init__(self):
        """Initialize the Airweave client."""
        self.api_url = get_env_var("AIRWEAVE_API_URL")
        self.api_key = get_env_var("AIRWEAVE_API_KEY")
        
        # Get Qdrant configuration from environment
        qdrant_host = os.getenv("QDRANT_HOST", "qdrant")
        qdrant_port = int(os.getenv("QDRANT_PORT", 6333))
        
        # Initialize Qdrant client
        try:
            self.qdrant_client = QdrantClient(
                host=qdrant_host,
                port=qdrant_port,
                prefer_grpc=False
            )
            # Test connection
            self.qdrant_client.get_collections()
        except Exception as e:
            raise RuntimeError(f"Failed to connect to Qdrant at {qdrant_host}:{qdrant_port}") from e
        
        self.headers = {"Authorization": f"Bearer {self.api_key}"}

    def search(self, collection: str, filter: Dict[str, Any], limit: int = 50) -> Dict[str, Any]:
        """Search documents in a collection.

        Args:
            collection: The collection name
            filter: Search filter criteria
            limit: Maximum number of results

        Returns:
            Search results as JSON

        Raises:
            requests.exceptions.RequestException: If the request fails
        """
        url = f"{self.api_url}/api/v1/search"
        response = requests.post(
            url,
            headers=self.headers,
            json={"collection": collection, "filter": filter, "limit": limit}
        )
        response.raise_for_status()
        return response.json()

    def create_collection(self, name: str, dimensions: int = 1536) -> Dict[str, Any]:
        """Create a new collection.

        Args:
            name: The collection name
            dimensions: Vector dimensions (default: 1536)

        Returns:
            Collection creation response

        Raises:
            requests.exceptions.RequestException: If the request fails
        """
        # Generate a readable ID from the name
        readable_id = name.lower().replace(' ', '-')
        
        # Remove trailing slash if present
        base_url = self.api_url.rstrip('/')
        url = f"{base_url}/collections"
        
        response = requests.post(
            url,
            headers=self.headers,
            json={
                "name": name,
                "readable_id": readable_id,
                "dimensions": dimensions
            }
        )
        # Ignore 409 (already exists) errors
        if response.status_code == 409:
            return {"message": f"Collection '{name}' already exists"}
        response.raise_for_status()
        return response.json()
