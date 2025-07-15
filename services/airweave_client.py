"""Airweave API client."""

import requests
from typing import Dict, Optional, Any

from utils.env import get_env_var

class AirweaveClient:
    """Client for interacting with the Airweave API."""

    def __init__(self):
        """Initialize the Airweave client."""
        self.api_url = get_env_var("AIRWEAVE_API_URL", required=True)
        self.api_key = get_env_var("AIRWEAVE_API_KEY", required=True)
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
        url = f"{self.api_url}/api/v1/collections"
        response = requests.post(
            url,
            headers=self.headers,
            json={"name": name, "dimensions": dimensions}
        )
        # Ignore 409 (already exists) errors
        if response.status_code == 409:
            return {"message": f"Collection '{name}' already exists"}
        response.raise_for_status()
        return response.json()
