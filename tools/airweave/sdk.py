"""Mock Airweave SDK for testing."""

class AirweaveClient:
    def __init__(self, api_key: str = "test_key"):
        self.api_key = api_key
        
    def bulk_ingest(self, collection: str, document: dict):
        """Mock bulk ingestion."""
        pass
