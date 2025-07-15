"""Mock Airweave SDK for testing."""

class AirweaveClient:
    # Class variable to store documents across instances
    _documents = []
    
    def __init__(self, api_key: str = "test_key"):
        self.api_key = api_key
        print(f"\nInitializing AirweaveClient with {len(AirweaveClient._documents)} existing documents")
        
    def bulk_ingest(self, collection: str, document: dict):
        """Mock bulk ingestion."""
        document['collection'] = collection
        AirweaveClient._documents.append(document)
        print(f"\nBulk ingested document into {collection}:")
        print(f"  Content: {document.get('content', '')[:100]}...")
        print(f"  Metadata: {document.get('metadata', {})}")
        print(f"Total documents after ingestion: {len(AirweaveClient._documents)}")
        
    def query(self, collection: str, query: dict = None, filters: dict = None):
        """Mock query functionality."""
        print(f"\nQuerying collection {collection} with filters {filters}")
        print(f"Total documents before query: {len(AirweaveClient._documents)}")
        
        results = []
        for doc in AirweaveClient._documents:
            print(f"\nChecking document:")
            print(f"  Collection: {doc.get('collection')}")
            print(f"  Content: {doc.get('content', '')[:100]}...")
            print(f"  Metadata: {doc.get('metadata', {})}")
            
            if doc.get('collection') == collection:
                if filters:
                    match = True
                    for key, value in filters.items():
                        if key not in doc.get('metadata', {}) or doc['metadata'][key] != value:
                            print(f"  Filter mismatch: {key}={value} not in metadata {doc.get('metadata', {})}")
                            match = False
                            break
                    if match:
                        print(f"  Document matches all filters, adding to results")
                        results.append(doc)
                else:
                    print(f"  No filters, adding document to results")
                    results.append(doc)
            else:
                print(f"  Collection mismatch: {doc.get('collection')} != {collection}")
        
        print(f"\nFound {len(results)} matching documents")
        return results
        
    def delete_collection(self, collection: str):
        """Mock delete collection."""
        old_count = len(AirweaveClient._documents)
        AirweaveClient._documents = [doc for doc in AirweaveClient._documents if doc.get('collection') != collection]
        new_count = len(AirweaveClient._documents)
        print(f"\nDeleted collection {collection}: removed {old_count - new_count} documents")
