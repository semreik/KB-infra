"""Setup script for Qdrant collections."""
import argparse
from datetime import datetime, timezone
import logging

from qdrant_client import QdrantClient
from qdrant_client.http import models
from sentence_transformers import SentenceTransformer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def setup_collections(
    qdrant_url: str,
    recreate: bool = False,
    encoder_name: str = "all-MiniLM-L6-v2"
):
    """Set up Qdrant collections for supplier documents."""
    logger.info("Connecting to Qdrant at %s", qdrant_url)
    client = QdrantClient(url=qdrant_url)
    
    # Load encoder to get vector size
    logger.info("Loading encoder model: %s", encoder_name)
    encoder = SentenceTransformer(encoder_name)
    vector_size = encoder.get_sentence_embedding_dimension()
    
    # Collection config
    collection_name = "supplier_docs"
    if recreate:
        try:
            logger.info("Recreating collection: %s", collection_name)
            client.delete_collection(collection_name)
        except Exception as e:
            logger.warning("Failed to delete collection: %s", str(e))
    
    # Create collection with optimized settings
    client.create_collection(
        collection_name=collection_name,
        vectors_config=models.VectorParams(
            size=vector_size,
            distance=models.Distance.COSINE
        ),
        optimizers_config=models.OptimizersConfigDiff(
            indexing_threshold=20000,  # Index after 20k vectors
            memmap_threshold=50000     # Use memmap after 50k vectors
        ),
        # Define schema for payload fields
        on_disk_payload=True,  # Store payload on disk for large collections
        hnsw_config=models.HnswConfigDiff(
            m=16,               # Number of edges per node in HNSW graph
            ef_construct=100    # Size of the dynamic candidate list
        )
    )
    
    # Create payload index for efficient filtering
    client.create_payload_index(
        collection_name=collection_name,
        field_name="metadata.supplier_id",
        field_schema=models.PayloadSchemaType.KEYWORD
    )
    
    client.create_payload_index(
        collection_name=collection_name,
        field_name="metadata.collection",
        field_schema=models.PayloadSchemaType.KEYWORD
    )
    
    client.create_payload_index(
        collection_name=collection_name,
        field_name="metadata.published",
        field_schema=models.PayloadSchemaType.DATETIME
    )
    
    client.create_payload_index(
        collection_name=collection_name,
        field_name="metadata.timestamp",
        field_schema=models.PayloadSchemaType.DATETIME
    )
    
    # Add example document for testing
    test_doc = {
        "title": "Example News Article",
        "content": "This is a test article about supplier performance.",
        "metadata": {
            "supplier_id": "SUP-000045",
            "collection": "news",
            "published": datetime.now(timezone.utc).isoformat(),
            "source": "Test Source",
            "tone": 0.5
        }
    }
    
    # Encode and upsert test document
    vector = encoder.encode(test_doc["content"]).tolist()
    client.upsert(
        collection_name=collection_name,
        points=[
            models.PointStruct(
                id=1,
                vector=vector,
                payload=test_doc
            )
        ]
    )
    
    logger.info("Collection setup complete!")
    logger.info("Collection info:")
    logger.info(client.get_collection(collection_name))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Set up Qdrant collections")
    parser.add_argument(
        "--qdrant-url",
        default="http://localhost:6333",
        help="URL of the Qdrant server"
    )
    parser.add_argument(
        "--recreate",
        action="store_true",
        help="Recreate collections if they exist"
    )
    parser.add_argument(
        "--encoder",
        default="all-MiniLM-L6-v2",
        help="Name of the sentence transformer model to use"
    )
    
    args = parser.parse_args()
    setup_collections(args.qdrant_url, args.recreate, args.encoder)
