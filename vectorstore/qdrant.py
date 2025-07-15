"""Qdrant vector store integration for Airweave."""
from typing import List, Dict, Optional
import numpy as np
from qdrant_client import QdrantClient


def get_qdrant_client() -> QdrantClient:
    """Get Qdrant client instance."""
    return QdrantClient(url="http://qdrant:6333")
from qdrant_client.http import models
from sentence_transformers import SentenceTransformer
from processors.text import ProcessedChunk

class QdrantStore:
    """Vector store implementation using Qdrant."""
    
    def __init__(
        self,
        url: str = "http://localhost:6333",
        collection_name: str = "kb_vectors",
        model_name: str = "all-MiniLM-L6-v2"
    ):
        self.client = QdrantClient(url=url)
        self.collection_name = collection_name
        self.model = SentenceTransformer(model_name)
        
        # Ensure collection exists
        self._create_collection_if_not_exists()
    
    def _create_collection_if_not_exists(self):
        """Create the vector collection if it doesn't exist."""
        collections = self.client.get_collections().collections
        exists = any(c.name == self.collection_name for c in collections)
        
        if not exists:
            self.client.create_collection(
                collection_name=self.collection_name,
                vectors_config=models.VectorParams(
                    size=self.model.get_sentence_embedding_dimension(),
                    distance=models.Distance.COSINE
                )
            )
    
    def _generate_embedding(self, text: str) -> np.ndarray:
        """Generate embedding for text."""
        return self.model.encode(text)
    
    def index_chunk(self, chunk: ProcessedChunk):
        """Index a processed chunk."""
        # Generate embedding
        embedding = self._generate_embedding(chunk.text)
        
        # Prepare payload
        payload = {
            "text": chunk.text,
            "metadata": chunk.metadata,
            "source_id": chunk.metadata.get("id", ""),
            "source_type": chunk.metadata.get("source", "")
        }
        
        # Index in Qdrant
        self.client.upsert(
            collection_name=self.collection_name,
            points=[
                models.PointStruct(
                    id=hash(f"{chunk.metadata.get('id', '')}_{chunk.metadata.get('chunk_index', 0)}"),
                    vector=embedding.tolist(),
                    payload=payload
                )
            ]
        )
    
    def search(
        self,
        query: str,
        limit: int = 5,
        source_type: Optional[str] = None,
        source_id: Optional[str] = None
    ) -> List[Dict]:
        """Search for similar chunks."""
        # Generate query embedding
        query_vector = self._generate_embedding(query)
        
        # Prepare filters
        filter_conditions = []
        if source_type:
            filter_conditions.append(
                models.FieldCondition(
                    key="source_type",
                    match=models.MatchValue(value=source_type)
                )
            )
        if source_id:
            filter_conditions.append(
                models.FieldCondition(
                    key="source_id",
                    match=models.MatchValue(value=source_id)
                )
            )
        
        # Search
        results = self.client.search(
            collection_name=self.collection_name,
            query_vector=query_vector.tolist(),
            limit=limit,
            query_filter=models.Filter(
                must=filter_conditions
            ) if filter_conditions else None
        )
        
        # Format results
        return [
            {
                "text": hit.payload["text"],
                "metadata": hit.payload["metadata"],
                "score": hit.score
            }
            for hit in results
        ]
