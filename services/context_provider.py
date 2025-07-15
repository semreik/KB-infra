"""Provider for retrieving supplier context from various sources."""
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional
import hashlib
import json

from qdrant_client import QdrantClient
from qdrant_client.http.models import Filter, FieldCondition, MatchValue
import redis
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

# Database models
metadata = sa.MetaData()

supplier_table = sa.Table(
    'suppliers',
    metadata,
    sa.Column('id', sa.String, primary_key=True),
    sa.Column('name', sa.String),
    sa.Column('annual_revenue', sa.Float),
    sa.Column('employee_count', sa.Integer),
    sa.Column('founded_year', sa.Integer),
    sa.Column('hq_location', sa.String),
    sa.Column('industry', sa.String)
)

class ContextProvider:
    def __init__(
        self,
        qdrant_url: str,
        postgres_url: str,
        redis_url: Optional[str] = None,
        cache_ttl: int = 3600  # 1 hour
    ):
        """Initialize the context provider with data source connections."""
        self.qdrant = QdrantClient(url=qdrant_url)
        self.db_engine = create_async_engine(postgres_url)
        self.redis = redis.from_url(redis_url) if redis_url else None
        self.cache_ttl = cache_ttl
        
    async def get_supplier_core(self, supplier_id: str) -> Dict:
        """Get core supplier information from Postgres."""
        async with AsyncSession(self.db_engine) as session:
            result = await session.execute(
                sa.select(supplier_table).where(supplier_table.c.id == supplier_id)
            )
            row = result.first()
            if not row:
                return {}
            
            return {
                "id": row.id,
                "name": row.name,
                "annual_revenue": row.annual_revenue,
                "employee_count": row.employee_count,
                "founded_year": row.founded_year,
                "hq_location": row.hq_location,
                "industry": row.industry
            }
    
    async def get_recent_news(
        self,
        supplier_id: str,
        lookback_days: int = 180  # 6 months
    ) -> List[Dict]:
        """Get recent news articles from Qdrant."""
        start_date = datetime.now(timezone.utc) - timedelta(days=lookback_days)
        
        # Query Qdrant with filters
        filter_conditions = Filter(
            must=[
                FieldCondition(
                    key="metadata.supplier_id",
                    match=MatchValue(value=supplier_id)
                ),
                FieldCondition(
                    key="metadata.published",
                    range={"gte": start_date.isoformat()}
                ),
                FieldCondition(
                    key="metadata.collection",
                    match=MatchValue(value="news")
                )
            ]
        )
        
        results = self.qdrant.search(
            collection_name="supplier_docs",
            query_vector=[1.0] * 384,  # Placeholder vector for filtering
            query_filter=filter_conditions,
            limit=25,
            with_payload=True
        )
        
        return [
            {
                "type": "news",
                "title": hit.payload.get("title", ""),
                "content": hit.payload.get("content", ""),
                "published": hit.payload["metadata"]["published"],
                "source": hit.payload["metadata"].get("source", "Unknown"),
                "tone": hit.payload["metadata"].get("tone", 0.0)
            }
            for hit in results
        ]
    
    async def get_internal_docs(
        self,
        supplier_id: str,
        lookback_days: int = 365  # 1 year
    ) -> List[Dict]:
        """Get internal documents from Qdrant."""
        start_date = datetime.now(timezone.utc) - timedelta(days=lookback_days)
        
        # Query Qdrant with filters
        filter_conditions = Filter(
            must=[
                FieldCondition(
                    key="metadata.supplier_id",
                    match=MatchValue(value=supplier_id)
                ),
                FieldCondition(
                    key="metadata.timestamp",
                    range={"gte": start_date.isoformat()}
                ),
                FieldCondition(
                    key="metadata.collection",
                    match=MatchValue(value="internal")
                )
            ]
        )
        
        results = self.qdrant.search(
            collection_name="supplier_docs",
            query_vector=[1.0] * 384,  # Placeholder vector for filtering
            query_filter=filter_conditions,
            limit=25,
            with_payload=True
        )
        
        return [
            {
                "type": hit.payload["metadata"].get("doc_type", "document"),
                "content": hit.payload.get("content", ""),
                "timestamp": hit.payload["metadata"]["timestamp"],
                "source": hit.payload["metadata"].get("source", "Unknown")
            }
            for hit in results
        ]
    
    def _compute_evidence_hash(self, evidence: Dict) -> str:
        """Compute a stable hash of the evidence set for caching."""
        # Sort and serialize the evidence dict for stable hashing
        evidence_str = json.dumps(evidence, sort_keys=True)
        return hashlib.sha256(evidence_str.encode()).hexdigest()
    
    async def get_cached_review(self, supplier_id: str, evidence_hash: str) -> Optional[Dict]:
        """Get a cached review if available."""
        if not self.redis:
            return None
            
        cache_key = f"review:{supplier_id}:{evidence_hash}"
        cached = self.redis.get(cache_key)
        if cached:
            return json.loads(cached)
        return None
    
    async def cache_review(self, supplier_id: str, evidence_hash: str, review: Dict):
        """Cache a review result."""
        if not self.redis:
            return
            
        cache_key = f"review:{supplier_id}:{evidence_hash}"
        self.redis.setex(
            cache_key,
            self.cache_ttl,
            json.dumps(review)
        )
    
    async def get_context_bundle(self, supplier_id: str) -> Dict:
        """Get all relevant context for a supplier review."""
        # Get core supplier info
        supplier_core = await self.get_supplier_core(supplier_id)
        if not supplier_core:
            raise ValueError(f"Supplier {supplier_id} not found")
        
        # Get recent news and internal docs
        news = await self.get_recent_news(supplier_id)
        internal_docs = await self.get_internal_docs(supplier_id)
        
        # Format evidence text
        evidence = []
        
        # Add internal evidence
        for i, doc in enumerate(internal_docs, 1):
            evidence.append(
                f"[E{i}] {doc['type'].title()} ({doc['timestamp']}): {doc['content']}"
            )
        
        # Add news evidence
        for i, article in enumerate(news, len(internal_docs) + 1):
            evidence.append(
                f"[E{i}] News ({article['published']}): {article['title']} - {article['content']}"
            )
        
        # Build context bundle
        context = {
            "supplier_core": supplier_core,
            "evidence": "\n### INTERNAL\n" + "\n".join(evidence[:len(internal_docs)]) +
                       "\n\n### EXTERNAL\n" + "\n".join(evidence[len(internal_docs):])
        }
        
        return context
