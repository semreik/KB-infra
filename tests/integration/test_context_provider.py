"""Integration tests for supplier context provider."""
import pytest
from datetime import datetime, timedelta, timezone
import json

from services.context_provider import ContextProvider

@pytest.fixture
def mock_supplier():
    """Mock supplier data."""
    return {
        "id": "SUP-000045",
        "name": "Metal-Can Co",
        "annual_revenue": 50000000.0,
        "employee_count": 250,
        "founded_year": 2010,
        "hq_location": "San Francisco, CA",
        "industry": "Manufacturing"
    }

@pytest.fixture
def mock_news_articles():
    """Mock news articles."""
    now = datetime.now(timezone.utc)
    return [
        {
            "title": "Metal-Can Co reports 15% revenue growth",
            "content": "Leading packaging supplier shows strong financial performance",
            "metadata": {
                "supplier_id": "SUP-000045",
                "published": (now - timedelta(days=5)).isoformat(),
                "collection": "news",
                "source": "Business Wire",
                "tone": 0.8
            }
        },
        {
            "title": "Industry ESG Report Q2 2025",
            "content": "Metal-Can Co shows progress on carbon reduction targets",
            "metadata": {
                "supplier_id": "SUP-000045",
                "published": (now - timedelta(days=10)).isoformat(),
                "collection": "news",
                "source": "ESG Today",
                "tone": 0.6
            }
        }
    ]

@pytest.fixture
def mock_internal_docs():
    """Mock internal documents."""
    now = datetime.now(timezone.utc)
    return [
        {
            "content": "On-time delivery rate 94%, Quality rating B+",
            "metadata": {
                "supplier_id": "SUP-000045",
                "timestamp": (now - timedelta(days=30)).isoformat(),
                "collection": "internal",
                "doc_type": "sap_data",
                "source": "SAP"
            }
        },
        {
            "content": "Production delay reported at main facility",
            "metadata": {
                "supplier_id": "SUP-000045",
                "timestamp": (now - timedelta(days=14)).isoformat(),
                "collection": "internal",
                "doc_type": "email",
                "source": "Operations Team"
            }
        }
    ]

@pytest.fixture(scope="function")
def context_provider(
    mock_supplier,
    mock_news_articles,
    mock_internal_docs,
    monkeypatch
):
    """Create context provider with mocked data sources."""
    provider = ContextProvider(
        qdrant_url="http://localhost:6333",
        postgres_url="postgresql+asyncpg://user:pass@localhost/test",
        redis_url="redis://localhost:6379"
    )
    
    # Mock database query
    def mock_get_supplier(*args, **kwargs):
        class MockResult:
            def first(self):
                return type('Row', (), mock_supplier)
        return MockResult()
    
    # Mock async execute to return a coroutine
    async def mock_execute(*args, **kwargs):
        return mock_get_supplier()
    
    monkeypatch.setattr(
        "sqlalchemy.ext.asyncio.AsyncSession.execute",
        mock_execute
    )
    
    # Mock Qdrant search
    def mock_qdrant_search(*args, **kwargs):
        collection = kwargs.get("collection_name")
        filter_conditions = kwargs.get("query_filter")
        
        # Parse filter conditions
        collection_filter = next(
            f for f in filter_conditions.must
            if f.key == "metadata.collection"
        ).match.value
        
        # Return appropriate mock data
        if collection_filter == "news":
            docs = mock_news_articles
        else:
            docs = mock_internal_docs
            
        # Convert to Qdrant response format
        class MockHit:
            def __init__(self, doc):
                self.payload = {
                    "title": doc.get("title", ""),
                    "content": doc["content"],
                    "metadata": doc["metadata"]
                }
                
        return [MockHit(doc) for doc in docs]
    
    # Mock Redis with in-memory storage
    class MockRedis:
        def __init__(self):
            self.store = {}
            
        def get(self, key):
            return self.store.get(key)
            
        def setex(self, key, ttl, value):
            self.store[key] = value
            
    mock_redis = MockRedis()
        
    # Replace Redis instance with our mock
    monkeypatch.setattr(
        provider,
        "redis",
        mock_redis
    )
    
    monkeypatch.setattr(
        "qdrant_client.QdrantClient.search",
        mock_qdrant_search
    )
    
    return provider

@pytest.mark.asyncio
async def test_get_supplier_core(context_provider, mock_supplier):
    """Test retrieving core supplier information."""
    result = await context_provider.get_supplier_core("SUP-000045")
    assert result["id"] == mock_supplier["id"]
    assert result["name"] == mock_supplier["name"]
    assert result["annual_revenue"] == mock_supplier["annual_revenue"]

@pytest.mark.asyncio
async def test_get_recent_news(context_provider, mock_news_articles):
    """Test retrieving recent news articles."""
    results = await context_provider.get_recent_news("SUP-000045")
    assert len(results) == len(mock_news_articles)
    assert results[0]["title"] == mock_news_articles[0]["title"]
    assert results[0]["type"] == "news"
    assert "published" in results[0]

@pytest.mark.asyncio
async def test_get_internal_docs(context_provider, mock_internal_docs):
    """Test retrieving internal documents."""
    results = await context_provider.get_internal_docs("SUP-000045")
    assert len(results) == len(mock_internal_docs)
    assert results[0]["content"] == mock_internal_docs[0]["content"]
    assert results[0]["type"] == mock_internal_docs[0]["metadata"]["doc_type"]

@pytest.mark.asyncio
async def test_get_context_bundle(context_provider):
    """Test retrieving complete context bundle."""
    result = await context_provider.get_context_bundle("SUP-000045")
    
    # Check structure
    assert "supplier_core" in result
    assert "evidence" in result
    
    # Check evidence formatting
    evidence = result["evidence"]
    assert "### INTERNAL" in evidence
    assert "### EXTERNAL" in evidence
    assert "[E1]" in evidence  # First internal doc
    assert "[E3]" in evidence  # First news article (after 2 internal docs)
    
    # Check evidence content
    assert "On-time delivery rate 94%" in evidence
    assert "Production delay reported" in evidence
    assert "15% revenue growth" in evidence
    assert "carbon reduction targets" in evidence

@pytest.mark.asyncio
async def test_caching(context_provider):
    """Test review caching functionality."""
    supplier_id = "SUP-000045"
    evidence = {"test": "data"}
    review = {"score": 0.5}
    
    # Compute hash and cache review
    evidence_hash = context_provider._compute_evidence_hash(evidence)
    await context_provider.cache_review(supplier_id, evidence_hash, review)
    
    # Retrieve cached review
    cached = await context_provider.get_cached_review(supplier_id, evidence_hash)
    assert cached == review
    
    # Check cache miss
    missing = await context_provider.get_cached_review(
        supplier_id,
        "nonexistent_hash"
    )
    assert missing is None
