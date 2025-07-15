"""Integration tests for LLM-based supplier review."""
import pytest
from fastapi.testclient import TestClient

from services.llm_reviewer import app, LLMReviewer

@pytest.fixture
def test_client():
    """Create a test client."""
    return TestClient(app)

@pytest.fixture
def mock_supplier_data():
    """Mock supplier test data."""
    return {
        "id": "SUP-000045",
        "name": "Metal-Can Co",
        "internal_docs": [
            {
                "type": "sap_data",
                "content": "On-time delivery rate 94%, Quality rating B+",
                "timestamp": "2025-06-15T10:00:00Z"
            },
            {
                "type": "email",
                "content": "Production delay reported at main facility",
                "timestamp": "2025-07-01T14:30:00Z"
            }
        ],
        "news_articles": [
            {
                "title": "Metal-Can Co reports 15% revenue growth",
                "content": "Leading packaging supplier shows strong financial performance",
                "published": "2025-07-10T08:15:00Z"
            },
            {
                "title": "Industry ESG Report Q2 2025",
                "content": "Metal-Can Co shows progress on carbon reduction targets",
                "published": "2025-07-05T16:45:00Z"
            }
        ]
    }

def test_health_check(test_client):
    """Test health check endpoint."""
    response = test_client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "healthy"}

def test_supplier_review(test_client, mock_supplier_data):
    """Test supplier review generation."""
    supplier_id = mock_supplier_data["id"]
    
    # Get review
    response = test_client.get(f"/review/{supplier_id}")
    assert response.status_code == 200
    
    # Validate response structure
    data = response.json()
    assert "supplier" in data
    assert "overall_risk" in data
    assert "dimensions" in data
    assert set(data["dimensions"].keys()) == {
        "financial", "supply", "reputation", "quality", "geo"
    }
    
    # Validate scores
    assert 0 <= data["overall_risk"]["score"] <= 1
    for dim in data["dimensions"].values():
        assert 0 <= dim["score"] <= 1
        assert len(dim["reason"]) > 0
    
    # Validate evidence citations
    assert "[E1]" in str(data)  # Should cite SAP data
    assert "[E2]" in str(data)  # Should cite email about delays
    
    # Specific assertions based on mock data
    risk_review = data["dimensions"]
    assert risk_review["supply"]["score"] > 0.5  # High due to production delay
    assert risk_review["financial"]["score"] < 0.3  # Low due to revenue growth
