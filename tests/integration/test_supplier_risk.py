from datetime import datetime, timezone
import pytest
from unittest.mock import patch

from dagster import build_op_context
from qdrant_client import QdrantClient

from dagster_jobs.ops.supplier_risk import (
    fetch_supplier_docs,
    score_supplier_risk,
    store_risk_profile,
    supplier_risk_job
)


@pytest.fixture
def seed_supplier_docs(qdrant_client: QdrantClient):
    """Seed test documents for SUP-DEMO supplier."""
    docs = [
        {
            "content": "Email: Supplier SUP-DEMO has missed payment deadline for Q2",
            "metadata": {
                "supplier_id": "SUP-DEMO",
                "collection": "emails",
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
        },
        {
            "content": "Email: Warning notice issued to SUP-DEMO for compliance violation",
            "metadata": {
                "supplier_id": "SUP-DEMO",
                "collection": "emails",
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
        },
        {
            "content": "News: Industry watchdog reports issues with SUP-DEMO practices",
            "metadata": {
                "supplier_id": "SUP-DEMO",
                "collection": "news",
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
        }
    ]
    
    # Insert documents into Qdrant
    for doc in docs:
        qdrant_client.upsert(
            collection_name="supplier_docs",
            points=[{
                "id": f"test-{doc['metadata']['collection']}-{doc['metadata']['timestamp']}",
                "payload": {
                    "content": doc["content"],
                    "metadata": doc["metadata"]
                },
                "vector": [0.1] * 384  # Dummy vector
            }]
        )
    
    yield docs
    
    # Cleanup
    qdrant_client.delete(
        collection_name="supplier_docs",
        points_selector={"filter": {
            "must": [{
                "key": "metadata.supplier_id",
                "match": {"value": "SUP-DEMO"}
            }]
        }}
    )


def test_fetch_supplier_docs(seed_supplier_docs):
    """Test fetching supplier documents."""
    context = build_op_context()
    docs = fetch_supplier_docs(context, "SUP-DEMO")
    
    assert len(docs) == 3
    assert all(doc["metadata"]["supplier_id"] == "SUP-DEMO" for doc in docs)
    assert len([doc for doc in docs if doc["metadata"]["collection"] == "emails"]) == 2
    assert len([doc for doc in docs if doc["metadata"]["collection"] == "news"]) == 1


def test_score_supplier_risk():
    """Test scoring supplier risk."""
    context = build_op_context()
    docs = [{"content": "Test document"}]
    risk = score_supplier_risk(context, docs)
    
    assert "overall_risk" in risk
    assert risk["overall_risk"]["score"] > 0.5
    assert "categories" in risk
    assert "evidence" in risk


def test_store_risk_profile():
    """Test storing risk profile."""
    context = build_op_context()
    risk_json = {
        "overall_risk": {"score": 0.75},
        "categories": {},
        "evidence": []
    }
    
    store_risk_profile(context, "SUP-DEMO", risk_json)


def test_supplier_risk_pipeline(seed_supplier_docs):
    """Test full supplier risk pipeline."""
    # Run the job
    result = supplier_risk_job.execute_in_process()
    
    # Assert success
    assert result.success
    
    # Get the risk score from the score_supplier_risk op
    risk_score = None
    for event in result.all_node_events:
        if event.node_name == "score_supplier_risk":
            risk_score = event.event_specific_data.output_data["result"]["overall_risk"]["score"]
            break
    
    assert risk_score is not None
    assert risk_score > 0.5
