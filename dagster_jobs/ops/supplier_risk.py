from datetime import datetime, timedelta, timezone
from typing import Dict, List

from dagster import job, op, OpExecutionContext
from prometheus_client import Counter
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.models.supplier_risk import SupplierRiskProfile
from vectorstore.qdrant import get_qdrant_client

# Prometheus metrics
SUPPLIER_RISK_JOB_SUCCESS = Counter(
    'supplier_risk_job_success',
    'Number of successful supplier risk job runs'
)


@op
def fetch_supplier_docs(context: OpExecutionContext, supplier_id: str) -> List[Dict]:
    """Fetch recent documents for a supplier from Qdrant."""
    client = get_qdrant_client()
    
    # Get documents from last 30 days
    start_time = datetime.now(timezone.utc) - timedelta(days=30)
    
    # Search for supplier documents
    response = client.scroll(
        collection_name="supplier_docs",
        scroll_filter={
            "must": [
                {
                    "key": "metadata.supplier_id",
                    "match": {"value": supplier_id}
                },
                {
                    "key": "metadata.timestamp",
                    "range": {"gte": start_time.isoformat()}
                }
            ]
        },
        limit=100  # Adjust as needed
    )
    
    # Extract documents and their metadata
    docs = []
    for point in response[0]:
        doc = {
            "content": point.payload.get("content", ""),
            "metadata": {
                "supplier_id": point.payload["metadata"]["supplier_id"],
                "collection": point.payload["metadata"]["collection"],
                "timestamp": point.payload["metadata"]["timestamp"]
            }
        }
        docs.append(doc)
    
    context.log.info(f"Found {len(docs)} documents for supplier {supplier_id}")
    return docs


@op
def score_supplier_risk(context: OpExecutionContext, docs: List[Dict]) -> Dict:
    """Score supplier risk using LLM service."""
    # TODO: Replace with actual LLM service call
    # For now, return mock risk assessment
    risk_json = {
        "overall_risk": {
            "score": 0.75,
            "explanation": "High risk due to recent negative news and compliance issues"
        },
        "categories": {
            "financial": {
                "score": 0.8,
                "explanation": "Multiple late payments reported"
            },
            "compliance": {
                "score": 0.7,
                "explanation": "Regulatory violations found in documentation"
            },
            "reputation": {
                "score": 0.6,
                "explanation": "Negative media coverage in industry publications"
            }
        },
        "evidence": [doc["content"] for doc in docs[:3]]  # Top 3 supporting documents
    }
    
    context.log.info("Generated risk assessment")
    return risk_json


@op
def store_risk_profile(
    context: OpExecutionContext,
    supplier_id: str,
    risk_json: Dict
) -> None:
    """Store risk profile in database."""
    # Create database engine and session
    engine = create_engine("postgresql://docker:docker@db:5432/airweave")
    Session = sessionmaker(engine)
    
    with Session() as session:
        # Create new risk profile
        profile = SupplierRiskProfile(
            supplier_id=supplier_id,
            timestamp=datetime.now(timezone.utc),
            risk_json=risk_json
        )
        
        # Save to database
        session.add(profile)
        session.commit()
    
    # Increment success metric
    SUPPLIER_RISK_JOB_SUCCESS.inc()
    context.log.info(f"Stored risk profile for supplier {supplier_id}")


@job
def supplier_risk_job():
    """Job to assess and store supplier risk."""
    supplier_id = "SUP-DEMO"  # For demo purposes
    docs = fetch_supplier_docs(supplier_id)
    risk = score_supplier_risk(docs)
    store_risk_profile(supplier_id, risk)
