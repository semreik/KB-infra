"""Risk scoring operations for supplier news."""
import os
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple, Iterable

from airweave import AirweaveSDK
from dagster import op, DagsterError, DynamicOut, DynamicOutput

@op(out=DynamicOut(str))
def list_supplier_ids(context) -> Iterable[DynamicOutput[str]]:
    """List active supplier IDs."""
    test_supplier_id = "SUP-000045"  # Using the test supplier ID from get_supplier_id
    yield DynamicOutput(test_supplier_id, mapping_key=test_supplier_id)



@op
def get_supplier_id() -> str:
    """Get supplier ID for risk scoring."""
    return 'SUP-000045'

# Risk scoring weights
WEIGHTS = {
    'tone': {
        'very_negative': (-1.0, -0.5, 2.0),  # range and weight
        'negative': (-0.5, -0.1, 1.0),
        'neutral': (-0.1, 0.1, 0.0),
        'positive': (0.1, 0.5, -0.5),
        'very_positive': (0.5, 1.0, -1.0)
    },
    'recency': {
        'last_day': (0, 24, 1.0),  # hours and weight
        'last_week': (24, 168, 0.5),
        'older': (168, float('inf'), 0.1)
    }
}

@op
def fetch_recent_news(context, supplier_id: str) -> List[Dict]:
    """Fetch last 180 days of news and emails for a supplier."""
    try:
        # Initialize Airweave client
        client = AirweaveClient()
        
        # Calculate time range - 180 days
        since_date = (datetime.now(timezone.utc) - timedelta(days=180)).strftime('%Y-%m-%d')
        
        # Query both emails and news
        context.log.info(f"Querying Airweave for supplier {supplier_id} since {since_date}")
        
        filters = {
            'supplier_id': supplier_id,
            'since': since_date
        }
        
        # Fetch emails and news in parallel
        email_results = client.query(collection='emails', filters=filters)
        news_results = client.query(collection='news', filters=filters)
        
        # Combine results
        all_results = email_results + news_results
        
        context.log.info(f"Found {len(all_results)} documents for {supplier_id}:")
        context.log.info(f"- {len(email_results)} emails")
        context.log.info(f"- {len(news_results)} news articles")
        
        # Log sample of results
        for doc in all_results[:3]:
            context.log.info(
                f"Sample {doc.get('metadata', {}).get('collection')}: "
                f"{doc.get('content', '')[:100]}..."
            )
        
        return all_results
        
    except Exception as e:
        raise DagsterError(f"Error fetching documents from Airweave: {str(e)}") from e

import requests
import json
from typing import Dict, List

@op
def analyze_documents(context, documents: List[Dict], supplier_id: str) -> List[Dict]:
    """Analyze documents using LLM scorer service."""
    scores = []
    for doc in documents:
        try:
            response = requests.post(
                "http://api:8080/llm/score",
                json={
                    "content": doc['content'],
                    "metadata": doc.get('metadata', {})
                }
            )
            response.raise_for_status()
            score = response.json()
            scores.append({
                "content": doc['content'],
                "metadata": doc.get('metadata', {}),
                "score": score
            })
        except Exception as e:
            context.log.error(f"Error analyzing document: {str(e)}")
            continue
    
    return scores

@op
async def compute_risk_score(context, analyzed_docs: List[Dict]) -> Dict:
    """Compute overall risk score from analyzed documents."""
    if not analyzed_docs:
        return {
            "score": 0.0,
            "explanation": "No documents found",
            "categories": {
                "financial": {"score": 0.0, "explanation": "No data"},
                "compliance": {"score": 0.0, "explanation": "No data"},
                "reputation": {"score": 0.0, "explanation": "No data"}
            }
        }
        
    # Calculate weighted average scores
    total_weight = 0
    scores = {
        "financial": 0.0,
        "compliance": 0.0,
        "reputation": 0.0
    }
    
    for doc in analyzed_docs:
        score = doc['score']
        weight = 1.0  # Could add recency-based weighting here
        total_weight += weight
        
        scores["financial"] += score['categories']['financial']['score'] * weight
        scores["compliance"] += score['categories']['compliance']['score'] * weight
        scores["reputation"] += score['categories']['reputation']['score'] * weight
    
    # Normalize scores
    if total_weight > 0:
        for category in scores:
            scores[category] /= total_weight
    
    # Calculate overall score (weighted average of categories)
    category_weights = {
        "financial": 0.4,
        "compliance": 0.3,
        "reputation": 0.3
    }
    
    overall_score = sum(
        scores[cat] * weight
        for cat, weight in category_weights.items()
    )
    
    return {
        "score": overall_score,
        "explanation": "Weighted average of category scores",
        "categories": {
            cat: {
                "score": scores[cat],
                "explanation": f"Weighted average of {cat} scores"
            }
            for cat in scores
        }
    }

@op
def store_risk_score(context, supplier_id: str, risk_profile: Dict):
    """Store risk score in feature store."""
    score = risk_profile['score']
    categories = risk_profile['categories']
    timestamp = datetime.utcnow().isoformat()
    
    # Store in feature store table
    # TODO: Replace with actual feature store implementation
    with open('feature_store.features', 'a') as f:
        f.write(f"{supplier_id}\t{score}\t{json.dumps(risk_profile)}\n")
    
    context.log.info(f"Stored risk profile for {supplier_id}: {risk_profile}")
