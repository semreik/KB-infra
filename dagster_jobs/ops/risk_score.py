"""Risk scoring operations for supplier news."""
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple

from dagster import op
from tools.airweave.sdk import AirweaveClient

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
    """Fetch last 24 hours of news for a supplier."""
    client = AirweaveClient()
    
    # Calculate time range
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(hours=24)
    
    # Query Airweave
    context.log.info(f"\nQuerying Airweave for supplier {supplier_id}")
    filters = { 'supplier_id': supplier_id }
    context.log.info(f"Using filters: {filters}")
    results = client.query('news', filters=filters)
    context.log.info(f"Got {len(results)} results from Airweave")
    context.log.info(f"Results: {results}")
    
    # Filter to last 24 hours since mock client doesn't support date filtering
    # Make sure all datetimes are timezone-aware in UTC
    start_time = datetime.now(timezone.utc) - timedelta(days=1)
    end_time = datetime.now(timezone.utc)
    context.log.info(f"\nFiltering by date range:")
    context.log.info(f"  Start: {start_time} ({start_time.isoformat()})")
    context.log.info(f"  End: {end_time} ({end_time.isoformat()})")
    
    filtered_results = []
    for doc in results:
        metadata = doc.get('metadata', {})
        published = metadata.get('published')  # Changed from published_date to published
        context.log.info(f"\nChecking document:")
        context.log.info(f"  Content: {doc.get('content', '')[:100]}...")
        context.log.info(f"  Published: {published}")
        
        if not published:
            context.log.info("  No published date found, skipping")
            continue
            
        try:
            # Try to parse the published date - will be timezone-aware due to Z suffix
            published_dt = datetime.fromisoformat(published.replace('Z', '+00:00'))
            context.log.info(f"  Published datetime: {published_dt}")
            
            # Compare datetime objects - both are now timezone-aware
            if start_time <= published_dt <= end_time:
                context.log.info(f"  Date in range, adding to filtered results")
                filtered_results.append(doc)
            else:
                context.log.info(f"  Date out of range")
        except ValueError as e:
            context.log.info(f"  Error parsing date: {e}")
            continue
    
    context.log.info(f"Found {len(filtered_results)} recent news items for {supplier_id}")
    for doc in filtered_results:
        context.log.info(f"News item: {doc['content'][:100]} with tone {doc.get('metadata', {}).get('tone')}")
    return filtered_results

@op
def compute_risk_score(context, news_items: List[Dict]) -> Tuple[float, str]:
    """Compute risk score from news items."""
    context.log.info(f"Computing risk score for {len(news_items)} news items")
    if not news_items:
        context.log.info("No news items found, returning 0.0")
        return 0.0, ""
    
    now = datetime.utcnow()
    max_score = 0.0
    top_snippet = ""
    
    for item in news_items:
        score = 0.0
        context.log.info(f"\nProcessing news item: {item}")
        
        # Score based on tone
        # Default to very negative tone for testing
        metadata = item.get('metadata', {})
        context.log.info(f"Metadata: {metadata}")
        
        tone = float(metadata.get('tone', -1.0))
        context.log.info(f"Tone score: {tone}")
        
        for category, (min_val, max_val, weight) in WEIGHTS['tone'].items():
            if min_val <= tone <= max_val:
                score += weight
                context.log.info(f"Matched tone category {category} with weight {weight}, score now {score}")
                break
        
        # Score based on recency
        now = datetime.now(timezone.utc)
        pub_date = datetime.fromisoformat(metadata.get('published', now.isoformat()).replace('Z', '+00:00'))
        hours_old = (now - pub_date).total_seconds() / 3600
        context.log.info(f"Published date: {pub_date}, hours old: {hours_old}")
        
        for category, (min_hours, max_hours, weight) in WEIGHTS['recency'].items():
            if min_hours <= hours_old < max_hours:
                old_score = score
                score *= weight
                context.log.info(f"Matched recency category {category} with weight {weight}, score changed from {old_score} to {score}")
                break
        
        context.log.info(f"Final score for this item: {score}")
        
        # Update max score and snippet if this is highest scoring
        if score > max_score:
            max_score = score
            top_snippet = item['content'][:500]  # Truncate long content
            context.log.info(f"New highest score: {max_score} with snippet: {top_snippet[:100]}...")
    
    context.log.info(f"Computed risk score: {max_score}")
    return max_score, top_snippet

@op
def store_risk_score(context, supplier_id: str, score_and_snippet: Tuple[float, str]):
    """Store risk score in feature store."""
    score, snippet = score_and_snippet
    timestamp = datetime.utcnow().isoformat()
    
    # Store in feature store table
    # TODO: Replace with actual feature store implementation
    with open('feature_store.features', 'a') as f:
        f.write(f"{supplier_id}\t{score}\n")
    
    context.log.info(f"Stored risk score {score} for {supplier_id}")
