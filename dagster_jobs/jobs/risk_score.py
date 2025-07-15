"""Dagster job for computing supplier risk scores."""
from dagster import job, op

from dagster_jobs.ops.risk_score import (
    fetch_recent_news,
    compute_risk_score,
    store_risk_score
)

@op
def get_supplier_id():
    return "SUP-000045"

@job
def risk_score():
    """Compute risk scores for suppliers based on recent news."""
    supplier_id = get_supplier_id()
    news = fetch_recent_news(supplier_id)
    score = compute_risk_score(news)
    store_risk_score(supplier_id, score)
