"""Job for supplier risk scoring."""
from dagster import job

from dagster_jobs.ops.risk_score import (
    fetch_recent_news,
    analyze_documents,
    store_risk_score,
    list_supplier_ids
)

from dagster import job, op
from typing import Dict

@op(config_schema={"model": str})
def store_risk_score(supplier_id: str, risk_profile: Dict):
    """Store the risk score for a supplier."""
    model = context.op_config["model"]
    # Implementation will be provided by the Airweave SDK
    pass

@job
def supplier_risk_job():
    """Job to score supplier risk."""
    list_supplier_ids().map(
        lambda supplier_id: store_risk_score(
            analyze_documents(
                fetch_recent_news(supplier_id),
                supplier_id
            )
        )
    )
