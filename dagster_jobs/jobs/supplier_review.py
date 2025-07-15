"""Dagster job for supplier 360° review pipeline."""
from dagster import job, op, Out, DagsterType, Nothing
from typing import Dict, List

from services.llm_reviewer import LLMReviewer, RiskReview

@op
def get_supplier_list() -> List[str]:
    """Get list of suppliers to review."""
    # TODO: Query from database
    return ["SUP-000045"]

@op(out={"review": Out(dagster_type=DagsterType.from_python_type(RiskReview))})
def llm_review_op(context, supplier_id: str) -> Dict:
    """Generate LLM review for a supplier."""
    import asyncio
    reviewer = LLMReviewer()
    review = asyncio.run(reviewer.review_supplier(supplier_id))
    
    context.log.info(
        f"Generated review for {supplier_id}:\n"
        f"Overall risk: {review.overall_risk.grade} ({review.overall_risk.score})\n"
        f"Reason: {review.overall_risk.reason}"
    )
    
    return {"review": review}

@op
def store_review_op(context, review: RiskReview) -> Nothing:
    """Store review results in feature store."""
    # TODO: Implement proper feature store integration
    context.log.info(f"Storing review for {review.supplier}")
    
    # Store overall score
    feature_key = f"supplier_risk_score.{review.supplier}"
    context.log.info(f"Writing {feature_key}: {review.overall_risk.score}")
    
    # Store dimension scores
    for dim, score in review.dimensions.items():
        feature_key = f"supplier_risk_{dim}.{review.supplier}"
        context.log.info(f"Writing {feature_key}: {score.score}")
    
    return Nothing

@job
def supplier_review_job():
    """Job to generate and store supplier risk reviews."""
    suppliers = get_supplier_list()
    for supplier_id in suppliers:
        review = llm_review_op(supplier_id)
        store_review_op(review["review"])
