"""Dagster repository definition."""
from dagster import repository

from dagster_jobs.jobs.load_new_docs import load_new_docs, hourly_doc_loader_schedule
from dagster_jobs.jobs.supplier_risk import supplier_risk_job

@repository
def airweave_repository():
    """Repository containing all Airweave jobs."""
    jobs = [load_new_docs, supplier_risk_job]
    schedules = [hourly_doc_loader_schedule]
    
    return jobs + schedules
