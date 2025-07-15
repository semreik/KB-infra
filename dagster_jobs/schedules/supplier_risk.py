from dagster import schedule

from dagster_jobs.ops.supplier_risk import supplier_risk_job


@schedule(
    cron_schedule="0 2 * * *",  # Daily at 02:00 UTC
    job=supplier_risk_job,
    execution_timezone="UTC"
)
def supplier_risk_schedule():
    """Schedule for daily supplier risk assessment."""
    return {}
