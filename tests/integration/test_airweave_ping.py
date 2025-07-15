"""Integration tests for Airweave ping job."""

import pytest
from dagster import execute_job
from dagster_jobs.airweave_ping import airweave_ping_job

@pytest.mark.requires_airweave
@pytest.mark.integration
def test_airweave_ping_job():
    """Test the Airweave ping job."""
    try:
        result = execute_job(airweave_ping_job)
        assert result.success
    except Exception as e:
        if "AIRWEAVE_API_URL" in str(e) or "AIRWEAVE_API_KEY" in str(e):
            pytest.skip("Airweave credentials not configured")
        raise
