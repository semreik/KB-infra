"""Smoke test job for Airweave integration."""

import dagster
from services.airweave_client import AirweaveClient

def create_collection_op(context: dagster.OpExecutionContext) -> str:
    """Create a test collection."""
    client = AirweaveClient()
    response = client.create_collection("windsurf_ping")
    context.log.info(f"Created collection: {response}")
    return "windsurf_ping"

def write_document_op(context: dagster.OpExecutionContext, collection: str) -> None:
    """Write a test document."""
    client = AirweaveClient()
    response = client.search(
        collection=collection,
        filter={"text": "hello ping"},
        limit=1
    )
    if response.get("total", 0) > 0:
        context.log.info("Found existing document, skipping write")
        return

    # Write the document
    url = f"{client.api_url}/api/v1/documents"
    response = requests.post(
        url,
        headers=client.headers,
        json={
            "collection": collection,
            "text": "hello ping",
            "metadata": {"test": True}
        }
    )
    response.raise_for_status()
    context.log.info(f"Wrote document: {response.json()}")

def verify_document_op(context: dagster.OpExecutionContext, collection: str) -> None:
    """Verify the document exists."""
    client = AirweaveClient()
    response = client.search(
        collection=collection,
        filter={"text": "hello ping"},
        limit=1
    )
    if response.get("total", 0) == 0:
        raise ValueError("Document not found in search results")
    context.log.info(f"Verified document: {response}")

@dagster.job(name="airweave_ping_job")
def airweave_ping_job():
    """Smoke test job for Airweave integration."""
    collection = create_collection_op()
    write_document_op(collection)
    verify_document_op(collection)

# Schedule the job to run hourly in dev
airweave_ping_job_schedule = dagster.ScheduleDefinition(
    job=airweave_ping_job,
    cron_schedule="0 * * * *",  # Every hour
    name="airweave_ping_job_schedule"
)
