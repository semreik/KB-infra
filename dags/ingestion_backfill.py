"""
Data ingestion backfill DAG using Dagster.
"""
from dagster import job, op, Out, In, Nothing
import subprocess
from pathlib import Path
import os

@op
def export_gmail_to_mbox() -> str:
    """Use sample Gmail mbox file for testing."""
    return "fixtures/gmail_5msgs.mbox"

@op
def export_drive_files() -> str:
    """Use sample Drive export file for testing."""
    return "fixtures/drive_small.csv"

@op
def snapshot_postgres() -> str:
    """Use sample PO data for testing."""
    return "fixtures/po.csv"

@op
def load_to_airweave(context, input_path: str):
    """Load data into Airweave using the loader CLI."""
    source_type = "mbox" if input_path.endswith(".mbox") else "csv"
    result = subprocess.run(
        [
            "python", "-m", "tools.airweave_loader",
            "--source", source_type,
            "--collection", "test_collection",
            input_path
        ],
        check=True
    )
    if result.returncode != 0:
        raise Exception(f"Loader failed with exit code {result.returncode}")

@job
def ingestion_backfill():
    """Main backfill job orchestrating the data ingestion pipeline."""
    # Export data from sources
    mbox_path = export_gmail_to_mbox()
    drive_path = export_drive_files()
    postgres_path = snapshot_postgres()

    # Load into Airweave
    load_to_airweave(mbox_path)
    load_to_airweave(drive_path)
    load_to_airweave(postgres_path)
