"""
Integration tests for bulk data ingestion.
"""
import pytest
from pathlib import Path
import tempfile
import os
from tools.airweave_loader import main as loader_main

@pytest.mark.integration
def test_mbox_ingestion(tmp_path):
    """Test ingestion of mbox files."""
    # Create test mbox file
    mbox_content = """From test@example.com Thu Jan  1 00:00:00 2024
Subject: Test Email
From: Test Sender <test@example.com>
To: recipient@example.com

Hello, this is a test email.
"""
    mbox_file = tmp_path / "test.mbox"
    mbox_file.write_text(mbox_content)
    
    # Run loader
    loader_main([
        "--source", "mbox",
        "--collection", "test_emails",
        str(mbox_file)
    ])
    
    # TODO: Verify ingestion via Airweave search API

@pytest.mark.integration
def test_csv_ingestion(tmp_path):
    """Test ingestion of CSV files."""
    # Create test CSV file
    csv_content = """po_number,supplier,amount
PO001,Acme Corp,1000.00
PO002,Globex Inc,2000.00
"""
    csv_file = tmp_path / "test.csv"
    csv_file.write_text(csv_content)
    
    # Run loader
    loader_main([
        "--source", "csv",
        "--collection", "test_pos",
        str(csv_file)
    ])
    
    # TODO: Verify ingestion via Airweave search API

@pytest.mark.integration
def test_parquet_ingestion(tmp_path):
    """Test ingestion of Parquet files."""
    import pandas as pd
    
    # Create test Parquet file
    df = pd.DataFrame({
        'id': range(3),
        'name': ['Test 1', 'Test 2', 'Test 3']
    })
    parquet_file = tmp_path / "test.parquet"
    df.to_parquet(parquet_file)
    
    # Run loader
    loader_main([
        "--source", "parquet",
        "--collection", "test_records",
        str(parquet_file)
    ])
    
    # TODO: Verify ingestion via Airweave search API
