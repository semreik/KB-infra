"""Unit tests for airweave_loader.py"""
import pytest
from pathlib import Path
from unittest.mock import Mock, patch
from tools.airweave_loader import main, load_csv, load_mbox, load_parquet

@pytest.fixture
def mock_airweave_client():
    with patch('tools.airweave_loader.AirweaveClient') as mock:
        client_instance = Mock()
        mock.return_value = client_instance
        yield client_instance

@pytest.fixture
def sample_csv_path():
    return Path('fixtures/po.csv')

def test_load_csv(sample_csv_path):
    """Test CSV loading functionality"""
    records = load_csv(sample_csv_path)
    assert len(records) == 5
    assert records[0]['po_number'] == 'PO12345'
    assert records[0]['supplier_name'] == 'Acme Corporation'

def test_load_mbox():
    """Test mbox loading functionality"""
    records = load_mbox(Path('fixtures/gmail_5msgs.mbox'))
    assert len(records) == 5
    assert records[0]['subject'] == 'RE: PO #12345 Confirmation'
    assert records[0]['from'] == 'Test Supplier <test.supplier@acme.com>'

def test_bulk_ingest_with_metrics(mock_airweave_client, sample_csv_path):
    """Test bulk ingestion with Prometheus metrics"""
    with patch('tools.airweave_loader.DOCS_INGESTED.inc') as mock_counter, \
         patch('tools.airweave_loader.INGEST_LATENCY.observe') as mock_latency, \
         patch('tools.airweave_loader.start_http_server'):
        
        # Run loader
        main(['--source', 'csv', '--collection', 'test_collection', str(sample_csv_path)])
        
        # Check if documents were ingested
        assert mock_airweave_client.bulk_ingest.call_count == 5
        
        # Verify metrics were updated
        assert mock_counter.call_count == 5
        assert mock_latency.call_count == 5
        
        # Check document format
        first_doc = mock_airweave_client.bulk_ingest.call_args_list[0][0][1]
        assert 'content' in first_doc
        assert 'metadata' in first_doc
        assert first_doc['metadata']['source'] == 'csv'
        assert first_doc['metadata']['collection'] == 'test_collection'
        assert first_doc['metadata']['po_number'] == 'PO12345'
