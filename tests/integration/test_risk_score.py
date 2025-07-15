"""Integration tests for risk scoring pipeline."""
import json
import os
from pathlib import Path
import pytest
import tempfile
import shutil
import sys
from pathlib import Path

# Add project root to Python path
project_root = str(Path(__file__).parent.parent.parent)
if project_root not in sys.path:
    sys.path.append(project_root)

from dagster import execute_job, DagsterInstance, reconstructable
from dagster_jobs.jobs.risk_score import risk_score

from tools.airweave.sdk import AirweaveClient
from tools.alias_map import AliasMap

@pytest.fixture
def setup_test_data():
    """Set up test data in Airweave."""
    # Initialize components
    client = AirweaveClient()
    alias_map = AliasMap()
    alias_map.add_supplier('SUP-000045', 'TechCorp', {'Tech Corporation', 'TechCorp Inc'})
    
    # Load sample GDELT data
    fixture_path = os.path.join(os.path.dirname(__file__), '..', '..', 'fixtures', 'gdelt_sample.json')
    with open(fixture_path) as f:
        records = json.load(f)
    
    print("\nLoaded fixture data:", records)
    
    # Process each record
    for record in records:
        text = f"{record['title']} {record['description']}"
        supplier_ids = alias_map.find_matches(text)
        print(f"\nProcessing record: {text}")
        print(f"Found supplier IDs: {supplier_ids}")
        
        for supplier_id in supplier_ids:
            document = {
                'content': text,
                'metadata': {
                    'url': record['url'],
                    'published': record['published'],  # Using 'published' to match fixture data
                    'source': record['source'],
                    'supplier_id': supplier_id,
                    'tone': record['tone']
                }
            }
            print(f"\nIngesting document:")
            print(f"  Content: {document['content']}")
            print(f"  Metadata: {document['metadata']}")
            client.bulk_ingest(
                collection='news',
                document=document
            )
    
    # Verify documents after ingestion
    print("\nVerifying ingested documents:")
    test_client = AirweaveClient()
    results = test_client.query('news', filters={'supplier_id': 'SUP-000045'})
    print(f"Found {len(results)} documents for SUP-000045")
    
    yield
    
    # Cleanup
    client.delete_collection('news')

def test_risk_score_pipeline(setup_test_data):
    """Test the complete risk scoring pipeline."""
    # Clear any existing feature store data
    if os.path.exists('feature_store.features'):
        os.remove('feature_store.features')
    
    # Run the job in-process so AirweaveClient documents persist
    temp_dir = tempfile.mkdtemp()
    instance = DagsterInstance.local_temp(temp_dir)
    try:
        result = execute_job(
            reconstructable(risk_score),
            instance=instance,
            run_config={"execution": {"config": {"in_process": {}}}}
        )
        assert result.success
        
        # Verify feature store output
        with open('feature_store.features', 'r') as f:
            lines = f.readlines()
        
        assert len(lines) == 1
        supplier_id, score = lines[0].strip().split('\t')
        assert supplier_id == 'SUP-000045'
        assert float(score) > 0  # Should have positive risk score due to negative news
        
        # Clean up
        os.remove('feature_store.features')
    finally:
        shutil.rmtree(temp_dir)  
