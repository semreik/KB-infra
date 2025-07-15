"""Unit tests for Parquet loading functionality."""
import pytest
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from tools.airweave_loader import load_parquet

@pytest.fixture
def test_parquet_file(tmp_path):
    """Create a test Parquet file with sample data."""
    # Create sample data
    data = {
        'id': [1, 2, 3],
        'name': ['Alice', 'Bob', None],  # Include None to test null handling
        'email': ['alice@example.com', 'bob@example.com', 'charlie@example.com'],
        'department': ['Engineering', 'Sales', 'Marketing']
    }
    
    # Create PyArrow table
    table = pa.Table.from_pydict(data)
    
    # Write to temporary file
    test_file = tmp_path / "test.parquet"
    pq.write_table(table, str(test_file))
    
    return test_file

def test_load_parquet(test_parquet_file):
    """Test loading records from a Parquet file."""
    records = load_parquet(test_parquet_file)
    
    # Verify number of records
    assert len(records) == 3
    
    # Verify record contents
    assert records[0] == {
        'id': 1,
        'name': 'Alice',
        'email': 'alice@example.com',
        'department': 'Engineering'
    }
    
    # Verify null handling
    assert records[2]['name'] == ''  # None should be converted to empty string

def test_load_parquet_empty_file(tmp_path):
    """Test loading an empty Parquet file."""
    # Create empty table
    table = pa.Table.from_pydict({
        'id': [],
        'name': [],
        'email': [],
    })
    
    # Write empty table
    test_file = tmp_path / "empty.parquet"
    pq.write_table(table, str(test_file))
    
    # Load and verify
    records = load_parquet(test_file)
    assert len(records) == 0

def test_load_parquet_file_not_found():
    """Test error handling for non-existent file."""
    with pytest.raises(FileNotFoundError):
        load_parquet(Path("nonexistent.parquet"))
