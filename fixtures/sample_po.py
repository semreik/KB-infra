"""Create a sample Parquet file for testing."""
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path

def create_sample_po():
    # Sample purchase order data
    data = {
        'po_number': ['PO001', 'PO002', 'PO003'],
        'vendor': ['Acme Corp', 'TechSupply', 'Office Depot'],
        'date': ['2025-01-15', '2025-01-16', '2025-01-17'],
        'total_amount': [1500.50, 2300.75, 450.25],
        'status': ['approved', 'pending', 'approved'],
        'items': [
            'Laptop, Mouse, Monitor',
            'Server, RAM, SSD',
            'Paper, Pens, Stapler'
        ]
    }
    
    # Create PyArrow table
    table = pa.Table.from_pydict(data)
    
    # Write to Parquet file
    output_path = Path(__file__).parent / 'sample_po.parquet'
    pq.write_table(table, str(output_path))
    print(f"Created sample PO Parquet file at: {output_path}")

if __name__ == '__main__':
    create_sample_po()
