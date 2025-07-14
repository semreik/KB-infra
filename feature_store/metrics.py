"""
Feature store implementation using DuckDB for supplier metrics.
"""
import duckdb
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta

class SupplierMetrics:
    def __init__(self, db_path: str = "features.duckdb"):
        """Initialize DuckDB connection and create tables if needed."""
        self.conn = duckdb.connect(db_path)
        self._init_schema()
    
    def _init_schema(self):
        """Initialize the feature store schema."""
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS supplier_metrics (
                supplier_id INTEGER,
                metric_name VARCHAR,
                metric_value DOUBLE,
                window_start DATE,
                window_end DATE,
                updated_at TIMESTAMP,
                PRIMARY KEY (supplier_id, metric_name, window_start, window_end)
            )
        """)
    
    def compute_delivery_metrics(self, po_data: pd.DataFrame, gr_data: pd.DataFrame):
        """Compute on-time delivery percentage and other metrics."""
        # Merge PO and GR data
        merged = po_data.merge(
            gr_data,
            on=['po_number', 'supplier_id'],
            how='left',
            suffixes=('_po', '_gr')
        )
        
        # Calculate delivery performance
        merged['on_time'] = merged.apply(
            lambda x: x['delivery_date'] <= x['promised_date'] 
            if pd.notnull(x['delivery_date']) else False,
            axis=1
        )
        
        # Compute metrics by supplier
        metrics = merged.groupby('supplier_id').agg({
            'on_time': 'mean',
            'po_number': 'count',
            'delivery_date': lambda x: x.notna().mean()  # Fill rate
        }).reset_index()
        
        # Store metrics
        now = datetime.now()
        window_end = now.date()
        window_start = window_end - timedelta(days=90)  # 90-day window
        
        for _, row in metrics.iterrows():
            self.store_metric(
                supplier_id=row['supplier_id'],
                metric_name='otd_90d',
                metric_value=row['on_time'],
                window_start=window_start,
                window_end=window_end
            )
            self.store_metric(
                supplier_id=row['supplier_id'],
                metric_name='po_count_90d',
                metric_value=row['po_number'],
                window_start=window_start,
                window_end=window_end
            )
            self.store_metric(
                supplier_id=row['supplier_id'],
                metric_name='fill_rate_90d',
                metric_value=row['delivery_date'],
                window_start=window_start,
                window_end=window_end
            )
    
    def store_metric(self, supplier_id: int, metric_name: str, 
                    metric_value: float, window_start: datetime.date,
                    window_end: datetime.date):
        """Store a metric value in the feature store."""
        self.conn.execute("""
            INSERT OR REPLACE INTO supplier_metrics 
            (supplier_id, metric_name, metric_value, window_start, window_end, updated_at)
            VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """, (supplier_id, metric_name, metric_value, window_start, window_end))
    
    def get_supplier_metrics(self, supplier_id: int) -> pd.DataFrame:
        """Retrieve all metrics for a supplier."""
        return self.conn.execute("""
            SELECT * FROM supplier_metrics
            WHERE supplier_id = ?
            ORDER BY window_end DESC, metric_name
        """, (supplier_id,)).df()

if __name__ == "__main__":
    # Example usage with synthetic data
    import numpy as np
    
    # Create synthetic PO data
    po_data = pd.DataFrame({
        'po_number': range(1000),
        'supplier_id': np.random.randint(1, 11, 1000),
        'promised_date': pd.date_range(start='2024-01-01', periods=1000)
    })
    
    # Create synthetic GR data with some late/missing deliveries
    gr_data = po_data.copy()
    gr_data['delivery_date'] = gr_data['promised_date'] + pd.to_timedelta(
        np.random.normal(0, 2, 1000), unit='D'
    )
    
    # Initialize metrics
    metrics = SupplierMetrics()
    
    # Compute and store metrics
    metrics.compute_delivery_metrics(po_data, gr_data)
    
    # Example: get metrics for supplier 1
    print(metrics.get_supplier_metrics(1))
