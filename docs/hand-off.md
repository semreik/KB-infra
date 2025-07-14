# Data Fusion Pipeline Hand-off Documentation

## Overview

This document describes how to run and verify the data fusion pipeline, which includes:
- Bulk data ingestion from multiple sources
- Entity resolution for suppliers
- Metrics computation and storage
- Observability setup

## Prerequisites

- Docker and Docker Compose
- Python 3.9+
- Airweave API key set in environment
- Access to required data sources (Gmail, Drive, Postgres)

## Running the Backfill DAG

1. Set up environment variables:
```bash
export AIRWEAVE_API_KEY=your_api_key
export GMAIL_CREDENTIALS=path_to_credentials.json
export DRIVE_CREDENTIALS=path_to_credentials.json
export POSTGRES_URL=postgresql://user:pass@host:port/db
```

2. Run the DAG:
```bash
cd dags
dagster job execute -f ingestion_backfill.py -j ingestion_backfill
```

## Viewing Metrics

1. Access Grafana dashboard:
```bash
open http://localhost:3000
```

2. Login with default credentials:
- Username: admin
- Password: admin

3. Navigate to "Airweave Operations" dashboard

## Verifying Search

1. Use the Airweave SDK to search across collections:
```python
from airweave.sdk import AirweaveClient

client = AirweaveClient()
results = client.search("hello", collections=["emails", "documents", "pos"])
```

2. Check entity resolution:
```python
from etl.entity_resolution import cluster_suppliers

suppliers = ["Acme Corp", "ACME Inc", "Globex Ltd"]
clusters = cluster_suppliers(suppliers)
print(clusters)
```

## Monitoring

- Prometheus metrics available at: http://localhost:9090
- Key metrics:
  - docs_ingested_total
  - ingest_latency_seconds
  - error_count

## Troubleshooting

1. Check Docker container logs:
```bash
docker-compose logs -f airweave-worker
```

2. Verify data ingestion:
```bash
python tools/airweave_loader.py --source mbox --collection test_emails path/to/file.mbox
```

3. Monitor feature computation:
```bash
tail -f logs/feature_store.log
```
