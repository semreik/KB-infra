#!/usr/bin/env python3
"""
Airweave Loader CLI for bulk data ingestion.
"""
import argparse
import sys
import time
from pathlib import Path
from typing import Optional, List, Dict, Any
import mailbox
import csv
import tqdm
from tools.airweave.sdk import AirweaveClient
from prometheus_client import Counter, Histogram, start_http_server

# Prometheus metrics
DOCS_INGESTED = Counter('docs_ingested_total', 'Number of documents ingested')
INGEST_LATENCY = Histogram('ingest_latency_seconds', 'Time taken to ingest documents')

def load_mbox(path: Path) -> List[Dict[str, Any]]:
    """Load emails from mbox file."""
    records = []
    mbox = mailbox.mbox(str(path))
    
    for message in mbox:
        # Extract email fields
        record = {
            'subject': message['subject'] or '',
            'from': message['from'] or '',
            'to': message['to'] or '',
            'date': message['date'] or '',
            'content': ''
        }
        
        # Handle multipart messages
        if message.is_multipart():
            for part in message.walk():
                if part.get_content_type() == 'text/plain':
                    record['content'] += part.get_payload(decode=True).decode('utf-8', errors='ignore')
        else:
            record['content'] = message.get_payload(decode=True).decode('utf-8', errors='ignore')
        
        records.append(record)
    
    return records

def load_csv(path: Path) -> List[Dict[str, Any]]:
    """Load records from CSV file."""
    records = []
    with open(path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            records.append(row)
    return records

def load_parquet(path: Path) -> List[Dict[str, Any]]:
    """Load records from Parquet file."""
    raise NotImplementedError("Parquet support temporarily disabled")

def main(args: Optional[list] = None):
    # Start Prometheus metrics server on a random available port
    port = 8000
    while True:
        try:
            start_http_server(port)
            break
        except OSError:
            port += 1
            if port > 9000:  # Max port to try
                raise
    
    parser = argparse.ArgumentParser(description="Bulk data loader for Airweave")
    parser.add_argument("--source", choices=["mbox", "csv", "parquet"], required=True,
                      help="Source file format")
    parser.add_argument("--collection", required=True,
                      help="Target collection name")
    parser.add_argument("file", type=Path,
                      help="Input file path")
    
    args = parser.parse_args(args)
    
    # Initialize client
    client = AirweaveClient()
    
    # Load data based on source type
    loaders = {
        "mbox": load_mbox,
        "csv": load_csv,
        "parquet": load_parquet
    }
    
    try:
        loader = loaders[args.source]
        records = loader(args.file)
        
        # Show progress bar during ingestion
        with tqdm.tqdm(total=len(records), desc="Ingesting") as pbar:
            for record in records:
                start_time = time.time()
                
                try:
                    # Convert record to format expected by Airweave
                    doc = {
                        'content': str(record),  # Convert entire record to string for indexing
                        'metadata': {
                            'source': args.source,
                            'collection': args.collection,
                            **record  # Include original fields in metadata
                        }
                    }
                    
                    # Ingest document
                    client.bulk_ingest(args.collection, doc)
                    
                    # Update Prometheus metrics
                    DOCS_INGESTED.inc()
                    INGEST_LATENCY.observe(time.time() - start_time)
                    
                except Exception as e:
                    print(f"Error ingesting record: {str(e)}", file=sys.stderr)
                    continue
                
                pbar.update(1)
                
    except Exception as e:
        print(f"Error: {str(e)}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
