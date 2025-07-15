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
import pyarrow.parquet as pq
import json
from tools.airweave.sdk import AirweaveClient
from tools.alias_map import AliasMap
from prometheus_client import Counter, Histogram, start_http_server
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from validators.schema import SchemaValidator
from validators.quality import QualityChecker

# Prometheus metrics
DOCS_INGESTED = Counter('docs_ingested_total', 'Number of documents ingested')
INGEST_LATENCY = Histogram('ingest_latency_seconds', 'Time taken to ingest documents')

def load_mbox(path: Path) -> List[Dict[str, Any]]:
    """Load and validate emails from mbox file."""
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
    """Load and validate records from CSV file."""
    records = []
    with open(path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            records.append(row)
    
    # Determine schema type from headers
    if 'po_number' in reader.fieldnames:
        schema_type = 'purchase_order'
    else:
        raise ValueError("Unsupported CSV format")
    
    # Validate records
    validated_records = SchemaValidator.validate_batch(records, schema_type)
    
    # Check quality
    quality_issues = QualityChecker.check_batch_quality(validated_records, schema_type)
    if quality_issues:
        for idx, issues in quality_issues.items():
            print(f"Quality issues in record {idx}:", file=sys.stderr)
            for issue in issues:
                print(f"  - {issue}", file=sys.stderr)
    
    return validated_records

def load_parquet(path: Path) -> List[Dict[str, Any]]:
    """Load and validate records from Parquet file using pyarrow.
    
    Args:
        path: Path to the Parquet file
        
    Returns:
        List of dictionaries containing the records
    """
    # Read the Parquet file
    table = pq.read_table(str(path))
    
    # Convert to list of dictionaries
    records = []
    for batch in table.to_batches():
        for i in range(len(batch)):
            record = {}
            for col in batch.schema.names:
                value = batch[col][i].as_py()
                # Handle null values
                record[col] = value if value is not None else ''
            records.append(record)
    
    # Determine schema type from column names
    schema_type = 'purchase_order' if 'po_number' in table.schema.names else 'drive'
    
    # Validate records
    validated_records = SchemaValidator.validate_batch(records, schema_type)
    
    # Check quality
    quality_issues = QualityChecker.check_batch_quality(validated_records, schema_type)
    if quality_issues:
        for idx, issues in quality_issues.items():
            print(f"Quality issues in record {idx}:", file=sys.stderr)
            for issue in issues:
                print(f"  - {issue}", file=sys.stderr)
    
    return validated_records

def load_jsonl(path: Path) -> List[Dict[str, Any]]:
    """Load and validate records from JSONL file."""
    records = []
    with open(path, 'r') as f:
        for line in f:
            records.append(json.loads(line))
    return records

class GDELTHandler(FileSystemEventHandler):
    """Watches for new GDELT data files and processes them."""
    
    def __init__(self, client: AirweaveClient, alias_map: AliasMap):
        self.client = client
        self.alias_map = alias_map
        
    def on_created(self, event):
        if not event.is_directory and event.src_path.endswith('.jsonl'):
            try:
                # Load GDELT records
                records = load_jsonl(Path(event.src_path))
                
                # Process each record
                for record in records:
                    # Extract article text
                    text = f"{record.get('title', '')} {record.get('description', '')}"
                    
                    # Find matching suppliers
                    supplier_ids = self.alias_map.find_matches(text)
                    
                    if supplier_ids:
                        # Add to Airweave for each matching supplier
                        for supplier_id in supplier_ids:
                            doc = {
                                'content': text,
                                'url': record.get('url', ''),
                                'published_date': record.get('published', ''),
                                'source': record.get('source', ''),
                                'supplier_id': supplier_id,
                                'tone': record.get('tone', 0.0)
                            }
                            self.client.add_document(
                                collection='external_news',
                                content=doc['content'],
                                metadata={
                                    'url': doc['url'],
                                    'published_date': doc['published_date'],
                                    'source': doc['source'],
                                    'supplier_id': doc['supplier_id'],
                                    'tone': doc['tone']
                                }
                            )
            except Exception as e:
                print(f"Error processing {event.src_path}: {str(e)}", file=sys.stderr)

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
