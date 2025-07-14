#!/bin/bash
set -euo pipefail

# Directory containing this script
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "Running smoke tests..."

# Create test data directory
TEST_DATA_DIR=$(mktemp -d)
trap 'rm -rf $TEST_DATA_DIR' EXIT

# Generate test files
echo "Generating test data..."

# Mbox file
cat > "$TEST_DATA_DIR/test.mbox" << EOL
From test@example.com Thu Jan  1 00:00:00 2024
Subject: Test Email
From: Test Sender <test@example.com>
To: recipient@example.com

Hello, this is a test email.
EOL

# CSV file
cat > "$TEST_DATA_DIR/test.csv" << EOL
po_number,supplier,amount
PO001,Acme Corp,1000.00
PO002,Globex Inc,2000.00
EOL

# Run integration tests
echo "Running integration tests..."
PYTHONPATH=$PROJECT_ROOT pytest tests/integration/test_bulk_ingest.py -v

echo "Smoke tests completed successfully!"
