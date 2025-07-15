#!/bin/bash

# Get supplier risk for SUP-DEMO
echo "Fetching risk profile for SUP-DEMO..."
curl -s http://localhost:8080/suppliers/SUP-DEMO/risk | jq '.'
