#!/usr/bin/env bash
set -e

echo "Checking Qdrant health..."
curl -f http://localhost:6333/readyz
echo "✓ Qdrant is healthy"

echo "Checking backend health..."
if curl -f http://localhost:8002/healthz 2>/dev/null; then
    echo "✓ Backend health endpoint is healthy"
elif curl -f http://localhost:8002/docs 2>/dev/null; then
    echo "✓ Backend docs endpoint is accessible"
else
    echo "❌ Backend is not responding"
    exit 1
fi

echo "✅ Stack is healthy"
