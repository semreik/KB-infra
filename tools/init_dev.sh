#!/bin/bash
set -e

# Start services
echo "Starting services..."
docker-compose up -d db redis qdrant

# Wait for services to be healthy
echo "Waiting for services to be healthy..."
sleep 10

# Run database migrations
echo "Running database migrations..."
docker exec -i kb-infra-api-1 alembic upgrade head

# Initialize database with sample data
echo "Initializing database with sample data..."
docker exec -i kb-infra-api-1 python tools/init_db.py

# Set up Qdrant collections
echo "Setting up Qdrant collections..."
docker exec -i kb-infra-api-1 python tools/setup_qdrant.py --qdrant-url http://qdrant:6333

echo "Development environment initialized successfully!"
