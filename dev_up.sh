#!/bin/bash
set -e

# Clone Airweave if not exists
if [ ! -d "airweave" ]; then
    echo "Cloning Airweave..."
    git clone https://github.com/airweave-ai/airweave.git airweave
    cd airweave
    # Get latest tag
    LATEST_TAG=$(git describe --tags `git rev-list --tags --max-count=1`)
    git checkout $LATEST_TAG
    cd ..
fi

# Start the stack
cd airweave
docker-compose up -d

# Health check function
check_health() {
    local service=$1
    local port=$2
    local max_attempts=30
    local attempt=1

    echo "Checking health of $service..."
    while [ $attempt -le $max_attempts ]; do
        if curl -s "http://localhost:$port/health" > /dev/null; then
            echo "$service is healthy!"
            return 0
        fi
        echo "Attempt $attempt/$max_attempts: $service not ready yet..."
        sleep 6
        attempt=$((attempt + 1))
    done
    echo "$service failed to become healthy"
    return 1
}

# Wait for services to be healthy
check_health "Airweave API" 8080
check_health "Vector Store" 6333

echo "All services are up and healthy!"
