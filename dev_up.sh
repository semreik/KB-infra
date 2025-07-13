#!/bin/bash
set -e

# Check Python version
if ! command -v python3.11 &> /dev/null; then
    echo "Error: Python 3.11 is required but not found"
    exit 1
fi

# Create virtual environment if it doesn't exist
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3.11 -m venv venv
fi

# Activate virtual environment
source venv/bin/activate

# Install dependencies
echo "Installing dependencies..."
pip install -r requirements.txt

# Create .env if it doesn't exist
if [ ! -f ".env" ]; then
    echo "Creating .env from example..."
    cp .env.example .env
    echo "Please update .env with your credentials"
fi

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
echo "Starting Docker services..."
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

echo "✨ Development environment is ready!"
echo "📝 Next steps:"
echo "  1. Update .env with your credentials"
echo "  2. Access Airweave at http://localhost:8080"
echo "  3. Access Qdrant dashboard at http://localhost:6333/dashboard"
