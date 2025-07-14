# Use the official Python image
FROM python:3.9-slim

# Install required packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install Prometheus client
RUN pip install --no-cache-dir prometheus_client

# Copy application code
COPY . .

# Add Prometheus metrics
ENV PROMETHEUS_MULTIPROC_DIR /tmp
RUN mkdir -p /tmp

# Expose Prometheus metrics port
EXPOSE 8000

# Start the worker with metrics
CMD ["python", "worker.py"]
