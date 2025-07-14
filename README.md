# Knowledge Base Infrastructure

Real-time knowledge base built on Airweave, integrating multiple data sources into a unified searchable interface. This project provides a complete data fusion pipeline that ingests data from various sources, processes it, and makes it available through a vector search interface.

## Features

- **Multi-source Data Ingestion**
  - Gmail integration with MIME parsing
  - Google Drive file metadata and content
  - Outlook/Microsoft Graph integration
  - OneDrive file system
  - PostgreSQL database connector
  - SAP S/4HANA integration

- **Data Processing Pipeline**
  - Dagster-based orchestration
  - Chunking and vectorization
  - Entity resolution
  - Feature extraction

- **Observability**
  - Prometheus metrics collection
  - Grafana dashboards
  - Custom metrics for:
    - Document ingestion rate
    - Processing latency
    - Error rates

- **Vector Search**
  - Qdrant vector database integration
  - Real-time indexing
  - Semantic search capabilities

## Quick Start

1. Clone this repository:
```bash
git clone https://github.com/semreik/KB-infra.git
cd KB-infra
```

2. Copy `.env.example` to `.env` and fill in your credentials:
```bash
cp .env.example .env
# Edit .env with your credentials
```

3. Start the development stack:
```bash
./dev_up.sh
```

The stack will be available at:
- Airweave API: http://localhost:8080
- Vector Store UI: http://localhost:6333/dashboard

## Architecture

### Components

1. **Data Sources**
   - Email Systems
     - Gmail (IMAP/MIME)
     - Outlook (Microsoft Graph API)
   - Cloud Storage
     - Google Drive (Google Drive API)
     - OneDrive (Microsoft Graph API)
   - Databases
     - PostgreSQL (direct connection)
     - SAP S/4HANA (OData API)

2. **Processing Pipeline**
   - Document chunking and normalization
   - Text extraction and cleaning
   - Vector embedding generation
   - Entity resolution and deduplication

3. **Storage Layer**
   - Qdrant vector database
   - PostgreSQL metadata store
   - Object storage for raw documents

4. **Monitoring Stack**
   - Prometheus metrics collection
   - Grafana dashboards
   - Custom alerting rules

### Data Flow

1. Source connectors fetch data using appropriate APIs
2. Dagster DAGs orchestrate the processing pipeline
3. Documents are chunked and vectorized
4. Vectors are stored in Qdrant
5. Metadata is indexed in PostgreSQL
6. Metrics are collected and visualized

## Development

### Prerequisites

- Docker + docker-compose v2.0+
- Python 3.11+
- [pre-commit](https://pre-commit.com/)
- Git
- 4GB+ RAM
- 10GB+ disk space

### Setup

1. Create and activate a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: .\venv\Scripts\activate
```

2. Install dependencies:
```bash
# Development dependencies
pip install -r requirements-dev.txt

# Production dependencies
pip install -r requirements.txt
```

3. Set up environment variables:
```bash
cp .env.example .env
# Edit .env with your credentials:
# - Gmail API credentials
# - Google Drive API credentials
# - Microsoft Graph credentials
# - Database credentials
# - Airweave API token
```

4. Initialize the development environment:
```bash
# Start all services
docker compose up -d

# Run database migrations
python -m tools.db_migrate

# Verify the setup
python -m pytest tests/smoke
```

2. Install pre-commit hooks:
```bash
pre-commit install
```

### Testing

```bash
# Run all tests
pytest

# Run only unit tests
pytest tests/unit

# Run integration tests
pytest tests/integration

# Run with coverage
pytest --cov=. --cov-report=html
```

### Development Workflow

1. Create a feature branch:
```bash
git checkout -b feature/your-feature-name
```

2. Make your changes and ensure tests pass:
```bash
# Run linting
pre-commit run --all-files

# Run tests
pytest
```

3. Submit a pull request

### Monitoring

1. Access Grafana:
   - URL: http://localhost:3001
   - Username: admin
   - Password: admin

2. View Prometheus metrics:
   - URL: http://localhost:9090

3. Check vector database:
   - Qdrant Dashboard: http://localhost:6333/dashboard

## Production Deployment

### Configuration

Before deployment, ensure these are configured:

1. **API Credentials**
   - Gmail OAuth2 credentials
   - Google Drive API access
   - Microsoft Graph API credentials
   - SAP S/4HANA connection

2. **Database Setup**
   - PostgreSQL connection string
   - Qdrant configuration
   - Backup strategy

3. **Security Measures**
   - API authentication
   - Database encryption
   - TLS certificates

### Kubernetes (Helm)

1. Add the Airweave Helm repository:
```bash
helm repo add airweave https://charts.airweave.ai
helm repo update
```

2. Install the chart:
```bash
helm install kb-prod airweave/kb \
  --namespace kb-prod \
  --create-namespace \
  -f values.yaml
```

### Security Notes

1. **Secrets Management**
   - All credentials are managed via Kubernetes secrets
   - Use external secret stores in production (e.g., AWS Secrets Manager)
   - Never commit `.env` files to git
   - Rotate credentials regularly
   
2. **Network Security**
   - API is protected by API key authentication
   - Internal services are not exposed outside the cluster
   - Use network policies to restrict pod-to-pod communication
   - Enable mTLS between services
   
3. **Data Security**
   - All external connections use TLS
   - Database connections require SSL
   - Webhook endpoints are protected by API keys
   - Data is encrypted at rest
   - Regular security audits

4. **Compliance**
   - GDPR considerations for email data
   - Data retention policies
   - Audit logging enabled
   - Access control documentation

## Troubleshooting

1. **Common Issues**
   - Port conflicts: Check if ports 3001, 6333, 8080, 9090 are available
   - Authentication errors: Verify credentials in `.env`
   - Database connection: Ensure PostgreSQL is running

2. **Logs**
   - Application logs: `docker compose logs -f api`
   - Pipeline logs: Check Dagster UI
   - Monitoring: Grafana dashboards

3. **Support**
   - Create an issue on GitHub
   - Check existing issues and documentation
   - Run diagnostics: `python -m tools.diagnose`

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

See [CONTRIBUTING.md](CONTRIBUTING.md) for detailed guidelines.

## Roadmap

- [ ] Additional data source connectors
- [ ] Enhanced entity resolution
- [ ] Real-time processing pipeline
- [ ] Advanced analytics dashboard
- [ ] Multi-language support

## License

Copyright (c) 2025 Your Organization. All rights reserved.
