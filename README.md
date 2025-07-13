# Knowledge Base Infrastructure

Real-time knowledge base built on Airweave, integrating multiple data sources into a unified searchable interface.

## Quick Start

1. Clone this repository:
```bash
git clone git@github.com:your-org/kb-infra.git
cd kb-infra
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

This knowledge base integrates with:
- Email: Gmail and Outlook mailboxes
- Cloud Storage: Google Drive and OneDrive
- Databases: Finance staging Postgres DB
- ERP: SAP S/4HANA via OData
- News: Custom webhook ingestor

## Development

### Prerequisites
- Docker + docker-compose
- Python 3.11+
- [pre-commit](https://pre-commit.com/)

### Setup
1. Install dependencies:
```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

2. Install pre-commit hooks:
```bash
pre-commit install
```

### Testing
```bash
pytest
```

## Production Deployment

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

1. Secrets Management
   - All credentials are managed via Kubernetes secrets
   - Use external secret stores in production (e.g., AWS Secrets Manager)
   
2. Network Security
   - API is protected by API key authentication
   - Internal services are not exposed outside the cluster
   - Use network policies to restrict pod-to-pod communication
   
3. Data Security
   - All external connections use TLS
   - Database connections require SSL
   - Webhook endpoints are protected by API keys

## License

Copyright (c) 2025 Your Organization. All rights reserved.
