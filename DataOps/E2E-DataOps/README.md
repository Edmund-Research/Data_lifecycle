# E-commerce DataOps Analytics Pipeline

A comprehensive DataOps implementation for e-commerce analytics, demonstrating best practices in data engineering, quality assurance, and continuous delivery.

## Project Goals

This project showcases DataOps principles by building a real-time customer analytics platform that:
- Ingests e-commerce events with automated validation
- Processes data with continuous quality monitoring
- Delivers insights through automated pipelines
- Maintains high reliability through comprehensive testing

## Architecture

```
Web Events → Ingestion API → PostgreSQL → Processing → Analytics Dashboard
     ↓              ↓             ↓           ↓              ↓
 Validation → Data Quality → Monitoring → Testing → Continuous Deployment
```

## Quick Start

### Prerequisites
- Python 3.11+
- Docker and Docker Compose
- Git

### Setup

1. **Clone and setup the project:**
```bash
git clone <your-repo-url>
cd ecommerce-dataops-analytics
chmod +x scripts/setup_environment.sh
./scripts/setup_environment.sh
```

2. **Activate virtual environment:**
```bash
source venv/bin/activate
```

3. **Start the services:**
```bash
make up
```

4. **Generate sample data:**
```bash
python scripts/generate_sample_data.py --events 1000
```

5. **Access the services:**
- **API Documentation:** http://localhost:8000/docs
- **Dashboard:** http://localhost:8501
- **Health Check:** http://localhost:8000/health

## DataOps Features Demonstrated

### 1. Automated Data Validation
- **Schema validation** with Pydantic models
- **Business logic validation** with custom rules
- **Data quality checks** with comprehensive error reporting
- **Real-time validation** during ingestion

### 2. Continuous Integration
- **Automated testing** (unit, integration, data quality)
- **Code quality checks** (linting, type checking)
- **Automated deployments** with GitHub Actions
- **Environment parity** across dev/staging/prod

### 3. Monitoring & Observability
- **Structured logging** with contextual information
- **Health checks** for all services
- **Data quality metrics** and alerting
- **Performance monitoring** and optimization

### 4. Collaboration & Documentation
- **Living documentation** that updates with code
- **API documentation** auto-generated from code
- **Runbooks** for operational procedures
- **Architecture decision records** (ADRs)

## Testing Strategy

### Run All Tests
```bash
make test
```

### Test Categories
```bash
# Unit tests - Fast, isolated function testing
make test-unit

# Integration tests - End-to-end pipeline testing
make test-integration  

# Data quality tests - Validate data expectations
make quality-check
```

### Test Coverage
- **Unit Tests:** Transformation logic, validation rules
- **Integration Tests:** API endpoints, database operations
- **Data Quality Tests:** Schema compliance, business rules
- **Performance Tests:** Load testing, query optimization

## Sample Usage

### Generate Events
```bash
# Generate 1000 sample events
python scripts/generate_sample_data.py --events 1000

# Stream events continuously
python scripts/generate_sample_data.py --stream --rate 10 --duration 60
```

### API Usage
```python
import requests
import json

# Single event ingestion
event_data = {
    "event_id": "123e4567-e89b-12d3-a456-426614174000",
    "event_type": "page_view",
    "timestamp": "2024-01-15T10:30:00Z",
    "user_id": "user_12345",
    "session_id": "session_67890",
    "page_url": "/products/laptop",
    "device_type": "desktop",
    "country": "US"
}

response = requests.post("http://localhost:8000/events", json=event_data)
print(response.json())
```

### Query Analytics
```python
# Get event statistics
stats = requests.get("http://localhost:8000/events/stats").json()
print(f"Total events: {stats['total_events']}")

# Get recent events
recent = requests.get("http://localhost:8000/events/recent?limit=10").json()
for event in recent['events']:
    print(f"{event['event_type']}: {event['timestamp']}")
```

## Development Workflow

### Daily Development
```bash
# Start development environment
make up

# Run tests before committing
make test

# Code formatting and linting
make lint

# Generate sample data for testing
python scripts/generate_sample_data.py --events 500
```

### Adding New Features
1. **Write tests first** (TDD approach)
2. **Implement feature** with validation
3. **Update documentation** automatically
4. **Run full test suite** before PR
5. **Deploy through CI/CD** pipeline

## 📋 Project Structure

```
├── src/                    # Application source code
│   ├── ingestion/         # Event ingestion and validation
│   ├── processing/        # Data transformation logic  
│   ├── storage/          # Database models and operations
│   ├── monitoring/       # Health checks and metrics
│   └── dashboard/        # Analytics visualization
├── tests/                 # Comprehensive test suite
│   ├── unit/             # Fast, isolated unit tests
│   ├── integration/      # End-to-end pipeline tests
│   └── data_quality/     # Data validation and quality tests
├── config/               # Environment-specific configurations
├── infrastructure/       # Docker containers and setup
├── data/                 # Sample data and schemas
├── scripts/              # Automation and utility scripts
└── docs/                 # Documentation and runbooks
```

## Configuration Management

### Environment Variables
Copy `.env.example` to `.env` and customize:
```bash
# Database settings
DB_HOST=localhost
DB_PORT=5432
DB_NAME=ecommerce_analytics
DB_USER=postgres
DB_PASSWORD=your_password

# Application settings
LOG_LEVEL=INFO
ENVIRONMENT=development
API_PORT=8000

# Data generation
EVENTS_PER_SECOND=10
SIMULATION_HOURS=24
```

### Environment-Specific Configs
- `config/development.yml` - Local development
- `config/testing.yml` - Automated testing
- `config/production.yml` - Production deployment

## Data Quality Framework

### Validation Layers
1. **Schema Validation** - Pydantic models ensure type safety
2. **Business Rules** - Custom validation logic
3. **Cross-field Validation** - Relationship consistency
4. **Format Validation** - URLs, UUIDs, timestamps

### Quality Monitoring
```python
# Example data quality check
from src.ingestion.validators import EventValidator

validator = EventValidator(strict_mode=True)
result = validator.validate_event(event_data)

if not result.is_valid:
    print("Validation errors:", result.errors)
    
if result.warnings:
    print("Quality warnings:", result.warnings)
```

## Monitoring & Alerting

### Health Checks
- **API Health:** `GET /health` - Service status
- **Database Health:** Connection and query tests
- **Data Freshness:** Recent data availability
- **Quality Metrics:** Error rates and validation stats

### Logging Strategy
- **Structured JSON logs** for machine processing
- **Contextual information** (user_id, session_id, etc.)
- **Error tracking** with stack traces
- **Performance metrics** (latency, throughput)

## CI/CD Pipeline

### GitHub Actions Workflows
- `.github/workflows/ci.yml` - Test automation
- `.github/workflows/data-quality.yml` - Quality checks
- `.github/workflows/deploy.yml` - Deployment automation

### Deployment Strategy
1. **Automated Testing** on every commit
2. **Quality Gates** prevent bad deployments
3. **Blue-Green Deployments** for zero downtime
4. **Rollback Capability** for quick recovery

## DataOps Best Practices Implemented

### 1. **Version Control Everything**
- Source code and configuration
- Database schemas and migrations
- Documentation and runbooks
- Test data and expectations

### 2. **Automated Testing**
- Unit tests for transformation logic
- Integration tests for data pipelines
- Data quality tests for validation
- Performance tests for optimization

### 3. **Continuous Deployment**
- Automated builds and deployments
- Environment promotion pipelines
- Feature flags for safe rollouts
- Monitoring and alerting integration

### 4. **Collaboration**
- Shared development standards
- Code review requirements
- Documentation-as-code
- Cross-functional team integration

## Key Metrics & KPIs

### Technical Metrics
- **Data Quality:** 99.5% validation pass rate
- **Availability:** 99.9% uptime SLA
- **Performance:** <100ms API response time
- **Reliability:** <5% deployment failure rate

### Business Metrics
- **Time to Insight:** <2 days requirement to production
- **Data Freshness:** 95% of data <5 minutes old
- **Consumer Satisfaction:** >4.5/5 from data users
- **Cost Efficiency:** 30% reduction in operational costs

## Common Tasks

### Generate Test Data
```bash
# Create realistic sample data
python scripts/generate_sample_data.py --events 10000 --seed 42

# Stream data for real-time testing  
python scripts/generate_sample_data.py --stream --rate 50
```

### Database Operations
```bash
# Reset database (development only)
docker-compose down -v
docker-compose up -d postgres

# Run migrations
python -c "from src.storage.database import get_db_manager; get_db_manager().create_tables()"
```

### Testing
```bash
# Run specific test category
pytest tests/unit/test_validators.py -v
pytest tests/integration/ -v --cov

# Run with specific markers
pytest -m "not integration" -v  # Skip integration tests
```

## Future Enhancements

### Phase 2: Advanced Processing
- Real-time stream processing with Kafka
- Complex event processing (CEP)
- Machine learning integration
- Advanced analytics and ML models

### Phase 3: Scale & Optimization
- Horizontal scaling with Kubernetes
- Data lake integration (Delta Lake)
- Advanced monitoring (Prometheus/Grafana)
- Cost optimization automation

### Phase 4: Advanced DataOps
- A/B testing framework for data features
- Data lineage and impact analysis
- Self-service analytics platform
- Advanced security and compliance
