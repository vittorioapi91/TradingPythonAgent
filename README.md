# TradingPythonAgent

A comprehensive Python platform for financial data collection, storage, and machine learning modeling. This project provides end-to-end infrastructure for downloading, storing, and analyzing financial and economic data from multiple sources, with built-in ML workflows for macro cycle modeling.

## 🎯 Overview

TradingPythonAgent is a production-ready system that:

- **Downloads** financial and economic data from multiple authoritative sources
- **Stores** data in PostgreSQL databases with environment-aware configuration
- **Models** macro economic cycles using Hidden Markov Models (HMM)
- **Tracks** ML experiments with MLflow
- **Serves** models via KServe
- **Monitors** with Prometheus and Grafana
- **Orchestrates** workflows with Kubeflow Pipelines
- **Deploys** via Jenkins CI/CD with branch-aware environments

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Data Collection Layer                      │
├─────────────────────────────────────────────────────────────┤
│  EDGAR (SEC) │ FRED │ BLS │ BIS │ Eurostat │ IMF │ Yahoo   │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│                  PostgreSQL Storage Layer                    │
│  (Environment-aware: dev/test/prod databases)                │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│                    ML Pipeline Layer                         │
│  HMM Models │ MLflow │ Feast │ KServe │ Kubeflow             │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│                  Monitoring & Deployment                     │
│  Prometheus │ Grafana │ Jenkins CI/CD                       │
└─────────────────────────────────────────────────────────────┘
```

## 📦 Project Structure

```
TradingPythonAgent/
├── src/                              # Application source code
│   ├── config.py                    # Environment configuration
│   ├── fundamentals/                 # Company fundamentals data
│   │   ├── edgar/                   # SEC EDGAR filings downloader
│   │   └── download_logger.py       # Download error logging
│   ├── macro/                       # Macroeconomic data sources
│   │   ├── fred/                    # Federal Reserve Economic Data
│   │   ├── bls/                     # Bureau of Labor Statistics
│   │   ├── bis/                     # Bank for International Settlements
│   │   ├── eurostat/                # Eurostat (EU statistics)
│   │   └── imf/                     # International Monetary Fund
│   ├── markets/                     # Market data
│   │   ├── equities/                # Stock market data (Yahoo Finance)
│   │   └── etf/                     # ETF data (iShares)
│   ├── model/                       # ML modeling
│   │   ├── hmm_model.py             # Hidden Markov Model for macro cycles
│   │   ├── data_loader.py           # Data loading from PostgreSQL
│   │   ├── training_script.py       # Training pipeline
│   │   └── config.yaml              # Model configuration
│   ├── mlflow.py                    # MLflow integration
│   ├── feast.py                     # Feast feature store
│   ├── kserve.py                    # KServe model serving
│   ├── kubeflow.py                  # Kubeflow pipelines
│   └── prometheus.py                # Prometheus metrics
├── src/                             # Application code (see above)
├── tests/                          # Test suite
├── requirements*.txt               # Python dependencies
└── README.md                        # This file

**Note**: Infrastructure components (Docker, Kubernetes, Jenkins, etc.) have been moved to a separate repository: [infra-platform](https://github.com/your-org/infra-platform)
├── .env.dev                         # Development environment config
├── .env.staging                     # Staging environment config
├── .env.prod                        # Production environment config
├── Jenkinsfile                      # CI/CD pipeline
├── requirements.txt                 # Base Python dependencies (shared)
├── requirements-dev.txt             # Development dependencies (dev/* branches)
├── requirements-staging.txt         # Staging dependencies (staging branch)
├── requirements-prod.txt            # Production dependencies (main branch)
└── README.md                        # This file
```

## 🔌 Data Sources

### Fundamentals

- **SEC EDGAR**: Company filings (10-K, 10-Q, 8-K, etc.) with XBRL support
  - Downloads all company filings from SEC EDGAR database
- **Company Data**: Ticker symbols, SIC codes, entity types

### Macroeconomic Data

- **FRED** (Federal Reserve Economic Data): US economic time series
- **BLS** (Bureau of Labor Statistics): US labor market statistics
- **BIS** (Bank for International Settlements): International banking statistics
- **Eurostat**: European Union statistics
- **IMF**: International Monetary Fund economic data

### Market Data

- **Yahoo Finance**: Stock prices, historical data, extended market data
- **iShares ETFs**: ETF holdings, details, and performance data

## 🗄️ Database Architecture

The project uses PostgreSQL databases with environment-aware configuration:

- **Development** (`dev/*` branches): `dev.tradingAgent@localhost:5432`
- **Staging** (`staging` branch): `test.tradingAgent@localhost:5432`
- **Production** (`main` branch): `prod.tradingAgent@localhost:5432`

Each environment has separate databases:
- `edgar` - SEC EDGAR filings
- `fred` - FRED economic data
- `bls` - BLS labor statistics
- `bis` - BIS banking data
- `eurostat` - Eurostat data
- `imf` - IMF data

## 🤖 Machine Learning

### Hidden Markov Model (HMM) for Macro Cycles

The project includes a complete ML workflow for modeling macro economic cycles:

- **Model**: Multi-regime HMM using Pyro (2, 3, or 4 regimes)
- **Features**: Time series transformations (percentage changes, differences)
- **Training**: Expectation-Maximization algorithm
- **Evaluation**: Log-likelihood, AIC, BIC metrics
- **Serving**: KServe deployment for real-time inference
- **Tracking**: MLflow experiment tracking
- **Feature Store**: Feast for online feature serving

See [`src/model/README.md`](src/model/README.md) for detailed ML documentation.

## 🚀 Quick Start

### Prerequisites

- Python 3.11+ (Python <3.12 for KServe support)
- PostgreSQL 15+
- Docker and Docker Compose (for monitoring services)
- Git

### Installation

1. **Clone the repository**:
   ```bash
   git clone https://github.com/vittorioapi91/TradingPythonAgent.git
   cd TradingPythonAgent
   ```

2. **Create virtual environment**:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies** (environment-specific):
   ```bash
   # The system automatically detects your environment based on git branch
   # For manual installation, use the appropriate file:
   pip install -r requirements-dev.txt      # For dev/* branches
   pip install -r requirements-staging.txt  # For staging branch
   pip install -r requirements-prod.txt     # For main branch
   
   # Or use the config module to get the right file:
   python -c "from src.config import get_requirements_file; print(get_requirements_file())"
   ```

4. **Configure environment**:
   - The system automatically detects your git branch and loads the appropriate `.env` file
   - For manual configuration, copy `.env.dev` and update with your credentials:
     ```bash
     cp .env.dev .env.local
     # Edit .env.local with your database credentials
     ```

5. **Start PostgreSQL** (if using Docker):
   ```bash
   # Infrastructure services are in the infra-platform repository
   # Clone and start services from: https://github.com/your-org/infra-platform
   cd ../infra-platform/.docker
   docker-compose -f docker-compose.infra-platform.yml up -d postgres
   ```

6. **Start monitoring services** (optional):
   ```bash
   # From the infra-platform repository
   cd ../infra-platform/.docker
   ./start-docker-monitoring.sh
   # Or manually:
   docker-compose -f docker-compose.infra-platform.yml up -d grafana prometheus mlflow
   ```

### Environment Configuration

The project uses environment-aware configuration that automatically detects your git branch:

- **`dev/*` branches** → Loads `.env.dev` → Uses `dev.tradingAgent@localhost`
- **`staging` branch** → Loads `.env.staging` → Uses `test.tradingAgent@localhost`
- **`main` branch** → Loads `.env.prod` → Uses `prod.tradingAgent@localhost`

Environment variables are loaded automatically when the package is imported. See [`src/config.py`](src/config.py) for details.

## 📖 Usage Examples

### Download SEC EDGAR Filings

```bash
# Generate catalog of companies and filings
python -m src.fundamentals.edgar \
    --generate-catalog \
    --download-companies \
    --start-year 2020

# Download filings from database
python -m src.fundamentals.edgar \
    --from-db \
    --ticker AAPL,MSFT,NVDA
```

### Download FRED Economic Data

```bash
# Generate database with all downloadable series
python -m src.macro.fred.main \
    --generate-db \
    --api-key YOUR_FRED_API_KEY

# Download specific series
python -m src.macro.fred.main \
    --series GDP UNRATE CPIAUCSL \
    --start-date 2000-01-01
```

### Download BLS Labor Statistics

```bash
python -m src.macro.bls.main \
    --api-key YOUR_BLS_API_KEY \
    --series CUUR0000SA0 \
    --start-year 2020
```

### Train HMM Model

```bash
cd src/model
python training_script.py \
    --series-ids GDP UNRATE CPIAUCSL \
    --start-date 2000-01-01 \
    --n-regimes 4 \
    --mlflow-tracking-uri http://localhost:55000
```

### Download Stock Market Data

```bash
python -m src.markets.equities.main \
    --tickers AAPL,MSFT,GOOGL \
    --start-date 2020-01-01 \
    --end-date 2024-01-01
```

## 🔧 Configuration

### Environment Variables

Create `.env.dev`, `.env.staging`, or `.env.prod` files with:

```bash
# PostgreSQL Configuration
POSTGRES_USER=dev.tradingAgent
POSTGRES_HOST=localhost
POSTGRES_PASSWORD=your_password
POSTGRES_PORT=5432

# API Keys
FRED_API_KEY=your_fred_api_key
BLS_API_KEY=your_bls_api_key

# MLflow
MLFLOW_TRACKING_URI=http://localhost:55000
```

### Database Setup

The system automatically creates database schemas when first connecting. Ensure PostgreSQL is running and accessible.

## 🏭 CI/CD Pipeline

The project includes separate Jenkins CI/CD pipelines for application code and infrastructure:

### Application Pipeline (`Jenkinsfile`)
- **Purpose**: Builds and validates trading_agent application code
- **Triggers**: On all code changes in this repository
- **Stages**:
  - JIRA validation (for feature branches)
  - Module validation
  - Airflow DAG validation (syntax/imports)
  - Unit tests
  - Docker image builds
  - Kubernetes deployments

### Infrastructure Pipeline
- **Repository**: [infra-platform](https://github.com/your-org/infra-platform)
- **Purpose**: Validates and builds infrastructure components
- **Triggers**: On infrastructure repository changes
- **Stages**:
  - Infrastructure configuration validation
  - Docker image builds (e.g., jenkins-custom)
  - Service validation

**Note**: Infrastructure CI/CD is managed in the separate `infra-platform` repository.

**Branch-aware deployments:**
- **Feature branches** (`dev/{jira_issue}/{project}-{subproject}`): Deploy to dev environment
- **Staging branch**: Deploy to staging environment
- **Main branch**: Deploy to production

See [`JENKINS.md`](JENKINS.md) for detailed Jenkins configuration.

### Branch Naming Convention

Feature branches must follow the pattern:
```
dev/{JIRA_ISSUE}/{project}-{subproject}
```

Examples:
- `dev/DEV-4/trading_agent-fundamentals`
- `dev/PROJ-123/trading_agent-macro`
- `dev/BUG-789/trading_agent-model`

**Important**: The Jenkins pipeline automatically validates branch names and JIRA issues:

- **Branch Name Validation**: The pipeline extracts the JIRA issue key (e.g., `DEV-4`) from the branch name using the pattern `dev/{JIRA_KEY-NUMBER}/{project}-{subproject}`
- **JIRA Issue Validation**: For feature branches, the pipeline:
  - Tests JIRA connection and authentication
  - Validates that the JIRA issue exists and is accessible
  - If validation fails, the pipeline continues with a warning (non-blocking)

### Commit Message Requirements

**All commit messages must include the JIRA issue key** in the format `[JIRA_ISSUE]` at the beginning of the commit message.

Examples:
- `[DEV-4] Add EDGAR download functionality`
- `[PROJ-123] Fix database connection issue`
- `[DEV-4] Update README with JIRA validation info`

The JIRA issue key in the commit message should match the one in the branch name. This ensures proper tracking and linking between commits and JIRA issues.

## 📊 Monitoring

### Prometheus Metrics

Metrics are exported at `http://localhost:8000/metrics`:
- Model predictions and latency
- Regime distributions
- Model performance (AIC, BIC, log-likelihood)

### Grafana Dashboards

Access Grafana at `http://localhost:3000` (admin/admin):
- Model monitoring dashboard
- Regime analysis dashboard
- Data pipeline metrics

### MLflow Tracking

Access MLflow UI at `http://localhost:55000`:
- Experiment tracking
- Model versioning
- Parameter and metric logging

## 🧪 Testing

```bash
# Run tests
pytest tests/

# Run with coverage
pytest --cov=trading_agent tests/
```

## 📚 Documentation

- **ML Modeling**: [`trading_agent/model/README.md`](trading_agent/model/README.md)
- **Jenkins CI/CD**: [`JENKINS.md`](JENKINS.md)
- **Infrastructure**: See [infra-platform](https://github.com/your-org/infra-platform) repository for Docker, Kubernetes, and infrastructure setup

## 🛠️ Development

### Adding a New Data Source

1. Create a new module in `trading_agent/macro/` or `trading_agent/markets/`
2. Implement downloader class with PostgreSQL integration
3. Add database schema initialization
4. Create main entry point script
5. Update this README

### Code Style

- Follow PEP 8
- Use type hints
- Document all public functions and classes
- Add docstrings to modules

## 🔐 Security

- **Never commit** `.env` files (they're in `.gitignore`)
- Use environment-specific database users
- Store API keys in environment variables or secure credential stores
- Use different credentials for dev/staging/prod environments

## 📝 License

[Add your license here]

## 🤝 Contributing

1. Create a feature branch following the naming convention: `dev/{JIRA_ISSUE}/{project}-{subproject}`
2. Ensure the JIRA issue exists before pushing
3. Make your changes
4. Test thoroughly
5. Submit a pull request

## 🐛 Troubleshooting

### Database Connection Issues

- Verify PostgreSQL is running: `docker ps | grep postgres`
- Check environment variables are loaded: `python -c "import os; print(os.getenv('POSTGRES_USER'))"`
- Verify database user exists: `docker exec postgres psql -U tradingAgent -c "\du"`

### Import Errors

- Ensure you're in the project root directory
- Activate virtual environment
- Install all dependencies: `pip install -r requirements.txt`

### Jenkins Pipeline Issues

- Check branch naming follows convention
- Verify JIRA issue exists
- Check Jenkins logs for detailed error messages

## 📞 Support

For issues or questions:
1. Check the relevant module's documentation
2. Review Jenkins build logs
3. Check database and service logs
4. Review GitHub issues

## 🎯 Roadmap

- [ ] Additional data sources
- [ ] Real-time data streaming
- [ ] Advanced ML models
- [ ] Web UI for data exploration
- [ ] API endpoints for data access

---

**Built with**: Python, PostgreSQL, PyTorch, MLflow, Feast, KServe, Kubeflow, Prometheus, Grafana, Jenkins
