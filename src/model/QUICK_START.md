# Quick Start Guide - Macro Cycle HMM Modeling

Complete guide to run the entire ML workflow for macro economic cycle modeling.

## Prerequisites

1. **Docker and Docker Compose** installed
2. **PostgreSQL database** with FRED data (see main project setup)
3. **Python 3.11+** with virtual environment (Python 3.13 works, but KServe requires <3.12)
4. **Environment variables** set:
   ```bash
   export POSTGRES_PASSWORD='your_password'
   ```

**Note:** The HMM model uses Pyro (probabilistic programming) instead of hmmlearn. This provides more flexibility and better integration with PyTorch.

## Step-by-Step Setup

### Step 1: Install Dependencies

```bash
cd /Users/Snake91/CursorProjects/TradingPythonAgent
source .venv/bin/activate  # or your virtual environment
pip install -r requirements.txt
```

### Step 2: Start Monitoring & Orchestration Services (Docker)

Start Grafana, Prometheus, MLflow, and Airflow:

```bash
cd ..ops/.docker
./start-monitoring.sh
```

Or manually:
```bash
cd ..ops/.docker
docker-compose up -d
```

**Verify services are running:**
```bash
cd ..ops/.docker && docker-compose ps
```

**Access services:**
- Grafana: http://localhost:3000 (admin/admin)
- Prometheus: http://localhost:9090
- MLflow: http://localhost:55000
- Airflow: http://localhost:8080

### Step 3: Train the HMM Model

Run the training script to:
- Load data from FRED database
- Train HMM model
- Log to MLflow
- Export Prometheus metrics

```bash
cd trading_agent/model

python training_script.py \
    --series-ids GDP UNRATE CPIAUCSL \
    --start-date 2000-01-01 \
    --n-regimes 4 \
    --mlflow-tracking-uri http://localhost:55000 \
    --prometheus-port 8000
```

**Parameters:**
- `--series-ids`: FRED series IDs to use (default: GDP UNRATE CPIAUCSL)
- `--start-date`: Start date for data (YYYY-MM-DD)
- `--end-date`: End date (optional, defaults to today)
- `--n-regimes`: Number of HMM regimes (default: 4)
- `--feature-method`: Feature engineering method (pct_change, diff, log_diff, raw)
- `--mlflow-tracking-uri`: MLflow server URL
- `--prometheus-port`: Port for Prometheus metrics (default: 8000)

### Step 4: View Results

#### MLflow (Experiment Tracking)
1. Open http://localhost:55000
2. Select experiment "macro-cycle-hmm"
3. View runs, metrics, and models

#### Grafana (Monitoring Dashboards)
1. Open http://localhost:3000
2. Login: admin/admin
3. Navigate to "Macro Cycle HMM" folder
4. View dashboards:
   - **HMM Model Monitoring**: Real-time metrics
   - **Regime Analysis**: Regime transitions and distributions

#### Prometheus (Metrics)
1. Open http://localhost:9090
2. Query metrics:
   - `hmm_predictions_total`
   - `hmm_model_log_likelihood`
   - `hmm_regime_distribution`

## Complete Workflow Examples

### Example 1: Basic Training

```bash
# 1. Start services
cd trading_agent/model
./start-monitoring.sh

# 2. Train model
python training_script.py \
    --series-ids GDP UNRATE \
    --start-date 2010-01-01 \
    --n-regimes 3

# 3. View in Grafana
# Open http://localhost:3000
```

### Example 2: Full Pipeline with Custom Parameters

```bash
# 1. Start services
cd ..ops/.docker && docker-compose up -d

# 2. Train with custom settings
python training_script.py \
    --series-ids GDP UNRATE CPIAUCSL FEDFUNDS INDPRO \
    --start-date 2000-01-01 \
    --end-date 2024-01-01 \
    --n-regimes 4 \
    --covariance-type full \
    --feature-method pct_change \
    --mlflow-tracking-uri http://localhost:55000 \
    --experiment-name macro-cycle-hmm-v2 \
    --prometheus-port 8000

# 3. Check MLflow for model version
# 4. View metrics in Grafana
```

### Example 3: Using Python API

```python
from src.model.data_loader import MacroDataLoader
from src.model.hmm_model import MacroCycleHMM
from src.mlflow import MLflowTracker
import numpy as np

# Load data
loader = MacroDataLoader(
    dbname='fred',
    user='tradingAgent',
    host='localhost'
)
data = loader.load_series(['GDP', 'UNRATE'], start_date='2000-01-01')
features = loader.prepare_features(data, method='pct_change')

# Train model
X = features[['GDP', 'UNRATE']].values
model = MacroCycleHMM(n_regimes=4)
model.fit(X)

# Log to MLflow
tracker = MLflowTracker(
    tracking_uri='http://localhost:55000',
    experiment_name='macro-cycle-hmm'
)
tracker.log_hmm_experiment(
    model=model,
    params={'n_regimes': 4, 'n_features': 2},
    metrics=model.get_model_metrics(X)
)
```

## Folder Structure Explained

```
trading_agent/
├── model/                      # ✅ Core modeling only
│   ├── hmm_model.py           # HMM model implementation
│   ├── data_loader.py         # Data loading from FRED
│   ├── training_script.py     # Main training script
│   ├── config.yaml            # Configuration
│   └── ...
│
├── .docker/                    # Docker configuration (hidden)
│   ├── docker-compose.yml
│   ├── start-monitoring.sh
│   └── ...
│
├── .mlflow/                    # MLflow integration (hidden)
├── .kubeflow/                  # Kubeflow pipelines (hidden)
├── .kserve/                    # KServe deployment (hidden)
├── .feast/                     # Feast feature store (hidden)
├── .prometheus/                # Prometheus (hidden)
├── .grafana/                   # Grafana (hidden)
│   ├── provisioning/         # Auto-configuration
│   └── dashboards/           # Dashboard JSON files
│
├── .kubernetes/                # Kubernetes manifests (hidden)
│
└── mlflow.py, prometheus.py,  # Wrapper modules for imports
    kubeflow.py, kserve.py,
    feast.py
```

**Why Grafana folders are still needed:**
- `.ops/.grafana/provisioning/` is mounted to `/etc/grafana/provisioning` in Docker
- `.ops/.grafana/dashboards/` is mounted to `/var/lib/grafana/dashboards` in Docker
- Grafana reads these folders to auto-configure datasources and import dashboards
- Without these mounts, you'd have to manually configure everything in Grafana UI

## Common Tasks

### View Logs

```bash
# All services
cd ..ops/.docker && docker-compose logs -f

# Specific service
cd ..ops/.docker && docker-compose logs -f grafana
cd ..ops/.docker && docker-compose logs -f prometheus
cd ..ops/.docker && docker-compose logs -f mlflow
```

### Stop Services

```bash
cd ..ops/.docker && ./stop-monitoring.sh
# or
cd ..ops/.docker && docker-compose down
```

### Restart Services

```bash
cd ..ops/.docker && docker-compose restart grafana
cd ..ops/.docker && docker-compose restart prometheus
```

### Update Dashboards

1. Edit JSON files in `.ops/.grafana/dashboards/`
2. Restart Grafana: `cd ..ops/.docker && docker-compose restart grafana`
3. Dashboards auto-update

### Change Grafana Password

Edit `docker-compose.yml`:
```yaml
environment:
  - GF_SECURITY_ADMIN_PASSWORD=your_new_password
```

Then restart: `docker-compose restart grafana`

### Access Prometheus Metrics from Training Script

The training script exposes metrics on port 8000. Prometheus scrapes from:
- Docker Desktop: `host.docker.internal:8000`
- Linux: Use host IP or add `network_mode: host`

## Troubleshooting

### Services won't start

```bash
# Check Docker is running
docker info

# Check for port conflicts
lsof -i :3000  # Grafana
lsof -i :9090  # Prometheus
lsof -i :5000  # MLflow

# View error logs
cd ..ops/.docker && docker-compose logs
```

### Grafana can't see Prometheus

1. Check Prometheus is running: http://localhost:9090
2. In Grafana: Configuration → Data Sources → Prometheus
3. Test connection
4. Check URL is `http://prometheus:9090` (not localhost)

### Prometheus can't scrape metrics

1. Ensure training script is running with `--prometheus-port 8000`
2. Check metrics endpoint: http://localhost:8000/metrics
3. For Docker Desktop, Prometheus uses `host.docker.internal:8000`
4. Check `prometheus.yml` scrape config

### Dashboards not appearing

1. Check JSON files are valid: `cat .grafana/dashboards/*.json | jq .`
2. Check Grafana logs: `cd ..ops/.docker && docker-compose logs grafana | grep dashboard`
3. Verify mount: `docker exec grafana ls -la /var/lib/grafana/dashboards`
4. Manually import if needed: Dashboards → Import → Upload JSON

### MLflow can't connect

1. Check MLflow is running: http://localhost:55000
2. Verify tracking URI: `export MLFLOW_TRACKING_URI=http://localhost:55000`
3. Check logs: `cd ..ops/.docker && docker-compose logs mlflow`

## Next Steps

### For Production

1. **Use PostgreSQL for MLflow**:
   - Edit `.ops/.docker/docker-compose.yml` to add PostgreSQL service
   - Update MLflow environment variables

2. **Secure Services**:
   - Change default passwords
   - Enable authentication
   - Use secrets management

3. **Kubernetes Deployment**:
   - Use `kubeflow_pipeline.py` for orchestration
   - Deploy with `kserve_deployment.py`
   - Use Kubernetes manifests in `.ops/.kubernetes/`

4. **Feature Store**:
   - Set up Feast repository
   - Materialize features for online serving

## Quick Reference

| Service | URL | Credentials |
|---------|-----|-------------|
| Grafana | http://localhost:3000 | admin/admin |
| Prometheus | http://localhost:9090 | - |
| MLflow | http://localhost:55000 | - |

| Command | Description |
|---------|-------------|
| `cd ..ops/.docker && ./start-monitoring.sh` | Start all services |
| `cd ..ops/.docker && ./stop-monitoring.sh` | Stop all services |
| `cd ..ops/.docker && docker-compose ps` | Check service status |
| `cd ..ops/.docker && docker-compose logs -f` | View logs |
| `python training_script.py --help` | Training options |

## Need Help?

- See `README.md` for detailed component documentation
- See `.ops/.docker/DOCKER_SETUP.md` for Docker-specific details
- Check service logs: `docker-compose logs [service-name]`

