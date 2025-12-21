# Macro Cycle HMM Modeling

This module implements a complete ML workflow for modeling macro economic cycles using Hidden Markov Models (HMM) with k regimes.

## 🚀 Quick Start

**For complete step-by-step instructions, see [QUICK_START.md](QUICK_START.md)**

```bash
# 1. Start monitoring services (Grafana, Prometheus, MLflow)
cd src/trading_agent/model
./start-monitoring.sh

# 2. Train model
python training_script.py --series-ids GDP UNRATE CPIAUCSL --start-date 2000-01-01

# 3. View results
# - Grafana: http://localhost:3000 (admin/admin)
# - MLflow: http://localhost:55000
# - Prometheus: http://localhost:9090
```

## Architecture

The ML workflow consists of:

1. **Data Pipeline**: Extract time series data from FRED PostgreSQL database
2. **Feature Engineering**: Transform raw time series into features suitable for HMM
3. **Model Training**: Train HMM model with k regimes
4. **Experiment Tracking**: Log experiments to MLflow
5. **Feature Store**: Store features in Feast for online serving
6. **Model Serving**: Deploy models via KServe
7. **Monitoring**: Collect metrics with Prometheus and visualize with Grafana
8. **Orchestration**: Manage workflows with Kubeflow Pipelines

## Components

### Core Models

- **`hmm_model.py`**: HMM implementation for macro cycles with k regimes using Pyro
- **`data_loader.py`**: Data loading from FRED database

### ML Infrastructure

- **`.mlflow/`**: MLflow integration for experiment tracking
  - `mlflow_tracking.py`: MLflow tracker implementation
- **`.kubeflow/`**: Kubeflow pipeline definitions
  - `kubeflow_pipeline.py`: Complete ML workflow pipeline
- **`.kserve/`**: KServe deployment configuration
  - `kserve_deployment.py`: Model serving deployment
- **`.feast/`**: Feast feature store
  - `feast_setup.py`: Setup and management functions
  - `feast_repo/`: Repository configuration

### Monitoring

- **`.prometheus/`**: Prometheus metrics collection
  - `prometheus_metrics.py`: Metrics export module
  - `prometheus.yml`: Scrape configuration
- **`.grafana/`**: Grafana configuration and dashboards
  - `provisioning/`: Auto-configuration
  - `dashboards/`: Dashboard JSON files

### Training

- **`training_script.py`**: Standalone training script

## Setup

### Prerequisites

1. PostgreSQL database with FRED data
2. Docker and Docker Compose (for Grafana, Prometheus, MLflow)
3. Kubernetes cluster (for Kubeflow, KServe) - optional
4. Python 3.11+

### Installation

```bash
# Install dependencies
pip install -r requirements.txt

# Set environment variables
export POSTGRES_PASSWORD='your_password'
export MLFLOW_TRACKING_URI='http://localhost:55000'
```

### Docker Setup (Recommended)

Start Grafana, Prometheus, MLflow, and Airflow using Docker:

```bash
cd .ops/.docker
./start-monitoring.sh
```

Or manually:
```bash
cd .ops/.docker
docker-compose up -d
```

This starts:
- **Grafana** on http://localhost:3000 (admin/admin)
- **Prometheus** on http://localhost:9090
- **MLflow** on http://localhost:55000
- **Airflow** on http://localhost:8080

See [.ops/.docker/DOCKER_SETUP.md](../.ops/.docker/DOCKER_SETUP.md) for detailed instructions.

## Usage

### Standalone Training

```bash
python src/trading_agent/model/training_script.py \
    --series-ids GDP UNRATE CPIAUCSL \
    --start-date 2000-01-01 \
    --n-regimes 4 \
    --mlflow-tracking-uri http://localhost:55000
```

### Kubeflow Pipeline

```python
from trading_agent.kubeflow import macro_cycle_hmm_pipeline

# Compile pipeline
from kfp import compiler
compiler.Compiler().compile(
    macro_cycle_hmm_pipeline,
    'macro_cycle_hmm_pipeline.yaml'
)

# Submit to Kubeflow
from kfp import Client
client = Client(host='http://kubeflow-pipelines:8080')
client.create_run_from_pipeline_package(
    'macro_cycle_hmm_pipeline.yaml',
    arguments={
        'n_regimes': 4,
        'series_ids': ['GDP', 'UNRATE', 'CPIAUCSL']
    }
)
```

### Feast Feature Store

```python
from trading_agent.feast import create_feast_repo, define_macro_entities_and_features

# Create repository
create_feast_repo('../../../.ops/.feast/feast_repo')

# Define entities and features
define_macro_entities_and_features('../../../.ops/.feast/feast_repo')

# Use feature store
from feast import FeatureStore
fs = FeatureStore(repo_path='../../../.ops/.feast/feast_repo')
features = fs.get_online_features(...)
```

### KServe Deployment

```python
from trading_agent.kserve import KServeDeployment

deployment = KServeDeployment(namespace='default')

# Deploy model
deployment.create_inference_service(
    service_name='macro-cycle-hmm',
    model_uri='models:/macro-cycle-hmm/Production',
    model_format='sklearn'
)
```

### Prometheus Metrics

Metrics are automatically exported when using the training script. Access at:
```
http://localhost:8000/metrics
```

### Grafana Dashboards

Import dashboards from `.grafana/dashboards/`:
1. HMM Model Monitoring Dashboard
2. Regime Analysis Dashboard

## Model Architecture

The HMM models macro economic cycles as k hidden states (regimes):

- **2 regimes**: Expansion, Contraction
- **3 regimes**: Expansion, Peak, Contraction
- **4 regimes**: Expansion, Peak, Contraction, Trough (default)

Each regime is characterized by:
- Mean observation values
- Covariance structure
- Transition probabilities to other regimes

## Configuration

Edit `config.yaml` to customize:
- Data sources and series IDs
- Model parameters (n_regimes, covariance_type)
- MLflow tracking URI
- Feast repository path
- KServe deployment settings
- Prometheus/Grafana configuration

## Monitoring

### Prometheus Metrics

- `hmm_predictions_total`: Total predictions by regime
- `hmm_prediction_latency_seconds`: Prediction latency
- `hmm_model_log_likelihood`: Model log likelihood
- `hmm_model_aic`: AIC score
- `hmm_model_bic`: BIC score
- `hmm_regime_distribution`: Distribution across regimes
- `hmm_feature_count`: Number of features
- `hmm_prediction_errors_total`: Prediction errors

### Grafana Dashboards

1. **Model Monitoring**: Real-time model performance and predictions
2. **Regime Analysis**: Regime transitions and distributions over time

## Workflow Steps

1. **Data Extraction**: Load time series from FRED database
2. **Feature Engineering**: Transform to percentage changes, differences, etc.
3. **Model Training**: Fit HMM with EM algorithm
4. **Evaluation**: Compute log-likelihood, AIC, BIC
5. **MLflow Logging**: Log parameters, metrics, and model
6. **Feature Store Update**: Materialize features to Feast
7. **Model Deployment**: Deploy to KServe for serving
8. **Monitoring**: Collect metrics for Prometheus/Grafana

## Examples

See `training_script.py` for a complete example of the training pipeline.

## Requirements

See `requirements.txt` for all dependencies including:
- pyro-ppl (Pyro probabilistic programming library)
- torch (PyTorch)
- mlflow
- feast
- prometheus_client
- kfp (Kubeflow Pipelines)

**Note:** KServe is optional and requires Python <3.12. If using Python 3.13, KServe deployment features will be unavailable.

