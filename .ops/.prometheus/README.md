# Prometheus Configuration

This folder contains all Prometheus-related files for monitoring the HMM model.

## Files

- **`prometheus.yml`**: Main Prometheus configuration file
  - Defines scrape targets
  - Configures metrics collection
  - Used by Docker Compose to configure Prometheus container

- **`prometheus_metrics.py`**: Python module for exporting metrics
  - `ModelMetrics` class: Defines and exports Prometheus metrics
  - `PredictionTimer` class: Context manager for timing predictions
  - Used by training scripts to expose metrics

## Usage

### In Training Scripts

```python
from trading_agent.model.prometheus import ModelMetrics

# Initialize metrics
metrics = ModelMetrics(port=8000)

# Record model metrics
metrics.record_model_metrics(
    model_name='macro-cycle-hmm',
    run_id='run-123',
    log_likelihood=-100.5,
    aic=250.0,
    bic=300.0
)

# Record predictions
metrics.record_prediction(
    model_name='macro-cycle-hmm',
    regime=1,
    latency=0.05
)
```

### Configuration

The `prometheus.yml` file is automatically mounted into the Prometheus Docker container. To modify scrape targets:

1. Edit `prometheus/prometheus.yml`
2. Restart Prometheus: `docker-compose restart prometheus`

### Metrics Exposed

The following metrics are available:

- `hmm_predictions_total`: Total number of predictions by regime
- `hmm_prediction_latency_seconds`: Prediction latency histogram
- `hmm_model_log_likelihood`: Model log likelihood
- `hmm_model_aic`: AIC score
- `hmm_model_bic`: BIC score
- `hmm_regime_distribution`: Distribution of predictions across regimes
- `hmm_feature_count`: Number of features used
- `hmm_prediction_errors_total`: Total prediction errors

### Accessing Metrics

Metrics are exposed on port 8000 (configurable) when running training scripts:

```bash
# View metrics
curl http://localhost:8000/metrics

# Or in browser
open http://localhost:8000/metrics
```

Prometheus automatically scrapes these metrics based on the configuration in `prometheus.yml`.

