# MLflow Integration

This folder contains MLflow integration for experiment tracking and model registry.

## Files

- **`mlflow_tracking.py`**: MLflow tracker implementation
  - `MLflowTracker` class for logging experiments
  - Model registration and versioning
  - Model loading from registry

## Usage

```python
from trading_agent.model.mlflow import MLflowTracker

# Initialize tracker
tracker = MLflowTracker(
    tracking_uri='http://localhost:5000',
    experiment_name='macro-cycle-hmm'
)

# Log experiment
run_id = tracker.log_hmm_experiment(
    model=model,
    params={'n_regimes': 4},
    metrics={'log_likelihood': -100.5}
)

# Register model
version = tracker.register_model(run_id, 'macro-cycle-hmm')

# Load model
model = tracker.load_model('macro-cycle-hmm', version=version)
```

## Configuration

Set MLflow tracking URI:
```bash
export MLFLOW_TRACKING_URI=http://localhost:5000
```

Or pass directly:
```python
tracker = MLflowTracker(tracking_uri='http://mlflow-service:5000')
```

