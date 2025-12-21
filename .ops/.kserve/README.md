# KServe Deployment

This folder contains KServe deployment configuration for model serving.

## Files

- **`kserve_deployment.py`**: KServe deployment manager
  - `KServeDeployment` class for managing inference services
  - Model deployment and updates
  - Service status monitoring

## Usage

```python
from trading_agent.model.kserve import KServeDeployment

# Initialize deployment manager
deployment = KServeDeployment(namespace='default')

# Deploy model
deployment.create_inference_service(
    service_name='macro-cycle-hmm',
    model_uri='models:/macro-cycle-hmm/Production',
    model_format='sklearn',
    min_replicas=1,
    max_replicas=3
)

# Update model
deployment.update_inference_service(
    service_name='macro-cycle-hmm',
    model_uri='models:/macro-cycle-hmm/Staging'
)

# Check status
status = deployment.get_service_status('macro-cycle-hmm')

# Delete service
deployment.delete_inference_service('macro-cycle-hmm')
```

## Requirements

- Kubernetes cluster
- KServe installed
- Model stored in MLflow or accessible storage
- Kubernetes Python client configured

## Model Formats

Currently supports:
- `sklearn`: Scikit-learn models (via MLflow)

## Note

KServe requires Python <3.12. For Python 3.13, KServe deployment features are unavailable.

