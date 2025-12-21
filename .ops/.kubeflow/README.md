# Kubeflow Pipelines

This folder contains Kubeflow pipeline definitions for the complete ML workflow.

## Files

- **`kubeflow_pipeline.py`**: Complete ML pipeline definition
  - Data extraction from FRED
  - Feature engineering
  - Model training
  - Model evaluation
  - Feature store updates
  - Model deployment

## Usage

### Compile Pipeline

```python
from trading_agent.model.kubeflow import macro_cycle_hmm_pipeline
from kfp import compiler

compiler.Compiler().compile(
    macro_cycle_hmm_pipeline,
    'macro_cycle_hmm_pipeline.yaml'
)
```

### Submit to Kubeflow

```python
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

## Pipeline Steps

1. **extract_fred_data**: Load data from PostgreSQL
2. **engineer_features**: Transform time series to features
3. **train_hmm_model**: Train HMM model with Pyro
4. **evaluate_model**: Evaluate model performance
5. **update_feature_store**: Materialize features to Feast
6. **deploy_model_kserve**: Deploy model to KServe

## Requirements

- Kubernetes cluster with Kubeflow installed
- Access to FRED PostgreSQL database
- MLflow server
- Feast feature store
- KServe for model serving

