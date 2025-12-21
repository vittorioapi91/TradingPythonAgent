# Feast Feature Store

This folder contains Feast feature store setup and configuration for macro economic features.

## Files

- **`feast_setup.py`**: Feast setup and management functions
  - `create_feast_repo()`: Create Feast repository structure
  - `define_macro_entities_and_features()`: Define entities and feature views
  - `materialize_features()`: Materialize features to online store
  - `get_online_features()`: Retrieve online features

- **`feast_repo/`**: Feast repository configuration
  - `feature_store.yaml`: Feast configuration
  - `definitions.py`: Feature definitions (entities, feature views)

## Usage

### Setup Feast Repository

```python
from trading_agent.feast import create_feast_repo, define_macro_entities_and_features

# Create repository
create_feast_repo('./feast_repo')

# Define entities and features
define_macro_entities_and_features('./feast_repo')
```

### Use Feature Store

```python
from feast import FeatureStore
from trading_agent.feast import materialize_features, get_online_features

# Initialize feature store
fs = FeatureStore(repo_path='./feast_repo')

# Materialize features
materialize_features(
    fs,
    start_date='2000-01-01',
    end_date='2024-01-01'
)

# Get online features
entity_rows = [{'date': 1609459200}]  # Unix timestamp
features = get_online_features(
    fs,
    entity_rows=entity_rows,
    feature_refs=['macro_indicators:GDP', 'macro_indicators:UNRATE']
)
```

## Configuration

The Feast repository is configured in `feast_repo/feature_store.yaml`:
- **Project**: macro_features
- **Provider**: local (SQLite for online store)
- **Offline store**: file-based

## Feature Views

Currently defined:
- **macro_indicators**: Macro economic indicators feature view
  - Entity: date (time-based)
  - Features: GDP, UNRATE, CPIAUCSL, etc.
  - TTL: 365 days

## Integration

Feast is integrated into:
- **Kubeflow Pipeline**: `update_feature_store` component
- **Training Script**: Can be used to store features after training

