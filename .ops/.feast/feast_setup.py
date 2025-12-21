"""
Feast feature store setup for macro economic features
"""

from feast import FeatureStore, Entity, FeatureView, ValueType
from datetime import timedelta
from feast.data_source import FileSource
import pandas as pd
from typing import List, Optional
import logging

logger = logging.getLogger(__name__)


def create_feast_repo(repo_path: str = './feast_repo'):
    """
    Create Feast feature repository structure
    
    Args:
        repo_path: Path to Feast repository
    """
    import os
    
    # Create directory structure
    os.makedirs(repo_path, exist_ok=True)
    os.makedirs(f'{repo_path}/data', exist_ok=True)
    
    # Create feature_store.yaml
    feature_store_yaml = f"""
project: macro_features
registry: {repo_path}/registry.db
provider: local
online_store:
    type: sqlite
    path: {repo_path}/online_store.db
"""
    
    with open(f'{repo_path}/feature_store.yaml', 'w') as f:
        f.write(feature_store_yaml)
    
    logger.info(f"Feast repository created at {repo_path}")


def define_macro_entities_and_features(repo_path: str = './feast_repo'):
    """
    Define Feast entities and feature views for macro economic data
    
    Args:
        repo_path: Path to Feast repository
    """
    from feast import Entity, FeatureView, ValueType
    from feast.data_source import FileSource
    from datetime import timedelta
    
    # Entity: Date (time-based entity)
    date_entity = Entity(
        name="date",
        value_type=ValueType.UNIX_TIMESTAMP,
        description="Date entity for time series features"
    )
    
    # Feature View: Macro Economic Indicators
    macro_features_source = FileSource(
        path=f"{repo_path}/data/macro_features.parquet",
        timestamp_field="date",
        created_timestamp_column="created_at"
    )
    
    macro_feature_view = FeatureView(
        name="macro_indicators",
        entities=[date_entity],
        ttl=timedelta(days=365),
        source=macro_features_source,
        online=True,
        tags={
            "team": "macro_modeling",
            "domain": "economics"
        }
    )
    
    # Save definitions to Python file
    definitions_content = f"""
from feast import Entity, FeatureView, ValueType
from feast.data_source import FileSource
from datetime import timedelta

# Entity
date_entity = Entity(
    name="date",
    value_type=ValueType.UNIX_TIMESTAMP,
    description="Date entity for time series features"
)

# Feature View
macro_features_source = FileSource(
    path="{repo_path}/data/macro_features.parquet",
    timestamp_field="date",
    created_timestamp_column="created_at"
)

macro_feature_view = FeatureView(
    name="macro_indicators",
    entities=[date_entity],
    ttl=timedelta(days=365),
    source=macro_features_source,
    online=True,
    tags={{
        "team": "macro_modeling",
        "domain": "economics"
    }}
)
"""
    
    with open(f'{repo_path}/definitions.py', 'w') as f:
        f.write(definitions_content)
    
    logger.info("Feast entities and feature views defined")


def materialize_features(fs: FeatureStore, start_date: str, end_date: str):
    """
    Materialize features to online store
    
    Args:
        fs: FeatureStore instance
        start_date: Start date for materialization
        end_date: End date for materialization
    """
    fs.materialize(start_date=start_date, end_date=end_date)
    logger.info(f"Features materialized from {start_date} to {end_date}")


def get_online_features(fs: FeatureStore, entity_rows: List[dict],
                       feature_refs: List[str]) -> pd.DataFrame:
    """
    Retrieve online features from Feast
    
    Args:
        fs: FeatureStore instance
        entity_rows: List of entity dictionaries
        feature_refs: List of feature references
        
    Returns:
        DataFrame with features
    """
    features = fs.get_online_features(
        entity_rows=entity_rows,
        features=feature_refs
    ).to_df()
    
    return features

