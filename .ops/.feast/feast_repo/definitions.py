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
    path="data/macro_features.parquet",
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

