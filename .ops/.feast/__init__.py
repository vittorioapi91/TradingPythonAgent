"""
Feast feature store setup and management
"""

from .feast_setup import (
    create_feast_repo,
    define_macro_entities_and_features,
    materialize_features,
    get_online_features
)

__all__ = [
    'create_feast_repo',
    'define_macro_entities_and_features',
    'materialize_features',
    'get_online_features'
]

