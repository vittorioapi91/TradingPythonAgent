"""
Feast feature store - wrapper for .feast module
"""
import importlib.util
import os

# Import from ops/.feast folder
_project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
_feast_file = os.path.join(_project_root, '.ops', '.feast', 'feast_setup.py')
spec = importlib.util.spec_from_file_location("feast_setup", _feast_file)
feast_setup = importlib.util.module_from_spec(spec)
spec.loader.exec_module(feast_setup)

create_feast_repo = feast_setup.create_feast_repo
define_macro_entities_and_features = feast_setup.define_macro_entities_and_features
materialize_features = feast_setup.materialize_features
get_online_features = feast_setup.get_online_features

__all__ = [
    'create_feast_repo',
    'define_macro_entities_and_features',
    'materialize_features',
    'get_online_features'
]

