"""
MLflow integration - wrapper for .mlflow module
"""
import importlib.util
import os

# Import from ops/.mlflow folder
_project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
_mlflow_file = os.path.join(_project_root, '.ops', '.mlflow', 'mlflow_tracking.py')
spec = importlib.util.spec_from_file_location("mlflow_tracking", _mlflow_file)
mlflow_tracking = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mlflow_tracking)

MLflowTracker = mlflow_tracking.MLflowTracker

__all__ = ['MLflowTracker']

