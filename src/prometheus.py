"""
Prometheus metrics - wrapper for .prometheus module
"""
import importlib.util
import os

# Import from ops/.prometheus folder
_project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
_prometheus_file = os.path.join(_project_root, '.ops', '.prometheus', 'prometheus_metrics.py')
spec = importlib.util.spec_from_file_location("prometheus_metrics", _prometheus_file)
prometheus_metrics = importlib.util.module_from_spec(spec)
spec.loader.exec_module(prometheus_metrics)

ModelMetrics = prometheus_metrics.ModelMetrics
PredictionTimer = prometheus_metrics.PredictionTimer

__all__ = ['ModelMetrics', 'PredictionTimer']

