"""
Kubeflow pipelines - wrapper for .kubeflow module
"""
import importlib.util
import os

# Import from ops/.kubeflow folder
_project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
_kubeflow_file = os.path.join(_project_root, '.ops', '.kubeflow', 'kubeflow_pipeline.py')
spec = importlib.util.spec_from_file_location("kubeflow_pipeline", _kubeflow_file)
kubeflow_pipeline = importlib.util.module_from_spec(spec)
spec.loader.exec_module(kubeflow_pipeline)

macro_cycle_hmm_pipeline = kubeflow_pipeline.macro_cycle_hmm_pipeline

__all__ = ['macro_cycle_hmm_pipeline']

