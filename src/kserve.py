"""
KServe deployment - wrapper for .kserve module
"""
import importlib.util
import os

# Import from ops/.kserve folder
_project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
_kserve_file = os.path.join(_project_root, '.ops', '.kserve', 'kserve_deployment.py')
spec = importlib.util.spec_from_file_location("kserve_deployment", _kserve_file)
kserve_deployment = importlib.util.module_from_spec(spec)
spec.loader.exec_module(kserve_deployment)

KServeDeployment = kserve_deployment.KServeDeployment

__all__ = ['KServeDeployment']

