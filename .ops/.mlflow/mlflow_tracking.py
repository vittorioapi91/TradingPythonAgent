"""
MLflow integration for experiment tracking and model registry
"""

import mlflow
import mlflow.sklearn
from typing import Dict, Optional, Any
import logging
import os

logger = logging.getLogger(__name__)


class MLflowTracker:
    """
    MLflow tracker for HMM model experiments
    """
    
    def __init__(self, tracking_uri: Optional[str] = None,
                 experiment_name: str = 'macro-cycle-hmm'):
        """
        Initialize MLflow tracker
        
        Args:
            tracking_uri: MLflow tracking URI (default: from env or local)
            experiment_name: Name of MLflow experiment
        """
        self.tracking_uri = tracking_uri or os.getenv(
            'MLFLOW_TRACKING_URI',
            'http://localhost:5000'
        )
        self.experiment_name = experiment_name
        
        mlflow.set_tracking_uri(self.tracking_uri)
        mlflow.set_experiment(experiment_name)
        
        logger.info(f"MLflow tracking initialized: {self.tracking_uri}")
        logger.info(f"Experiment: {experiment_name}")
    
    def log_hmm_experiment(self,
                          model: Any,
                          params: Dict[str, Any],
                          metrics: Dict[str, float],
                          artifacts: Optional[Dict[str, str]] = None,
                          tags: Optional[Dict[str, str]] = None) -> str:
        """
        Log HMM model experiment to MLflow
        
        Args:
            model: Trained HMM model
            params: Model parameters
            metrics: Model metrics
            artifacts: Optional artifacts to log (dict of name: path)
            tags: Optional tags for the run
            
        Returns:
            Run ID
        """
        with mlflow.start_run() as run:
            # Log parameters
            mlflow.log_params(params)
            
            # Log metrics
            mlflow.log_metrics(metrics)
            
            # Log tags
            if tags:
                mlflow.set_tags(tags)
            
            # Log model
            mlflow.sklearn.log_model(
                model,
                'model',
                registered_model_name='macro-cycle-hmm'
            )
            
            # Log artifacts
            if artifacts:
                for name, path in artifacts.items():
                    mlflow.log_artifact(path, name)
            
            run_id = run.info.run_id
            model_uri = mlflow.get_artifact_uri('model')
            
            logger.info(f"Experiment logged. Run ID: {run_id}")
            logger.info(f"Model URI: {model_uri}")
            
            return run_id
    
    def register_model(self, run_id: str, model_name: str = 'macro-cycle-hmm',
                      stage: str = 'Production') -> str:
        """
        Register model in MLflow model registry
        
        Args:
            run_id: MLflow run ID
            model_name: Model name in registry
            stage: Model stage (Staging, Production, Archived)
            
        Returns:
            Model version
        """
        model_uri = f"runs:/{run_id}/model"
        
        model_version = mlflow.register_model(
            model_uri,
            model_name
        )
        
        # Transition to stage
        client = mlflow.tracking.MlflowClient()
        client.transition_model_version_stage(
            name=model_name,
            version=model_version.version,
            stage=stage
        )
        
        logger.info(f"Model registered: {model_name} v{model_version.version} -> {stage}")
        
        return model_version.version
    
    def load_model(self, model_name: str, version: Optional[int] = None,
                  stage: Optional[str] = None) -> Any:
        """
        Load model from MLflow registry
        
        Args:
            model_name: Model name
            version: Model version (optional)
            stage: Model stage (optional)
            
        Returns:
            Loaded model
        """
        if stage:
            model_uri = f"models:/{model_name}/{stage}"
        elif version:
            model_uri = f"models:/{model_name}/{version}"
        else:
            model_uri = f"models:/{model_name}/latest"
        
        model = mlflow.sklearn.load_model(model_uri)
        
        logger.info(f"Model loaded: {model_uri}")
        
        return model

