"""
KServe deployment configuration for HMM model serving
"""

from kubernetes import client
from kserve import (
    KServeClient,
    V1beta1InferenceService,
    V1beta1InferenceServiceSpec,
    V1beta1PredictorSpec,
    V1beta1SKLearnSpec,
)
from typing import Dict, Optional
import logging

logger = logging.getLogger(__name__)


class KServeDeployment:
    """
    KServe deployment manager for HMM models
    """
    
    def __init__(self, namespace: str = 'default'):
        """
        Initialize KServe deployment manager
        
        Args:
            namespace: Kubernetes namespace
        """
        self.namespace = namespace
        self.kserve_client = KServeClient()
    
    def create_inference_service(self,
                                service_name: str,
                                model_uri: str,
                                model_format: str = 'sklearn',
                                min_replicas: int = 1,
                                max_replicas: int = 3,
                                resources: Optional[Dict] = None) -> str:
        """
        Create KServe inference service
        
        Args:
            service_name: Name of the inference service
            model_uri: URI to the model (MLflow, S3, etc.)
            model_format: Model format (sklearn, pytorch, tensorflow, etc.)
            min_replicas: Minimum number of replicas
            max_replicas: Maximum number of replicas
            resources: Resource requirements
            
        Returns:
            Service name
        """
        # Default resources
        if resources is None:
            resources = {
                'requests': {'cpu': '100m', 'memory': '512Mi'},
                'limits': {'cpu': '1000m', 'memory': '1Gi'}
            }
        
        # Create predictor spec based on model format
        if model_format == 'sklearn':
            predictor = V1beta1PredictorSpec(
                sklearn=V1beta1SKLearnSpec(
                    storage_uri=model_uri,
                    resources=resources
                ),
                min_replicas=min_replicas,
                max_replicas=max_replicas
            )
        else:
            raise ValueError(f"Unsupported model format: {model_format}")
        
        # Create inference service
        inference_service = V1beta1InferenceService(
            api_version='serving.kserve.io/v1beta1',
            kind='InferenceService',
            metadata=client.V1ObjectMeta(
                name=service_name,
                namespace=self.namespace,
                annotations={
                    'serving.kserve.io/deploymentMode': 'Serverless'
                }
            ),
            spec=V1beta1InferenceServiceSpec(
                predictor=predictor
            )
        )
        
        # Deploy
        self.kserve_client.create(inference_service)
        
        logger.info(f"Inference service created: {service_name}")
        
        return service_name
    
    def update_inference_service(self,
                                service_name: str,
                                model_uri: str,
                                traffic_percent: int = 100) -> str:
        """
        Update existing inference service with new model
        
        Args:
            service_name: Name of the inference service
            model_uri: New model URI
            traffic_percent: Percentage of traffic to route to new model
            
        Returns:
            Service name
        """
        # Get existing service
        service = self.kserve_client.get(service_name, namespace=self.namespace)
        
        # Update model URI
        if hasattr(service.spec.predictor, 'sklearn'):
            service.spec.predictor.sklearn.storage_uri = model_uri
        
        # Update service
        self.kserve_client.patch(service_name, service, namespace=self.namespace)
        
        logger.info(f"Inference service updated: {service_name}")
        
        return service_name
    
    def delete_inference_service(self, service_name: str) -> bool:
        """
        Delete inference service
        
        Args:
            service_name: Name of the inference service
            
        Returns:
            True if successful
        """
        self.kserve_client.delete(service_name, namespace=self.namespace)
        logger.info(f"Inference service deleted: {service_name}")
        return True
    
    def get_service_status(self, service_name: str) -> Dict:
        """
        Get inference service status
        
        Args:
            service_name: Name of the inference service
            
        Returns:
            Service status dictionary
        """
        service = self.kserve_client.get(service_name, namespace=self.namespace)
        
        return {
            'name': service_name,
            'namespace': self.namespace,
            'status': service.status,
            'url': service.status.url if hasattr(service.status, 'url') else None
        }

