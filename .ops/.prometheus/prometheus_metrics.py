"""
Prometheus metrics collection for HMM model monitoring
"""

from prometheus_client import Counter, Histogram, Gauge, start_http_server
from typing import Dict, Optional
import time
import logging

logger = logging.getLogger(__name__)


class ModelMetrics:
    """
    Prometheus metrics for HMM model monitoring
    """
    
    def __init__(self, port: int = 8000):
        """
        Initialize Prometheus metrics
        
        Args:
            port: Port for metrics HTTP server
        """
        # Prediction metrics
        self.predictions_total = Counter(
            'hmm_predictions_total',
            'Total number of predictions',
            ['model_name', 'regime']
        )
        
        self.prediction_latency = Histogram(
            'hmm_prediction_latency_seconds',
            'Prediction latency in seconds',
            ['model_name']
        )
        
        # Model performance metrics
        self.model_log_likelihood = Gauge(
            'hmm_model_log_likelihood',
            'Model log likelihood',
            ['model_name', 'run_id']
        )
        
        self.model_aic = Gauge(
            'hmm_model_aic',
            'Model AIC',
            ['model_name', 'run_id']
        )
        
        self.model_bic = Gauge(
            'hmm_model_bic',
            'Model BIC',
            ['model_name', 'run_id']
        )
        
        # Regime distribution metrics
        self.regime_distribution = Gauge(
            'hmm_regime_distribution',
            'Distribution of predictions across regimes',
            ['model_name', 'regime']
        )
        
        # Feature metrics
        self.feature_count = Gauge(
            'hmm_feature_count',
            'Number of features used',
            ['model_name']
        )
        
        # Error metrics
        self.prediction_errors = Counter(
            'hmm_prediction_errors_total',
            'Total prediction errors',
            ['model_name', 'error_type']
        )
        
        # Start metrics server
        start_http_server(port)
        logger.info(f"Prometheus metrics server started on port {port}")
    
    def record_prediction(self, model_name: str, regime: int, latency: float):
        """
        Record a prediction
        
        Args:
            model_name: Name of the model
            regime: Predicted regime
            latency: Prediction latency in seconds
        """
        self.predictions_total.labels(
            model_name=model_name,
            regime=str(regime)
        ).inc()
        
        self.prediction_latency.labels(model_name=model_name).observe(latency)
    
    def record_model_metrics(self, model_name: str, run_id: str,
                            log_likelihood: float, aic: float, bic: float):
        """
        Record model performance metrics
        
        Args:
            model_name: Name of the model
            run_id: MLflow run ID
            log_likelihood: Log likelihood
            aic: AIC score
            bic: BIC score
        """
        self.model_log_likelihood.labels(
            model_name=model_name,
            run_id=run_id
        ).set(log_likelihood)
        
        self.model_aic.labels(
            model_name=model_name,
            run_id=run_id
        ).set(aic)
        
        self.model_bic.labels(
            model_name=model_name,
            run_id=run_id
        ).set(bic)
    
    def record_regime_distribution(self, model_name: str,
                                  regime_counts: Dict[int, int]):
        """
        Record regime distribution
        
        Args:
            model_name: Name of the model
            regime_counts: Dictionary of regime -> count
        """
        for regime, count in regime_counts.items():
            self.regime_distribution.labels(
                model_name=model_name,
                regime=str(regime)
            ).set(count)
    
    def record_feature_count(self, model_name: str, n_features: int):
        """
        Record number of features
        
        Args:
            model_name: Name of the model
            n_features: Number of features
        """
        self.feature_count.labels(model_name=model_name).set(n_features)
    
    def record_error(self, model_name: str, error_type: str):
        """
        Record a prediction error
        
        Args:
            model_name: Name of the model
            error_type: Type of error
        """
        self.prediction_errors.labels(
            model_name=model_name,
            error_type=error_type
        ).inc()


class PredictionTimer:
    """
    Context manager for timing predictions
    """
    
    def __init__(self, metrics: ModelMetrics, model_name: str):
        """
        Initialize timer
        
        Args:
            metrics: ModelMetrics instance
            model_name: Name of the model
        """
        self.metrics = metrics
        self.model_name = model_name
        self.start_time = None
    
    def __enter__(self):
        self.start_time = time.time()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        latency = time.time() - self.start_time
        if exc_type is None:
            # Only record latency if no error occurred
            self.metrics.prediction_latency.labels(
                model_name=self.model_name
            ).observe(latency)

