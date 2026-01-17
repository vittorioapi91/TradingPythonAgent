"""
Training script for HMM macro cycle model

This script orchestrates the complete training pipeline:
1. Load data from FRED
2. Engineer features
3. Train HMM model
4. Evaluate model
5. Log to MLflow
6. Update feature store
7. Export metrics for Prometheus
"""

import argparse
import logging
import os
from datetime import datetime
import pandas as pd
import numpy as np

from src.model.data_loader import MacroDataLoader
from src.model.hmm_model import MacroCycleHMM
from src.mlflow import MLflowTracker
from src.prometheus import ModelMetrics

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description='Train HMM model for macro cycles')
    
    # Data parameters
    parser.add_argument('--series-ids', nargs='+', 
                       default=['GDP', 'UNRATE', 'CPIAUCSL'],
                       help='FRED series IDs to use')
    parser.add_argument('--start-date', type=str, default='2000-01-01',
                       help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, default=None,
                       help='End date (YYYY-MM-DD)')
    parser.add_argument('--feature-method', type=str, default='pct_change',
                       choices=['pct_change', 'diff', 'log_diff', 'raw'],
                       help='Feature engineering method')
    
    # Model parameters
    parser.add_argument('--n-regimes', type=int, default=4,
                       help='Number of HMM regimes')
    parser.add_argument('--covariance-type', type=str, default='full',
                       choices=['full', 'diag', 'spherical', 'tied'],
                       help='Covariance type')
    parser.add_argument('--random-state', type=int, default=42,
                       help='Random seed')
    
    # MLflow parameters
    parser.add_argument('--mlflow-tracking-uri', type=str,
                       default=os.getenv('MLFLOW_TRACKING_URI', 'http://localhost:55000'),
                       help='MLflow tracking URI')
    parser.add_argument('--experiment-name', type=str, default='macro-cycle-hmm',
                       help='MLflow experiment name')
    
    # Database parameters
    parser.add_argument('--dbname', type=str, default='fred',
                       help='PostgreSQL database name')
    parser.add_argument('--dbuser', type=str, default='tradingAgent',
                       help='PostgreSQL user')
    parser.add_argument('--dbhost', type=str, default='localhost',
                       help='PostgreSQL host')
    parser.add_argument('--dbpassword', type=str,
                       default=os.getenv('POSTGRES_PASSWORD', ''),
                       help='PostgreSQL password')
    
    # Prometheus metrics
    parser.add_argument('--prometheus-port', type=int, default=8000,
                       help='Prometheus metrics port')
    
    args = parser.parse_args()
    
    # Set end date to today if not provided
    if args.end_date is None:
        args.end_date = datetime.now().strftime('%Y-%m-%d')
    
    logger.info("Starting HMM model training pipeline")
    logger.info(f"Parameters: {vars(args)}")
    
    # Step 1: Load data
    logger.info("Step 1: Loading data from FRED database")
    loader = MacroDataLoader(
        dbname=args.dbname,
        user=args.dbuser,
        host=args.dbhost,
        password=args.dbpassword
    )
    
    data = loader.load_series(
        args.series_ids,
        start_date=args.start_date,
        end_date=args.end_date
    )
    
    logger.info(f"Loaded {len(data)} samples with {len(args.series_ids)} series")
    
    # Step 2: Engineer features
    logger.info("Step 2: Engineering features")
    features = loader.prepare_features(data, method=args.feature_method)
    
    # Prepare data for training (exclude date column)
    feature_cols = [col for col in features.columns if col != 'date']
    X = features[feature_cols].values
    
    logger.info(f"Features shape: {X.shape}")
    
    # Step 3: Train model
    logger.info("Step 3: Training HMM model")
    model = MacroCycleHMM(
        n_regimes=args.n_regimes,
        n_features=len(feature_cols),
        covariance_type=args.covariance_type,
        random_state=args.random_state
    )
    
    model.fit(X)
    
    # Step 4: Evaluate model
    logger.info("Step 4: Evaluating model")
    metrics = model.get_model_metrics(X)
    states = model.predict_regimes(X)
    
    # Get regime distribution
    unique, counts = np.unique(states, return_counts=True)
    regime_dist = {int(r): int(c) for r, c in zip(unique, counts)}
    
    logger.info(f"Model metrics: {metrics}")
    logger.info(f"Regime distribution: {regime_dist}")
    
    # Step 5: Log to MLflow
    logger.info("Step 5: Logging to MLflow")
    mlflow_tracker = MLflowTracker(
        tracking_uri=args.mlflow_tracking_uri,
        experiment_name=args.experiment_name
    )
    
    params = {
        'n_regimes': args.n_regimes,
        'n_features': len(feature_cols),
        'covariance_type': args.covariance_type,
        'random_state': args.random_state,
        'feature_method': args.feature_method,
        'series_ids': ','.join(args.series_ids),
        'start_date': args.start_date,
        'end_date': args.end_date,
        'n_samples': len(X)
    }
    
    mlflow_metrics = {
        'log_likelihood': metrics['log_likelihood'],
        'aic': metrics['aic'],
        'bic': metrics['bic']
    }
    
    tags = {
        'model_type': 'hmm',
        'domain': 'macro_economics',
        'training_date': datetime.now().isoformat()
    }
    
    run_id = mlflow_tracker.log_hmm_experiment(
        model=model,
        params=params,
        metrics=mlflow_metrics,
        tags=tags
    )
    
    logger.info(f"MLflow run ID: {run_id}")
    
    # Step 6: Export Prometheus metrics
    logger.info("Step 6: Setting up Prometheus metrics")
    prom_metrics = ModelMetrics(port=args.prometheus_port)
    
    prom_metrics.record_model_metrics(
        model_name='macro-cycle-hmm',
        run_id=run_id,
        log_likelihood=metrics['log_likelihood'],
        aic=metrics['aic'],
        bic=metrics['bic']
    )
    
    prom_metrics.record_regime_distribution(
        model_name='macro-cycle-hmm',
        regime_counts=regime_dist
    )
    
    prom_metrics.record_feature_count(
        model_name='macro-cycle-hmm',
        n_features=len(feature_cols)
    )
    
    logger.info("Training pipeline completed successfully")
    logger.info(f"Prometheus metrics available on port {args.prometheus_port}")
    logger.info(f"MLflow tracking: {args.mlflow_tracking_uri}")


if __name__ == '__main__':
    main()

