"""
Hidden Markov Model (HMM) for Macro Economic Cycles using Pyro

This module implements a HMM with k regimes to model macro economic cycles
using time series data from FRED database. Uses Pyro for probabilistic modeling.
"""

import numpy as np
import pandas as pd
import torch
import torch.distributions as dist
from pyro.distributions import Normal, Categorical
from pyro.infer import SVI, Trace_ELBO
from pyro.optim import Adam
from pyro import poutine
import pyro
import pyro.poutine as poutine
from typing import Dict, List, Optional, Tuple
from sklearn.preprocessing import StandardScaler
import logging

logger = logging.getLogger(__name__)


class MacroCycleHMM:
    """
    Hidden Markov Model for macro economic cycle analysis using Pyro
    
    Models economic cycles as k hidden states (regimes) such as:
    - Expansion
    - Peak
    - Contraction
    - Trough
    """
    
    def __init__(self, n_regimes: int = 4, n_features: int = 1, 
                 random_state: int = 42, learning_rate: float = 0.01,
                 num_steps: int = 1000):
        """
        Initialize HMM model
        
        Args:
            n_regimes: Number of hidden states/regimes (default: 4)
            n_features: Number of features/dimensions (default: 1 for univariate)
            random_state: Random seed for reproducibility
            learning_rate: Learning rate for optimization
            num_steps: Number of optimization steps
        """
        self.n_regimes = n_regimes
        self.n_features = n_features
        self.random_state = random_state
        self.learning_rate = learning_rate
        self.num_steps = num_steps
        
        # Set random seed
        torch.manual_seed(random_state)
        np.random.seed(random_state)
        pyro.set_rng_seed(random_state)
        
        # Feature scaler
        self.scaler = StandardScaler()
        
        # Model parameters (will be learned)
        self.transition_probs = None
        self.initial_probs = None
        self.means = None
        self.covariances = None
        
        # Model metadata
        self.is_fitted = False
        self.regime_names = self._generate_regime_names(n_regimes)
        self.loss_history = []
        
    def _generate_regime_names(self, n_regimes: int) -> List[str]:
        """Generate regime names based on number of regimes"""
        if n_regimes == 2:
            return ['Expansion', 'Contraction']
        elif n_regimes == 3:
            return ['Expansion', 'Peak', 'Contraction']
        elif n_regimes == 4:
            return ['Expansion', 'Peak', 'Contraction', 'Trough']
        else:
            return [f'Regime_{i+1}' for i in range(n_regimes)]
    
    def model(self, data: torch.Tensor):
        """
        Pyro model definition for HMM
        
        Args:
            data: Tensor of shape (T, n_features) where T is sequence length
        """
        T, n_features = data.shape
        n_regimes = self.n_regimes
        
        # Initial state distribution
        initial_probs = pyro.param(
            "initial_probs",
            torch.ones(n_regimes) / n_regimes,
            constraint=dist.constraints.simplex
        )
        
        # Transition matrix
        transition_probs = pyro.param(
            "transition_probs",
            torch.ones(n_regimes, n_regimes) / n_regimes,
            constraint=dist.constraints.simplex
        )
        
        # Emission parameters for each regime
        means = pyro.param(
            "means",
            torch.randn(n_regimes, n_features) * 0.1
        )
        
        # Covariance matrices (diagonal for simplicity)
        scale = pyro.param(
            "scale",
            torch.ones(n_regimes, n_features),
            constraint=dist.constraints.positive
        )
        
        # Sample initial state
        z_prev = pyro.sample("z_0", Categorical(initial_probs))
        
        # Sample observations and transitions
        for t in range(T):
            # Sample observation given current state
            pyro.sample(
                f"x_{t}",
                dist.Normal(means[z_prev], scale[z_prev]).to_event(1),
                obs=data[t]
            )
            
            # Sample next state (if not last time step)
            if t < T - 1:
                z_prev = pyro.sample(
                    f"z_{t+1}",
                    Categorical(transition_probs[z_prev])
                )
    
    def guide(self, data: torch.Tensor):
        """
        Pyro guide (variational posterior) for HMM
        
        Args:
            data: Tensor of shape (T, n_features)
        """
        # Guide uses the same structure as model
        # Parameters are learned through optimization
        T, n_features = data.shape
        n_regimes = self.n_regimes
        
        # Initial state distribution
        initial_probs = pyro.param(
            "initial_probs",
            torch.ones(n_regimes) / n_regimes,
            constraint=dist.constraints.simplex
        )
        
        # Transition matrix
        transition_probs = pyro.param(
            "transition_probs",
            torch.ones(n_regimes, n_regimes) / n_regimes,
            constraint=dist.constraints.simplex
        )
        
        # Emission parameters
        means = pyro.param(
            "means",
            torch.randn(n_regimes, n_features) * 0.1
        )
        
        scale = pyro.param(
            "scale",
            torch.ones(n_regimes, n_features),
            constraint=dist.constraints.positive
        )
        
        # Sample states (this is a simplified guide)
        z_prev = pyro.sample("z_0", Categorical(initial_probs))
        
        for t in range(T - 1):
            z_prev = pyro.sample(
                f"z_{t+1}",
                Categorical(transition_probs[z_prev])
            )
    
    def prepare_data(self, data: pd.DataFrame, 
                    series_columns: List[str],
                    date_column: str = 'date') -> np.ndarray:
        """
        Prepare time series data for HMM training
        
        Args:
            data: DataFrame with time series data
            series_columns: List of column names to use as features
            date_column: Name of date column (for sorting)
            
        Returns:
            Numpy array of shape (n_samples, n_features)
        """
        # Sort by date
        if date_column in data.columns:
            data = data.sort_values(date_column)
        
        # Extract features
        features = data[series_columns].values
        
        # Handle missing values
        features_df = pd.DataFrame(features)
        features_df = features_df.ffill().bfill()
        features = features_df.values
        
        # Scale features
        features_scaled = self.scaler.fit_transform(features)
        
        return features_scaled
    
    def fit(self, X: np.ndarray) -> 'MacroCycleHMM':
        """
        Fit HMM model to data using Pyro SVI
        
        Args:
            X: Training data of shape (n_samples, n_features)
            
        Returns:
            Self for method chaining
        """
        logger.info(f"Fitting HMM with {self.n_regimes} regimes on {X.shape[0]} samples using Pyro")
        
        # Convert to torch tensor
        X_tensor = torch.tensor(X, dtype=torch.float32)
        
        # Setup SVI
        svi = SVI(
            self.model,
            self.guide,
            Adam({"lr": self.learning_rate}),
            loss=Trace_ELBO()
        )
        
        # Training loop
        logger.info("Starting optimization...")
        for step in range(self.num_steps):
            loss = svi.step(X_tensor)
            self.loss_history.append(loss)
            
            if (step + 1) % 100 == 0:
                logger.info(f"Step {step + 1}/{self.num_steps}, Loss: {loss:.4f}")
        
        # Extract learned parameters
        self.initial_probs = pyro.param("initial_probs").detach().numpy()
        self.transition_probs = pyro.param("transition_probs").detach().numpy()
        self.means = pyro.param("means").detach().numpy()
        self.covariances = pyro.param("scale").detach().numpy() ** 2  # Convert scale to variance
        
        self.is_fitted = True
        
        logger.info(f"Model fitted successfully. Final loss: {self.loss_history[-1]:.4f}")
        
        return self
    
    def predict_regimes(self, X: np.ndarray) -> np.ndarray:
        """
        Predict hidden states (regimes) for given data using Viterbi algorithm
        
        Args:
            X: Data of shape (n_samples, n_features)
            
        Returns:
            Array of predicted regime indices
        """
        if not self.is_fitted:
            raise ValueError("Model must be fitted before prediction")
        
        # Scale data
        X_scaled = self.scaler.transform(X)
        X_tensor = torch.tensor(X_scaled, dtype=torch.float32)
        
        # Use Viterbi algorithm for most likely sequence
        states = self._viterbi(X_tensor)
        
        return states.numpy()
    
    def _viterbi(self, observations: torch.Tensor) -> torch.Tensor:
        """
        Viterbi algorithm for finding most likely state sequence
        
        Args:
            observations: Tensor of shape (T, n_features)
            
        Returns:
            Tensor of state indices
        """
        T, n_features = observations.shape
        n_regimes = self.n_regimes
        
        # Convert parameters to tensors
        initial_probs = torch.tensor(self.initial_probs, dtype=torch.float32)
        transition_probs = torch.tensor(self.transition_probs, dtype=torch.float32)
        means = torch.tensor(self.means, dtype=torch.float32)
        covariances = torch.tensor(self.covariances, dtype=torch.float32)
        
        # Initialize Viterbi table
        viterbi = torch.zeros(T, n_regimes)
        backpointer = torch.zeros(T, n_regimes, dtype=torch.long)
        
        # Initialization
        for s in range(n_regimes):
            # Log probability of observation given state
            obs_prob = dist.Normal(means[s], torch.sqrt(covariances[s])).log_prob(observations[0]).sum()
            viterbi[0, s] = torch.log(initial_probs[s]) + obs_prob
        
        # Recursion
        for t in range(1, T):
            for s in range(n_regimes):
                # Find best previous state
                probs = viterbi[t-1] + torch.log(transition_probs[:, s])
                best_prev = torch.argmax(probs)
                viterbi[t, s] = probs[best_prev]
                backpointer[t, s] = best_prev
                
                # Add observation probability
                obs_prob = dist.Normal(means[s], torch.sqrt(covariances[s])).log_prob(observations[t]).sum()
                viterbi[t, s] += obs_prob
        
        # Backtracking
        states = torch.zeros(T, dtype=torch.long)
        states[T-1] = torch.argmax(viterbi[T-1])
        
        for t in range(T-2, -1, -1):
            states[t] = backpointer[t+1, states[t+1]]
        
        return states
    
    def predict_proba(self, X: np.ndarray) -> np.ndarray:
        """
        Predict probability distribution over regimes using forward algorithm
        
        Args:
            X: Data of shape (n_samples, n_features)
            
        Returns:
            Array of shape (n_samples, n_regimes) with probabilities
        """
        if not self.is_fitted:
            raise ValueError("Model must be fitted before prediction")
        
        # Scale data
        X_scaled = self.scaler.transform(X)
        X_tensor = torch.tensor(X_scaled, dtype=torch.float32)
        
        # Use forward algorithm
        T = len(X_tensor)
        n_regimes = self.n_regimes
        
        # Convert parameters to tensors
        initial_probs = torch.tensor(self.initial_probs, dtype=torch.float32)
        transition_probs = torch.tensor(self.transition_probs, dtype=torch.float32)
        means = torch.tensor(self.means, dtype=torch.float32)
        covariances = torch.tensor(self.covariances, dtype=torch.float32)
        
        # Forward algorithm
        alpha = torch.zeros(T, n_regimes)
        
        # Initialization
        for s in range(n_regimes):
            obs_prob = dist.Normal(means[s], torch.sqrt(covariances[s])).log_prob(X_tensor[0]).sum()
            alpha[0, s] = torch.log(initial_probs[s]) + obs_prob
        
        # Recursion
        for t in range(1, T):
            for s in range(n_regimes):
                # Sum over previous states
                log_sum = torch.logsumexp(alpha[t-1] + torch.log(transition_probs[:, s]), dim=0)
                obs_prob = dist.Normal(means[s], torch.sqrt(covariances[s])).log_prob(X_tensor[t]).sum()
                alpha[t, s] = log_sum + obs_prob
        
        # Normalize to get probabilities
        proba = torch.exp(alpha - torch.logsumexp(alpha, dim=1, keepdim=True))
        
        return proba.detach().numpy()
    
    def get_transition_matrix(self) -> np.ndarray:
        """
        Get state transition matrix
        
        Returns:
            Transition matrix of shape (n_regimes, n_regimes)
        """
        if not self.is_fitted:
            raise ValueError("Model must be fitted")
        
        return self.transition_probs
    
    def get_emission_parameters(self) -> Dict:
        """
        Get emission (observation) parameters for each regime
        
        Returns:
            Dictionary with means and covariances for each regime
        """
        if not self.is_fitted:
            raise ValueError("Model must be fitted")
        
        return {
            'means': self.means,
            'covariances': self.covariances,
            'regime_names': self.regime_names
        }
    
    def get_stationary_distribution(self) -> np.ndarray:
        """
        Get stationary distribution of states
        
        Returns:
            Stationary probability distribution over regimes
        """
        if not self.is_fitted:
            raise ValueError("Model must be fitted")
        
        # Compute stationary distribution (left eigenvector of transition matrix)
        eigenvals, eigenvecs = np.linalg.eig(self.transition_probs.T)
        stationary_idx = np.argmax(np.real(eigenvals))
        stationary_dist = np.real(eigenvecs[:, stationary_idx])
        stationary_dist = stationary_dist / stationary_dist.sum()
        
        return stationary_dist
    
    def analyze_regime_characteristics(self, X: np.ndarray, 
                                      dates: Optional[pd.Series] = None) -> pd.DataFrame:
        """
        Analyze characteristics of each regime
        
        Args:
            X: Data of shape (n_samples, n_features)
            dates: Optional date series for time-based analysis
            
        Returns:
            DataFrame with regime statistics
        """
        if not self.is_fitted:
            raise ValueError("Model must be fitted")
        
        # Predict regimes
        states = self.predict_regimes(X)
        
        # Create results DataFrame
        results = []
        for regime_idx in range(self.n_regimes):
            regime_mask = states == regime_idx
            regime_data = X[regime_mask]
            
            if len(regime_data) > 0:
                results.append({
                    'regime': self.regime_names[regime_idx],
                    'regime_idx': regime_idx,
                    'n_observations': len(regime_data),
                    'mean': regime_data.mean(axis=0).tolist(),
                    'std': regime_data.std(axis=0).tolist(),
                    'min': regime_data.min(axis=0).tolist(),
                    'max': regime_data.max(axis=0).tolist(),
                })
        
        return pd.DataFrame(results)
    
    def get_model_metrics(self, X: np.ndarray) -> Dict:
        """
        Get model performance metrics
        
        Args:
            X: Data to evaluate on
            
        Returns:
            Dictionary with model metrics
        """
        if not self.is_fitted:
            raise ValueError("Model must be fitted")
        
        X_scaled = self.scaler.transform(X)
        X_tensor = torch.tensor(X_scaled, dtype=torch.float32)
        
        # Compute log likelihood using forward algorithm
        T = len(X_tensor)
        n_regimes = self.n_regimes
        
        initial_probs = torch.tensor(self.initial_probs, dtype=torch.float32)
        transition_probs = torch.tensor(self.transition_probs, dtype=torch.float32)
        means = torch.tensor(self.means, dtype=torch.float32)
        covariances = torch.tensor(self.covariances, dtype=torch.float32)
        
        # Forward algorithm for log likelihood
        alpha = torch.zeros(T, n_regimes)
        
        for s in range(n_regimes):
            obs_prob = dist.Normal(means[s], torch.sqrt(covariances[s])).log_prob(X_tensor[0]).sum()
            alpha[0, s] = torch.log(initial_probs[s]) + obs_prob
        
        for t in range(1, T):
            for s in range(n_regimes):
                log_sum = torch.logsumexp(alpha[t-1] + torch.log(transition_probs[:, s]), dim=0)
                obs_prob = dist.Normal(means[s], torch.sqrt(covariances[s])).log_prob(X_tensor[t]).sum()
                alpha[t, s] = log_sum + obs_prob
        
        log_likelihood = torch.logsumexp(alpha[T-1], dim=0).item()
        
        # Compute AIC and BIC
        n_params = self._count_parameters()
        aic = -2 * log_likelihood + 2 * n_params
        bic = -2 * log_likelihood + np.log(len(X)) * n_params
        
        return {
            'log_likelihood': log_likelihood,
            'aic': aic,
            'bic': bic,
            'n_regimes': self.n_regimes,
            'n_samples': len(X),
            'n_features': self.n_features
        }
    
    def _count_parameters(self) -> int:
        """Count number of parameters in the model"""
        # Initial distribution: n_regimes - 1 (sums to 1)
        n_init = self.n_regimes - 1
        
        # Transition matrix: n_regimes * (n_regimes - 1) (rows sum to 1)
        n_trans = self.n_regimes * (self.n_regimes - 1)
        
        # Emission parameters: means and variances
        n_emission = self.n_regimes * self.n_features * 2  # mean and variance per feature
        
        return n_init + n_trans + n_emission
