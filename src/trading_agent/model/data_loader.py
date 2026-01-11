"""
Data loader for retrieving macro economic data from FRED database
"""

import pandas as pd
import numpy as np
from typing import List, Optional, Dict
from datetime import datetime, timedelta
import logging
import os

from trading_agent.macro.fred.fred_postgres import (
    get_postgres_connection,
    load_time_series_from_postgres
)

logger = logging.getLogger(__name__)


class MacroDataLoader:
    """
    Load macro economic time series data from FRED PostgreSQL database
    """
    
    def __init__(self, dbname: str = "fred", user: Optional[str] = None,
                 host: Optional[str] = None, password: Optional[str] = None,
                 port: Optional[int] = None):
        """
        Initialize data loader
        
        Args:
            dbname: Database name
            user: Database user (optional, can use POSTGRES_USER env var, default: 'tradingAgent')
            host: Database host (optional, can use POSTGRES_HOST env var, default: 'localhost')
            password: Database password (optional, can use POSTGRES_PASSWORD env var)
            port: Database port (optional, can use POSTGRES_PORT env var, default: 55432 for Docker Compose)
        """
        self.dbname = dbname
        # Use env vars with fallbacks
        self.user = user or os.getenv('POSTGRES_USER', 'tradingAgent')
        self.host = host or os.getenv('POSTGRES_HOST', 'localhost')
        self.password = password or os.getenv('POSTGRES_PASSWORD', '')
        # Support port from env var for Docker setups (e.g., 55432)
        if port is None:
            port_str = os.getenv('POSTGRES_PORT', '55432')
            self.port = int(port_str)
        else:
            self.port = port
        
    def get_connection(self):
        """Get PostgreSQL connection"""
        return get_postgres_connection(
            dbname=self.dbname,
            user=self.user,
            host=self.host,
            password=self.password,
            port=self.port
        )
    
    def load_series(self, series_ids: List[str], 
                   start_date: Optional[str] = None,
                   end_date: Optional[str] = None) -> pd.DataFrame:
        """
        Load multiple time series from FRED database
        
        Args:
            series_ids: List of FRED series IDs
            start_date: Start date (YYYY-MM-DD format)
            end_date: End date (YYYY-MM-DD format)
            
        Returns:
            DataFrame with columns: date, and one column per series_id
        """
        conn = self.get_connection()
        
        try:
            all_data = []
            for series_id in series_ids:
                data = load_time_series_from_postgres(
                    conn,
                    series_id=series_id,
                    start_date=start_date,
                    end_date=end_date
                )
                
                if data:
                    df = pd.DataFrame(data)
                    df['date'] = pd.to_datetime(df['date'])
                    df = df.rename(columns={'value': series_id})
                    all_data.append(df[['date', series_id]])
            
            if not all_data:
                logger.warning("No data found for any series")
                return pd.DataFrame()
            
            # Merge all series on date
            result = all_data[0]
            for df in all_data[1:]:
                result = result.merge(df, on='date', how='outer')
            
            # Sort by date
            result = result.sort_values('date').reset_index(drop=True)
            
            return result
            
        finally:
            conn.close()
    
    def load_key_macro_indicators(self, 
                                  start_date: Optional[str] = None,
                                  end_date: Optional[str] = None) -> pd.DataFrame:
        """
        Load key macro economic indicators for cycle analysis
        
        Common indicators:
        - GDP: Gross Domestic Product
        - UNRATE: Unemployment Rate
        - CPIAUCSL: Consumer Price Index
        - FEDFUNDS: Federal Funds Rate
        - INDPRO: Industrial Production Index
        
        Args:
            start_date: Start date (YYYY-MM-DD format)
            end_date: End date (YYYY-MM-DD format)
            
        Returns:
            DataFrame with key macro indicators
        """
        key_series = [
            'GDP',           # Gross Domestic Product
            'UNRATE',        # Unemployment Rate
            'CPIAUCSL',      # Consumer Price Index
            'FEDFUNDS',      # Federal Funds Rate
            'INDPRO',        # Industrial Production Index
            'PAYEMS',        # Total Nonfarm Payrolls
            'UMCSENT',       # University of Michigan Consumer Sentiment
        ]
        
        logger.info(f"Loading key macro indicators: {key_series}")
        
        return self.load_series(key_series, start_date=start_date, end_date=end_date)
    
    def load_gdp_components(self,
                           start_date: Optional[str] = None,
                           end_date: Optional[str] = None) -> pd.DataFrame:
        """
        Load GDP and its components
        
        Args:
            start_date: Start date
            end_date: End date
            
        Returns:
            DataFrame with GDP components
        """
        gdp_series = [
            'GDP',           # Gross Domestic Product
            'GDPC1',         # Real GDP
            'PCECC96',       # Real Personal Consumption Expenditures
            'GPDIC1',        # Real Gross Private Domestic Investment
            'GCEC1',         # Real Government Consumption Expenditures
            'NETEXP',        # Net Exports
        ]
        
        logger.info(f"Loading GDP components: {gdp_series}")
        
        return self.load_series(gdp_series, start_date=start_date, end_date=end_date)
    
    def prepare_features(self, data: pd.DataFrame, 
                        method: str = 'pct_change') -> pd.DataFrame:
        """
        Prepare features for HMM modeling
        
        Args:
            data: DataFrame with time series
            method: Feature engineering method
                - 'pct_change': Percentage change
                - 'diff': First difference
                - 'log_diff': Log difference
                - 'raw': Raw values
                
        Returns:
            DataFrame with prepared features
        """
        features = data.copy()
        
        # Drop date column for feature engineering
        date_col = features.pop('date') if 'date' in features.columns else None
        
        if method == 'pct_change':
            features = features.pct_change().dropna()
        elif method == 'diff':
            features = features.diff().dropna()
        elif method == 'log_diff':
            # Handle negative values by adding offset
            min_val = features.min().min()
            if min_val <= 0:
                features = features + abs(min_val) + 1
            features = np.log(features).diff().dropna()
        elif method == 'raw':
            features = features.dropna()
        else:
            raise ValueError(f"Unknown method: {method}")
        
        # Re-add date column if it exists
        if date_col is not None:
            # Align dates with features (drop first row if needed)
            if method in ['pct_change', 'diff', 'log_diff']:
                date_col = date_col.iloc[1:].reset_index(drop=True)
            features.insert(0, 'date', date_col.values[:len(features)])
        
        return features

