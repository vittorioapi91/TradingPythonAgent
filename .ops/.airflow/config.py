"""
Configuration for Trading Agent Airflow workflows
"""

import os
from typing import Dict, Optional


class MacroAirflowConfig:
    """Configuration class for Macro Airflow workflows"""
    
    # Database connection defaults
    DEFAULT_USER = "tradingAgent"
    DEFAULT_HOST = "localhost"
    DEFAULT_PORT = 5432
    
    # Module database names
    MODULE_DATABASES = {
        'fred': 'fred',
        'bis': 'bis',
        'bls': 'bls',
        'eurostat': 'eurostat',
        'imf': 'imf',
    }
    
    # SQL file paths by module (relative to module directory)
    SQL_FILES = {
        'fred': {
            'categories_tree': 'categories_tree.sql',
            'category_analysis': 'category_analysis.sql',
            'rates_categories_query': 'rates_categories_query.sql',
        },
    }
    
    # View names by module
    VIEWS = {
        'fred': {
            'category_paths': 'fred_category_paths',
            'category_analysis': 'category_analysis',
        },
    }
    
    @classmethod
    def get_db_config(cls, 
                     module_name: str,
                     dbname: Optional[str] = None,
                     user: Optional[str] = None,
                     host: Optional[str] = None,
                     password: Optional[str] = None,
                     port: Optional[int] = None) -> Dict[str, str]:
        """
        Get database configuration for a module
        
        Args:
            module_name: Module name (fred, bis, bls, eurostat, imf)
            dbname: Database name (defaults to module database)
            user: Database user (defaults to DEFAULT_USER)
            host: Database host (defaults to DEFAULT_HOST)
            password: Database password (from env or None)
            port: Database port (defaults to DEFAULT_PORT)
        
        Returns:
            Dictionary with database configuration
        """
        if module_name not in cls.MODULE_DATABASES:
            raise ValueError(f"Unknown module: {module_name}")
        
        return {
            'dbname': dbname or cls.MODULE_DATABASES[module_name],
            'user': user or cls.DEFAULT_USER,
            'host': host or cls.DEFAULT_HOST,
            'password': password or os.getenv('POSTGRES_PASSWORD', ''),
            'port': port or cls.DEFAULT_PORT,
        }
    
    @classmethod
    def get_sql_file_path(cls, module_name: str, file_key: str) -> str:
        """
        Get full path to SQL file
        
        Args:
            module_name: Module name
            file_key: Key from SQL_FILES dictionary
        
        Returns:
            Full path to SQL file
        """
        if module_name not in cls.SQL_FILES:
            raise ValueError(f"No SQL files defined for module: {module_name}")
        
        if file_key not in cls.SQL_FILES[module_name]:
            raise ValueError(f"Unknown SQL file key: {file_key} for module {module_name}")
        
        # Get module directory: src/trading_agent/macro/{module_name}/
        current_dir = os.path.dirname(os.path.abspath(__file__))
        trading_agent_dir = os.path.dirname(current_dir)
        macro_dir = os.path.join(trading_agent_dir, 'macro')
        module_dir = os.path.join(macro_dir, module_name)
        
        return os.path.join(module_dir, cls.SQL_FILES[module_name][file_key])
    
    @classmethod
    def get_view_name(cls, module_name: str, view_key: str) -> str:
        """
        Get view name by key
        
        Args:
            module_name: Module name
            view_key: Key from VIEWS dictionary
        
        Returns:
            View name
        """
        if module_name not in cls.VIEWS:
            raise ValueError(f"No views defined for module: {module_name}")
        
        if view_key not in cls.VIEWS[module_name]:
            raise ValueError(f"Unknown view key: {view_key} for module {module_name}")
        
        return cls.VIEWS[module_name][view_key]

