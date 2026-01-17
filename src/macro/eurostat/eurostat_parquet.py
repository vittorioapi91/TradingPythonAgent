"""
Eurostat Parquet Database Management

This module handles all DuckDB and Parquet file operations for Eurostat data storage.
"""

import os
from typing import Dict, Optional, List
from datetime import datetime
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd


def init_eurostat_parquet_tables(parquet_dir: str, conn: Optional[duckdb.DuckDBPyConnection] = None) -> duckdb.DuckDBPyConnection:
    """
    Initialize DuckDB connection and create tables for Eurostat datasets if they don't exist
    
    Args:
        parquet_dir: Directory where parquet files will be stored
        conn: Existing DuckDB connection (optional)
    
    Returns:
        DuckDB connection
    """
    if conn is None:
        conn = duckdb.connect()
    
    # Create directory if it doesn't exist
    os.makedirs(parquet_dir, exist_ok=True)
    
    datasets_parquet = os.path.join(parquet_dir, 'eurostat_datasets.parquet')
    metadata_parquet = os.path.join(parquet_dir, 'eurostat_metadata.parquet')
    
    # Create datasets table schema if file doesn't exist
    if not os.path.exists(datasets_parquet):
        datasets_schema = pa.schema([
            ('dataset_code', pa.string()),
            ('title', pa.string()),
            ('description', pa.string()),
            ('last_update', pa.string()),
            ('frequency', pa.string()),
            ('theme', pa.string()),
            ('keywords', pa.string()),
            ('created_at', pa.string()),
            ('updated_at', pa.string()),
        ])
        empty_df = pa.Table.from_pylist([], schema=datasets_schema)
        pq.write_table(empty_df, datasets_parquet)
    
    # Create metadata table schema if file doesn't exist
    if not os.path.exists(metadata_parquet):
        metadata_schema = pa.schema([
            ('key', pa.string()),
            ('value', pa.string()),
            ('updated_at', pa.string()),
        ])
        initial_metadata = [
            {'key': 'generated_at', 'value': datetime.now().isoformat(), 'updated_at': datetime.now().isoformat()},
            {'key': 'total_datasets', 'value': '0', 'updated_at': datetime.now().isoformat()},
            {'key': 'status', 'value': 'in_progress', 'updated_at': datetime.now().isoformat()},
            {'key': 'source', 'value': 'Eurostat API', 'updated_at': datetime.now().isoformat()},
        ]
        empty_df = pa.Table.from_pylist(initial_metadata, schema=metadata_schema)
        pq.write_table(empty_df, metadata_parquet)
    
    return conn


def get_eurostat_parquet_paths(parquet_file: str) -> Dict[str, str]:
    """
    Get parquet file paths from base path (handles both file and directory inputs)
    
    Args:
        parquet_file: Path to parquet file or directory
    
    Returns:
        Dictionary with 'datasets', 'metadata', 'base_dir' paths
    """
    if os.path.isdir(parquet_file):
        base_dir = parquet_file
    else:
        base_dir = os.path.dirname(parquet_file) if os.path.dirname(parquet_file) else '.'
    
    return {
        'base_dir': base_dir,
        'datasets': os.path.join(base_dir, 'eurostat_datasets.parquet'),
        'metadata': os.path.join(base_dir, 'eurostat_metadata.parquet'),
    }


def add_eurostat_datasets_fast(datasets_file: str, new_datasets: List[Dict]) -> int:
    """
    Add Eurostat datasets to parquet file in batch (fast, non-atomic).
    Reads existing file once, adds all new datasets, writes back.
    
    Args:
        datasets_file: Path to the datasets parquet file
        new_datasets: List of dataset dictionaries to add
    
    Returns:
        Number of datasets successfully added
    """
    if not new_datasets:
        return 0
    
    # Get existing schema if file exists
    existing_schema = None
    if os.path.exists(datasets_file):
        try:
            existing_table = pq.read_table(datasets_file)
            existing_schema = existing_table.schema
            existing_df = existing_table.to_pandas()
            # Get existing dataset codes to avoid duplicates
            existing_codes = set(existing_df['dataset_code'].dropna().unique())
        except Exception:
            existing_schema = None
            existing_df = pd.DataFrame()
            existing_codes = set()
    else:
        existing_df = pd.DataFrame()
        existing_codes = set()
    
    # If no schema exists, use the standard schema
    if existing_schema is None:
        existing_schema = pa.schema([
            ('dataset_code', pa.string()),
            ('title', pa.string()),
            ('description', pa.string()),
            ('last_update', pa.string()),
            ('frequency', pa.string()),
            ('theme', pa.string()),
            ('keywords', pa.string()),
            ('created_at', pa.string()),
            ('updated_at', pa.string()),
        ])
    
    # Filter out datasets that already exist
    new_datasets_filtered = [
        d for d in new_datasets 
        if d.get('dataset_code') and d.get('dataset_code') not in existing_codes
    ]
    
    if not new_datasets_filtered:
        return 0
    
    # Ensure all required fields exist
    for dataset in new_datasets_filtered:
        for field in ['dataset_code', 'title', 'description', 'last_update', 
                     'frequency', 'theme', 'keywords', 'created_at', 'updated_at']:
            if field not in dataset:
                dataset[field] = ''
    
    # Create new datasets table with proper schema
    new_table = pa.Table.from_pylist(new_datasets_filtered, schema=existing_schema)
    new_df = new_table.to_pandas()
    
    # Combine with existing
    if len(existing_df) > 0:
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
    else:
        combined_df = new_df
    
    # Write directly (non-atomic, but fast)
    combined_table = pa.Table.from_pandas(combined_df, schema=existing_schema)
    pq.write_table(combined_table, datasets_file)
    
    return len(new_datasets_filtered)


def load_eurostat_datasets_from_parquet(parquet_file: str) -> List[Dict]:
    """
    Load Eurostat datasets from parquet file
    
    Args:
        parquet_file: Path to parquet directory or file
    
    Returns:
        List of dataset dictionaries
    """
    paths = get_eurostat_parquet_paths(parquet_file)
    
    if not os.path.exists(paths['datasets']):
        return []
    
    try:
        df = pq.read_table(paths['datasets']).to_pandas()
        return df.to_dict('records')
    except Exception as e:
        print(f"Error loading Eurostat datasets from parquet: {e}")
        return []


def update_eurostat_metadata(parquet_file: str, key: str, value: str) -> None:
    """
    Update metadata value in Eurostat parquet file
    
    Args:
        parquet_file: Path to parquet directory or file
        key: Metadata key
        value: Metadata value
    """
    paths = get_eurostat_parquet_paths(parquet_file)
    
    if not os.path.exists(paths['metadata']):
        init_eurostat_parquet_tables(paths['base_dir'])
    
    try:
        df = pq.read_table(paths['metadata']).to_pandas()
        
        if key in df['key'].values:
            df.loc[df['key'] == key, 'value'] = value
            df.loc[df['key'] == key, 'updated_at'] = datetime.now().isoformat()
        else:
            new_row = pd.DataFrame([{
                'key': key,
                'value': value,
                'updated_at': datetime.now().isoformat()
            }])
            df = pd.concat([df, new_row], ignore_index=True)
        
        table = pa.Table.from_pandas(df)
        pq.write_table(table, paths['metadata'])
    except Exception as e:
        print(f"Error updating Eurostat metadata: {e}")
