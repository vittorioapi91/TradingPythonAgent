"""
BLS Parquet Database Management

This module handles all DuckDB and Parquet file operations for BLS data storage.
"""

import os
from typing import Dict, Optional, List
from datetime import datetime
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd


def init_bls_parquet_tables(parquet_dir: str, conn: Optional[duckdb.DuckDBPyConnection] = None) -> duckdb.DuckDBPyConnection:
    """
    Initialize DuckDB connection and create tables for BLS series if they don't exist
    
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
    
    series_parquet = os.path.join(parquet_dir, 'bls_series.parquet')
    metadata_parquet = os.path.join(parquet_dir, 'bls_metadata.parquet')
    
    # Create series table schema if file doesn't exist
    if not os.path.exists(series_parquet):
        series_schema = pa.schema([
            ('series_id', pa.string()),
            ('survey_abbreviation', pa.string()),
            ('survey_name', pa.string()),
            ('seasonal', pa.string()),
            ('area_code', pa.string()),
            ('area_name', pa.string()),
            ('item_code', pa.string()),
            ('item_name', pa.string()),
            ('created_at', pa.string()),
            ('updated_at', pa.string()),
        ])
        empty_df = pa.Table.from_pylist([], schema=series_schema)
        pq.write_table(empty_df, series_parquet)
    
    # Create metadata table schema if file doesn't exist
    if not os.path.exists(metadata_parquet):
        metadata_schema = pa.schema([
            ('key', pa.string()),
            ('value', pa.string()),
            ('updated_at', pa.string()),
        ])
        initial_metadata = [
            {'key': 'generated_at', 'value': datetime.now().isoformat(), 'updated_at': datetime.now().isoformat()},
            {'key': 'total_series', 'value': '0', 'updated_at': datetime.now().isoformat()},
            {'key': 'status', 'value': 'in_progress', 'updated_at': datetime.now().isoformat()},
            {'key': 'source', 'value': 'BLS API', 'updated_at': datetime.now().isoformat()},
        ]
        empty_df = pa.Table.from_pylist(initial_metadata, schema=metadata_schema)
        pq.write_table(empty_df, metadata_parquet)
    
    return conn


def get_bls_parquet_paths(parquet_file: str) -> Dict[str, str]:
    """
    Get parquet file paths from base path (handles both file and directory inputs)
    
    Args:
        parquet_file: Path to parquet file or directory
    
    Returns:
        Dictionary with 'series', 'metadata', 'base_dir' paths
    """
    if os.path.isdir(parquet_file):
        base_dir = parquet_file
    else:
        base_dir = os.path.dirname(parquet_file) if os.path.dirname(parquet_file) else '.'
    
    return {
        'base_dir': base_dir,
        'series': os.path.join(base_dir, 'bls_series.parquet'),
        'metadata': os.path.join(base_dir, 'bls_metadata.parquet'),
    }


def add_bls_series_fast(series_file: str, new_series: List[Dict]) -> int:
    """
    Add BLS series to parquet file in batch (fast, non-atomic).
    Reads existing file once, adds all new series, writes back.
    
    Args:
        series_file: Path to the series parquet file
        new_series: List of series dictionaries to add
    
    Returns:
        Number of series successfully added
    """
    if not new_series:
        return 0
    
    # Get existing schema if file exists
    existing_schema = None
    if os.path.exists(series_file):
        try:
            existing_table = pq.read_table(series_file)
            existing_schema = existing_table.schema
            existing_df = existing_table.to_pandas()
            # Get existing series IDs to avoid duplicates
            existing_ids = set(existing_df['series_id'].dropna().unique())
        except Exception:
            existing_schema = None
            existing_df = pd.DataFrame()
            existing_ids = set()
    else:
        existing_df = pd.DataFrame()
        existing_ids = set()
    
    # If no schema exists, use the standard schema
    if existing_schema is None:
        existing_schema = pa.schema([
            ('series_id', pa.string()),
            ('survey_abbreviation', pa.string()),
            ('survey_name', pa.string()),
            ('seasonal', pa.string()),
            ('area_code', pa.string()),
            ('area_name', pa.string()),
            ('item_code', pa.string()),
            ('item_name', pa.string()),
            ('created_at', pa.string()),
            ('updated_at', pa.string()),
        ])
    
    # Filter out series that already exist
    new_series_filtered = [
        s for s in new_series 
        if s.get('series_id') and s.get('series_id') not in existing_ids
    ]
    
    if not new_series_filtered:
        return 0
    
    # Ensure all required fields exist
    for series in new_series_filtered:
        for field in ['series_id', 'survey_abbreviation', 'survey_name', 'seasonal', 
                     'area_code', 'area_name', 'item_code', 'item_name', 'created_at', 'updated_at']:
            if field not in series:
                series[field] = ''
    
    # Create new series table with proper schema
    new_table = pa.Table.from_pylist(new_series_filtered, schema=existing_schema)
    new_df = new_table.to_pandas()
    
    # Combine with existing
    if len(existing_df) > 0:
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
    else:
        combined_df = new_df
    
    # Write directly (non-atomic, but fast)
    combined_table = pa.Table.from_pandas(combined_df, schema=existing_schema)
    pq.write_table(combined_table, series_file)
    
    return len(new_series_filtered)


def load_bls_series_from_parquet(parquet_file: str) -> List[Dict]:
    """
    Load BLS series from parquet file
    
    Args:
        parquet_file: Path to parquet directory or file
    
    Returns:
        List of series dictionaries
    """
    paths = get_bls_parquet_paths(parquet_file)
    
    if not os.path.exists(paths['series']):
        return []
    
    try:
        df = pq.read_table(paths['series']).to_pandas()
        return df.to_dict('records')
    except Exception as e:
        print(f"Error loading BLS series from parquet: {e}")
        return []


def update_bls_metadata(parquet_file: str, key: str, value: str) -> None:
    """
    Update metadata value in BLS parquet file
    
    Args:
        parquet_file: Path to parquet directory or file
        key: Metadata key
        value: Metadata value
    """
    paths = get_bls_parquet_paths(parquet_file)
    
    if not os.path.exists(paths['metadata']):
        init_bls_parquet_tables(paths['base_dir'])
    
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
        print(f"Error updating BLS metadata: {e}")
