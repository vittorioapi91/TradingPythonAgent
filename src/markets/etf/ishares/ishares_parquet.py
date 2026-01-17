"""
iShares Parquet Database Management

This module handles all Parquet file operations for iShares ETF data storage.
"""

import os
from typing import Dict, Optional
from datetime import datetime
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd


def init_ishares_parquet_tables(parquet_dir: str) -> None:
    """
    Initialize Parquet files for iShares ETF data if they don't exist
    
    Args:
        parquet_dir: Directory where parquet files will be stored
    """
    os.makedirs(parquet_dir, exist_ok=True)
    
    etfs_parquet = os.path.join(parquet_dir, 'ishares_etfs.parquet')
    metadata_parquet = os.path.join(parquet_dir, 'ishares_metadata.parquet')
    
    # Create ETFs table schema if file doesn't exist
    if not os.path.exists(etfs_parquet):
        etfs_schema = pa.schema([
            ('ticker', pa.string()),
            ('name', pa.string()),
            pa.field('asset_class', pa.string(), nullable=True),
            pa.field('expense_ratio', pa.string(), nullable=True),
            pa.field('total_net_assets', pa.string(), nullable=True),
            pa.field('ytd_return', pa.string(), nullable=True),
            pa.field('one_year_return', pa.string(), nullable=True),
            pa.field('three_year_return', pa.string(), nullable=True),
            pa.field('five_year_return', pa.string(), nullable=True),
            pa.field('ten_year_return', pa.string(), nullable=True),
            pa.field('inception_date', pa.string(), nullable=True),
            pa.field('primary_benchmark', pa.string(), nullable=True),
            pa.field('fund_url', pa.string(), nullable=True),
            ('created_at', pa.string()),
            ('updated_at', pa.string()),
        ])
        empty_df = pa.Table.from_pylist([], schema=etfs_schema)
        pq.write_table(empty_df, etfs_parquet)
    
    # Create metadata table schema if file doesn't exist
    if not os.path.exists(metadata_parquet):
        metadata_schema = pa.schema([
            ('key', pa.string()),
            ('value', pa.string()),
            ('updated_at', pa.string()),
        ])
        initial_metadata = [
            {'key': 'generated_at', 'value': datetime.now().isoformat(), 'updated_at': datetime.now().isoformat()},
            {'key': 'total_etfs', 'value': '0', 'updated_at': datetime.now().isoformat()},
            {'key': 'status', 'value': 'in_progress', 'updated_at': datetime.now().isoformat()},
            {'key': 'source', 'value': 'iShares website', 'updated_at': datetime.now().isoformat()},
        ]
        empty_df = pa.Table.from_pylist(initial_metadata, schema=metadata_schema)
        pq.write_table(empty_df, metadata_parquet)


def get_ishares_parquet_paths(parquet_file: str) -> Dict[str, str]:
    """
    Get parquet file paths from base path (handles both file and directory inputs)
    
    Args:
        parquet_file: Path to parquet file or directory
    
    Returns:
        Dictionary with 'etfs', 'metadata', 'base_dir' paths
    """
    if os.path.isdir(parquet_file):
        base_dir = parquet_file
    else:
        base_dir = os.path.dirname(parquet_file) if os.path.dirname(parquet_file) else '.'
    
    return {
        'base_dir': base_dir,
        'etfs': os.path.join(base_dir, 'ishares_etfs.parquet'),
        'metadata': os.path.join(base_dir, 'ishares_metadata.parquet'),
    }


def add_ishares_etfs_fast(etfs_file: str, new_etfs: list) -> int:
    """
    Add iShares ETFs to parquet file in batch (fast, non-atomic)
    
    Args:
        etfs_file: Path to the ETFs parquet file
        new_etfs: List of ETF dictionaries to add
    
    Returns:
        Number of ETFs successfully added
    """
    if not new_etfs:
        return 0
    
    # Get existing schema if file exists
    existing_schema = None
    if os.path.exists(etfs_file):
        try:
            existing_table = pq.read_table(etfs_file)
            existing_schema = existing_table.schema
            existing_df = existing_table.to_pandas()
            # Get existing tickers to avoid duplicates
            existing_tickers = set(existing_df['ticker'].dropna().unique())
        except Exception:
            existing_schema = None
            existing_df = pd.DataFrame()
            existing_tickers = set()
    else:
        existing_df = pd.DataFrame()
        existing_tickers = set()
    
    # If no schema exists, use the standard schema
    if existing_schema is None:
        existing_schema = pa.schema([
            ('ticker', pa.string()),
            ('name', pa.string()),
            pa.field('asset_class', pa.string(), nullable=True),
            pa.field('expense_ratio', pa.string(), nullable=True),
            pa.field('total_net_assets', pa.string(), nullable=True),
            pa.field('ytd_return', pa.string(), nullable=True),
            pa.field('one_year_return', pa.string(), nullable=True),
            pa.field('three_year_return', pa.string(), nullable=True),
            pa.field('five_year_return', pa.string(), nullable=True),
            pa.field('ten_year_return', pa.string(), nullable=True),
            pa.field('inception_date', pa.string(), nullable=True),
            pa.field('primary_benchmark', pa.string(), nullable=True),
            pa.field('fund_url', pa.string(), nullable=True),
            ('created_at', pa.string()),
            ('updated_at', pa.string()),
        ])
    
    # Filter out ETFs that already exist
    new_etfs_filtered = [
        e for e in new_etfs 
        if e.get('ticker') and e.get('ticker') not in existing_tickers
    ]
    
    if not new_etfs_filtered:
        return 0
    
    # Create new ETFs table with proper schema
    new_table = pa.Table.from_pylist(new_etfs_filtered, schema=existing_schema)
    new_df = new_table.to_pandas()
    
    # Combine with existing
    if len(existing_df) > 0:
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
    else:
        combined_df = new_df
    
    # Write directly (non-atomic, but fast)
    combined_table = pa.Table.from_pandas(combined_df, schema=existing_schema)
    pq.write_table(combined_table, etfs_file)
    
    return len(new_etfs_filtered)


def update_ishares_metadata(parquet_file: str, key: str, value: str) -> None:
    """
    Update metadata value in iShares parquet file
    
    Args:
        parquet_file: Path to parquet directory or file
        key: Metadata key
        value: Metadata value
    """
    paths = get_ishares_parquet_paths(parquet_file)
    
    if not os.path.exists(paths['metadata']):
        init_ishares_parquet_tables(paths['base_dir'])
    
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
        print(f"Error updating iShares metadata: {e}")

