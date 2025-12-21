"""
iShares ETF Details Parquet Database Management

This module handles all Parquet file operations for detailed iShares ETF data storage.
"""

import os
from typing import Dict, Optional, List
from datetime import datetime
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd


def init_ishares_details_parquet_tables(parquet_dir: str) -> None:
    """
    Initialize Parquet files for iShares ETF detailed data if they don't exist
    
    Args:
        parquet_dir: Directory where parquet files will be stored
    """
    os.makedirs(parquet_dir, exist_ok=True)
    
    details_parquet = os.path.join(parquet_dir, 'ishares_etf_details.parquet')
    metadata_parquet = os.path.join(parquet_dir, 'ishares_details_metadata.parquet')
    
    # Create ETF details table schema if file doesn't exist
    if not os.path.exists(details_parquet):
        details_schema = pa.schema([
            ('ticker', pa.string()),
            ('fund_url', pa.string()),
            ('data_date', pa.string()),  # Date found on the page (e.g., "As of 12/31/2024")
            ('section', pa.string()),  # 'key_facts' or 'portfolio_characteristics'
            ('key', pa.string()),  # Field name (e.g., 'Net Assets', 'Expense Ratio')
            ('value', pa.string()),  # Field value
            ('created_at', pa.string()),
            ('updated_at', pa.string()),
        ])
        empty_df = pa.Table.from_pylist([], schema=details_schema)
        pq.write_table(empty_df, details_parquet)
    
    # Create metadata table schema if file doesn't exist
    if not os.path.exists(metadata_parquet):
        metadata_schema = pa.schema([
            ('key', pa.string()),
            ('value', pa.string()),
            ('updated_at', pa.string()),
        ])
        initial_metadata = [
            {'key': 'generated_at', 'value': datetime.now().isoformat(), 'updated_at': datetime.now().isoformat()},
            {'key': 'total_records', 'value': '0', 'updated_at': datetime.now().isoformat()},
            {'key': 'total_etfs_scraped', 'value': '0', 'updated_at': datetime.now().isoformat()},
            {'key': 'status', 'value': 'in_progress', 'updated_at': datetime.now().isoformat()},
            {'key': 'source', 'value': 'iShares website - individual ETF pages', 'updated_at': datetime.now().isoformat()},
        ]
        empty_df = pa.Table.from_pylist(initial_metadata, schema=metadata_schema)
        pq.write_table(empty_df, metadata_parquet)


def get_ishares_details_parquet_paths(parquet_file: str) -> Dict[str, str]:
    """
    Get parquet file paths from base path (handles both file and directory inputs)
    
    Args:
        parquet_file: Path to parquet file or directory
    
    Returns:
        Dictionary with 'details', 'metadata', 'base_dir' paths
    """
    if os.path.isdir(parquet_file):
        base_dir = parquet_file
    else:
        base_dir = os.path.dirname(parquet_file) if os.path.dirname(parquet_file) else '.'
    
    return {
        'base_dir': base_dir,
        'details': os.path.join(base_dir, 'ishares_etf_details.parquet'),
        'metadata': os.path.join(base_dir, 'ishares_details_metadata.parquet'),
    }


def add_ishares_details_fast(details_file: str, new_details: List[Dict]) -> int:
    """
    Add iShares ETF details to parquet file in batch (fast, non-atomic)
    
    Args:
        details_file: Path to the details parquet file
        new_details: List of detail dictionaries to add
    
    Returns:
        Number of details successfully added
    """
    if not new_details:
        return 0
    
    # Get existing schema if file exists
    existing_schema = None
    if os.path.exists(details_file):
        try:
            existing_table = pq.read_table(details_file)
            existing_schema = existing_table.schema
            existing_df = existing_table.to_pandas()
        except Exception:
            existing_schema = None
            existing_df = pd.DataFrame()
    else:
        existing_df = pd.DataFrame()
    
    # If no schema exists, use the standard schema
    if existing_schema is None:
        existing_schema = pa.schema([
            ('ticker', pa.string()),
            ('fund_url', pa.string()),
            ('data_date', pa.string()),
            ('section', pa.string()),
            ('key', pa.string()),
            ('value', pa.string()),
            ('created_at', pa.string()),
            ('updated_at', pa.string()),
        ])
    
    # Create new details table with proper schema
    new_table = pa.Table.from_pylist(new_details, schema=existing_schema)
    new_df = new_table.to_pandas()
    
    # Combine with existing
    if len(existing_df) > 0:
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
    else:
        combined_df = new_df
    
    # Write directly (non-atomic, but fast)
    combined_table = pa.Table.from_pandas(combined_df, schema=existing_schema)
    pq.write_table(combined_table, details_file)
    
    return len(new_details)


def update_ishares_details_metadata(parquet_file: str, key: str, value: str) -> None:
    """
    Update metadata value in iShares details parquet file
    
    Args:
        parquet_file: Path to parquet directory or file
        key: Metadata key
        value: Metadata value
    """
    paths = get_ishares_details_parquet_paths(parquet_file)
    
    if not os.path.exists(paths['metadata']):
        init_ishares_details_parquet_tables(paths['base_dir'])
    
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
        print(f"Error updating iShares details metadata: {e}")


def load_ishares_etfs_from_parquet(etfs_parquet_file: str) -> List[Dict]:
    """
    Load iShares ETFs from the main ETFs parquet file
    
    Args:
        etfs_parquet_file: Path to ishares_etfs.parquet file
    
    Returns:
        List of ETF dictionaries
    """
    if not os.path.exists(etfs_parquet_file):
        return []
    
    try:
        df = pq.read_table(etfs_parquet_file).to_pandas()
        return df.to_dict('records')
    except Exception as e:
        print(f"Error loading iShares ETFs from parquet: {e}")
        return []

