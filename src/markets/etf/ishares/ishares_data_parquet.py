"""
iShares ETF Data Parquet Database Management

This module handles all Parquet file operations for iShares ETF Holdings and Historical data.
"""

import os
from typing import Dict, Optional, List
from datetime import datetime
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd


def init_ishares_data_parquet_tables(parquet_dir: str) -> None:
    """
    Initialize Parquet files for iShares ETF Holdings and Historical data if they don't exist
    
    Args:
        parquet_dir: Directory where parquet files will be stored
    """
    os.makedirs(parquet_dir, exist_ok=True)
    
    holdings_parquet = os.path.join(parquet_dir, 'ishares_holdings.parquet')
    historical_parquet = os.path.join(parquet_dir, 'ishares_historical.parquet')
    metadata_parquet = os.path.join(parquet_dir, 'ishares_data_metadata.parquet')
    
    # Create Holdings table schema if file doesn't exist
    # Schema will be flexible to accommodate varying columns
    if not os.path.exists(holdings_parquet):
        # Use a flexible schema - we'll store all columns as strings initially
        # Common columns: ticker, data_date, and then dynamic columns from Excel
        holdings_schema = pa.schema([
            ('ticker', pa.string()),
            ('data_date', pa.string()),
            ('fund_url', pa.string()),
            ('excel_file', pa.string()),
            ('created_at', pa.string()),
        ])
        empty_df = pa.Table.from_pylist([], schema=holdings_schema)
        pq.write_table(empty_df, holdings_parquet)
    
    # Create Historical table schema if file doesn't exist
    if not os.path.exists(historical_parquet):
        historical_schema = pa.schema([
            ('ticker', pa.string()),
            ('data_date', pa.string()),
            ('fund_url', pa.string()),
            ('excel_file', pa.string()),
            ('created_at', pa.string()),
        ])
        empty_df = pa.Table.from_pylist([], schema=historical_schema)
        pq.write_table(empty_df, historical_parquet)
    
    # Create metadata table schema if file doesn't exist
    if not os.path.exists(metadata_parquet):
        metadata_schema = pa.schema([
            ('key', pa.string()),
            ('value', pa.string()),
            ('updated_at', pa.string()),
        ])
        initial_metadata = [
            {'key': 'generated_at', 'value': datetime.now().isoformat(), 'updated_at': datetime.now().isoformat()},
            {'key': 'total_holdings_records', 'value': '0', 'updated_at': datetime.now().isoformat()},
            {'key': 'total_historical_records', 'value': '0', 'updated_at': datetime.now().isoformat()},
            {'key': 'total_files_downloaded', 'value': '0', 'updated_at': datetime.now().isoformat()},
            {'key': 'status', 'value': 'in_progress', 'updated_at': datetime.now().isoformat()},
            {'key': 'source', 'value': 'iShares website - Excel data downloads', 'updated_at': datetime.now().isoformat()},
        ]
        empty_df = pa.Table.from_pylist(initial_metadata, schema=metadata_schema)
        pq.write_table(empty_df, metadata_parquet)


def get_ishares_data_parquet_paths(parquet_file: str) -> Dict[str, str]:
    """
    Get parquet file paths from base path (handles both file and directory inputs)
    
    Args:
        parquet_file: Path to parquet file or directory
    
    Returns:
        Dictionary with 'holdings', 'historical', 'metadata', 'base_dir' paths
    """
    if os.path.isdir(parquet_file):
        base_dir = parquet_file
    else:
        base_dir = os.path.dirname(parquet_file) if os.path.dirname(parquet_file) else '.'
    
    return {
        'base_dir': base_dir,
        'holdings': os.path.join(base_dir, 'ishares_holdings.parquet'),
        'historical': os.path.join(base_dir, 'ishares_historical.parquet'),
        'metadata': os.path.join(base_dir, 'ishares_data_metadata.parquet'),
    }


def add_ishares_data_fast(data_file: str, new_data: List[Dict], schema: Optional[pa.Schema] = None) -> int:
    """
    Add iShares ETF data (Holdings or Historical) to parquet file in batch (fast, non-atomic)
    
    Args:
        data_file: Path to the data parquet file
        new_data: List of data dictionaries to add
        schema: Optional schema (if None, will infer from data or use existing)
    
    Returns:
        Number of records successfully added
    """
    if not new_data:
        return 0
    
    # Get existing schema if file exists
    existing_schema = None
    if os.path.exists(data_file):
        try:
            existing_table = pq.read_table(data_file)
            existing_schema = existing_table.schema
            existing_df = existing_table.to_pandas()
        except Exception:
            existing_schema = None
            existing_df = pd.DataFrame()
    else:
        existing_df = pd.DataFrame()
    
    # Use provided schema or existing schema, or infer from data
    if schema is None:
        if existing_schema is None:
            # Infer schema from first record
            if new_data:
                df_sample = pd.DataFrame([new_data[0]])
                schema = pa.Schema.from_pandas(df_sample)
        else:
            schema = existing_schema
    
    # Create DataFrame from new data
    new_df = pd.DataFrame(new_data)
    
    # Ensure all columns from schema exist in new_df
    for field in schema:
        if field.name not in new_df.columns:
            new_df[field.name] = None
    
    # Reorder columns to match schema
    new_df = new_df[[field.name for field in schema if field.name in new_df.columns]]
    
    # Combine with existing
    if len(existing_df) > 0:
        # Align columns
        all_columns = set(existing_df.columns) | set(new_df.columns)
        for col in all_columns:
            if col not in existing_df.columns:
                existing_df[col] = None
            if col not in new_df.columns:
                new_df[col] = None
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
    else:
        combined_df = new_df
    
    # Ensure schema compatibility
    try:
        combined_table = pa.Table.from_pandas(combined_df, schema=schema)
    except:
        # If schema doesn't match, create new schema from combined data
        schema = pa.Schema.from_pandas(combined_df)
        combined_table = pa.Table.from_pandas(combined_df, schema=schema)
    
    # Write directly (non-atomic, but fast)
    pq.write_table(combined_table, data_file)
    
    return len(new_data)


def update_ishares_data_metadata(parquet_file: str, key: str, value: str) -> None:
    """
    Update metadata value in iShares data parquet file
    
    Args:
        parquet_file: Path to parquet directory or file
        key: Metadata key
        value: Metadata value
    """
    paths = get_ishares_data_parquet_paths(parquet_file)
    
    if not os.path.exists(paths['metadata']):
        init_ishares_data_parquet_tables(paths['base_dir'])
    
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
        print(f"Error updating iShares data metadata: {e}")

