"""
Yahoo Finance Equities Parquet Database Management

This module handles all Parquet file operations for Yahoo Finance equities data storage.
"""

import os
from typing import Dict, Optional, List
from datetime import datetime
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd


def init_yahoo_equities_parquet_tables(parquet_dir: str) -> None:
    """
    Initialize Parquet files for Yahoo Finance equities data if they don't exist
    
    Args:
        parquet_dir: Directory where parquet files will be stored
    """
    os.makedirs(parquet_dir, exist_ok=True)
    
    equities_parquet = os.path.join(parquet_dir, 'yahoo_equities.parquet')
    equities_essentials_parquet = os.path.join(parquet_dir, 'yahoo_equities_essentials.parquet')
    valuation_measures_parquet = os.path.join(parquet_dir, 'yahoo_valuation_measures.parquet')
    eps_revisions_parquet = os.path.join(parquet_dir, 'yahoo_eps_revisions.parquet')
    revenue_estimates_parquet = os.path.join(parquet_dir, 'yahoo_revenue_estimates.parquet')
    analyst_recommendations_parquet = os.path.join(parquet_dir, 'yahoo_analyst_recommendations.parquet')
    analyst_price_targets_parquet = os.path.join(parquet_dir, 'yahoo_analyst_price_targets.parquet')
    time_series_parquet = os.path.join(parquet_dir, 'yahoo_time_series.parquet')
    metadata_parquet = os.path.join(parquet_dir, 'yahoo_equities_metadata.parquet')
    
    # Create equities essentials table (minimal data for fast cataloging)
    if not os.path.exists(equities_essentials_parquet):
        essentials_schema = pa.schema([
            ('ticker', pa.string()),
            ('name', pa.string()),
            pa.field('country', pa.string(), nullable=True),
            pa.field('exchange', pa.string(), nullable=True),
            pa.field('industry', pa.string(), nullable=True),
            pa.field('sector', pa.string(), nullable=True),
            ('created_at', pa.string()),
        ])
        empty_df = pa.Table.from_pylist([], schema=essentials_schema)
        pq.write_table(empty_df, equities_essentials_parquet)
    
    # Create equities table schema - flexible schema to accommodate all metadata fields
    if not os.path.exists(equities_parquet):
        # Core fields that will always be present
        equities_schema = pa.schema([
            ('symbol', pa.string()),
            ('name', pa.string()),
            pa.field('sector', pa.string(), nullable=True),
            pa.field('industry', pa.string(), nullable=True),
            pa.field('country', pa.string(), nullable=True),
            ('created_at', pa.string()),
            ('updated_at', pa.string()),
        ])
        empty_df = pa.Table.from_pylist([], schema=equities_schema)
        pq.write_table(empty_df, equities_parquet)
    
    # Create valuation measures table (historical data across time periods)
    if not os.path.exists(valuation_measures_parquet):
        valuation_schema = pa.schema([
            ('symbol', pa.string()),
            ('period', pa.string()),  # Current, 9/30/2025, etc.
            ('market_cap', pa.string()),
            ('enterprise_value', pa.string()),
            ('trailing_pe', pa.string()),
            ('forward_pe', pa.string()),
            ('pegratio_5y', pa.string()),
            ('price_sales', pa.string()),
            ('price_book', pa.string()),
            ('ev_revenue', pa.string()),
            ('ev_ebitda', pa.string()),
            ('created_at', pa.string()),
        ])
        empty_df = pa.Table.from_pylist([], schema=valuation_schema)
        pq.write_table(empty_df, valuation_measures_parquet)
    
    # Create EPS revisions table
    if not os.path.exists(eps_revisions_parquet):
        eps_schema = pa.schema([
            ('symbol', pa.string()),
            ('period_type', pa.string()),  # Current Qtr, Next Qtr, Current Year, Next Year
            ('period', pa.string()),  # Dec 2025, Mar 2026, 2025, 2026
            ('up_last_7_days', pa.string()),
            ('up_last_30_days', pa.string()),
            ('down_last_7_days', pa.string()),
            ('down_last_30_days', pa.string()),
            ('created_at', pa.string()),
        ])
        empty_df = pa.Table.from_pylist([], schema=eps_schema)
        pq.write_table(empty_df, eps_revisions_parquet)
    
    # Create revenue estimates table
    if not os.path.exists(revenue_estimates_parquet):
        revenue_schema = pa.schema([
            ('symbol', pa.string()),
            ('period_type', pa.string()),  # Current Qtr, Next Qtr, Current Year, Next Year
            ('period', pa.string()),  # Dec 2025, Mar 2026, 2025, 2026
            ('num_analysts', pa.string()),
            ('avg_estimate', pa.string()),
            ('low_estimate', pa.string()),
            ('high_estimate', pa.string()),
            ('year_ago_sales', pa.string()),
            ('sales_growth', pa.string()),
            ('created_at', pa.string()),
        ])
        empty_df = pa.Table.from_pylist([], schema=revenue_schema)
        pq.write_table(empty_df, revenue_estimates_parquet)
    
    # Create analyst recommendations table (all recommendations over time)
    if not os.path.exists(analyst_recommendations_parquet):
        rec_schema = pa.schema([
            ('symbol', pa.string()),
            ('date', pa.string()),
            ('firm', pa.string()),  # Analyst firm name
            ('to_grade', pa.string()),  # The rating (Buy, Hold, etc.)
            ('from_grade', pa.string()),  # Previous rating (if upgrade/downgrade)
            ('action', pa.string()),  # Maintains, Upgrades, Downgrades
            pa.field('buy_count', pa.string(), nullable=True),  # Count of Buy recommendations on this date
            pa.field('hold_count', pa.string(), nullable=True),  # Count of Hold recommendations on this date
            pa.field('sell_count', pa.string(), nullable=True),  # Count of Sell recommendations on this date
            pa.field('strong_buy_count', pa.string(), nullable=True),  # Count of Strong Buy recommendations on this date
            pa.field('strong_sell_count', pa.string(), nullable=True),  # Count of Strong Sell recommendations on this date
            ('created_at', pa.string()),
        ])
        empty_df = pa.Table.from_pylist([], schema=rec_schema)
        pq.write_table(empty_df, analyst_recommendations_parquet)
    
    # Create analyst price targets table
    if not os.path.exists(analyst_price_targets_parquet):
        price_target_schema = pa.schema([
            ('symbol', pa.string()),
            ('date', pa.string()),
            ('firm', pa.string()),  # Analyst firm name
            ('price_target', pa.string()),
            ('current_price', pa.string()),
            ('created_at', pa.string()),
        ])
        empty_df = pa.Table.from_pylist([], schema=price_target_schema)
        pq.write_table(empty_df, analyst_price_targets_parquet)
    
    # Create time series table
    if not os.path.exists(time_series_parquet):
        time_series_schema = pa.schema([
            ('symbol', pa.string()),
            ('date', pa.string()),  # Trading date
            ('open', pa.string()),
            ('high', pa.string()),
            ('low', pa.string()),
            ('close', pa.string()),
            ('volume', pa.string()),
            ('adj_close', pa.string()),  # Adjusted close
            ('created_at', pa.string()),
        ])
        empty_df = pa.Table.from_pylist([], schema=time_series_schema)
        pq.write_table(empty_df, time_series_parquet)
    
    # Create metadata table schema if file doesn't exist
    if not os.path.exists(metadata_parquet):
        metadata_schema = pa.schema([
            ('key', pa.string()),
            ('value', pa.string()),
            ('updated_at', pa.string()),
        ])
        initial_metadata = [
            {'key': 'generated_at', 'value': datetime.now().isoformat(), 'updated_at': datetime.now().isoformat()},
            {'key': 'total_equities', 'value': '0', 'updated_at': datetime.now().isoformat()},
            {'key': 'last_update', 'value': datetime.now().isoformat(), 'updated_at': datetime.now().isoformat()},
            {'key': 'status', 'value': 'in_progress', 'updated_at': datetime.now().isoformat()},
            {'key': 'source', 'value': 'Yahoo Finance', 'updated_at': datetime.now().isoformat()},
        ]
        empty_df = pa.Table.from_pylist(initial_metadata, schema=metadata_schema)
        pq.write_table(empty_df, metadata_parquet)


def get_yahoo_equities_parquet_paths(parquet_file: str) -> Dict[str, str]:
    """
    Get parquet file paths from base path (handles both file and directory inputs)
    
    Args:
        parquet_file: Path to parquet file or directory
    
    Returns:
        Dictionary with all parquet file paths
    """
    if os.path.isdir(parquet_file):
        base_dir = parquet_file
    else:
        base_dir = os.path.dirname(parquet_file) if os.path.dirname(parquet_file) else '.'
    
    return {
        'base_dir': base_dir,
        'equities': os.path.join(base_dir, 'yahoo_equities.parquet'),
        'equities_essentials': os.path.join(base_dir, 'yahoo_equities_essentials.parquet'),
        'valuation_measures': os.path.join(base_dir, 'yahoo_valuation_measures.parquet'),
        'eps_revisions': os.path.join(base_dir, 'yahoo_eps_revisions.parquet'),
        'revenue_estimates': os.path.join(base_dir, 'yahoo_revenue_estimates.parquet'),
        'analyst_recommendations': os.path.join(base_dir, 'yahoo_analyst_recommendations.parquet'),
        'analyst_price_targets': os.path.join(base_dir, 'yahoo_analyst_price_targets.parquet'),
        'time_series': os.path.join(base_dir, 'yahoo_time_series.parquet'),
        'metadata': os.path.join(base_dir, 'yahoo_equities_metadata.parquet'),
    }


def add_data_fast(data_file: str, new_data: List[Dict], schema: Optional[pa.Schema] = None, primary_key: Optional[List[str]] = None) -> int:
    """
    Generic function to add data to parquet file in batch (fast, non-atomic)
    
    Args:
        data_file: Path to the parquet file
        new_data: List of data dictionaries to add
        schema: Optional schema (if None, will infer from data or use existing)
        primary_key: Optional list of column names that form the primary key for deduplication
    
    Returns:
        Number of records successfully added
    """
    if not new_data:
        return 0
    
    # Get existing data if file exists
    existing_df = pd.DataFrame()
    existing_schema = None
    if os.path.exists(data_file):
        try:
            existing_table = pq.read_table(data_file)
            existing_schema = existing_table.schema
            existing_df = existing_table.to_pandas()
        except Exception:
            existing_df = pd.DataFrame()
    
    # Convert new data to DataFrame
    new_df = pd.DataFrame(new_data)
    
    # Use provided schema or existing schema, or infer
    if schema is None:
        if existing_schema:
            schema = existing_schema
        else:
            # Infer schema from first record
            if new_data:
                schema = pa.Schema.from_pandas(new_df)
    
    # Ensure all columns from schema exist in new_df
    if schema:
        for field in schema:
            if field.name not in new_df.columns:
                new_df[field.name] = None
        # Reorder columns to match schema
        new_df = new_df[[field.name for field in schema if field.name in new_df.columns]]
    
    # Deduplicate based on primary key if provided
    if primary_key:
        # Check if all primary key columns exist
        if all(col in new_df.columns for col in primary_key):
            # Remove duplicates from new data (keep last)
            new_df = new_df.drop_duplicates(subset=primary_key, keep='last')
            
            if len(existing_df) > 0:
                if all(col in existing_df.columns for col in primary_key):
                    # Remove duplicates from existing data (keep last)
                    existing_df = existing_df.drop_duplicates(subset=primary_key, keep='last')
                    
                    # Remove new records that already exist in existing data
                    existing_keys = existing_df[primary_key].apply(tuple, axis=1)
                    new_keys = new_df[primary_key].apply(tuple, axis=1)
                    new_df = new_df[~new_keys.isin(existing_keys)]
    
    # Combine with existing
    if len(existing_df) > 0:
        # Align columns
        all_columns = set(existing_df.columns) | set(new_df.columns)
        for col in all_columns:
            if col not in existing_df.columns:
                existing_df[col] = None
            if col not in new_df.columns:
                new_df[col] = None
        
        if len(new_df) > 0:
            combined_df = pd.concat([existing_df, new_df], ignore_index=True)
            # Final deduplication on combined data if primary key provided
            if primary_key and all(col in combined_df.columns for col in primary_key):
                combined_df = combined_df.drop_duplicates(subset=primary_key, keep='last')
        else:
            combined_df = existing_df
    else:
        combined_df = new_df
    
    # Write to parquet
    if schema:
        try:
            combined_table = pa.Table.from_pandas(combined_df, schema=schema)
        except:
            # If schema doesn't match, create new schema from combined data
            schema = pa.Schema.from_pandas(combined_df)
            combined_table = pa.Table.from_pandas(combined_df, schema=schema)
    else:
        combined_table = pa.Table.from_pandas(combined_df)
    
    pq.write_table(combined_table, data_file)
    
    return len(new_data)


def add_yahoo_equities_essentials_fast(essentials_file: str, new_essentials: List[Dict], update_existing: bool = True) -> int:
    """
    Add Yahoo Finance equity essentials to parquet file in batch (fast, non-atomic)
    
    Args:
        essentials_file: Path to the essentials parquet file
        new_essentials: List of essential equity dictionaries (ticker, name, country, exchange, created_at)
        update_existing: If True, update existing records; if False, skip duplicates
    
    Returns:
        Number of essentials successfully added/updated
    """
    if not new_essentials:
        return 0
    
    # Get existing data if file exists
    existing_df = pd.DataFrame()
    if os.path.exists(essentials_file):
        try:
            existing_table = pq.read_table(essentials_file)
            existing_df = existing_table.to_pandas()
        except Exception:
            existing_df = pd.DataFrame()
    
    # Convert new essentials to DataFrame
    new_df = pd.DataFrame(new_essentials)
    
    if len(existing_df) == 0:
        # No existing data, just write new data
        combined_df = new_df
    else:
        if update_existing:
            # Update existing records and add new ones
            # Set ticker as index for easier merging
            existing_df = existing_df.set_index('ticker')
            new_df_indexed = new_df.set_index('ticker')
            
            # Update existing and add new
            combined_df = existing_df.combine_first(new_df_indexed).reset_index()
        else:
            # Only add new tickers that don't exist
            existing_tickers = set(existing_df['ticker'].unique())
            new_df_filtered = new_df[~new_df['ticker'].isin(existing_tickers)]
            if len(new_df_filtered) > 0:
                combined_df = pd.concat([existing_df, new_df_filtered], ignore_index=True)
            else:
                combined_df = existing_df
    
    # Write to parquet (schema will be inferred from DataFrame)
    table = pa.Table.from_pandas(combined_df)
    pq.write_table(table, essentials_file)
    
    return len(new_essentials)


def add_yahoo_equities_fast(equities_file: str, new_equities: List[Dict], update_existing: bool = True) -> int:
    """
    Add Yahoo Finance equities to parquet file in batch (fast, non-atomic)
    
    Args:
        equities_file: Path to the equities parquet file
        new_equities: List of equity dictionaries to add/update
        update_existing: If True, update existing records; if False, skip duplicates
    
    Returns:
        Number of equities successfully added/updated
    """
    if not new_equities:
        return 0
    
    # Get existing data if file exists
    existing_df = pd.DataFrame()
    if os.path.exists(equities_file):
        try:
            existing_table = pq.read_table(equities_file)
            existing_df = existing_table.to_pandas()
        except Exception:
            existing_df = pd.DataFrame()
    
    # Convert new equities to DataFrame
    new_df = pd.DataFrame(new_equities)
    
    if len(existing_df) == 0:
        # No existing data, just write new data
        combined_df = new_df
    else:
        if update_existing:
            # Update existing records and add new ones
            # Set symbol as index for easier merging
            existing_df = existing_df.set_index('symbol')
            new_df_indexed = new_df.set_index('symbol')
            
            # Update existing and add new
            combined_df = existing_df.combine_first(new_df_indexed).reset_index()
        else:
            # Only add new symbols that don't exist
            existing_symbols = set(existing_df['symbol'].unique())
            new_df_filtered = new_df[~new_df['symbol'].isin(existing_symbols)]
            if len(new_df_filtered) > 0:
                combined_df = pd.concat([existing_df, new_df_filtered], ignore_index=True)
            else:
                combined_df = existing_df
    
    # Write to parquet (schema will be inferred from DataFrame)
    table = pa.Table.from_pandas(combined_df)
    pq.write_table(table, equities_file)
    
    return len(new_equities)


def update_yahoo_equities_metadata(parquet_file: str, key: str, value: str) -> None:
    """
    Update metadata value in Yahoo equities parquet file
    
    Args:
        parquet_file: Path to parquet directory or file
        key: Metadata key
        value: Metadata value
    """
    paths = get_yahoo_equities_parquet_paths(parquet_file)
    
    if not os.path.exists(paths['metadata']):
        init_yahoo_equities_parquet_tables(paths['base_dir'])
    
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
        print(f"Error updating Yahoo equities metadata: {e}")
