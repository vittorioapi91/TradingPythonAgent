"""
EDGAR Parquet Database Management

This module handles all DuckDB and Parquet file operations for EDGAR data storage.
"""

import os
import tempfile
from typing import Dict, Optional, List
from datetime import datetime
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd


def init_duckdb_tables(parquet_dir: str, conn: Optional[duckdb.DuckDBPyConnection] = None) -> duckdb.DuckDBPyConnection:
    """
    Initialize DuckDB connection and create tables for companies and filings if they don't exist
    
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
    
    companies_parquet = os.path.join(parquet_dir, 'companies.parquet')
    filings_parquet = os.path.join(parquet_dir, 'filings.parquet')
    metadata_parquet = os.path.join(parquet_dir, 'metadata.parquet')
    company_history_parquet = os.path.join(parquet_dir, 'company_history.parquet')
    
    # Create companies table schema if file doesn't exist
    if not os.path.exists(companies_parquet):
        companies_schema = pa.schema([
            ('cik', pa.string()),
            pa.field('ticker', pa.string(), nullable=True),
            ('name', pa.string()),
            pa.field('sic_code', pa.string(), nullable=True),
            pa.field('entity_type', pa.string(), nullable=True),
            ('updated_at', pa.string()),
        ])
        empty_df = pa.Table.from_pylist([], schema=companies_schema)
        pq.write_table(empty_df, companies_parquet)
    
    # Create filings table schema if file doesn't exist
    if not os.path.exists(filings_parquet):
        filings_schema = pa.schema([
            ('cik', pa.string()),
            ('accession_number', pa.string()),
            ('filing_date', pa.string()),
            ('filing_type', pa.string()),
            ('description', pa.string()),
            ('is_xbrl', pa.bool_()),
            ('is_inline_xbrl', pa.bool_()),
            pa.field('amends_accession', pa.string(), nullable=True),
            pa.field('amends_filing_date', pa.string(), nullable=True),
            pa.field('downloaded_file_path', pa.string(), nullable=True),  # Path to downloaded file (relative to output_dir)
        ])
        empty_df = pa.Table.from_pylist([], schema=filings_schema)
        pq.write_table(empty_df, filings_parquet)
    
    # Create metadata table schema if file doesn't exist
    if not os.path.exists(metadata_parquet):
        metadata_schema = pa.schema([
            ('key', pa.string()),
            ('value', pa.string()),
            ('updated_at', pa.string()),
        ])
        initial_metadata = [
            {'key': 'generated_at', 'value': datetime.now().isoformat(), 'updated_at': datetime.now().isoformat()},
            {'key': 'total_companies', 'value': '0', 'updated_at': datetime.now().isoformat()},
            {'key': 'total_filings', 'value': '0', 'updated_at': datetime.now().isoformat()},
            {'key': 'status', 'value': 'in_progress', 'updated_at': datetime.now().isoformat()},
            {'key': 'source', 'value': 'SEC EDGAR API', 'updated_at': datetime.now().isoformat()},
        ]
        empty_df = pa.Table.from_pylist(initial_metadata, schema=metadata_schema)
        pq.write_table(empty_df, metadata_parquet)

    # Create company history table if it doesn't exist.
    # To avoid "wasting" existing runs, if companies.parquet already exists we seed
    # company_history.parquet with its current contents as the initial snapshot.
    if not os.path.exists(company_history_parquet):
        history_schema = pa.schema([
            ('cik', pa.string()),
            pa.field('ticker', pa.string(), nullable=True),
            ('name', pa.string()),
            pa.field('sic_code', pa.string(), nullable=True),
            pa.field('entity_type', pa.string(), nullable=True),
            ('updated_at', pa.string()),
        ])

        try:
            if os.path.exists(companies_parquet):
                # Seed history with current companies snapshot
                companies_table = pq.read_table(companies_parquet)
                companies_df = companies_table.to_pandas()
                history_table = pa.Table.from_pandas(companies_df, schema=history_schema)
            else:
                history_table = pa.Table.from_pylist([], schema=history_schema)
            pq.write_table(history_table, company_history_parquet)
        except Exception:
            # If anything goes wrong, fall back to an empty history file
            empty_history = pa.Table.from_pylist([], schema=history_schema)
            pq.write_table(empty_history, company_history_parquet)
    
    return conn


def get_parquet_paths(parquet_file: str) -> Dict[str, str]:
    """
    Get parquet file paths from base path (handles both file and directory inputs)
    
    Args:
        parquet_file: Path to parquet file or directory
    
    Returns:
        Dictionary with 'companies', 'company_history', 'filings', 'metadata', 'base_dir' paths
    """
    if os.path.isdir(parquet_file):
        base_dir = parquet_file
    else:
        base_dir = os.path.dirname(parquet_file) if os.path.dirname(parquet_file) else '.'
    
    return {
        'base_dir': base_dir,
        'companies': os.path.join(base_dir, 'companies.parquet'),
        'company_history': os.path.join(base_dir, 'company_history.parquet'),
        'filings': os.path.join(base_dir, 'filings.parquet'),
        'metadata': os.path.join(base_dir, 'metadata.parquet'),
    }


def add_filings_fast(filings_file: str, new_filings: List[Dict]) -> int:
    """
    Add filings to parquet file in batch (fast, non-atomic).
    Reads existing file once, adds all new filings, writes back.
    
    Args:
        filings_file: Path to the filings parquet file
        new_filings: List of filing dictionaries to add
    
    Returns:
        Number of filings successfully added
    """
    if not new_filings:
        return 0
    
    # Get existing schema if file exists
    existing_schema = None
    if os.path.exists(filings_file):
        try:
            existing_table = pq.read_table(filings_file)
            existing_schema = existing_table.schema
            existing_df = existing_table.to_pandas()
            # Get existing accession numbers to avoid duplicates
            existing_accessions = set(existing_df['accession_number'].dropna().unique())
        except Exception:
            existing_schema = None
            existing_df = pd.DataFrame()
            existing_accessions = set()
    else:
        existing_df = pd.DataFrame()
        existing_accessions = set()
    
    # If no schema exists, use the standard schema
    if existing_schema is None:
        existing_schema = pa.schema([
            ('cik', pa.string()),
            ('accession_number', pa.string()),
            ('filing_date', pa.string()),
            ('filing_type', pa.string()),
            ('description', pa.string()),
            ('is_xbrl', pa.bool_()),
            ('is_inline_xbrl', pa.bool_()),
            pa.field('amends_accession', pa.string(), nullable=True),
            pa.field('amends_filing_date', pa.string(), nullable=True),
            pa.field('downloaded_file_path', pa.string(), nullable=True),
        ])
    
    # Filter out filings that already exist
    new_filings_filtered = [
        f for f in new_filings 
        if f.get('accession_number') and f.get('accession_number') not in existing_accessions
    ]
    
    if not new_filings_filtered:
        return 0
    
    # Create new filings table with proper schema
    new_table = pa.Table.from_pylist(new_filings_filtered, schema=existing_schema)
    new_df = new_table.to_pandas()
    
    # Combine with existing
    if len(existing_df) > 0:
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
    else:
        combined_df = new_df
    
    # Write directly (non-atomic, but fast)
    combined_table = pa.Table.from_pandas(combined_df, schema=existing_schema)
    pq.write_table(combined_table, filings_file)
    
    return len(new_filings_filtered)


def add_filings_atomically(filings_file: str, new_filings: List[Dict], accession_key: str = 'accession_number') -> int:
    """
    Add filings to parquet file atomically, one accession_number at a time.
    Each accession_number is written atomically - either all data for that accession is written, or none.
    
    Args:
        filings_file: Path to the filings parquet file
        new_filings: List of filing dictionaries to add
        accession_key: Key to use for identifying unique filings (default: 'accession_number')
    
    Returns:
        Number of filings successfully added
    """
    if not new_filings:
        return 0
    
    # Get existing schema if file exists
    existing_schema = None
    if os.path.exists(filings_file):
        try:
            existing_table = pq.read_table(filings_file)
            existing_schema = existing_table.schema
        except Exception:
            # If file is corrupted, we'll create a new one
            existing_schema = None
    
    # If no schema exists, use the standard schema
    if existing_schema is None:
        existing_schema = pa.schema([
            ('cik', pa.string()),
            ('accession_number', pa.string()),
            ('filing_date', pa.string()),
            ('filing_type', pa.string()),
            ('description', pa.string()),
            ('is_xbrl', pa.bool_()),
            ('is_inline_xbrl', pa.bool_()),
            pa.field('amends_accession', pa.string(), nullable=True),
            pa.field('amends_filing_date', pa.string(), nullable=True),
            pa.field('downloaded_file_path', pa.string(), nullable=True),
        ])
    
    # Group filings by accession_number to ensure atomicity per accession
    filings_by_accession = {}
    for filing in new_filings:
        accession = filing.get(accession_key)
        if accession:
            if accession not in filings_by_accession:
                filings_by_accession[accession] = []
            filings_by_accession[accession].append(filing)
    
    added_count = 0
    
    # Process each accession_number atomically
    for accession, filings in filings_by_accession.items():
        try:
            # Read existing filings
            if os.path.exists(filings_file):
                try:
                    existing_table = pq.read_table(filings_file)
                    existing_df = existing_table.to_pandas()
                    
                    # Check if this accession_number already exists
                    if accession in existing_df[accession_key].values:
                        continue  # Skip if already exists
                except Exception:
                    # If file is corrupted, start fresh
                    existing_df = pd.DataFrame()
            else:
                existing_df = pd.DataFrame()
            
            # Create new filings table with proper schema
            new_table = pa.Table.from_pylist(filings, schema=existing_schema)
            new_df = new_table.to_pandas()
            
            # Combine with existing
            if len(existing_df) > 0:
                combined_df = pd.concat([existing_df, new_df], ignore_index=True)
            else:
                combined_df = new_df
            
            # Write atomically using temp file + rename
            temp_file = filings_file + '.tmp'
            try:
                # Convert back to table with schema to ensure consistency
                combined_table = pa.Table.from_pandas(combined_df, schema=existing_schema)
                
                # Write to temp file
                pq.write_table(combined_table, temp_file)
                
                # Atomic rename (works on most filesystems)
                os.replace(temp_file, filings_file)
                added_count += len(filings)
            except Exception as e:
                # Clean up temp file on error
                if os.path.exists(temp_file):
                    try:
                        os.remove(temp_file)
                    except:
                        pass
                raise
                
        except Exception as e:
            # Log error but continue with next accession_number
            import logging
            logger = logging.getLogger('edgar_parquet')
            logger.error(f"Error adding filings for accession {accession}: {e}")
            continue
    
    return added_count
