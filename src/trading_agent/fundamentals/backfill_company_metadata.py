#!/usr/bin/env python3
"""
Standalone utility to backfill missing SIC code, entityType, and ticker for existing companies in parquet files.

This is a one-shot utility to pad existing datasets. After running this, all normal EDGAR operations
will automatically fetch and include SIC code, entityType, and ticker for new companies.
"""

import os
import time
from pathlib import Path
from typing import Dict
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import requests
from tqdm import tqdm

# Handle import for both module import and direct script execution
try:
    from .edgar_parquet import get_parquet_paths
except ImportError:
    from edgar_parquet import get_parquet_paths


def get_ticker_mapping(user_agent: str = "VittorioApicella apicellavittorio@hotmail.it") -> Dict[str, str]:
    """
    Fetch ticker mapping from SEC company_tickers.json (CIK -> ticker)
    
    Args:
        user_agent: User-Agent string for SEC requests
        
    Returns:
        Dictionary mapping CIK (10-digit zero-padded) to ticker symbol
    """
    base_url = "https://www.sec.gov"
    headers = {
        'User-Agent': user_agent,
        'Accept-Encoding': 'gzip, deflate',
        'Host': 'www.sec.gov'
    }
    
    ticker_map = {}
    try:
        ticker_url = f"{base_url}/files/company_tickers.json"
        response = requests.get(ticker_url, headers=headers, timeout=30)
        response.raise_for_status()
        ticker_data = response.json()
        
        for entry in ticker_data.values():
            cik_str = str(entry.get('cik_str', '')).zfill(10)
            ticker = entry.get('ticker', '')
            if cik_str and ticker:
                ticker_map[cik_str] = ticker
        
        return ticker_map
    except Exception as e:
        print(f"  Warning: Could not fetch ticker information: {e}")
        return {}


def get_company_details(cik: str, user_agent: str = "VittorioApicella apicellavittorio@hotmail.it") -> Dict[str, str]:
    """
    Fetch company details from SEC Submissions API (SIC code, entityType, name)
    
    Args:
        cik: Company CIK (10-digit zero-padded)
        user_agent: User-Agent string for SEC requests
        
    Returns:
        Dictionary with 'name', 'sic_code', 'entity_type' (None if not available)
    """
    # Ensure CIK is 10-digit zero-padded
    cik_normalized = str(cik).zfill(10)
    
    data_base_url = "https://data.sec.gov"
    data_headers = {
        'User-Agent': user_agent,
        'Accept-Encoding': 'gzip, deflate',
        'Host': 'data.sec.gov'
    }
    
    submissions_url = f"{data_base_url}/submissions/CIK{cik_normalized}.json"
    
    try:
        response = requests.get(submissions_url, headers=data_headers, timeout=10)
        if response.status_code == 200:
            data = response.json()
            
            # Extract information
            name = data.get('name', '')
            sic = data.get('sic')
            sic_code = str(sic) if sic is not None else None
            entity_type = data.get('entityType', '')
            
            return {
                'name': name,
                'sic_code': sic_code,
                'entity_type': entity_type if entity_type else None
            }
        else:
            # Rate limiting or not found
            return {
                'name': None,
                'sic_code': None,
                'entity_type': None
            }
    except Exception as e:
        # Log error but don't fail
        return {
            'name': None,
            'sic_code': None,
            'entity_type': None
        }


def backfill_company_metadata(parquet_dir: str, user_agent: str = "VittorioApicella apicellavittorio@hotmail.it") -> Dict[str, int]:
    """
    Backfill missing SIC code, entityType, and ticker for existing companies in parquet file
    
    Args:
        parquet_dir: Path to directory containing companies.parquet
        user_agent: User-Agent string for SEC requests
        
    Returns:
        Dictionary with statistics: {'updated': count, 'failed': count, 'skipped': count}
    """
    paths = get_parquet_paths(parquet_dir)
    companies_file = paths['companies']
    
    if not os.path.exists(companies_file):
        print(f"Companies parquet file not found: {companies_file}")
        return {'updated': 0, 'failed': 0, 'skipped': 0}
    
    print("Backfilling missing company metadata (SIC code, entityType, ticker)...")
    
    # Read existing companies
    try:
        companies_table = pq.read_table(companies_file)
        companies_df = companies_table.to_pandas()
    except Exception as e:
        print(f"Error reading companies parquet: {e}")
        return {'updated': 0, 'failed': 0, 'skipped': 0}
    
    # Check if schema has required fields
    has_sic = 'sic_code' in companies_df.columns
    has_entity_type = 'entity_type' in companies_df.columns
    has_ticker = 'ticker' in companies_df.columns
    
    # If schema doesn't have these fields, add them
    if not has_sic:
        companies_df['sic_code'] = None
    if not has_entity_type:
        companies_df['entity_type'] = None
    if not has_ticker:
        companies_df['ticker'] = None
    
    # Fetch ticker mapping from company_tickers.json
    print("  Fetching ticker mapping from company_tickers.json...")
    ticker_map = get_ticker_mapping(user_agent)
    print(f"  Found {len(ticker_map)} ticker mappings")
    
    # Find companies missing SIC code, entityType, or ticker
    # Check for missing ticker: None, empty string, or NaN
    missing_ticker_mask = (
        companies_df['ticker'].isna() | 
        (companies_df['ticker'] == '') | 
        (companies_df['ticker'].astype(str).str.strip() == '')
    )
    missing_sic_or_entity_mask = companies_df['sic_code'].isna() | companies_df['entity_type'].isna()
    missing_mask = missing_ticker_mask | missing_sic_or_entity_mask
    companies_to_update = companies_df[missing_mask].copy()
    
    if len(companies_to_update) == 0:
        print("  All companies already have complete metadata")
        return {'updated': 0, 'failed': 0, 'skipped': 0}
    
    print(f"  Found {len(companies_to_update)} companies with missing metadata")
    
    updated_count = 0
    failed_count = 0
    
    # Update each company
    for idx, row in tqdm(companies_to_update.iterrows(), total=len(companies_to_update), desc="Backfilling metadata", unit="company"):
        cik = str(row['cik']).zfill(10)
        
        # Fetch details from API
        details = get_company_details(cik, user_agent)
        
        # Update dataframe
        if details.get('sic_code'):
            companies_df.at[idx, 'sic_code'] = details['sic_code']
        if details.get('entity_type'):
            companies_df.at[idx, 'entity_type'] = details['entity_type']
        if details.get('name') and pd.isna(companies_df.at[idx, 'name']):
            companies_df.at[idx, 'name'] = details['name']
        
        # Update ticker from mapping if missing
        current_ticker = companies_df.at[idx, 'ticker']
        if pd.isna(current_ticker) or str(current_ticker).strip() == '':
            ticker = ticker_map.get(cik, '')
            if ticker:
                companies_df.at[idx, 'ticker'] = ticker
        
        if details.get('sic_code') or details.get('entity_type') or ticker_map.get(cik):
            updated_count += 1
        else:
            failed_count += 1
        
        # Rate limiting
        time.sleep(0.1)
    
    # Update updated_at timestamp
    companies_df['updated_at'] = pd.Timestamp.now().isoformat()
    
    # Write back to parquet with updated schema
    try:
        # Ensure schema includes new fields
        schema = pa.schema([
            ('cik', pa.string()),
            pa.field('ticker', pa.string(), nullable=True),
            ('name', pa.string()),
            pa.field('sic_code', pa.string(), nullable=True),
            pa.field('entity_type', pa.string(), nullable=True),
            ('updated_at', pa.string()),
        ])
        
        companies_table = pa.Table.from_pandas(companies_df, schema=schema)
        pq.write_table(companies_table, companies_file)
        
        print(f"  Updated {updated_count} companies, {failed_count} failed to fetch")
        return {'updated': updated_count, 'failed': failed_count, 'skipped': 0}
    except Exception as e:
        print(f"Error writing updated companies parquet: {e}")
        return {'updated': 0, 'failed': failed_count, 'skipped': 0}


def main():
    """Main function for backfilling company metadata"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Backfill missing SIC code, entityType, and ticker for existing companies in parquet files')
    parser.add_argument('parquet_dir', type=str, help='Path to parquet directory containing companies.parquet')
    parser.add_argument('--user-agent', type=str, 
                       default='VittorioApicella apicellavittorio@hotmail.it',
                       help='User-Agent string for SEC requests')
    
    args = parser.parse_args()
    
    stats = backfill_company_metadata(args.parquet_dir, args.user_agent)
    print(f"\nBackfill complete: {stats['updated']} updated, {stats['failed']} failed")
    return 0 if stats['failed'] == 0 else 1


if __name__ == "__main__":
    main()

