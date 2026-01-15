"""
SEC EDGAR Filings Downloader

This module downloads all company filings from SEC EDGAR database.
Focuses on XBRL filings from 2009 onwards.
"""

# Load environment configuration early
try:
    from ...config import load_environment_config
    load_environment_config()
except (ImportError, ValueError):
    # Handle both relative import errors and when run as a script
    import sys
    from pathlib import Path
    # Add project root to path for absolute import
    # File is at: src/trading_agent/fundamentals/edgar/edgar.py
    # Need to go up 5 levels to get to project root
    file_path = Path(__file__).resolve()
    project_root = file_path.parent.parent.parent.parent.parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))
    try:
        from src.trading_agent.config import load_environment_config
        load_environment_config()
    except ImportError as e:
        # Config module is required - fail if not found
        raise ImportError(
            f"Failed to import config module. This is required for environment configuration. "
            f"Original error: {e}. "
            f"Please ensure the config module exists at src/trading_agent/config.py"
        ) from e


import os
import time
import shutil
import requests
import asyncio
import aiohttp
from typing import List, Dict, Optional, Set
from datetime import datetime
import json
import warnings
import zipfile
import io
import xml.etree.ElementTree as ET
from collections import defaultdict
import re
from urllib.parse import urljoin, urlparse
import logging
import traceback
import gzip
from html.parser import HTMLParser
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import pandas as pd
from pathlib import Path
import psycopg2
from psycopg2.extras import execute_values

from tqdm import tqdm

# Handle import for both module import and direct script execution
try:
    from ..download_logger import get_download_logger
    from .edgar_postgres import (
        get_postgres_connection, init_edgar_postgres_tables,
        add_companies_fast, add_filings_fast, get_existing_accessions,
        load_companies_from_postgres, load_filings_from_postgres,
        add_company_history_snapshot, update_edgar_metadata,
        get_edgar_metadata, get_edgar_statistics, update_filing_downloaded_path,
        get_processed_years, mark_year_processed, get_enriched_ciks,
        get_year_completion_status, update_year_completion_ledger,
        get_db_filing_counts_by_year, is_year_complete, get_incomplete_years,
        get_master_idx_download_status, mark_master_idx_download_success,
        mark_master_idx_download_failed, get_pending_or_failed_quarters,
        get_quarters_with_data
    )
except ImportError:
    # Handle direct script execution - use absolute imports
    import sys
    from pathlib import Path
    # Add project root to path if not already there
    file_path = Path(__file__).resolve()
    project_root = file_path.parent.parent.parent.parent.parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))
    from src.trading_agent.fundamentals.download_logger import get_download_logger
    from src.trading_agent.fundamentals.edgar.edgar_postgres import (
        get_postgres_connection, init_edgar_postgres_tables,
        add_companies_fast, add_filings_fast, get_existing_accessions,
        load_companies_from_postgres, load_filings_from_postgres,
        add_company_history_snapshot, update_edgar_metadata,
        get_edgar_metadata, get_edgar_statistics, update_filing_downloaded_path,
        get_processed_years, mark_year_processed, get_enriched_ciks,
        get_year_completion_status, update_year_completion_ledger,
        get_db_filing_counts_by_year, is_year_complete, get_incomplete_years,
        get_master_idx_download_status, mark_master_idx_download_success,
        mark_master_idx_download_failed, get_pending_or_failed_quarters,
        get_quarters_with_data
    )

warnings.filterwarnings('ignore')


class EDGARDownloader:
    """Class to download SEC EDGAR filings"""
    
    def __init__(self, user_agent: str = "VittorioApicella apicellavittorio@hotmail.it"):
        """
        Initialize EDGAR downloader
        
        Args:
            user_agent: User-Agent string for SEC EDGAR requests (required by SEC)
        """
        self.user_agent = user_agent
        self.base_url = "https://www.sec.gov"
        self.data_base_url = "https://data.sec.gov"
        self.headers = {
            'User-Agent': user_agent,
            'Accept-Encoding': 'gzip, deflate',
            'Host': 'www.sec.gov'
        }
        self.data_headers = {
            'User-Agent': user_agent,
            'Accept-Encoding': 'gzip, deflate',
            'Host': 'data.sec.gov'
        }
        
        # Set up master.idx storage directory
        edgar_dir = Path(__file__).parent
        self.master_dir = edgar_dir / "master"
        self.master_dir.mkdir(exist_ok=True)
        
    def save_master_idx_to_disk(self, conn, start_year: Optional[int] = None) -> None:
        """
        Download master.idx files from SEC EDGAR, save to local storage, parse them, and save as CSV
        Only downloads new or previously failed quarters based on the ledger
        
        Args:
            conn: PostgreSQL connection for checking/updating ledger
            start_year: Start year for downloading (default: 1993)
        """
        print("Downloading master.idx files...")
        start_year = start_year or 1993
        current_year = datetime.now().year
        quarters = ['QTR1', 'QTR2', 'QTR3', 'QTR4']
        
        # Get all possible quarters
        all_quarters = []
        for year in range(start_year, current_year + 1):
            for quarter in quarters:
                # Skip future quarters
                if year == current_year:
                    current_quarter = (datetime.now().month - 1) // 3 + 1
                    quarter_num = int(quarter[3])
                    if quarter_num > current_quarter:
                        continue
                all_quarters.append((year, quarter))
        
        # Get quarters that already have data in the database
        quarters_with_data = set(get_quarters_with_data(conn, start_year))
        
        # Filter to only quarters that are missing from database
        # Check both ledger status and actual database content
        total_items = []
        for year, quarter in all_quarters:
            # Skip if data already exists in database
            if (year, quarter) in quarters_with_data:
                continue
            
            # Check ledger status - only download if pending, failed, or not in ledger
            status = get_master_idx_download_status(conn, year, quarter)
            if status is None or status['status'] in ('pending', 'failed'):
                total_items.append((year, quarter))
        
        if not total_items:
            print("No new or failed quarters to download.")
            return
        
        print(f"Found {len(total_items)} quarters to download (new or failed)")
        
        # Progress bar for downloading
        with tqdm(total=len(total_items), desc="Downloading master.idx files", unit="file") as pbar:
            for year, quarter in total_items:
                try:
                    # Try uncompressed first
                    master_url = f"{self.base_url}/Archives/edgar/full-index/{year}/{quarter}/master.idx"
                    response = requests.get(master_url, headers=self.headers, timeout=30)
                    if response.status_code == 200:
                        self._save_master_idx_content(response.content, str(year), quarter, is_compressed=False)
                        mark_master_idx_download_success(conn, year, quarter)
                        pbar.set_postfix_str(f"{year}/{quarter} (uncompressed)")
                        pbar.update(1)
                        continue
                    
                    # Try compressed version
                    master_gz_url = f"{self.data_base_url}/files/edgar/full-index/{year}/{quarter}/master.idx.gz"
                    response = requests.get(master_gz_url, headers=self.data_headers, timeout=30)
                    if response.status_code == 200:
                        self._save_master_idx_content(response.content, str(year), quarter, is_compressed=True)
                        mark_master_idx_download_success(conn, year, quarter)
                        pbar.set_postfix_str(f"{year}/{quarter} (compressed)")
                        pbar.update(1)
                    else:
                        error_msg = f"Failed to download master.idx for {year}/{quarter}: HTTP {response.status_code}"
                        mark_master_idx_download_failed(conn, year, quarter, error_msg)
                        raise Exception(error_msg)
                except Exception as e:
                    error_msg = str(e)
                    mark_master_idx_download_failed(conn, year, quarter, error_msg)
                    raise
    
    def _save_master_idx_content(self, content: bytes, year: str, quarter: str, 
                        is_compressed: bool = False) -> None:
        """
        Save master.idx file content to local storage, parse it, and save as CSV
        
        Args:
            content: File content (bytes)
            year: Year (e.g., '2024')
            quarter: Quarter (e.g., 'QTR1')
            is_compressed: Whether the content is gzipped
        """
        # Save to local file system
        # Create year directory
        year_dir = self.master_dir / year
        year_dir.mkdir(exist_ok=True)
        
        # Determine filename
        filename = f"master.idx.gz" if is_compressed else f"master.idx"
        filepath = year_dir / f"{quarter}_{filename}"
        
        # Save raw file
        with open(filepath, 'wb') as f:
            f.write(content)
        
        # Parse and save as CSV
        df = self._parse_master_idx(content)
        
        csv_filename = f"{quarter}_master_parsed.csv"
        csv_filepath = year_dir / csv_filename
        df.to_csv(csv_filepath, index=False)

    def _parse_master_idx(self, content: bytes) -> pd.DataFrame:
        """
        Parse master.idx file content into a DataFrame
        
        Args:
            content: Content of master.idx file (bytes, may be gzipped)
            
        Returns:
            DataFrame with columns: cik, company_name, form_type, filing_date, filename, accession_number
        """
        rows = []
        
        # Try to decompress if it's gzipped
        try:
            content = gzip.decompress(content)
        except (gzip.BadGzipFile, OSError):
            # Not gzipped, use as-is
            pass
        
        # Decode to string
        try:
            text = content.decode('utf-8', errors='ignore')
        except:
            text = content.decode('latin-1', errors='ignore')
        
        # Parse each line (skip header lines)
        # Expected format: CIK|Company Name|Form Type|Date Filed|Filename
        # Example: 1000045|NICHOLAS FINANCIAL INC|10-Q|2022-11-14|edgar/data/1000045/0000950170-22-024756.txt
        for line in text.split('\n'):
            line = line.strip()
            # Skip empty lines, separator lines, and header lines
            if not line or line.startswith('---') or 'CIK' in line.upper():
                continue
            
            # Format: CIK|Company Name|Form Type|Date Filed|Filename
            parts = line.split('|')
            # Must have exactly 5 parts separated by |
            if len(parts) != 5:
                continue
            
            # Validate pattern: CIK should be numeric, filename should start with 'edgar/data/'
            line_cik = parts[0].strip()
            filename = parts[4].strip()
            
            # Skip if CIK is not numeric or filename doesn't match expected pattern
            if not line_cik.isdigit() or not filename.startswith('edgar/data/'):
                continue
            
            # Extract all parts
            company_name = parts[1].strip()
            form_type = parts[2].strip()
            date_filed_str = parts[3].strip()
            
            # Normalize CIK to 10 digits
            cik_int = int(line_cik)  # Remove leading zeros if any
            cik_normalized = str(cik_int).zfill(10)
        
            
            # Parse date (format: YYYYMMDD or YYYY-MM-DD)
            filing_date = None
            if len(date_filed_str) == 8 and date_filed_str.isdigit():
                # YYYYMMDD format
                filing_date = f"{date_filed_str[0:4]}-{date_filed_str[4:6]}-{date_filed_str[6:8]}"
            elif len(date_filed_str) == 10 and date_filed_str[4] == '-' and date_filed_str[7] == '-':
                # YYYY-MM-DD format
                filing_date = date_filed_str
            else:
                continue  # Skip invalid dates
            
            # Extract accession number from filename
            # Format: edgar/data/{cik}/{accession_number}.txt
            # Example: edgar/data/926688/9999999997-05-015654.txt
            accession_number = None
            if filename:
                path_parts = filename.split('/')
                if len(path_parts) >= 4:
                    # Get the filename part (last element)
                    filename_part = path_parts[-1]
                    # Remove .txt extension to get accession number
                    if filename_part.endswith('.txt'):
                        accession_number = filename_part[:-4]  # Remove '.txt'
                    else:
                        accession_number = filename_part
            
            if not accession_number:
                continue  # Skip if we can't extract accession number
            
            rows.append({
                'cik': cik_normalized,
                'company_name': company_name,
                'form_type': form_type,
                'filing_date': filing_date,
                'filename': filename,
                'accession_number': accession_number
            })
            
        
        # Create DataFrame
        if rows:
            df = pd.DataFrame(rows)
            return df
        else:
            return pd.DataFrame(columns=['cik', 'company_name', 'form_type', 'filing_date', 'filename', 'accession_number'])

    def _save_master_idx_to_db(self, conn) -> None:
        """
        Load all parsed CSV files and save them to database
        
        Args:
            conn: PostgreSQL connection
        """
        if not self.master_dir.exists():
            return
        
        # Get quarters that already have data in the database
        quarters_with_data = set(get_quarters_with_data(conn))
        
        # Collect all CSV files first, but only for quarters missing from database
        csv_files = []
        for year_dir in sorted(self.master_dir.iterdir()):
            if not year_dir.is_dir():
                continue
            
            year = year_dir.name
            try:
                year_int = int(year)
            except ValueError:
                continue
            
            # Find all CSV files in this year directory
            for filepath in sorted(year_dir.iterdir()):
                if not filepath.is_file():
                    continue
                
                # Check if it's a parsed CSV file (QTR1_master_parsed.csv, etc.)
                filename = filepath.name
                if not filename.endswith('_master_parsed.csv'):
                    continue
                
                # Extract quarter from filename (QTR1, QTR2, QTR3, QTR4)
                quarter_match = re.match(r'QTR[1-4]', filename)
                if not quarter_match:
                    continue
                quarter = quarter_match.group(0)
                
                # Only process if data doesn't already exist in database
                if (year_int, quarter) not in quarters_with_data:
                    csv_files.append((year_int, quarter, filepath))
                else:
                    print(f"Skipping {year_int}/{quarter} - data already exists in database")
        
        # Progress bar for database saving
        with tqdm(total=len(csv_files), desc="Saving to database", unit="file") as pbar:
            for year_int, quarter, filepath in csv_files:
                # Read CSV file
                df = pd.read_csv(filepath)
                
                # Save to database if DataFrame is not empty
                if df is not None and not df.empty:
                    cur = conn.cursor()
                    try:
                        # Prepare data for bulk insert
                        records = []
                        for _, row in df.iterrows():
                            records.append((
                                year_int,
                                quarter,
                                row['cik'],                    # CIK
                                row['company_name'],           # Company Name
                                row['form_type'],              # Form Type
                                row['filing_date'],            # Date Filed
                                row['filename'],               # Filename
                                row.get('accession_number', '')  # Additional: accession_number
                            ))
                        
                        # Bulk insert with conflict handling (skip duplicates)
                        execute_values(
                            cur,
                            """
                            INSERT INTO master_idx_files 
                                (year, quarter, cik, company_name, form_type, date_filed, filename, accession_number)
                            VALUES %s
                            ON CONFLICT (year, quarter, cik, form_type, date_filed, filename) DO NOTHING
                            """,
                            records
                        )
                        conn.commit()
                        pbar.set_postfix_str(f"{year_int}/{quarter} ({len(df)} records)")
                    except Exception as e:
                        conn.rollback()
                        raise e
                    finally:
                        cur.close()
                else:
                    pbar.set_postfix_str(f"{year_int}/{quarter} (empty)")
                pbar.update(1)


    def get_filing_url(self, cik: str, accession_number: str, filing_type: str = '') -> Optional[str]:
        """
        Get filing URL for a specific filing
        
        Args:
            cik: Company CIK
            accession_number: Filing accession number (e.g., 0001234567-12-000001)
            filing_type: Type of filing (e.g., '10-K', '4', '8-K')
        
        Returns:
            Tuple of (URL, file_extension) or (None, None)
        """
        try:
            # Format: https://www.sec.gov/Archives/edgar/data/{CIK}/{accession_number}/...
            # Remove dashes from accession number for folder path
            accession_clean = accession_number.replace('-', '')
            cik_int = int(cik.lstrip('0'))  # Remove leading zeros for URL
            
            # Base path for the filing
            base_path = f"{self.base_url}/Archives/edgar/data/{cik_int}/{accession_clean}"
            
            # Form 144 is a folder - need to download all files in the folder
            if filing_type and filing_type.upper() in ['144', '144/A']:
                # Form 144 URL is just the folder path
                return base_path, 'folder'
            
            # Form 4 and some other forms are XML/TXT files directly (no zip)
            # Check if this is a form type that uses XML directly
            if filing_type and filing_type.upper() in ['4', '4/A', '3', '3/A', '5', '5/A']:
                # Form 4 files are stored as .txt files (even though they contain XML)
                # URL format: base_path/accession_number.txt
                txt_url = f"{base_path}/{accession_number}.txt"
                return txt_url, '.txt'
            
            # XBRL files are typically named: {accession_number}-xbrl.zip
            xbrl_zip_url = f"{base_path}/{accession_number}-xbrl.zip"
            
            # Check if XBRL zip exists
            try:
                test_response = requests.head(xbrl_zip_url, headers=self.headers, timeout=10)
                if test_response.status_code == 200:
                    return xbrl_zip_url, '.zip'
            except:
                pass
            
            return None, None
            
        except Exception as e:
            return None, None
    

def main():
    """Main function to download EDGAR filings"""
    import argparse
    import traceback as tb
    
    parser = argparse.ArgumentParser(
        description='Download SEC EDGAR filings',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate catalog with companies and filings:
  python -m trading_agent.fundamentals.edgar --generate-catalog --download-companies
  
  # Generate catalog using existing companies (no company download):
  python -m trading_agent.fundamentals.edgar --generate-catalog
  
  # Download filings from database:
  python -m trading_agent.fundamentals.edgar --from-db
        """
    )
    parser.add_argument('--start-year', type=int, default=None,
                       help='Start year for filings (default: None = all available from earliest date, typically 1993)')
    parser.add_argument('--output-dir', type=str, default='edgar_filings',
                       help='Output directory for downloaded filing files (default: edgar_filings)')
    parser.add_argument('--user-agent', type=str,
                       default='VittorioApicella apicellavittorio@hotmail.it',
                       help='User-Agent string for SEC EDGAR requests (required by SEC). '
                            'Default: VittorioApicella apicellavittorio@hotmail.it')
    
    # Catalog generation arguments
    catalog_group = parser.add_argument_group('Catalog Generation',
                                             'Options for --generate-catalog mode')
    catalog_group.add_argument('--generate-catalog', action='store_true',
                              help='Generate catalog of companies and filings, save to PostgreSQL database. '
                                   'This mode processes filings from SEC EDGAR and stores metadata in the database.')
    catalog_group.add_argument('--download-companies', action='store_true',
                              help='[REQUIRED for first run] Fetch and enrich companies from EDGAR when generating catalog. '
                                   'If not specified, only existing companies from the database are used. '
                                   'Company details (ticker, SIC code, entity type) are fetched and updated. '
                                   'New companies are added, and existing companies are updated if details change.')
    
    # Database connection arguments
    db_group = parser.add_argument_group('Database Connection',
                                        'PostgreSQL database connection options')
    db_group.add_argument('--dbname', type=str, default='edgar',
                         help='PostgreSQL database name (default: edgar)')
    db_group.add_argument('--dbuser', type=str, default='postgres',
                         help='PostgreSQL database user (default: postgres)')
    db_group.add_argument('--dbhost', type=str, default='localhost',
                         help='PostgreSQL database host (default: localhost)')
    db_group.add_argument('--dbpassword', type=str, default=None,
                         help='PostgreSQL database password. If not provided, uses POSTGRES_PASSWORD environment variable.')
    db_group.add_argument('--dbport', type=int, default=None,
                         help='PostgreSQL database port. If not provided, uses POSTGRES_PORT environment variable or defaults to 5432.')
    
    # Other modes
    parser.add_argument('--from-db', action='store_true',
                       help='Download actual filing files from companies already in PostgreSQL database. '
                            'Use this after generating the catalog to download the filing documents themselves.')
    parser.add_argument('--process-zips', type=str,
                       help='Process existing ZIP files in a directory. Specify directory path. '
                            'Processes recursively by default. Extracts and processes filing documents from ZIP archives.')
    parser.add_argument('--no-recursive', action='store_true',
                       help='When used with --process-zips, only process ZIPs in the specified directory (not subdirectories). '
                            'By default, processing is recursive.')
    args = parser.parse_args()
    
    try:
        downloader = EDGARDownloader(user_agent=args.user_agent)
        
        # If process-zips is requested, process existing ZIP files
        if args.process_zips:
            stats = downloader.process_zip_files(args.process_zips, recursive=not args.no_recursive)
            return 0
        
        # If generate-catalog is requested, generate the catalog in PostgreSQL
        if args.generate_catalog:
            print("Generating catalog with all companies and filings...")
            
            # Download and save master.idx files
            try:
                conn = get_postgres_connection(
                    dbname=args.dbname,
                    user=args.dbuser,
                    host=args.dbhost,
                    password=args.dbpassword,
                    port=args.dbport
                )
                # Initialize tables including ledger
                init_edgar_postgres_tables(conn)
                # Download only new/failed quarters
                downloader.save_master_idx_to_disk(conn, start_year=args.start_year)
                # Save parsed master.idx CSV files to database
                downloader._save_master_idx_to_db(conn)
                conn.close()
            except Exception as e:
                print(f"  Error: Failed to process master.idx files: {e}")
                raise
            
        
    except Exception as e:
        print(f"Error: {e}")
        tb.print_exc()
        return 1


if __name__ == "__main__":
    main()

