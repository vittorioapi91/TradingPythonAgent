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
    # File is at: src/fundamentals/edgar/edgar.py
    # Need to go up 5 levels to get to project root
    file_path = Path(__file__).resolve()
    project_root = file_path.parent.parent.parent.parent.parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))
    try:
        from src.config import load_environment_config
        load_environment_config()
    except ImportError as e:
        # Config module is required - fail if not found
        raise ImportError(
            f"Failed to import config module. This is required for environment configuration. "
            f"Original error: {e}. "
            f"Please ensure the config module exists at src/config.py"
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
from html.parser import HTMLParser
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import pandas as pd
from pathlib import Path
import psycopg2

from tqdm import tqdm

# Handle import for both module import and direct script execution
try:
    from .edgar_postgres import (
        get_postgres_connection, init_edgar_postgres_tables
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
    from src.fundamentals.edgar.edgar_postgres import (
        get_postgres_connection, init_edgar_postgres_tables
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
  python -m src.fundamentals.edgar --generate-catalog --download-companies
  
  # Generate catalog using existing companies (no company download):
  python -m src.fundamentals.edgar --generate-catalog
  
  # Download filings from database with filters:
  python -m src.fundamentals.edgar --year 2005 --quarter QTR2 --form-type 10-K --output-dir ./filings
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
    
    # Filing download filter arguments
    download_group = parser.add_argument_group('Filing Download Filters',
                                              'Filter options for downloading filings from database')
    download_group.add_argument('--year', type=int, default=None,
                                help='Year filter for filings (e.g., 2005)')
    download_group.add_argument('--quarter', type=str, default=None,
                               help='Quarter filter for filings (e.g., QTR1, QTR2, QTR3, QTR4)')
    download_group.add_argument('--form-type', type=str, default=None,
                               help='Form type filter for filings (e.g., 10-K, 10-Q)')
    download_group.add_argument('--cik', type=str, default=None,
                               help='CIK (Central Index Key) filter for filings')
    download_group.add_argument('--company-name', type=str, default=None,
                               help='Company name filter (partial match, case-insensitive)')
    download_group.add_argument('--limit', type=int, default=None,
                               help='Limit number of filings to download')
    
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
    parser.add_argument('--filings', action='store_true',
                       help='Download actual filing files from companies in PostgreSQL database. '
                            'Use this with filter arguments (--year, --quarter, --form-type, etc.) to download specific filings.')
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
                # Import MasterIdxManager
                try:
                    from .master_idx import MasterIdxManager
                except ImportError:
                    from src.fundamentals.edgar.master_idx import MasterIdxManager
                
                conn = get_postgres_connection(
                    dbname=args.dbname,
                    user=args.dbuser,
                    host=args.dbhost,
                    password=args.dbpassword,
                    port=args.dbport
                )
                # Initialize tables including ledger
                init_edgar_postgres_tables(conn)
                
                # Use MasterIdxManager for master.idx operations
                master_idx_manager = MasterIdxManager(user_agent=args.user_agent)
                # Download only new/failed quarters
                master_idx_manager.save_master_idx_to_disk(conn, start_year=args.start_year)
                # Save parsed master.idx CSV files to database
                master_idx_manager.save_master_idx_to_db(conn)
                conn.close()
            except Exception as e:
                print(f"  Error: Failed to process master.idx files: {e}")
                raise
        
        # Download filings from database (only if --filings is specified)
        if args.filings:
            # Import FilingDownloader
            try:
                from .filings import FilingDownloader
            except ImportError:
                from src.fundamentals.edgar.filings import FilingDownloader
            
            print("Downloading filings from database...")
            filing_downloader = FilingDownloader(user_agent=args.user_agent)
            
            # Build filter dictionary
            filters = {}
            if args.year is not None:
                filters['year'] = args.year
            if args.quarter is not None:
                filters['quarter'] = args.quarter
            if args.form_type is not None:
                filters['form_type'] = args.form_type
            if args.cik is not None:
                filters['cik'] = args.cik
            if args.company_name is not None:
                filters['company_name'] = args.company_name
            
            # Download filings
            downloaded_files = filing_downloader.download_filings(
                dbname=args.dbname,
                output_dir=args.output_dir,
                limit=args.limit,
                **filters
            )
            
            print(f"Successfully downloaded {len(downloaded_files)} filing(s)")
            return 0
            
        
    except Exception as e:
        print(f"Error: {e}")
        tb.print_exc()
        return 1


if __name__ == "__main__":
    main()

