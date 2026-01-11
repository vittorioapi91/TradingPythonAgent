"""
SEC EDGAR Filings Downloader

This module downloads all company filings from SEC EDGAR database.
Focuses on XBRL filings from 2009 onwards.
"""

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

from tqdm import tqdm

# Handle import for both module import and direct script execution
try:
    from .download_logger import get_download_logger
    from .edgar_postgres import (
        get_postgres_connection, init_edgar_postgres_tables,
        add_companies_fast, add_filings_fast, get_existing_accessions,
        load_companies_from_postgres, load_filings_from_postgres,
        add_company_history_snapshot, update_edgar_metadata,
        get_edgar_metadata, get_edgar_statistics, update_filing_downloaded_path,
        get_processed_years, mark_year_processed, get_enriched_ciks,
        get_year_completion_status, update_year_completion_ledger,
        get_db_filing_counts_by_year, is_year_complete, get_incomplete_years
    )
except ImportError:
    from download_logger import get_download_logger
    from edgar_postgres import (
        get_postgres_connection, init_edgar_postgres_tables,
        add_companies_fast, add_filings_fast, get_existing_accessions,
        load_companies_from_postgres, load_filings_from_postgres,
        add_company_history_snapshot, update_edgar_metadata,
        get_edgar_metadata, get_edgar_statistics, update_filing_downloaded_path,
        get_processed_years, mark_year_processed, get_enriched_ciks,
        get_year_completion_status, update_year_completion_ledger,
        get_db_filing_counts_by_year, is_year_complete, get_incomplete_years
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
        
    def _parse_directory_listing(self, html_content: str) -> List[str]:
        """
        Parse HTML directory listing to extract directory/file names
        
        Args:
            html_content: HTML content from directory listing
            
        Returns:
            List of directory/file names
        """
        class DirectoryParser(HTMLParser):
            def __init__(self):
                super().__init__()
                self.items = []
                self.in_link = False
                
            def handle_starttag(self, tag, attrs):
                if tag == 'a':
                    self.in_link = True
                    
            def handle_endtag(self, tag):
                if tag == 'a':
                    self.in_link = False
                    
            def handle_data(self, data):
                if self.in_link:
                    item = data.strip()
                    # Skip parent directory link and empty items
                    if item and item != 'Parent Directory' and not item.startswith('?'):
                        # Remove trailing slash for directories
                        if item.endswith('/'):
                            item = item[:-1]
                        self.items.append(item)
        
        parser = DirectoryParser()
        parser.feed(html_content)
        return parser.items
    
    def _parse_master_idx(self, content: bytes, target_forms: Optional[Set[str]] = None) -> Dict[str, str]:
        """
        Parse master.idx file content to extract CIKs and company names for target form types
        
        Args:
            content: Content of master.idx file (bytes, may be gzipped)
            target_forms: Optional set of form types to filter (None for all forms)
            
        Returns:
            Dictionary mapping CIK (as 10-digit string) to company name
        """
        companies = {}
        
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
        for line in text.split('\n'):
            line = line.strip()
            if not line or line.startswith('---') or 'CIK' in line.upper():
                continue
            
            # Format: CIK|Company Name|Form Type|Date Filed|Filename
            parts = line.split('|')
            if len(parts) >= 3:
                try:
                    cik = parts[0].strip()
                    company_name = parts[1].strip()
                    form_type = parts[2].strip()
                    
                    # Check if this form type matches our target forms (or include all if None)
                    if target_forms is None or form_type in target_forms:
                        # Normalize CIK to 10 digits (master.idx CIKs don't have leading zeros)
                        # Convert to int first to remove any leading zeros, then pad to 10 digits
                        try:
                            cik_int = int(cik)  # Remove leading zeros if any
                            cik_normalized = str(cik_int).zfill(10)
                        except ValueError:
                            # If not a number, just pad as-is
                            cik_normalized = cik.zfill(10)
                        # Store company name (keep first occurrence or most recent)
                        if cik_normalized not in companies:
                            companies[cik_normalized] = company_name
                except (ValueError, IndexError):
                    continue
        
        return companies
    
    def _count_filings_from_master_idx(self, content: bytes, year: int, 
                                       target_forms: Optional[Set[str]] = None) -> Dict[str, int]:
        """
        Count filings from master.idx file content by filing type for a specific year
        
        Args:
            content: Content of master.idx file (bytes, may be gzipped)
            year: Year to filter by
            target_forms: Optional set of form types to filter (None for all)
            
        Returns:
            Dictionary mapping filing_type to count
        """
        counts = {}
        
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
        for line in text.split('\n'):
            line = line.strip()
            if not line or line.startswith('---') or 'CIK' in line.upper():
                continue
            
            # Format: CIK|Company Name|Form Type|Date Filed|Filename
            parts = line.split('|')
            if len(parts) >= 4:
                try:
                    form_type = parts[2].strip()
                    date_filed = parts[3].strip()
                    
                    # Check if date matches year
                    if date_filed.startswith(str(year)):
                        # Check if form type matches target forms (if specified)
                        if target_forms is None or form_type in target_forms:
                            counts[form_type] = counts.get(form_type, 0) + 1
                except (ValueError, IndexError):
                    continue
        
        return counts
    
    async def count_sec_index_filings_by_year(self, year: int, 
                                               filing_types: Optional[List[str]] = None) -> Dict[str, int]:
        """
        Count filings from SEC index for a specific year, grouped by filing type
        
        Args:
            year: Year to count
            filing_types: Optional list of filing types to filter (None for all)
            
        Returns:
            Dictionary mapping filing_type to count
        """
        target_forms = set(filing_types) if filing_types else None
        counts = {}
        
        try:
            # Base URL for full-index
            base_url = f"{self.base_url}/Archives/edgar/full-index/"
            year_url = f"{base_url}{year}/"
            
            # Get list of quarters (async)
            async with aiohttp.ClientSession() as session:
                async with session.get(year_url, headers=self.headers, timeout=aiohttp.ClientTimeout(total=30)) as response:
                    response.raise_for_status()
                    text = await response.text()
                    quarters = self._parse_directory_listing(text)
                    quarters = [q for q in quarters if q.startswith('QTR')]
                
                # Process each quarter concurrently
                async def process_quarter(quarter: str):
                    quarter_url = f"{year_url}{quarter}/"
                    quarter_counts = {}
                    
                    try:
                        # Try to get master.idx (uncompressed) first
                        master_url = f"{quarter_url}master.idx"
                        async with session.get(master_url, headers=self.headers, timeout=aiohttp.ClientTimeout(total=30)) as response:
                            if response.status == 200:
                                content = await response.read()
                                quarter_counts = self._count_filings_from_master_idx(
                                    content, year, target_forms
                                )
                            else:
                                # Try compressed version
                                master_gz_url = f"{quarter_url}master.idx.gz"
                                async with session.get(master_gz_url, headers=self.headers, timeout=aiohttp.ClientTimeout(total=30)) as gz_response:
                                    if gz_response.status == 200:
                                        content = await gz_response.read()
                                        quarter_counts = self._count_filings_from_master_idx(
                                            content, year, target_forms
                                        )
                    except Exception as e:
                        # Continue on error
                        pass
                    
                    return quarter_counts
                
                # Process all quarters concurrently
                tasks = [process_quarter(quarter) for quarter in quarters]
                quarter_results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Aggregate counts
                for quarter_counts in quarter_results:
                    if isinstance(quarter_counts, dict):
                        for form_type, count in quarter_counts.items():
                            counts[form_type] = counts.get(form_type, 0) + count
                    
        except Exception as e:
            print(f"Error counting SEC index filings for year {year}: {e}")
        
        return counts
    
    def _get_company_ticker(self, cik: str) -> Optional[str]:
        """
        Fetch ticker symbol for a CIK from company_tickers.json
        
        Args:
            cik: Company CIK (10-digit zero-padded)
            
        Returns:
            Ticker symbol if found, None otherwise
        """
        # Ensure CIK is 10-digit zero-padded
        cik_normalized = str(cik).zfill(10)
        
        try:
            ticker_url = f"{self.base_url}/files/company_tickers.json"
            response = requests.get(ticker_url, headers=self.headers, timeout=30)
            response.raise_for_status()
            ticker_data = response.json()
            
            for entry in ticker_data.values():
                entry_cik = str(entry.get('cik_str', '')).zfill(10)
                if entry_cik == cik_normalized:
                    ticker = entry.get('ticker', '')
                    if ticker:
                        return ticker
            return None
        except Exception as e:
            # Log error but don't fail
            return None
    
    async def _fetch_ticker_map_async(self) -> Dict[str, str]:
        """
        Async fetch of company_tickers.json to get ticker mappings
        
        Returns:
            Dictionary mapping CIK -> ticker
        """
        ticker_map = {}
        try:
            ticker_url = f"{self.base_url}/files/company_tickers.json"
            async with aiohttp.ClientSession() as session:
                async with session.get(ticker_url, headers=self.headers, timeout=aiohttp.ClientTimeout(total=30)) as response:
                    response.raise_for_status()
                    ticker_data = await response.json()
                    
                    for entry in ticker_data.values():
                        # company_tickers.json has 'cik_str' which is already a string, may or may not have leading zeros
                        # Normalize to ensure consistent 10-digit format
                        cik_value = entry.get('cik_str', '')
                        if cik_value:
                            # Convert to int first to remove any leading zeros, then pad to 10 digits
                            try:
                                cik_int = int(cik_value)  # Remove leading zeros if any
                                cik_str = str(cik_int).zfill(10)
                            except (ValueError, TypeError):
                                # If not a number, try to pad as string
                                cik_str = str(cik_value).zfill(10)
                            ticker = entry.get('ticker', '')
                            if cik_str and ticker:
                                ticker_map[cik_str] = ticker
        except Exception as e:
            print(f"  Warning: Could not fetch ticker information: {e}")
        return ticker_map
    
    async def _get_company_details_async(self, cik: str) -> Dict[str, Optional[str]]:
        """
        Async fetch of company details from SEC Submissions API
        
        Args:
            cik: Company CIK (10-digit zero-padded)
            
        Returns:
            Dictionary with 'name', 'sic_code', 'entity_type' (None if not available)
        """
        # Ensure CIK is 10-digit zero-padded
        cik_normalized = str(cik).zfill(10)
        
        submissions_url = f"{self.data_base_url}/submissions/CIK{cik_normalized}.json"
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(submissions_url, headers=self.data_headers, timeout=aiohttp.ClientTimeout(total=10)) as response:
                    if response.status == 200:
                        data = await response.json()
                        
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
                        return {
                            'name': None,
                            'sic_code': None,
                            'entity_type': None
                        }
        except Exception as e:
            return {
                'name': None,
                'sic_code': None,
                'entity_type': None
            }
    
    async def _fetch_company_details_async(self, all_companies: Dict[str, str], ticker_map: Dict[str, str],
                                          enriched_ciks: Optional[Set[str]] = None,
                                          conn = None) -> tuple:
        """
        Async fetch of company details from Submissions API
        
        Args:
            all_companies: Dictionary of CIK -> company name
            ticker_map: Dictionary of CIK -> ticker
            enriched_ciks: Set of CIKs that are already enriched (will skip fetching details for these)
            conn: Optional PostgreSQL connection to load already enriched companies from database
            
        Returns:
            Tuple of (companies list, details_fetched count, details_failed count)
        """
        companies = []
        details_fetched = 0
        details_failed = 0
        
        enriched_ciks = enriched_ciks or set()
        
        # Load already enriched companies from database if connection is provided
        enriched_companies_dict = {}
        if conn and enriched_ciks:
            from psycopg2.extras import RealDictCursor
            cur = conn.cursor(cursor_factory=RealDictCursor)
            placeholders = ','.join(['%s'] * len(enriched_ciks))
            query = f"""
                SELECT cik, ticker, name, sic_code, entity_type
                FROM companies
                WHERE cik IN ({placeholders})
            """
            cur.execute(query, list(enriched_ciks))
            for row in cur.fetchall():
                cik = str(row['cik']).zfill(10)
                enriched_companies_dict[cik] = {
                    'cik': cik,
                    'ticker': row['ticker'],
                    'name': row['name'],
                    'sic_code': row['sic_code'],
                    'entity_type': row['entity_type'],
                    'title': row['name']  # For compatibility
                }
            cur.close()
            if enriched_companies_dict:
                companies.extend(enriched_companies_dict.values())
                print(f"  Loaded {len(enriched_companies_dict)} already enriched companies from database")
        
        # DIAGNOSTIC: Check normalization before filtering
        print(f"\n  === DIAGNOSTIC: Enrichment Processing ===")
        print(f"  all_companies: {len(all_companies)} CIKs")
        print(f"  enriched_ciks (raw): {len(enriched_ciks)} CIKs")
        
        # Normalize enriched_ciks for comparison
        enriched_ciks_normalized = {str(cik).zfill(10) for cik in enriched_ciks}
        print(f"  enriched_ciks (normalized): {len(enriched_ciks_normalized)} CIKs")
        
        # Check how many all_companies CIKs are in enriched_ciks (with normalization)
        all_companies_normalized_set = {str(cik).zfill(10) for cik in all_companies.keys()}
        already_enriched_count = len(all_companies_normalized_set & enriched_ciks_normalized)
        print(f"  CIKs in all_companies that are already enriched: {already_enriched_count}")
        print(f"  CIKs in all_companies that need enrichment: {len(all_companies) - already_enriched_count}")
        
        # Check how many CIKs in all_companies have tickers
        # IMPORTANT: ticker_map keys are already normalized, so we need to normalize all_companies keys for lookup
        all_companies_with_tickers = sum(1 for cik in all_companies.keys() 
                                        if str(cik).zfill(10) in ticker_map)
        print(f"  CIKs in all_companies that have tickers: {all_companies_with_tickers}")
        
        # DEBUG: Show a few examples of CIKs that should match but don't
        if all_companies_with_tickers < len(all_companies):
            sample_ciks = list(all_companies.keys())[:10]
            print(f"  DEBUG: Checking first 10 CIKs from all_companies:")
            for cik in sample_ciks:
                cik_norm = str(cik).zfill(10)
                in_ticker = cik_norm in ticker_map
                ticker_value = ticker_map.get(cik_norm, "NOT FOUND")
                print(f"    CIK: {cik} (type: {type(cik).__name__}) -> normalized: {cik_norm} -> in ticker_map: {in_ticker} -> ticker: {ticker_value}")
        print(f"  === END DIAGNOSTIC ===\n")
        
        # Filter out already enriched CIKs from the list to process
        # IMPORTANT: Compare normalized CIKs
        ciks_to_enrich = {
            cik: name for cik, name in all_companies.items()
            if str(cik).zfill(10) not in enriched_ciks_normalized
        }
        
        if not ciks_to_enrich:
            print(f"  All {len(all_companies)} companies are already enriched, skipping API calls")
            return companies, details_fetched, details_failed
        
        print(f"  Fetching details for {len(ciks_to_enrich)} companies (skipping {len(enriched_ciks_normalized)} already enriched)")
        
        # Semaphore to limit concurrent requests (10 req/sec = max 10 concurrent)
        semaphore = asyncio.Semaphore(10)
        
        async def fetch_single_company(cik: str, name: str) -> tuple:
            """Fetch details for a single company"""
            async with semaphore:
                # Normalize CIK first (needed for both API call and ticker lookup)
                cik_normalized = str(cik).zfill(10)
                details = await self._get_company_details_async(cik_normalized)
                
                # Use name from Submissions API if available, otherwise use name from master.idx
                company_name = details.get('name') or name
                
                # Look up ticker using normalized CIK
                ticker = ticker_map.get(cik_normalized) if ticker_map.get(cik_normalized) else None
                
                company_data = {
                    'cik': cik_normalized,  # Store normalized CIK
                    'ticker': ticker,
                    'name': company_name,
                    'sic_code': details.get('sic_code'),
                    'entity_type': details.get('entity_type'),
                    'title': company_name  # For compatibility
                }
                
                # Small delay to respect SEC rate limit
                await asyncio.sleep(0.1)
                return company_data, details
        
        # Create tasks only for companies that need enrichment
        tasks = [fetch_single_company(cik, name) for cik, name in ciks_to_enrich.items()]
        
        # Process with progress bar
        with tqdm(total=len(tasks), desc="Fetching company details", unit="CIK", leave=False) as pbar:
            for coro in asyncio.as_completed(tasks):
                try:
                    company_data, details = await coro
                    companies.append(company_data)
                    
                    if details.get('sic_code') or details.get('entity_type'):
                        details_fetched += 1
                    else:
                        details_failed += 1
                    pbar.update(1)
                except Exception as e:
                    details_failed += 1
                    pbar.update(1)
        
        return companies, details_fetched, details_failed
    
    def _get_company_details(self, cik: str) -> Dict[str, Optional[str]]:
        """
        Fetch company details from SEC Submissions API (SIC code, entityType, name)
        
        Args:
            cik: Company CIK (10-digit zero-padded)
            
        Returns:
            Dictionary with 'name', 'sic_code', 'entity_type' (None if not available)
        """
        # Ensure CIK is 10-digit zero-padded
        cik_normalized = str(cik).zfill(10)
        
        submissions_url = f"{self.data_base_url}/submissions/CIK{cik_normalized}.json"
        
        try:
            response = requests.get(submissions_url, headers=self.data_headers, timeout=10)
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
    
    async def get_all_companies_async(self, conn = None) -> List[Dict[str, str]]:
        """
        Async version: Get list of ALL companies (CIKs) from EDGAR full-index (all form types)
        
        This method:
        1. Parses all master.idx files once to extract all unique CIKs and company names
        2. Enriches with ticker information from company_tickers.json
        3. Enriches with SIC code and entity_type from SEC Submissions API
        
        This retrieves CIKs from the full-index archive, which includes delisted and defaulted companies
        that are not in company_tickers.json. The details fetching phase runs asynchronously.
        
        If a database connection is provided, CIKs that are already enriched will be skipped during enrichment,
        making the process resumable.
        
        Args:
            conn: Optional PostgreSQL connection to check for already enriched CIKs
        
        Returns:
            List of company dictionaries with CIK, name, ticker, sic_code, entity_type
        """
        print("Fetching all companies from EDGAR full-index...")
        print("  Parsing all master.idx files to extract ALL companies (all form types)")
        
        # Use DataFrame to collect all companies from all master.idx files
        all_companies_df_list = []
        
        try:
            # Base URL for full-index
            base_url = f"{self.base_url}/Archives/edgar/full-index/"
            
            # Get list of years
            print("  Fetching year directories...")
            response = requests.get(base_url, headers=self.headers, timeout=30)
            response.raise_for_status()
            
            years = self._parse_directory_listing(response.text)
            # Filter to only numeric years (4 digits)
            years = [y for y in years if y.isdigit() and len(y) == 4]
            years.sort(reverse=True)  # Process recent years first
            
            print(f"  Found {len(years)} years: {min(years)} to {max(years)}")
            
            # Process each year
            for year in tqdm(years, desc="Processing years", unit="year"):
                year_url = f"{base_url}{year}/"
                
                try:
                    # Get list of quarters
                    response = requests.get(year_url, headers=self.headers, timeout=30)
                    response.raise_for_status()
                    
                    quarters = self._parse_directory_listing(response.text)
                    quarters = [q for q in quarters if q.startswith('QTR')]
                    
                    # Process each quarter
                    for quarter in quarters:
                        quarter_url = f"{year_url}{quarter}/"
                        
                        try:
                            # Try to get master.idx (uncompressed) first
                            master_url = f"{quarter_url}master.idx"
                            response = requests.get(master_url, headers=self.headers, timeout=30)
                            
                            if response.status_code == 200:
                                # Parse master.idx to DataFrame (all form types, no filtering)
                                try:
                                    df = self._parse_master_idx_to_dataframe(response.content)
                                    if not df.empty:
                                        all_companies_df_list.append(df)
                                except Exception:
                                    pass
                            else:
                                # Try compressed version
                                master_gz_url = f"{quarter_url}master.idx.gz"
                                response = requests.get(master_gz_url, headers=self.headers, timeout=30)
                                
                                if response.status_code == 200:
                                    # Parse compressed master.idx to DataFrame (all form types, no filtering)
                                    try:
                                        df = self._parse_master_idx_to_dataframe(response.content)
                                        if not df.empty:
                                            all_companies_df_list.append(df)
                                    except Exception:
                                        pass
                                    
                        except Exception as e:
                            # Continue with next quarter on error
                            continue
                            
                except Exception as e:
                    # Continue with next year on error
                    continue
            
            # Combine all DataFrames and extract unique CIKs and company names
            if all_companies_df_list:
                print("  Combining all master.idx files and extracting unique companies...")
                combined_df = pd.concat(all_companies_df_list, ignore_index=True)
                
                # Get unique CIKs and company names (keep most recent company name for each CIK)
                # Group by CIK and take the last company_name (most recent)
                unique_companies_df = combined_df.groupby('cik').agg({
                    'company_name': 'last'  # Take the last (most recent) company name
                }).reset_index()
                
                # Convert to dict for compatibility with existing code
                all_companies = dict(zip(unique_companies_df['cik'], unique_companies_df['company_name']))
                print(f"  Found {len(all_companies)} unique companies from master.idx files")
            else:
                print("  No companies found in master.idx files")
                all_companies = {}
            
            # Get already enriched CIKs from database if connection is provided
            enriched_ciks = set()
            if conn:
                enriched_ciks = get_enriched_ciks(conn)
                if enriched_ciks:
                    print(f"  Found {len(enriched_ciks)} CIKs already enriched in database, will skip enrichment for these")
            
            # Enrich with ticker information from company_tickers.json (async)
            print("  Enriching with ticker information from company_tickers.json...")
            ticker_map = await self._fetch_ticker_map_async()
            print(f"  Found {len(ticker_map)} ticker mappings")
            
            # DIAGNOSTIC: Analyze CIK normalization and overlap
            print("\n  === DIAGNOSTIC: CIK Analysis ===")
            print(f"  all_companies: {len(all_companies)} CIKs (from full-index)")
            print(f"  ticker_map: {len(ticker_map)} CIKs (from company_tickers.json)")
            
            # Normalize all_companies CIKs to 10 digits for comparison
            # Note: all_companies keys are already normalized from _parse_master_idx, but ensure consistency
            all_companies_normalized = {str(cik).zfill(10): name for cik, name in all_companies.items()}
            # ticker_map keys are already normalized, but ensure consistency
            ticker_map_normalized = {str(cik).zfill(10): ticker for cik, ticker in ticker_map.items()}
            
            # Check overlap
            overlap = set(all_companies_normalized.keys()) & set(ticker_map_normalized.keys())
            only_in_all_companies = set(all_companies_normalized.keys()) - set(ticker_map_normalized.keys())
            only_in_ticker_map = set(ticker_map_normalized.keys()) - set(all_companies_normalized.keys())
            
            print(f"  Overlap (in both): {len(overlap)} CIKs")
            print(f"  Only in all_companies: {len(only_in_all_companies)} CIKs")
            print(f"  Only in ticker_map: {len(only_in_ticker_map)} CIKs")
            
            # Check for normalization issues - sample some CIKs and show actual values
            if len(all_companies) > 0:
                sample_ciks = list(all_companies.keys())[:5]
                print(f"  Sample all_companies CIKs (raw keys): {sample_ciks}")
                print(f"  Sample all_companies CIKs (types): {[type(c).__name__ for c in sample_ciks]}")
                print(f"  Sample all_companies CIKs (normalized): {[str(c).zfill(10) for c in sample_ciks]}")
                # Check if any are in ticker_map
                sample_in_ticker = [str(c).zfill(10) in ticker_map_normalized for c in sample_ciks]
                print(f"  Sample all_companies CIKs in ticker_map: {sample_in_ticker}")
            
            if len(ticker_map) > 0:
                sample_ticker_ciks = list(ticker_map.keys())[:5]
                print(f"  Sample ticker_map CIKs (raw keys): {sample_ticker_ciks}")
                print(f"  Sample ticker_map CIKs (types): {[type(c).__name__ for c in sample_ticker_ciks]}")
                print(f"  Sample ticker_map CIKs (normalized): {[str(c).zfill(10) for c in sample_ticker_ciks]}")
                # Check if any are in all_companies
                sample_in_all = [str(c).zfill(10) in all_companies_normalized for c in sample_ticker_ciks]
                print(f"  Sample ticker_map CIKs in all_companies: {sample_in_all}")
            
            # Show some examples of mismatches
            if len(only_in_all_companies) > 0:
                sample_missing = list(only_in_all_companies)[:3]
                print(f"  Example CIKs in all_companies but NOT in ticker_map: {sample_missing}")
                # Try to find them in ticker_map with different normalization
                for missing_cik in sample_missing:
                    # Try without leading zeros
                    try:
                        cik_int = str(int(missing_cik))
                        if cik_int in ticker_map or cik_int.zfill(10) in ticker_map_normalized:
                            print(f"    {missing_cik} found in ticker_map as {cik_int} (normalization mismatch!)")
                    except:
                        pass
            
            if len(only_in_ticker_map) > 0:
                sample_missing = list(only_in_ticker_map)[:3]
                print(f"  Example CIKs in ticker_map but NOT in all_companies: {sample_missing}")
            
            # Check enriched_ciks normalization
            if enriched_ciks:
                enriched_normalized = {str(cik).zfill(10) for cik in enriched_ciks}
                print(f"  enriched_ciks: {len(enriched_ciks)} CIKs (raw)")
                print(f"  enriched_ciks (normalized): {len(enriched_normalized)} CIKs")
                print(f"  enriched_ciks in all_companies: {len(enriched_normalized & set(all_companies_normalized.keys()))}")
                print(f"  enriched_ciks in ticker_map: {len(enriched_normalized & set(ticker_map_normalized.keys()))}")
            
            print("  === END DIAGNOSTIC ===\n")
            
            # Fetch company details (SIC code, entityType) from Submissions API (async)
            # This phase now runs asynchronously with respect to other operations
            # Skip CIKs that are already enriched
            print("  Fetching company details (SIC code, entityType) from SEC Submissions API...")
            companies, details_fetched, details_failed = await self._fetch_company_details_async(
                all_companies, ticker_map, enriched_ciks, conn
            )
            
            print(f"  Found {len(companies)} distinct CIKs from all master.idx files (all form types)")
            print(f"  {sum(1 for c in companies if c['ticker'])} CIKs have ticker symbols")
            print(f"  {details_fetched} CIKs have SIC code or entityType, {details_failed} missing")
            return companies
            
        except Exception as e:
            print(f"Error fetching companies from full-index: {e}")
            traceback.print_exc()
            return []
    
    def get_all_companies(self) -> List[Dict[str, str]]:
        """
        Synchronous wrapper for get_all_companies_async()
        
        Returns:
            List of company dictionaries with CIK and name (ticker may be empty)
        """
        return asyncio.run(self.get_all_companies_async())
    
    def _parse_master_idx_to_dataframe(self, content: bytes) -> pd.DataFrame:
        """
        Parse master.idx file content into a DataFrame with all filings (all CIKs)
        
        Args:
            content: Content of master.idx file (bytes, may be gzipped)
            
        Returns:
            DataFrame with columns: cik, company_name, form_type, filing_date, filename, accession_number, file_year
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
        for line in text.split('\n'):
            line = line.strip()
            if not line or line.startswith('---') or 'CIK' in line.upper():
                continue
            
            # Format: CIK|Company Name|Form Type|Date Filed|Filename
            parts = line.split('|')
            if len(parts) >= 5:
                try:
                    line_cik = parts[0].strip()
                    company_name = parts[1].strip()
                    form_type = parts[2].strip()
                    date_filed_str = parts[3].strip()
                    filename = parts[4].strip()
                    
                    # Normalize CIK to 10 digits
                    try:
                        cik_int = int(line_cik)  # Remove leading zeros if any
                        cik_normalized = str(cik_int).zfill(10)
                    except ValueError:
                        cik_normalized = line_cik.zfill(10)
                    
                    # Parse date (format: YYYYMMDD or YYYY-MM-DD)
                    filing_date = None
                    file_year = None
                    if len(date_filed_str) == 8 and date_filed_str.isdigit():
                        # YYYYMMDD format
                        filing_date = f"{date_filed_str[0:4]}-{date_filed_str[4:6]}-{date_filed_str[6:8]}"
                        file_year = int(date_filed_str[0:4])
                    elif len(date_filed_str) == 10 and date_filed_str[4] == '-' and date_filed_str[7] == '-':
                        # YYYY-MM-DD format
                        filing_date = date_filed_str
                        file_year = int(date_filed_str[0:4])
                    else:
                        continue  # Skip invalid dates
                    
                    # Skip Form 144 filings
                    if form_type and form_type.upper() in ['144', '144/A']:
                        continue
                    
                    # Extract accession number from filename
                    # Format: edgar/data/{cik}/{accession_without_dashes}/{filename}
                    accession_number = None
                    if filename:
                        path_parts = filename.split('/')
                        if len(path_parts) >= 3:
                            accession_without_dashes = path_parts[-2]
                            # Convert to standard accession format: 00001234567-98-000001
                            if len(accession_without_dashes) == 18 and accession_without_dashes.isdigit():
                                accession_number = f"{accession_without_dashes[0:10]}-{accession_without_dashes[10:12]}-{accession_without_dashes[12:18]}"
                            elif '-' in accession_without_dashes:
                                accession_number = accession_without_dashes
                    
                    if not accession_number:
                        continue  # Skip if we can't extract accession number
                    
                    rows.append({
                        'cik': cik_normalized,
                        'company_name': company_name,
                        'form_type': form_type,
                        'filing_date': filing_date,
                        'file_year': file_year,
                        'filename': filename,
                        'accession_number': accession_number
                    })
                except (ValueError, IndexError) as e:
                    continue
        
        # Create DataFrame
        if rows:
            df = pd.DataFrame(rows)
            return df
        else:
            raise Exception('No filings found')
    
    async def get_company_filings_async(self, cik: str, start_year: Optional[int] = None, 
                            master_idx_cache: Optional[Dict[str, pd.DataFrame]] = None) -> List[Dict]:
        """
        Get all filings for a specific company from master.idx files (SEC full-index archive)
        
        This method uses the SEC full-index archive master.idx files instead of the Submissions API,
        which provides complete historical data for all years including older years like 1993.
        
        Uses DataFrame caching to parse each master.idx file only once, then filters by CIK.
        Returns ALL filing types (no filtering).
        
        Args:
            cik: Company CIK (10-digit zero-padded)
            start_year: Start year for filings (None = all available filings from earliest date)
            master_idx_cache: Optional dict to cache parsed DataFrames (key: "year/quarter", value: DataFrame)
        
        Returns:
            List of filing dictionaries (all form types)
        """
        filings = []
        
        # Normalize CIK for filtering
        try:
            cik_int = int(cik)  # Remove leading zeros
            cik_normalized = str(cik_int).zfill(10)
        except ValueError:
            cik_normalized = str(cik).zfill(10)
        
        # Initialize cache if not provided
        if master_idx_cache is None:
            master_idx_cache = {}
        
        try:
            # Base URL for full-index
            base_url = f"{self.base_url}/Archives/edgar/full-index/"
            
            # Create session that will stay alive for all async operations
            async with aiohttp.ClientSession() as session:
                # Get list of years
                async with session.get(base_url, headers=self.headers, timeout=aiohttp.ClientTimeout(total=30)) as response:
                    response.raise_for_status()
                    text = await response.text()
                    years = self._parse_directory_listing(text)
                    # Filter to only numeric years (4 digits)
                    years = [y for y in years if y.isdigit() and len(y) == 4]
                    
                    # Filter by start_year if specified
                    if start_year is not None:
                        years = [y for y in years if int(y) >= start_year]
                    
                    years.sort(reverse=False)  # Process oldest first
                
                # Process each year concurrently
                async def fetch_year_filings(year_str: str):
                    year = int(year_str)
                    year_url = f"{base_url}{year}/"
                    year_filings = []
                    
                    try:
                        # Get list of quarters
                        async with session.get(year_url, headers=self.headers, timeout=aiohttp.ClientTimeout(total=60)) as response:
                            response.raise_for_status()
                            text = await response.text()
                            quarters = self._parse_directory_listing(text)
                            quarters = [q for q in quarters if q.startswith('QTR')]
                        
                        # Process each quarter concurrently
                        async def fetch_quarter_master_idx(quarter: str):
                            cache_key = f"{year}/{quarter}"
                            
                            # Check cache first
                            if cache_key in master_idx_cache:
                                df = master_idx_cache[cache_key]
                            else:
                                # Download and parse master.idx file
                                quarter_url = f"{year_url}{quarter}/"
                                df = None
                                
                                # Try to get master.idx (uncompressed) first
                                master_url = f"{quarter_url}master.idx"
                                try:
                                    response = await session.get(master_url, headers=self.headers, timeout=aiohttp.ClientTimeout(total=120))
                                    try:
                                        if response.status == 200:
                                            content = await response.read()
                                            if content:
                                                df = self._parse_master_idx_to_dataframe(content)
                                    finally:
                                        response.close()
                                except Exception:
                                    pass
                                
                                # Try compressed version if uncompressed failed
                                if df is None or df.empty:
                                    master_gz_url = f"{quarter_url}master.idx.gz"
                                    try:
                                        response = await session.get(master_gz_url, headers=self.headers, timeout=aiohttp.ClientTimeout(total=120))
                                        try:
                                            if response.status == 200:
                                                content = await response.read()
                                                if content:
                                                    df = self._parse_master_idx_to_dataframe(content)
                                        finally:
                                            response.close()
                                    except Exception:
                                        df = pd.DataFrame(columns=['cik', 'company_name', 'form_type', 'filing_date', 'file_year', 'filename', 'accession_number'])
                                
                                # Cache the DataFrame
                                if df is not None and not df.empty:
                                    master_idx_cache[cache_key] = df
                                else:
                                    return []
                            
                            # Filter DataFrame by CIK and year (no form type filtering - get all filings)
                            filtered_df = df[df['cik'] == cik_normalized].copy()
                            
                            # Filter by start_year if specified
                            if start_year is not None:
                                filtered_df = filtered_df[filtered_df['file_year'] >= start_year]
                            
                            # Convert to list of dicts
                            result = []
                            for _, row in filtered_df.iterrows():
                                result.append({
                                    'cik': row['cik'],
                                    'filing_date': row['filing_date'],
                                    'filing_type': row['form_type'],
                                    'accession_number': row['accession_number'],
                                    'description': '',  # master.idx doesn't have description
                                    'is_xbrl': False,  # master.idx doesn't have XBRL info
                                    'is_inline_xbrl': False,
                                })
                            
                            return result
                        
                        # Fetch all quarters concurrently
                        quarter_tasks = [fetch_quarter_master_idx(q) for q in quarters]
                        quarter_results = await asyncio.gather(*quarter_tasks, return_exceptions=True)
                        
                        # Combine results from all quarters
                        for result in quarter_results:
                            if isinstance(result, list):
                                year_filings.extend(result)
                            elif isinstance(result, Exception):
                                # Log error but continue
                                continue
                    
                    except Exception as e:
                        # Continue with next year on error
                        return []
                    
                    return year_filings
                
                # Fetch all years concurrently (with semaphore to limit concurrent requests)
                semaphore = asyncio.Semaphore(5)  # Limit to 5 concurrent years
                
                async def fetch_with_limit(year_str):
                    async with semaphore:
                        return await fetch_year_filings(year_str)
                
                year_tasks = [fetch_with_limit(y) for y in years]
                year_results = await asyncio.gather(*year_tasks, return_exceptions=True)
                
                # Combine results from all years
                for result in year_results:
                    if isinstance(result, list):
                        filings.extend(result)
                    elif isinstance(result, Exception):
                        # Log error but continue
                                continue
                
                # Remove duplicates based on accession_number
                seen_accessions = set()
                unique_filings = []
                for filing in filings:
                    accession = filing.get('accession_number', '')
                    if accession and accession not in seen_accessions:
                        seen_accessions.add(accession)
                        unique_filings.append(filing)
                
                # Identify amendment relationships (same as API version)
                unique_filings = self._identify_amendment_relationships(unique_filings)
                
                return unique_filings
            
        except Exception as e:
            return []
    
    def _identify_amendment_relationships(self, filings: List[Dict]) -> List[Dict]:
        """
        Identify which filing each amendment amends and add 'amends_accession' field
        
        Args:
            filings: List of filing dictionaries
        
        Returns:
            List of filing dictionaries with 'amends_accession' field added for amendments
        """
        # Sort filings by date (oldest first) to process in chronological order
        sorted_filings = sorted(filings, key=lambda x: x.get('filing_date', ''))
        
        # Group filings by base form type (e.g., '10-K', '10-Q', '8-K')
        filings_by_base_type = {}
        for filing in sorted_filings:
            filing_type = filing.get('filing_type', '')
            if not filing_type:
                continue
            
            base_type = filing_type.split('/')[0]
            if base_type not in filings_by_base_type:
                filings_by_base_type[base_type] = []
            filings_by_base_type[base_type].append(filing)
        
        # For each base type, identify amendment relationships
        for base_type, type_filings in filings_by_base_type.items():
            # Separate originals and amendments
            originals = [f for f in type_filings if '/' not in f.get('filing_type', '')]
            amendments = [f for f in type_filings if '/' in f.get('filing_type', '')]
            
            # For each amendment, find the original it amends
            for amendment in amendments:
                amendment_date = amendment.get('filing_date', '')
                if not amendment_date:
                    continue
                
                # Find the most recent original filing of the same type filed before this amendment
                # Match by: same base type, filed before amendment, closest date
                matching_originals = [
                    orig for orig in originals
                    if orig.get('filing_date', '') < amendment_date
                ]
                
                if matching_originals:
                    # Sort by date descending to get most recent
                    matching_originals.sort(key=lambda x: x.get('filing_date', ''), reverse=True)
                    amended_filing = matching_originals[0]
                    amendment['amends_accession'] = amended_filing.get('accession_number', '')
                    amendment['amends_filing_date'] = amended_filing.get('filing_date', '')
                else:
                    # No matching original found (shouldn't happen, but handle gracefully)
                    amendment['amends_accession'] = None
                    amendment['amends_filing_date'] = None
        
        return sorted_filings
    
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
    
    def download_folder_contents(self, folder_url: str, output_dir: str) -> bool:
        """
        Download all files from a folder (e.g., Form 144)
        
        Args:
            folder_url: URL of the folder
            output_dir: Directory to save files
        
        Returns:
            True if successful, False otherwise
        """
        try:
            # Fetch the folder index page
            response = requests.get(folder_url + '/', headers=self.headers, timeout=30)
            if response.status_code != 200:
                return False
            
            # Parse HTML to find all file links
            html_content = response.text
            
            # SEC EDGAR directory listings typically have links in this format:
            # <a href="filename">filename</a>
            # Find all anchor tags with href attributes
            file_links = set()
            
            # Pattern: <a href="filename">filename</a>
            link_pattern = r'<a[^>]+href=["\']([^"\']+)["\']'
            matches = re.findall(link_pattern, html_content, re.IGNORECASE)
            
            for match in matches:
                file_name = match.strip()
                # Skip navigation links
                if file_name in ['../', './', '/', 'Parent Directory', '..', '.']:
                    continue
                # Skip query parameters and anchors
                if '?' in file_name or '#' in file_name:
                    continue
                # Include files (check if it looks like a filename with extension or is a simple filename)
                # Skip subdirectories (contain '/' and don't have common file extensions)
                if '/' not in file_name:
                    # Simple filename without path separator - include it
                    file_links.add(file_name)
                elif file_name.endswith(('.txt', '.xml', '.htm', '.html', '.pdf', '.zip', '.xlsx', '.doc', '.csv')):
                    # Has path separator but ends with file extension - might be in subfolder, include it
                    file_links.add(file_name)
            
            # If no files found with links, try common SEC EDGAR file patterns
            if not file_links:
                # Try common file names
                base_name = os.path.basename(folder_url.rstrip('/'))
                common_patterns = [
                    f"{base_name}.txt",
                    "index.htm",
                    "index.html",
                    "xslF144X05.txt",  # Common Form 144 filename
                ]
                for pattern in common_patterns:
                    file_links.add(pattern)
            
            if not file_links:
                return False
            
            # Download each file
            downloaded_count = 0
            index_url = folder_url.rstrip('/') + '/'
            
            for file_name in file_links:
                try:
                    # Build full URL
                    file_url = urljoin(index_url, file_name)
                    
                    file_response = requests.get(file_url, headers=self.headers, timeout=120, stream=True)
                    if file_response.status_code == 200:
                        # Clean filename (remove query params, etc.)
                        clean_filename = os.path.basename(urlparse(file_url).path)
                        if not clean_filename or clean_filename == '/':
                            clean_filename = file_name
                        
                        # Remove any problematic characters
                        clean_filename = re.sub(r'[<>:"|?*]', '_', clean_filename)
                        
                        filepath = os.path.join(output_dir, clean_filename)
                        
                        # Skip if already exists
                        if os.path.exists(filepath):
                            downloaded_count += 1
                            continue
                        
                        with open(filepath, 'wb') as f:
                            for chunk in file_response.iter_content(chunk_size=65536):
                                if chunk:
                                    f.write(chunk)
                        
                        downloaded_count += 1
                        time.sleep(0.05)  # Rate limiting
                except Exception as e:
                    # Log error with traceback
                    logger = get_download_logger('edgar_downloader')
                    logger.error(
                        f"Error downloading file from folder {folder_url}:\n"
                        f"  File: {file_name}\n"
                        f"  Exception: {str(e)}\n"
                        f"  Traceback:\n{traceback.format_exc()}",
                        exc_info=False  # We're already formatting the traceback
                    )
                    continue
            
            return downloaded_count > 0
            
        except Exception as e:
            # Log error with traceback
            logger = get_download_logger('edgar_downloader')
            logger.error(
                f"Error downloading folder contents from {folder_url}:\n"
                f"  Exception: {str(e)}\n"
                f"  Traceback:\n{traceback.format_exc()}",
                exc_info=False
            )
            return False
    
    def download_filing(self, cik: str, accession_number: str, 
                       output_dir: str, ticker: Optional[str] = None,
                       filing_type: str = '', is_xbrl: bool = True, 
                       is_inline_xbrl: bool = True,
                       amends_accession: Optional[str] = None,
                       amends_filing_date: Optional[str] = None,
                       filing_date: Optional[str] = None,
                       db_conn=None) -> bool:
        """
        Download a specific filing
        
        Args:
            cik: Company CIK
            accession_number: Filing accession number
            output_dir: Directory to save filing
            ticker: Company ticker symbol (if None, will use CIK)
            filing_type: Type of filing (for organization)
            is_xbrl: Whether the filing is XBRL format
            is_inline_xbrl: Whether the filing is inline XBRL format (only matters if is_xbrl=True)
                           If is_xbrl=True and is_inline_xbrl=False, downloads .txt directly
        
        Returns:
            True if successful, False otherwise
        """
        try:
            # Get ticker if not provided
            if not ticker:
                companies = self.get_all_companies()
                company = next((c for c in companies if c['cik'] == cik), None)
                ticker = company.get('ticker', cik) if company else cik
            
            # Create folder structure: ticker/form_type/
            # Normalize form type (remove /A for amended forms)
            form_type = filing_type.split('/')[0] if filing_type else 'OTHER'
            form_type_dir = os.path.join(output_dir, ticker, form_type)
            os.makedirs(form_type_dir, exist_ok=True)
            
            # If not XBRL, or if XBRL but not inline XBRL, download directly: /edgar/data/cik/accession_number_no_dashes/accession_number.txt
            filename = None
            if not is_xbrl or (is_xbrl and not is_inline_xbrl):
                accession_clean = accession_number.replace('-', '')
                cik_int = int(cik.lstrip('0'))
                
                # Construct the URL: /edgar/data/cik/accession_number_no_dashes/accession_number.txt
                filing_url = f"{self.base_url}/Archives/edgar/data/{cik_int}/{accession_clean}/{accession_number}.txt"
                url_extension = '.txt'
                filename = f"{accession_number}.txt"
            else:
                # Get filing URL first to determine file extension (inline XBRL case)
                result = self.get_filing_url(cik, accession_number, filing_type)
                if not result or result[0] is None:
                    return False
                
                filing_url, url_extension = result
            
            # Handle Form 144 - download entire folder
            if url_extension == 'folder':
                # For Form 144, create a subfolder with accession_number (no dashes)
                accession_clean = accession_number.replace('-', '')
                folder_dir = os.path.join(form_type_dir, accession_clean)
                os.makedirs(folder_dir, exist_ok=True)
                
                # Check if folder already exists and has files
                if os.path.exists(folder_dir) and os.listdir(folder_dir):
                    return True  # Already downloaded
                
                # Download all files in the folder
                return self.download_folder_contents(filing_url, folder_dir)
            
            # Determine filename based on URL extension (only if not already set from metadata)
            if filename is None:
                if url_extension == '.txt':
                    filename = f"{accession_number}.txt"
                elif url_extension == '.xml':
                    filename = f"{accession_number}.xml"
                elif url_extension == '.zip':
                    filename = f"{accession_number}.zip"
                elif url_extension == '.html':
                    filename = f"{accession_number}.html"
                elif url_extension == '.pdf':
                    filename = f"{accession_number}.pdf"
                else:
                    # Default based on filing type
                    if filing_type and filing_type.upper() in ['4', '4/A', '3', '3/A', '5', '5/A']:
                        filename = f"{accession_number}.txt"
                    else:
                        filename = f"{accession_number}.txt"
            
            # Note: Filenames use unique accession_number, so no overwriting risk
            # Each filing (including amendments) has a unique accession number
            
            filepath = os.path.join(form_type_dir, filename)
            
            # Check if filing already exists (by unique accession number - prevents overwriting)
            if os.path.exists(filepath):
                return True  # Already downloaded
            
            # Check database for recorded file path
            if db_conn:
                try:
                    cur = db_conn.cursor()
                    cur.execute(
                        "SELECT downloaded_file_path FROM filings WHERE cik = %s AND accession_number = %s",
                        (cik, accession_number)
                    )
                    result = cur.fetchone()
                    cur.close()
                    if result and result[0]:
                        recorded_path = result[0]
                        # Check if recorded path exists (may be relative to output_dir)
                        check_path = os.path.join(output_dir, recorded_path) if not os.path.isabs(recorded_path) else recorded_path
                        if os.path.exists(check_path):
                            return True  # File exists as recorded in database
                except Exception:
                    pass  # If database check fails, continue with normal download
            
            # For 10-K/10-Q ZIP files, also check if a .htm file might exist (from processed ZIP)
            if url_extension == '.zip':
                base_form_type = form_type.split('/')[0] if form_type else ''
                if base_form_type in ['10-K', '10-Q']:
                    # Check for .htm files in the directory that could be from processed ZIP
                    # Look for .htm files containing form identifier (10q/10-q for 10-Q, 10k/10-k for 10-K)
                    if base_form_type == '10-Q':
                        search_terms = ['10q', '10-q']
                    else:  # 10-K
                        search_terms = ['10k', '10-k']
                    
                    if os.path.exists(form_type_dir):
                        for existing_file in os.listdir(form_type_dir):
                            file_lower = existing_file.lower()
                            if (file_lower.endswith('.htm') or file_lower.endswith('.html')) and any(term in file_lower for term in search_terms):
                                # Could be a processed ZIP file, skip download
                                return True
            
            # Download the filing with optimized streaming
            response = requests.get(filing_url, headers=self.headers, timeout=120, stream=True)
            
            if response.status_code == 200:
                # Use larger chunk size (64KB) for faster downloads
                with open(filepath, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=65536):  # 64KB chunks
                        if chunk:  # filter out keep-alive new chunks
                            f.write(chunk)
                
                # Process ZIP files: unzip, check for .xsd/.htm pairs, clean up
                # Only process ZIP files for 10-K and 10-Q filings
                kept_file = None
                if url_extension == '.zip':
                    base_form_type = form_type.split('/')[0] if form_type else ''
                    if base_form_type in ['10-K', '10-Q']:
                        kept_file = self._process_zip_file(filepath, form_type_dir, filing_type=filing_type)
                        # If ZIP was processed and a file was kept, update database
                        if kept_file and db_conn:
                            relative_path = os.path.relpath(os.path.join(form_type_dir, kept_file), output_dir)
                            self._update_filing_downloaded_path(
                                db_conn=db_conn,
                                cik=cik, accession_number=accession_number, 
                                file_path=relative_path, output_dir=output_dir
                            )
                
                # Record downloaded file path in database
                if db_conn and not kept_file:  # Only record if ZIP wasn't processed (kept_file is None)
                    relative_path = os.path.relpath(filepath, output_dir)
                    self._update_filing_downloaded_path(
                        db_conn=db_conn,
                        cik=cik, accession_number=accession_number,
                        file_path=relative_path, output_dir=output_dir
                    )
                
                # If this is an amendment, save the relationship info
                if amends_accession:
                    self._save_amendment_relationship(
                        form_type_dir, accession_number, filing_type,
                        amends_accession, amends_filing_date, filing_date or ''
                    )
                
                return True
            else:
                # Log non-200 status code
                logger = get_download_logger('edgar_downloader')
                logger.error(
                    f"Download failed - Non-200 status code:\n"
                    f"  CIK: {cik}\n"
                    f"  Ticker: {ticker}\n"
                    f"  Accession: {accession_number}\n"
                    f"  Filing Type: {filing_type}\n"
                    f"  URL: {filing_url}\n"
                    f"  Status Code: {response.status_code}"
                )
                return False
                
        except Exception as e:
            # Log error with full traceback
            logger = get_download_logger('edgar_downloader')
            logger.error(
                f"Error downloading filing:\n"
                f"  CIK: {cik}\n"
                f"  Ticker: {ticker}\n"
                f"  Accession: {accession_number}\n"
                f"  Filing Type: {filing_type}\n"
                f"  Exception: {str(e)}\n"
                f"  Traceback:\n{traceback.format_exc()}",
                exc_info=False
            )
            return False
    
    def _process_zip_file(self, zip_path: str, extract_dir: str, filing_type: Optional[str] = None) -> Optional[str]:
        """
        Process downloaded ZIP file:
        1. Unzip it
        2. Check if there's a .xsd file
        3. If .xsd exists, check if there's a matching .htm file (same name)
        4. If both exist: keep only the .htm file, delete the .xsd, delete the .zip, and delete all other files
        5. If no matching .htm and filing_type is 10-K/10-Q: look for .htm containing "10q"/"10-q" (10-Q) or "10k"/"10-k" (10-K)
        6. If no match found: keep the .zip and delete all unzipped files
        
        Args:
            zip_path: Path to the ZIP file
            extract_dir: Directory where ZIP should be extracted
            filing_type: Filing type (e.g., "10-K", "10-Q") for fallback matching
        
        Returns:
            Filename (basename) of the file that was kept, or None if ZIP was kept or no match found
        """
        try:
            # Create temporary extraction directory
            temp_extract_dir = os.path.join(extract_dir, f"_temp_{os.path.basename(zip_path)}")
            os.makedirs(temp_extract_dir, exist_ok=True)
            
            # Unzip to temporary directory
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(temp_extract_dir)
            
            # Get list of extracted files
            extracted_files = []
            for root, dirs, files in os.walk(temp_extract_dir):
                for file in files:
                    extracted_files.append(os.path.join(root, file))
            
            # Find .xsd files
            xsd_files = [f for f in extracted_files if f.lower().endswith('.xsd')]
            
            if xsd_files:
                # Check each .xsd file for matching .htm
                found_match = False
                htm_files_to_keep = []
                
                for xsd_file in xsd_files:
                    xsd_name = os.path.splitext(os.path.basename(xsd_file))[0]
                    
                    # Look for matching .htm file (case-insensitive)
                    htm_file = None
                    for extracted_file in extracted_files:
                        file_basename = os.path.basename(extracted_file)
                        file_name_no_ext = os.path.splitext(file_basename)[0]
                        if file_name_no_ext.lower() == xsd_name.lower() and extracted_file.lower().endswith('.htm'):
                            htm_file = extracted_file
                            break
                    
                    if htm_file:
                        # Found matching pair - keep only the .htm file
                        found_match = True
                        htm_files_to_keep.append(htm_file)
                
                if found_match:
                    # Move only .htm files to extract_dir, delete everything else including ZIP and .xsd
                    kept_file = None
                    for htm_file in htm_files_to_keep:
                        dest_path = os.path.join(extract_dir, os.path.basename(htm_file))
                        os.rename(htm_file, dest_path)
                        if kept_file is None:
                            kept_file = os.path.basename(htm_file)  # Return first file kept
                    
                    # Delete temporary extraction directory (contains .xsd and all other files)
                    shutil.rmtree(temp_extract_dir, ignore_errors=True)
                    
                    # Delete the ZIP file
                    try:
                        os.remove(zip_path)
                    except:
                        pass
                    
                    return kept_file
                else:
                    # No matching .htm found - try fallback rule for 10-K/10-Q
                    if filing_type:
                        base_form_type = filing_type.split('/')[0] if '/' in filing_type else filing_type
                        if base_form_type in ['10-K', '10-Q']:
                            # Look for .htm files containing form identifier
                            if base_form_type == '10-Q':
                                search_terms = ['10q', '10-q']
                            else:  # 10-K
                                search_terms = ['10k', '10-k']
                            
                            matching_htm_files = []
                            for extracted_file in extracted_files:
                                file_basename = os.path.basename(extracted_file).lower()
                                if file_basename.endswith(('.htm', '.html')):
                                    if any(term in file_basename for term in search_terms):
                                        matching_htm_files.append(extracted_file)
                            
                            if matching_htm_files:
                                # Keep the first matching .htm file
                                htm_file_to_keep = matching_htm_files[0]
                                kept_filename = os.path.basename(htm_file_to_keep)
                                dest_path = os.path.join(extract_dir, kept_filename)
                                os.rename(htm_file_to_keep, dest_path)
                                
                                # Delete temporary extraction directory (contains all other files)
                                shutil.rmtree(temp_extract_dir, ignore_errors=True)
                                
                                # Delete the ZIP file
                                try:
                                    os.remove(zip_path)
                                except:
                                    pass
                                return kept_filename  # Successfully processed with fallback rule
                    
                    # No fallback match found - keep ZIP, delete all unzipped files
                    shutil.rmtree(temp_extract_dir, ignore_errors=True)
                    return None  # ZIP was kept
            else:
                # No .xsd files found - try fallback rule for 10-K/10-Q
                if filing_type:
                    base_form_type = filing_type.split('/')[0] if '/' in filing_type else filing_type
                    if base_form_type in ['10-K', '10-Q']:
                        # Look for .htm files containing form identifier
                        if base_form_type == '10-Q':
                            search_terms = ['10q', '10-q']
                        else:  # 10-K
                            search_terms = ['10k', '10-k']
                        
                        matching_htm_files = []
                        for extracted_file in extracted_files:
                            file_basename = os.path.basename(extracted_file).lower()
                            if file_basename.endswith(('.htm', '.html')):
                                if any(term in file_basename for term in search_terms):
                                    matching_htm_files.append(extracted_file)
                        
                        if matching_htm_files:
                            # Keep the first matching .htm file
                            htm_file_to_keep = matching_htm_files[0]
                            dest_path = os.path.join(extract_dir, os.path.basename(htm_file_to_keep))
                            os.rename(htm_file_to_keep, dest_path)
                            
                            # Delete temporary extraction directory (contains all other files)
                            shutil.rmtree(temp_extract_dir, ignore_errors=True)
                            
                            # Delete the ZIP file
                            try:
                                os.remove(zip_path)
                            except:
                                pass
                            return kept_filename  # Successfully processed with fallback rule
                
                # No fallback match found - keep ZIP, delete all unzipped files
                shutil.rmtree(temp_extract_dir, ignore_errors=True)
                return None  # ZIP was kept
                
        except Exception as e:
            # If processing fails, log error but don't fail the download
            logger = get_download_logger('edgar_downloader')
            logger.error(
                f"Error processing ZIP file:\n"
                f"  ZIP: {zip_path}\n"
                f"  Exception: {str(e)}\n"
                f"  Traceback:\n{traceback.format_exc()}",
                exc_info=False
            )
            # Clean up temp directory on error
            try:
                temp_extract_dir = os.path.join(extract_dir, f"_temp_{os.path.basename(zip_path)}")
                if os.path.exists(temp_extract_dir):
                    shutil.rmtree(temp_extract_dir, ignore_errors=True)
            except:
                pass
    
    def process_zip_files(self, directory: str, recursive: bool = True) -> Dict:
        """
        Process all ZIP files in a directory (recursively by default)
        
        Args:
            directory: Directory to search for ZIP files
            recursive: Whether to search subdirectories recursively
        
        Returns:
            Dictionary with processing statistics
        """
        stats = {
            'total_zip_files': 0,
            'processed': 0,
            'kept_htm': 0,
            'kept_zip': 0,
            'errors': 0
        }
        
        print("=" * 60)
        print("Processing ZIP files")
        print("=" * 60)
        print(f"Directory: {directory}")
        print(f"Recursive: {recursive}")
        
        if not os.path.exists(directory):
            print(f"Error: Directory not found: {directory}")
            return stats
        
        # Find all ZIP files
        zip_files = []
        if recursive:
            for root, dirs, files in os.walk(directory):
                for file in files:
                    if file.lower().endswith('.zip'):
                        zip_files.append(os.path.join(root, file))
        else:
            for file in os.listdir(directory):
                if file.lower().endswith('.zip'):
                    zip_files.append(os.path.join(directory, file))
        
        stats['total_zip_files'] = len(zip_files)
        print(f"\nFound {len(zip_files)} ZIP files to process")
        
        if not zip_files:
            print("No ZIP files found")
            return stats
        
        # Process each ZIP file
        zip_pbar = tqdm(zip_files, desc="Processing ZIPs", unit="file", leave=True, total=len(zip_files))
        for zip_path in zip_pbar:
            zip_pbar.set_description(f"Processing: {os.path.basename(zip_path)}")
            
            # Get the directory containing the ZIP file
            zip_dir = os.path.dirname(zip_path)
            
            # Check if this is a 10-K or 10-Q filing by checking the directory structure
            # Path format: .../ticker/form_type/accession.zip
            # Extract form_type from parent directory name
            base_form_type = os.path.basename(zip_dir)
            # Remove /A suffix if present (e.g., "10-K/A" -> "10-K")
            if '/' in base_form_type:
                base_form_type = base_form_type.split('/')[0]
            
            # Only process ZIP files for 10-K and 10-Q filings
            if base_form_type not in ['10-K', '10-Q']:
                continue
            
            try:
                # Check if ZIP still exists (might have been deleted by previous processing)
                if not os.path.exists(zip_path):
                    continue
                
                # Process the ZIP file with filing type for fallback matching
                self._process_zip_file(zip_path, zip_dir, filing_type=base_form_type)
                
                # Check results
                if not os.path.exists(zip_path):
                    # ZIP was deleted, check if .htm was created
                    htm_files = [f for f in os.listdir(zip_dir) if f.lower().endswith('.htm')]
                    if htm_files:
                        stats['kept_htm'] += 1
                    stats['processed'] += 1
                else:
                    # ZIP was kept
                    stats['kept_zip'] += 1
                    stats['processed'] += 1
                
                zip_pbar.set_postfix({
                    'processed': stats['processed'],
                    'kept_htm': stats['kept_htm'],
                    'kept_zip': stats['kept_zip'],
                    'errors': stats['errors']
                })
                
            except Exception as e:
                stats['errors'] += 1
                zip_pbar.write(f"  Error processing {os.path.basename(zip_path)}: {e}")
                logger = get_download_logger('edgar_downloader')
                logger.error(
                    f"Error processing ZIP file:\n"
                    f"  ZIP: {zip_path}\n"
                    f"  Exception: {str(e)}\n"
                    f"  Traceback:\n{traceback.format_exc()}",
                    exc_info=False
                )
        
        zip_pbar.close()
        
        print("\n" + "=" * 60)
        print("Processing Summary:")
        print(f"  Total ZIP files: {stats['total_zip_files']}")
        print(f"  Processed: {stats['processed']}")
        print(f"  Kept .htm files: {stats['kept_htm']}")
        print(f"  Kept .zip files: {stats['kept_zip']}")
        print(f"  Errors: {stats['errors']}")
        print("=" * 60)
        
        return stats
    
    def _update_filing_downloaded_path(self, db_conn, cik: str, accession_number: str, 
                                       file_path: str, output_dir: str = None) -> None:
        """
        Update the downloaded_file_path for a filing in PostgreSQL
        
        Args:
            db_conn: PostgreSQL connection
            cik: Company CIK
            accession_number: Filing accession number
            file_path: Path to the downloaded file (absolute or relative to output_dir)
            output_dir: Base output directory (for making relative paths)
        """
        if not db_conn or not cik or not accession_number or not file_path:
            return
        
        try:
            # Make file_path relative to output_dir if it's absolute
            if output_dir and os.path.isabs(file_path):
                try:
                    file_path = os.path.relpath(file_path, output_dir)
                except ValueError:
                    # If paths are on different drives (Windows), keep absolute
                    pass
            
            update_filing_downloaded_path(db_conn, cik, accession_number, file_path)
        except Exception as e:
            # Don't fail on update errors, just log
            logger = get_download_logger('edgar_downloader')
            logger.error(f"Error updating filing downloaded_file_path: {e}")
    
    def _save_amendment_relationship(self, form_type_dir: str, amendment_accession: str,
                                     amendment_type: str, amends_accession: str,
                                     amends_filing_date: Optional[str],
                                     amendment_filing_date: Optional[str]) -> None:
        """
        Save amendment relationship to a metadata file
        
        Args:
            form_type_dir: Directory where filings are stored
            amendment_accession: Accession number of the amendment
            amendment_type: Type of amendment (e.g., '10-K/A')
            amends_accession: Accession number of the filing being amended
            amends_filing_date: Filing date of the original filing
            amendment_filing_date: Filing date of the amendment
        """
        try:
            metadata_file = os.path.join(form_type_dir, '_amendments.json')
            
            # Load existing metadata if it exists
            amendments_data = {}
            if os.path.exists(metadata_file):
                try:
                    with open(metadata_file, 'r') as f:
                        amendments_data = json.load(f)
                except:
                    amendments_data = {}
            
            # Add or update this amendment relationship
            amendments_data[amendment_accession] = {
                'amendment_type': amendment_type,
                'amends_accession': amends_accession,
                'amends_filing_date': amends_filing_date,
                'amendment_filing_date': amendment_filing_date,
                'updated_at': datetime.now().isoformat()
            }
            
            # Also create reverse mapping (which amendments amend this filing)
            if 'amended_by' not in amendments_data:
                amendments_data['amended_by'] = {}
            if amends_accession not in amendments_data['amended_by']:
                amendments_data['amended_by'][amends_accession] = []
            if amendment_accession not in amendments_data['amended_by'][amends_accession]:
                amendments_data['amended_by'][amends_accession].append(amendment_accession)
            
            # Save metadata
            with open(metadata_file, 'w') as f:
                json.dump(amendments_data, f, indent=2, sort_keys=True)
                
        except Exception as e:
            # Don't fail the download if metadata save fails
            logger = get_download_logger('edgar_downloader')
            logger.error(f"Could not save amendment relationship: {e}")
    
    
    def download_all_filings(self, start_year: Optional[int] = None,
                             output_dir: str = 'edgar_filings',
                             ticker: Optional[str] = None) -> Dict:
        """
        Download all filings from all companies (all form types, no filtering)
        
        Args:
            start_year: Start year for filings (None = all available filings from earliest date)
            output_dir: Directory to save filings
            ticker: Filter by ticker symbol (optional)
        
        Returns:
            Dictionary with download statistics
        """
        print("=" * 60)
        print("EDGAR Filings Downloader")
        print("=" * 60)
        if start_year:
            print(f"Start year: {start_year}")
        else:
            print("Start year: All available (from earliest date)")
        print(f"Output directory: {output_dir}")
        
        os.makedirs(output_dir, exist_ok=True)
        
        # Initialize stats early
        stats = {
            'total_companies': 0,
            'companies_processed': 0,
            'total_filings_found': 0,
            'total_filings_downloaded': 0,
            'errors': 0
        }
        
        # Get all companies
        companies = self.get_all_companies()
        
        # Filter by ticker if specified
        if ticker:
            ticker_filter = ticker.upper()
            companies = [c for c in companies if c.get('ticker', '').upper() == ticker_filter]
            if not companies:
                print(f"No company found with ticker: {ticker_filter}")
                return stats
            print(f"Filtering for ticker: {ticker_filter}")
        
        print(f"\nProcessing {len(companies)} companies...")
        
        # Update stats with actual count
        stats['total_companies'] = len(companies)
        
        # Progress bar for companies
        company_pbar = tqdm(companies, desc="Processing companies", unit="company", leave=True, total=len(companies))
        for company in company_pbar:
            cik = company['cik']
            ticker = company.get('ticker', 'N/A')
            name = company.get('title', 'N/A')
            
            company_pbar.set_description(f"Processing: {ticker}")
              # Breakpoint: total_filings_downloaded, total_filings_found
            company_pbar.set_postfix({
                'downloaded': stats['total_filings_downloaded'],
                'found': stats['total_filings_found'],
                'errors': stats['errors']
            })
            
            try:
                # Get filings for this company
                filings = self.get_company_filings(cik, start_year=start_year)
                  # Breakpoint: total_filings_found
                stats['total_filings_found'] += len(filings)
                
                if len(filings) > 0:
                    # Progress bar for filings
                    filing_pbar = tqdm(filings, desc=f"{ticker} filings", unit="filing", leave=False, total=len(filings))
                    for filing in filing_pbar:
                        accession = filing.get('accession_number', '')
                        filing_type = filing.get('filing_type', 'N/A')
                        if accession:
                            filing_pbar.set_description(f"{ticker}: {filing_type}")
                            
                            # Get is_xbrl and is_inline_xbrl flags from filing if available
                            is_xbrl = filing.get('is_xbrl', True) if isinstance(filing, dict) else True
                            is_inline_xbrl = filing.get('is_inline_xbrl', True) if isinstance(filing, dict) else True
                            if self.download_filing(cik, accession, output_dir, ticker=ticker, filing_type=filing_type, is_xbrl=is_xbrl, is_inline_xbrl=is_inline_xbrl):
                                  # Breakpoint: total_filings_downloaded
                                stats['total_filings_downloaded'] += 1
                            
                              # Breakpoint: total_filings_downloaded
                            filing_pbar.set_postfix({
                                'downloaded': stats['total_filings_downloaded'],
                                'current': filing_type
                            })
                            
                            time.sleep(0.1)  # Rate limiting - SEC recommends max 10 requests/second
                    filing_pbar.close()
                
                stats['companies_processed'] += 1
                
            except Exception as e:
                company_pbar.write(f"  Error processing {ticker}: {e}")
                logger = get_download_logger('edgar_downloader')
                logger.error(
                    f"Error processing company:\n"
                    f"  CIK: {cik}\n"
                    f"  Ticker: {ticker}\n"
                    f"  Name: {name}\n"
                    f"  Exception: {str(e)}\n"
                    f"  Traceback:\n{traceback.format_exc()}",
                    exc_info=False
                )
                stats['errors'] += 1
        company_pbar.close()
        
        print("\n" + "=" * 60)
        print("Download Summary:")
        print(f"  Companies processed: {stats['companies_processed']}/{stats['total_companies']}")
          # Breakpoint: total_filings_found
        print(f"  Filings found: {stats['total_filings_found']}")
          # Breakpoint: total_filings_downloaded
        print(f"  Filings downloaded: {stats['total_filings_downloaded']}")
        print(f"  Errors: {stats['errors']}")
        print("=" * 60)
        
        return stats
    
    async def get_all_companies_and_filings_async(self, start_year: Optional[int] = None,
                                                   dbname: str = "edgar",
                                                   dbuser: str = "postgres",
                                                   dbhost: str = "localhost",
                                                   dbpassword: Optional[str] = None,
                                                   dbport: Optional[int] = None,
                                                   tickers: Optional[List[str]] = None) -> Dict:
        """
        Async version: Get all companies and their filings, save to PostgreSQL database
        Batches are organized by year - processes all companies for a year before moving to next year
        Catalogs ALL filing types (no filtering)
        
        Args:
            start_year: Start year for filings (None = all available from earliest date, default: None)
            dbname: PostgreSQL database name (default: 'edgar')
            dbuser: PostgreSQL user (default: 'postgres')
            dbhost: PostgreSQL host (default: 'localhost')
            dbpassword: PostgreSQL password (optional, can use POSTGRES_PASSWORD env var)
            dbport: PostgreSQL port (default: 5432, can use POSTGRES_PORT env var)
            tickers: Optional list of ticker symbols to filter companies (None for all companies)
        
        Returns:
            Dictionary with companies and filings
        """
        print("=" * 60)
        print("EDGAR Companies and Filings Catalog Generator (PostgreSQL)")
        print("=" * 60)
        if start_year:
            print(f"Start year: {start_year}")
        else:
            print("Start year: All available (from earliest date)")
        # Get password and port (matching FRED pattern exactly)
        # FRED does: password = args.dbpassword or os.getenv('POSTGRES_PASSWORD', '2014')
        password = dbpassword or os.getenv('POSTGRES_PASSWORD', '2014')
        # Port: use provided value, or env var, or default to 5432 (like FRED but with env var support)
        port = dbport if dbport is not None else int(os.getenv('POSTGRES_PORT', '5432'))
        
        print(f"Database: {dbname}@{dbhost}:{port}")
        
        # Initialize PostgreSQL connection
        conn = get_postgres_connection(dbname=dbname, user=dbuser, host=dbhost, 
                                      password=password, port=port)
        init_edgar_postgres_tables(conn)
        
        # Get existing statistics
        stats = get_edgar_statistics(conn)
        
        print(f"Existing data: {stats['total_companies']} companies, {stats['total_filings']} filings")
        
        # Determine years to process using ledger pattern (incomplete years)
        current_year = datetime.now().year
        incomplete_years = set(get_incomplete_years(conn))
        
        if start_year:
            # Process from start_year to current year, including incomplete years
            all_years = set(range(start_year, current_year + 1))
            years_to_process = sorted(all_years - {y for y in all_years if is_year_complete(conn, y)}, reverse=True)
        else:
            # Process all years from 1993 to current, excluding complete years
            # Process in reverse order: most recent year first, going back to earliest
            all_years = set(range(1993, current_year + 1))
            years_to_process = sorted(all_years - {y for y in all_years if is_year_complete(conn, y)}, reverse=True)
        
        if incomplete_years:
            print(f"Incomplete years found: {sorted(incomplete_years, reverse=True)} (reverse chronological order)")
        print(f"  Years to process: {len(years_to_process)} years")
        
        if not years_to_process:
            start_range = start_year if start_year else 1993
            print(f"\nAll years from {start_range} to {current_year} are complete.")
            conn.close()
              # Breakpoint: total_filings
            return {
                'total_companies': stats['total_companies'],
                'total_filings': stats['total_filings'],
                'new_filings': 0
            }
        
        print(f"Years to process: {years_to_process[0]} (most recent) down to {years_to_process[-1]} ({len(years_to_process)} years, reverse chronological order)")
        
        # Always fetch all companies from EDGAR master.idx files and enrich them
        # This async call ensures companies are enriched with tickers, SIC codes, and entity types
        # Pass connection to skip already enriched CIKs (resumable process)
        print("\nFetching all companies from EDGAR master.idx files...")
        print("  NOTE: This will parse ALL master.idx files to extract ALL companies (all form types)")
        print("  Companies already in database will be skipped during enrichment")
        companies = await self.get_all_companies_async(conn=conn)
        print(f"  Total companies fetched from EDGAR: {len(companies)}")
        
        # Filter by tickers if specified
        if tickers:
            ticker_set = {t.upper() for t in tickers}
            companies = [c for c in companies if c.get('ticker', '').upper() in ticker_set]
            if not companies:
                print(f"Error: No companies found with ticker(s): {', '.join(tickers)}")
                conn.close()
                  # Breakpoint: total_filings
                return {'total_companies': 0, 'total_filings': 0}
            print(f"Filtered to {len(companies)} companies matching ticker(s): {', '.join(tickers)}")
        
        print(f"Processing {len(companies)} companies across {len(years_to_process)} years...")
        
        # Batch storage for companies and filings
        # Companies are committed immediately (to avoid re-processing)
        # Filings are committed in smaller batches for immediate persistence
        companies_batch = []
        filings_batch = []
        FILINGS_BATCH_SIZE = 100  # Commit filings every 100 items for immediate persistence
        total_new_filings = 0
        companies_processed = set()  # Track which companies we've already added
        
        # Cache for master.idx DataFrames - shared across all companies to avoid re-parsing
        master_idx_cache: Dict[str, pd.DataFrame] = {}
        
        # Thread-safe locks for shared data structures
        batch_lock = Lock()
        processed_lock = Lock()
        
        # Capture connection parameters for worker function
        worker_password = password
        worker_port = port
        
        async def process_company_async(company: Dict, year: int) -> Dict:
            """Process a single company and return results (async)"""
            cik = company['cik']
            ticker = company.get('ticker', 'N/A')
            name = company.get('name') or company.get('title', 'N/A')
            
            result = {
                'cik': cik,
                'ticker': ticker,
                'name': name,
                'company_data': None,
                'filings': [],
                'error': None
            }
            
            try:
                # Get existing accessions for this company to skip duplicates
                # Note: Each thread needs its own DB connection or we need connection pooling
                # For now, we'll create a temporary connection for this query
                temp_conn = get_postgres_connection(dbname=dbname, user=dbuser, host=dbhost, 
                                                   password=worker_password, port=worker_port)
                existing_accessions = get_existing_accessions(temp_conn, cik)
                temp_conn.close()
                
                # Get filings for this company starting from this year (start_year filter for efficiency)
                # Then filter to only this exact year for batch processing
                # Pass master_idx_cache to reuse parsed DataFrames across companies
                # Get ALL filing types (no filtering)
                all_filings = await self.get_company_filings_async(cik, start_year=year, master_idx_cache=master_idx_cache)
                
                # DEBUG: Track filtering steps
                debug_stats = {
                    'api_filings': len(all_filings),
                    'existing_accessions_count': len(existing_accessions),
                    'year_filings': 0,
                    'truly_new': 0
                }
                
                # Filter filings to only include those from this exact year
                year_filings = [
                    f for f in all_filings
                    if f.get('filing_date') and f.get('filing_date').startswith(str(year))
                ]
                debug_stats['year_filings'] = len(year_filings)
                
                # Filter out filings we already have (by accession_number)
                truly_new_filings = [
                    f for f in year_filings 
                    if f.get('accession_number') and f.get('accession_number') not in existing_accessions
                ]
                debug_stats['truly_new'] = len(truly_new_filings)
                
                # Store debug stats in result for aggregation
                result['debug_stats'] = debug_stats
                
                # Prepare company data
                result['company_data'] = {
                                'cik': cik,
                    'ticker': company.get('ticker'),
                    'name': name,
                    'sic_code': company.get('sic_code'),
                    'entity_type': company.get('entity_type')
                }
                
                # Prepare filings data
                for filing in truly_new_filings:
                    result['filings'].append({
                                'cik': cik,
                                'accession_number': filing.get('accession_number', ''),
                                'filing_date': filing.get('filing_date', ''),
                                'filing_type': filing.get('filing_type', ''),
                                'description': filing.get('description', ''),
                                'is_xbrl': filing.get('is_xbrl', False),
                                'is_inline_xbrl': filing.get('is_inline_xbrl', False),
                                'amends_accession': filing.get('amends_accession'),
                                'amends_filing_date': filing.get('amends_filing_date'),
                        'downloaded_file_path': None
                    })
                
            except Exception as e:
                result['error'] = str(e)
                # Still add company data even if filings fetch fails
                result['company_data'] = {
                        'cik': cik,
                    'ticker': company.get('ticker'),
                        'name': name,
                        'sic_code': company.get('sic_code'),
                    'entity_type': company.get('entity_type')
                }
            
            return result
        
        # Process year by year (async)
        for year in years_to_process:
            print(f"\n{'='*60}")
            print(f"Processing year {year}...")
            print(f"{'='*60}")
            
            year_filings_count = 0
            year_companies_count = 0
            
            # Track debug statistics for filtering analysis
            year_debug_stats = {
                'total_companies': len(companies),
                'companies_processed': 0,
                'total_api_filings': 0,
                'total_year_filings': 0,
                'total_existing_accessions': 0,
                'total_truly_new_filings': 0,
                'total_added_to_result': 0
            }
            
            # Use async processing with semaphore for rate limiting
            # SEC allows 10 requests per second, so we limit concurrent requests
            semaphore = asyncio.Semaphore(10)
            
            async def process_with_semaphore(company: Dict):
                async with semaphore:
                    return await process_company_async(company, year)
            
            # Create tasks for all companies
            tasks = [process_with_semaphore(company) for company in companies]
            
            # Progress bar for companies
            company_pbar = tqdm(total=len(companies), desc=f"Year {year}", unit="company", leave=True)
            
            # Process completed tasks
            for coro in asyncio.as_completed(tasks):
                try:
                    result = await coro
                    
                    if isinstance(result, dict):
                        # Aggregate debug statistics
                        if 'debug_stats' in result:
                            debug_stats = result['debug_stats']
                            year_debug_stats['companies_processed'] += 1
                            year_debug_stats['total_api_filings'] += debug_stats.get('api_filings', 0)
                            year_debug_stats['total_year_filings'] += debug_stats.get('year_filings', 0)
                            year_debug_stats['total_existing_accessions'] += debug_stats.get('existing_accessions_count', 0)
                            year_debug_stats['total_truly_new_filings'] += debug_stats.get('truly_new', 0)
                            year_debug_stats['total_added_to_result'] += len(result.get('filings', []))
                        
                        # Thread-safe batch updates (still need locks for shared data structures)
                        new_filings_count = len(result.get('filings', []))
                        with batch_lock, processed_lock:
                            # Handle company data: commit immediately (to avoid re-processing)
                            if result.get('company_data'):
                                cik = result.get('cik')
                                if cik and cik not in companies_processed:
                                    try:
                                        add_companies_fast(conn, [result['company_data']])
                                        companies_processed.add(cik)
                                        year_companies_count += 1
                                        # Update metadata after companies table update
                                        stats = get_edgar_statistics(conn)
                                        update_edgar_metadata(conn, 'total_companies', str(stats['total_companies']))
                                    except Exception as e:
                                        company_pbar.write(f"  Error adding company {cik} to DB: {e}")
                            
                            # Add filings to batch (commit in smaller batches for immediate persistence)
                            year_filings_count += new_filings_count
                            total_new_filings += new_filings_count
                            
                            for filing in result.get('filings', []):
                                filings_batch.append(filing)
                            
                            # Commit filings in batches to avoid losing work
                            if len(filings_batch) >= FILINGS_BATCH_SIZE:
                                try:
                                    added = add_filings_fast(conn, filings_batch)
                                    filings_batch.clear()
                                    # Update metadata after filings table update
                                    stats = get_edgar_statistics(conn)
                                    update_edgar_metadata(conn, 'total_filings', str(stats['total_filings']))
                                except Exception as e:
                                    company_pbar.write(f"  Error adding filings batch to DB: {e}")
                            
                            # Update progress bar INSIDE the lock to ensure we read the latest values
                            ticker = result.get('ticker', 'N/A')
                            company_pbar.set_postfix({
                                'ticker': ticker,
                                'year_filings': year_filings_count,
                                'total_filings': total_new_filings
                            })
                        company_pbar.update(1)
                        
                        # Log errors if any
                        if result.get('error'):
                            company_pbar.write(f"  Error processing {ticker} for year {year}: {result['error']}")
                
                except Exception as e:
                    company_pbar.write(f"  Error processing company for year {year}: {e}")
                    company_pbar.update(1)
        
        company_pbar.close()
        
        # Print debug statistics showing where filings are filtered
        print(f"\n  📊 Year {year} filtering statistics:")
        print(f"     Companies processed: {year_debug_stats['companies_processed']}/{year_debug_stats['total_companies']}")
        print(f"     Filings from SEC API: {year_debug_stats['total_api_filings']}")
        print(f"     After year filter ({year}): {year_debug_stats['total_year_filings']}")
        print(f"     Already in DB (accessions): {year_debug_stats['total_existing_accessions']}")
        print(f"     Truly new (after duplicate filter): {year_debug_stats['total_truly_new_filings']}")
        print(f"     Added to result: {year_debug_stats['total_added_to_result']}")
        print(f"     Final count (year_filings_count): {year_filings_count}")
        
        # Write remaining batches to database
        # Companies are already committed immediately
        print(f"\n  Saving remaining data for year {year} to database...")
        try:
            # Companies are already committed immediately, so companies_batch should be empty
            # But check just in case
            if companies_batch:
                add_companies_fast(conn, companies_batch)
                companies_batch.clear()
                # Update metadata after companies table update
                stats = get_edgar_statistics(conn)
                update_edgar_metadata(conn, 'total_companies', str(stats['total_companies']))
            
            # Commit remaining filings batch
            if filings_batch:
                added = add_filings_fast(conn, filings_batch)
                filings_batch.clear()
                print(f"  Added {added} filings for year {year}")
                # Update metadata after filings table update
                stats = get_edgar_statistics(conn)
                update_edgar_metadata(conn, 'total_filings', str(stats['total_filings']))
            
            # Update ledger: Count SEC index filings and DB filings, then compare
            print(f"  Updating completion ledger for year {year}...")
            
            # Get DB counts by filing type (all filing types, no filtering)
            db_counts = get_db_filing_counts_by_year(conn, year, None)
        
            # Get SEC index counts by filing type (all filing types, no filtering)
            sec_counts = await self.count_sec_index_filings_by_year(year, None)
            
            # Update ledger for each filing type
            for filing_type in set(list(db_counts.keys()) + list(sec_counts.keys())):
                if filing_type is None:
                    continue  # Skip total, we'll handle it separately
                sec_count = sec_counts.get(filing_type, 0)
                db_count = db_counts.get(filing_type, 0)
                update_year_completion_ledger(conn, year, filing_type, sec_count, db_count)
            
            # Update ledger for total
            # For SEC counts, sum all filing type values
            total_sec = sum(sec_counts.values())
            # For DB counts, exclude None key (which is already the pre-calculated total) and sum filing types
            total_db = sum(v for k, v in db_counts.items() if k is not None)
            update_year_completion_ledger(conn, year, 'TOTAL', total_sec, total_db)
            
            # Check if year is now complete
            is_complete = is_year_complete(conn, year)
            status = "✓ COMPLETE" if is_complete else "⚠ INCOMPLETE"
            
            # Note: Companies and filings already committed immediately above
            # Ledger updates also commit internally, so no need for another commit here
            print(f"  {status} Year {year}: {year_filings_count} new filings, {year_companies_count} companies processed")
            print(f"     SEC index: {total_sec} filings | DB: {total_db} filings")
        except Exception as e:
            # Note: Since we commit immediately, rollback won't help here, but log the error
            print(f"  ✗ Error processing year {year}: {e}")
        
        # Add company history snapshot
        try:
            add_company_history_snapshot(conn)
            conn.commit()
        except Exception as e:
            conn.rollback()
            print(f"  Warning: Error adding company history snapshot: {e}")
        
        # Update metadata
        final_stats = get_edgar_statistics(conn)
        incomplete_years = get_incomplete_years(conn)
        current_year = datetime.now().year
        start_range = start_year if start_year else 1993
        complete_years = [y for y in range(start_range, current_year + 1) if is_year_complete(conn, y)]
        
        update_edgar_metadata(conn, 'total_companies', str(final_stats['total_companies']))
          # Breakpoint: total_filings
        update_edgar_metadata(conn, 'total_filings', str(final_stats['total_filings']))
        update_edgar_metadata(conn, 'generated_at', datetime.now().isoformat())
        update_edgar_metadata(conn, 'status', 'complete')
        update_edgar_metadata(conn, 'start_year', str(start_year) if start_year else 'None')
        update_edgar_metadata(conn, 'filing_types', 'ALL')  # Catalog all filing types
        
        # Final commit for metadata updates
        conn.commit()
        conn.close()
            
        print(f"\n{'='*60}")
        print(f"✓ Database finalized: {dbname}@{dbhost}")
        print(f"{'='*60}")
        print(f"  Total companies: {final_stats['total_companies']}")
          # Breakpoint: total_filings
        print(f"  Total filings: {final_stats['total_filings']}")
        print(f"  New filings added in this run: {total_new_filings}")
        print(f"  Complete years: {len(complete_years)}")
        print(f"  Incomplete years: {len(incomplete_years)}")
        if incomplete_years:
            print(f"    {sorted(incomplete_years)}")
        
          # Breakpoint: total_filings
        return {
            'total_companies': final_stats['total_companies'],
            'total_filings': final_stats['total_filings'],
            'new_filings': total_new_filings,
            'complete_years': sorted(complete_years),
            'incomplete_years': sorted(incomplete_years)
        }
    
    def get_all_companies_and_filings(self, start_year: Optional[int] = None,
                                      dbname: str = "edgar",
                                      dbuser: str = "postgres",
                                      dbhost: str = "localhost",
                                      dbpassword: Optional[str] = None,
                                      dbport: Optional[int] = None,
                                      tickers: Optional[List[str]] = None) -> Dict:
        """
        Synchronous wrapper for get_all_companies_and_filings_async()
        
        Args:
            start_year: Start year for filings (None = all available from earliest date, default: None)
            dbname: PostgreSQL database name (default: 'edgar')
            dbuser: PostgreSQL user (default: 'postgres')
            dbhost: PostgreSQL host (default: 'localhost')
            dbpassword: PostgreSQL password (optional, can use POSTGRES_PASSWORD env var)
            dbport: PostgreSQL port (default: 5432, can use POSTGRES_PORT env var)
            tickers: Optional list of ticker symbols to filter companies (None for all companies)
        
        Returns:
            Dictionary with companies and filings
        """
        return asyncio.run(self.get_all_companies_and_filings_async(
            start_year=start_year,
            dbname=dbname,
            dbuser=dbuser,
            dbhost=dbhost,
            dbpassword=dbpassword,
            dbport=dbport,
            tickers=tickers
        ))


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
  
  # Generate catalog for specific tickers:
  python -m trading_agent.fundamentals.edgar --generate-catalog --download-companies --ticker AAPL,MSFT
  
  # Download filings from database:
  python -m trading_agent.fundamentals.edgar --from-db --ticker NVDA
        """
    )
    parser.add_argument('--start-year', type=int, default=None,
                       help='Start year for filings (default: None = all available from earliest date, typically 1993)')
    parser.add_argument('--output-dir', type=str, default='edgar_filings',
                       help='Output directory for downloaded filing files (default: edgar_filings)')
    parser.add_argument('--ticker', type=str,
                       help='Filter by specific ticker symbol(s). Can be comma-separated (e.g., AAPL,MSFT,NVDA) '
                            'or space-separated (e.g., AAPL MSFT NVDA). Case-insensitive.')
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
            # Support ticker filtering for catalog generation
            tickers = None
            if args.ticker:
                # Support comma-separated or space-separated tickers
                if ',' in args.ticker:
                    tickers = [t.strip().upper() for t in args.ticker.split(',')]
                else:
                    tickers = [args.ticker.upper()]
                print(f"Generating catalog for ticker(s): {', '.join(tickers)}")
            else:
                print("Generating catalog with all companies and filings...")
            
            result = downloader.get_all_companies_and_filings(
                start_year=args.start_year,
                dbname=args.dbname,
                dbuser=args.dbuser,
                dbhost=args.dbhost,
                dbpassword=args.dbpassword,
                dbport=args.dbport,
                tickers=tickers
            )
            
            print(f"\nCatalog generated successfully in PostgreSQL!")
            print(f"  Companies: {result['total_companies']}")
              # Breakpoint: total_filings
            print(f"  Total filings: {result['total_filings']}")
            print(f"  New filings in this run: {result['new_filings']}")
            if result.get('complete_years'):
                print(f"  Complete years: {len(result['complete_years'])}")
            if result.get('incomplete_years'):
                print(f"  Incomplete years: {len(result['incomplete_years'])}")
                print(f"    {result['incomplete_years']}")
            print(f"\nNote: All companies include ticker, SIC code, and entityType from the start.")
            print(f"      Batches are organized by year to prevent data loss on long runs.")
            return 0
        
        # Otherwise, download all filings directly
        stats = downloader.download_all_filings(
            start_year=args.start_year,
            output_dir=args.output_dir,
            ticker=getattr(args, 'ticker', None)
        )
        print("\nDownload complete!")
        return 0
    except Exception as e:
        print(f"Error: {e}")
        tb.print_exc()
        return 1


if __name__ == "__main__":
    main()

