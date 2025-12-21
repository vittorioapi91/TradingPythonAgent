"""
SEC EDGAR Filings Downloader

This module downloads all company filings from SEC EDGAR database.
Focuses on XBRL filings from 2009 onwards.
"""

import os
import time
import shutil
import requests
from typing import List, Dict, Optional, Set
from datetime import datetime
import json
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
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

from tqdm import tqdm

# Handle import for both module import and direct script execution
try:
    from .download_logger import get_download_logger
    from .edgar_parquet import init_duckdb_tables, get_parquet_paths, add_filings_atomically, add_filings_fast
except ImportError:
    from download_logger import get_download_logger
    from edgar_parquet import init_duckdb_tables, get_parquet_paths, add_filings_atomically, add_filings_fast

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
    
    def _parse_master_idx(self, content: bytes, target_forms: Set[str]) -> Dict[str, str]:
        """
        Parse master.idx file content to extract CIKs and company names for target form types
        
        Args:
            content: Content of master.idx file (bytes, may be gzipped)
            target_forms: Set of form types to filter (e.g., {'10-K', '10-Q', '10-K/A', '10-Q/A'})
            
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
                    
                    # Check if this form type matches our target forms
                    if form_type in target_forms:
                        # Normalize CIK to 10 digits
                        cik_normalized = cik.zfill(10)
                        # Store company name (keep first occurrence or most recent)
                        if cik_normalized not in companies:
                            companies[cik_normalized] = company_name
                except (ValueError, IndexError):
                    continue
        
        return companies
    
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
    
    def get_all_companies(self) -> List[Dict[str, str]]:
        """
        Get list of all companies (CIKs) from EDGAR full-index that have filed 10-K, 10-Q, 10-K/A, or 10-Q/A
        
        This method retrieves CIKs from the full-index archive, which includes delisted and defaulted companies
        that are not in company_tickers.json
        
        Returns:
            List of company dictionaries with CIK and name (ticker may be empty)
        """
        print("Fetching all companies from EDGAR full-index...")
        print("  Looking for distinct CIKs that have filed 10-K, 10-Q, 10-K/A, or 10-Q/A")
        
        target_forms = {'10-K', '10-Q', '10-K/A', '10-Q/A'}
        all_companies: Dict[str, str] = {}  # CIK -> Company Name
        
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
                                # Parse master.idx
                                quarter_companies = self._parse_master_idx(response.content, target_forms)
                                all_companies.update(quarter_companies)
                            else:
                                # Try compressed version
                                master_gz_url = f"{quarter_url}master.idx.gz"
                                response = requests.get(master_gz_url, headers=self.headers, timeout=30)
                                
                                if response.status_code == 200:
                                    # Parse compressed master.idx
                                    quarter_companies = self._parse_master_idx(response.content, target_forms)
                                    all_companies.update(quarter_companies)
                                    
                        except Exception as e:
                            # Continue with next quarter on error
                            continue
                            
                except Exception as e:
                    # Continue with next year on error
                    continue
            
            # Enrich with ticker information from company_tickers.json
            print("  Enriching with ticker information from company_tickers.json...")
            ticker_map = {}  # CIK -> ticker
            try:
                ticker_url = f"{self.base_url}/files/company_tickers.json"
                response = requests.get(ticker_url, headers=self.headers, timeout=30)
                response.raise_for_status()
                ticker_data = response.json()
                
                for entry in ticker_data.values():
                    cik_str = str(entry.get('cik_str', '')).zfill(10)
                    ticker = entry.get('ticker', '')
                    if cik_str and ticker:
                        ticker_map[cik_str] = ticker
                
                print(f"  Found {len(ticker_map)} ticker mappings")
            except Exception as e:
                print(f"  Warning: Could not fetch ticker information: {e}")
            
            # Fetch company details (SIC code, entityType) from Submissions API
            print("  Fetching company details (SIC code, entityType) from SEC Submissions API...")
            companies = []
            details_fetched = 0
            details_failed = 0
            
            for cik, name in tqdm(all_companies.items(), desc="Fetching company details", unit="CIK", leave=False):
                # Fetch details from Submissions API
                details = self._get_company_details(cik)
                
                # Use name from Submissions API if available, otherwise use name from master.idx
                company_name = details.get('name') or name
                
                companies.append({
                    'cik': cik,
                    'ticker': ticker_map.get(cik) if ticker_map.get(cik) else None,  # Get ticker from mapping, None if not found
                    'name': company_name,
                    'sic_code': details.get('sic_code'),
                    'entity_type': details.get('entity_type'),
                    'title': company_name  # For compatibility
                })
                
                if details.get('sic_code') or details.get('entity_type'):
                    details_fetched += 1
                else:
                    details_failed += 1
                
                # Rate limiting: SEC allows 10 requests per second
                time.sleep(0.1)
            
            print(f"  Found {len(companies)} distinct CIKs that have filed 10-K, 10-Q, 10-K/A, or 10-Q/A")
            print(f"  {sum(1 for c in companies if c['ticker'])} CIKs have ticker symbols")
            print(f"  {details_fetched} CIKs have SIC code or entityType, {details_failed} missing")
            return companies
            
        except Exception as e:
            print(f"Error fetching companies from full-index: {e}")
            traceback.print_exc()
            return []
    
    def get_company_filings(self, cik: str, start_year: Optional[int] = None, 
                            filing_types: Optional[List[str]] = None) -> List[Dict]:
        """
        Get all filings for a specific company
        
        Args:
            cik: Company CIK (10-digit zero-padded)
            start_year: Start year for filings (None = all available filings from earliest date)
            filing_types: List of filing types to filter (e.g., ['10-K', '10-Q', '8-K'])
                         If None, gets all filings
        
        Returns:
            List of filing dictionaries
        """
        try:
            # SEC EDGAR submissions JSON endpoint format: /data/submissions/CIK{cik}.json
            # NOTE: This endpoint's 'recent' field is limited to approximately the most recent 1000-2000 filings
            # For older historical filings (pre-2010s), you may need to use alternative methods
            submissions_url = f"{self.data_base_url}/submissions/CIK{cik}.json"
            
            response = requests.get(submissions_url, headers=self.data_headers, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                filings = []
                
                # Check if there are any other fields that might contain historical data
                # The API response structure might vary
                if 'filings' not in data:
                    print(f"  Warning: No 'filings' key in API response for CIK {cik}")
                    return []
                
                # Process filings from 'recent' field
                # Note: The SEC API 'recent' field actually contains ALL available filings, not just recent ones
                # Despite the name "recent", it includes all historical filings available through the submissions endpoint
                filing_sources = []
                
                if 'filings' in data:
                    # Process 'recent' field which contains filings (typically limited to ~1000-2000 most recent)
                    if 'recent' in data['filings']:
                        recent_data = data['filings']['recent']
                        filing_dates_list = recent_data.get('filingDate', [])
                        
                        if filing_dates_list:
                            filing_sources.append(('recent', recent_data))
                            filing_dates_list_sorted = sorted([d for d in filing_dates_list if d])
                            if filing_dates_list_sorted:
                                oldest_date = filing_dates_list_sorted[0]
                                # Note: API returned filings, processing silently
                    
                    # Process 'files' field which contains references to additional JSON files with older data
                    if 'files' in data['filings']:
                        files_list = data['filings']['files']
                        if isinstance(files_list, list) and len(files_list) > 0:
                            # Found additional JSON files for historical filings, processing silently
                            
                            # Fetch each additional JSON file
                            for file_info in files_list:
                                if isinstance(file_info, dict):
                                    # The file_info should contain 'name' or 'filename' with the JSON file path
                                    file_name = file_info.get('name') or file_info.get('filename')
                                    if file_name:
                                        try:
                                            # Construct URL for the additional JSON file
                                            # These files are typically in the same submissions directory
                                            file_url = f"{self.data_base_url}/submissions/{file_name}"
                                            
                                            # Fetch the additional JSON file
                                            file_response = requests.get(file_url, headers=self.data_headers, timeout=30)
                                            if file_response.status_code == 200:
                                                file_data = file_response.json()
                                                
                                                # The file should have the same structure as 'recent'
                                                if isinstance(file_data, dict):
                                                    # If it's a dict with filingDate, form, etc., use it directly
                                                    if 'filingDate' in file_data:
                                                        filing_sources.append((f'file_{file_name}', file_data))
                                                        # File loaded, processing silently
                                                    # Or if it has 'filings' -> 'recent' structure
                                                    elif 'filings' in file_data and 'recent' in file_data['filings']:
                                                        file_recent = file_data['filings']['recent']
                                                        filing_sources.append((f'file_{file_name}', file_recent))
                                                        # File loaded, processing silently
                                                
                                                time.sleep(0.1)  # Rate limiting between requests
                                        except Exception as e:
                                            print(f"    Warning: Could not fetch additional file {file_name}: {e}")
                                            continue
                
                # Process all filing sources
                for source_name, source_data in filing_sources:
                    # Get all filing dates, forms, and accession numbers (they're lists)
                    filing_dates = source_data.get('filingDate', [])
                    filing_forms = source_data.get('form', [])
                    accession_numbers = source_data.get('accessionNumber', [])
                    primary_doc_descriptions = source_data.get('primaryDocDescription', [])
                    is_xbrl_list = source_data.get('isXBRL', [])
                    is_inline_xbrl_list = source_data.get('isInlineXBRL', [])
                    
                    # Process each filing
                    num_filings = len(filing_dates) if isinstance(filing_dates, list) else 0
                    
                    for i in range(num_filings):
                        filing_date = filing_dates[i] if i < len(filing_dates) else ''
                        if filing_date:
                            try:
                                # Parse date and validate format (should be YYYY-MM-DD)
                                # Check if date format is valid
                                date_parts = filing_date.split('-')
                                if len(date_parts) == 3:
                                    year = int(date_parts[0])
                                    month = int(date_parts[1])
                                    day = int(date_parts[2])
                                    
                                    # Validate date is reasonable (not in future, not before 1900)
                                    current_year = datetime.now().year
                                    if year > current_year or year < 1900:
                                        continue  # Skip invalid dates
                                    
                                    # If start_year is specified, filter by year
                                    if start_year is not None and year < start_year:
                                        continue
                                else:
                                    # Invalid date format, skip
                                    continue
                                
                                filing_type = filing_forms[i] if i < len(filing_forms) else ''
                                
                                # Skip Form 144 filings for now (user requested exclusion)
                                if filing_type and filing_type.upper() in ['144', '144/A']:
                                    continue
                                
                                if filing_types is None or filing_type in filing_types:
                                    accession = accession_numbers[i] if i < len(accession_numbers) else ''
                                    description = primary_doc_descriptions[i] if i < len(primary_doc_descriptions) else ''
                                    is_xbrl = is_xbrl_list[i] if i < len(is_xbrl_list) else 0
                                    is_inline_xbrl = is_inline_xbrl_list[i] if i < len(is_inline_xbrl_list) else 0
                                    
                                    filings.append({
                                        'cik': cik,
                                        'filing_date': filing_date,
                                        'filing_type': filing_type,
                                        'accession_number': accession,
                                        'description': description,
                                        'is_xbrl': bool(is_xbrl),
                                        'is_inline_xbrl': bool(is_inline_xbrl),
                                    })
                            except (ValueError, IndexError) as e:
                                continue
                
                # Remove duplicates based on accession_number (in case both 'recent' and 'files' have same filings)
                seen_accessions = set()
                unique_filings = []
                for filing in filings:
                    accession = filing.get('accession_number', '')
                    if accession and accession not in seen_accessions:
                        seen_accessions.add(accession)
                        unique_filings.append(filing)
                
                # Identify amendment relationships
                unique_filings = self._identify_amendment_relationships(unique_filings)
                
                return unique_filings
            else:
                return []
            
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
                       parquet_file: Optional[str] = None) -> bool:
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
            # Also check Parquet for recorded file paths and check for .htm files if ZIP might have been processed
            if os.path.exists(filepath):
                return True  # Already downloaded
            
            # Check Parquet for recorded file path
            if parquet_file:
                try:
                    paths = get_parquet_paths(parquet_file)
                    if os.path.exists(paths['filings']):
                        conn = init_duckdb_tables(paths['base_dir'])
                        try:
                            query = f"""
                                SELECT downloaded_file_path 
                                FROM read_parquet('{paths['filings']}') 
                                WHERE cik = '{cik}' AND accession_number = '{accession_number}'
                            """
                            result = conn.execute(query).fetchone()
                            if result and result[0]:
                                recorded_path = result[0]
                                # Check if recorded path exists (may be relative to output_dir)
                                check_path = os.path.join(output_dir, recorded_path) if not os.path.isabs(recorded_path) else recorded_path
                                if os.path.exists(check_path):
                                    conn.close()
                                    return True  # File exists as recorded in Parquet
                        except Exception:
                            pass  # Column might not exist yet, continue
                        finally:
                            conn.close()
                except Exception:
                    pass  # If Parquet check fails, continue with normal download
            
            # For 10-K/10-Q ZIP files, also check if a .htm file might exist (from processed ZIP)
            # This is a fallback check if Parquet wasn't updated yet
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
                        # If ZIP was processed and a file was kept, update Parquet
                        if kept_file and parquet_file:
                            relative_path = os.path.relpath(os.path.join(form_type_dir, kept_file), output_dir)
                            self._update_filing_downloaded_path(parquet_file, cik, accession_number, relative_path, output_dir)
                
                # Record downloaded file path in Parquet
                if parquet_file and not kept_file:  # Only record if ZIP wasn't processed (kept_file is None)
                    relative_path = os.path.relpath(filepath, output_dir)
                    self._update_filing_downloaded_path(parquet_file, cik, accession_number, relative_path, output_dir)
                
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
    
    def _update_filing_downloaded_path(self, parquet_file: str, cik: str, accession_number: str, 
                                       file_path: str, output_dir: str) -> None:
        """
        Update the downloaded_file_path for a filing in Parquet
        
        Args:
            parquet_file: Path to parquet directory or file
            cik: Company CIK
            accession_number: Filing accession number
            file_path: Path to the downloaded file (absolute or relative to output_dir)
            output_dir: Base output directory
        """
        try:
            paths = get_parquet_paths(parquet_file)
            if not os.path.exists(paths['filings']):
                return  # No filings file, nothing to update
            
            conn = init_duckdb_tables(paths['base_dir'])
            
            # Make file_path relative to output_dir if it's absolute
            if os.path.isabs(file_path):
                try:
                    file_path = os.path.relpath(file_path, output_dir)
                except ValueError:
                    # If paths are on different drives (Windows), keep absolute
                    pass
            
            # Check if downloaded_file_path column exists, if not, add it atomically
            try:
                conn.execute(f"SELECT downloaded_file_path FROM read_parquet('{paths['filings']}') LIMIT 1")
            except:
                # Column doesn't exist, need to add it atomically
                existing_table = pq.read_table(paths['filings'])
                existing_df = existing_table.to_pandas()
                existing_df['downloaded_file_path'] = None
                
                # Write atomically using temp file + rename
                temp_file = paths['filings'] + '.tmp'
                try:
                    # Get updated schema with new column
                    updated_schema = existing_table.schema.append(pa.field('downloaded_file_path', pa.string(), nullable=True))
                    updated_table = pa.Table.from_pandas(existing_df, schema=updated_schema)
                    pq.write_table(updated_table, temp_file)
                    os.replace(temp_file, paths['filings'])
                except Exception as e:
                    if os.path.exists(temp_file):
                        try:
                            os.remove(temp_file)
                        except:
                            pass
                    raise
            
            # Update the filing record
            update_query = f"""
                UPDATE read_parquet('{paths['filings']}') 
                SET downloaded_file_path = '{file_path.replace("'", "''")}'
                WHERE cik = '{cik}' AND accession_number = '{accession_number}'
            """
            
            # DuckDB UPDATE doesn't work directly on Parquet, need to use pandas
            # Update atomically using temp file + rename
            filings_df = pq.read_table(paths['filings']).to_pandas()
            mask = (filings_df['cik'] == cik) & (filings_df['accession_number'] == accession_number)
            if mask.any():
                filings_df.loc[mask, 'downloaded_file_path'] = file_path
                
                # Write atomically using temp file + rename
                temp_file = paths['filings'] + '.tmp'
                try:
                    # Get schema from existing file
                    existing_table = pq.read_table(paths['filings'])
                    schema = existing_table.schema
                    
                    # Write to temp file
                    updated_table = pa.Table.from_pandas(filings_df, schema=schema)
                    pq.write_table(updated_table, temp_file)
                    
                    # Atomic rename
                    os.replace(temp_file, paths['filings'])
                except Exception as e:
                    # Clean up temp file on error
                    if os.path.exists(temp_file):
                        try:
                            os.remove(temp_file)
                        except:
                            pass
                    raise
            
            conn.close()
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
    
    def update_company_parquet_by_cik(self, cik: str, parquet_file: str,
                                      start_year: Optional[int] = None,
                                      filing_types: Optional[List[str]] = None,
                                      safe_write: bool = False) -> Dict:
        """
        Update Parquet files with filings for a specific company by CIK, appending only new filings
        Always preserves all existing companies - never overwrites the file
        
        Args:
            cik: Company CIK (10-digit zero-padded)
            parquet_file: Path to parquet directory or file
            start_year: Start year for filings
            filing_types: List of filing types to include
        
        Returns:
            Dictionary with update statistics
        """
        paths = get_parquet_paths(parquet_file)
        conn = init_duckdb_tables(paths['base_dir'])
        
        # Load existing company info
        existing_company_query = f"""
            SELECT cik, ticker, name, sic_code, entity_type
            FROM read_parquet('{paths['companies']}')
            WHERE cik = '{cik}'
        """
        existing_company_result = conn.execute(existing_company_query).fetchone()
        
        if existing_company_result:
            ticker = existing_company_result[1] or cik
            company_name = existing_company_result[2] or 'N/A'
            existing_sic_code = existing_company_result[3] if len(existing_company_result) > 3 else None
            existing_entity_type = existing_company_result[4] if len(existing_company_result) > 4 else None
        else:
            # New company - fetch ticker from company_tickers.json
            fetched_ticker = self._get_company_ticker(cik)
            ticker = fetched_ticker  # None if not found (ticker is nullable)
            company_name = 'N/A'
            existing_sic_code = None
            existing_entity_type = None
        
        # Fetch company details if missing
        if not existing_sic_code or not existing_entity_type:
            details = self._get_company_details(cik)
            sic_code = details.get('sic_code') or existing_sic_code
            entity_type = details.get('entity_type') or existing_entity_type
            if not company_name or company_name == 'N/A':
                company_name = details.get('name') or company_name
            time.sleep(0.1)  # Rate limiting
        else:
            sic_code = existing_sic_code
            entity_type = existing_entity_type
        
        # Get existing accession numbers for this company
        existing_accessions_query = f"""
            SELECT DISTINCT accession_number
            FROM read_parquet('{paths['filings']}')
            WHERE cik = '{cik}'
        """
        existing_accessions = {row[0] for row in conn.execute(existing_accessions_query).fetchall() if row[0]}
        
        # Get new filings from SEC API (includes older filings from 'files' field)
        new_filings = self.get_company_filings(cik, start_year=start_year, filing_types=filing_types)
        
        # Filter out filings we already have
        truly_new_filings = [
            f for f in new_filings 
            if f.get('accession_number') and f.get('accession_number') not in existing_accessions
        ]
        
        new_filings_count = len(truly_new_filings)
        
        # Prepare new filings data for insertion
        if truly_new_filings:
            filings_data = []
            for filing in truly_new_filings:
                filings_data.append({
                    'cik': cik,
                    'accession_number': filing.get('accession_number', ''),
                    'filing_date': filing.get('filing_date', ''),
                    'filing_type': filing.get('filing_type', ''),
                    'description': filing.get('description', ''),
                    'is_xbrl': filing.get('is_xbrl', False),
                    'is_inline_xbrl': filing.get('is_inline_xbrl', False),
                    'amends_accession': filing.get('amends_accession'),
                    'amends_filing_date': filing.get('amends_filing_date'),
                    'downloaded_file_path': None,  # Will be set when filing is downloaded
                })
            
            # Insert new filings (atomic if safe_write=True, fast batch otherwise)
            if safe_write:
                add_filings_atomically(paths['filings'], filings_data)
            else:
                add_filings_fast(paths['filings'], filings_data)
        
        # Update or insert company record
        companies_df = pq.read_table(paths['companies']).to_pandas()
        # Ensure schema has new fields
        if 'sic_code' not in companies_df.columns:
            companies_df['sic_code'] = None
        if 'entity_type' not in companies_df.columns:
            companies_df['entity_type'] = None
        
        if cik in companies_df['cik'].values:
            # Update existing company - fetch details if missing
            existing_sic = companies_df.loc[companies_df['cik'] == cik, 'sic_code'].iloc[0] if len(companies_df.loc[companies_df['cik'] == cik]) > 0 else None
            existing_entity = companies_df.loc[companies_df['cik'] == cik, 'entity_type'].iloc[0] if len(companies_df.loc[companies_df['cik'] == cik]) > 0 else None
            
            if not existing_sic or not existing_entity:
                details = self._get_company_details(cik)
                sic_code = details.get('sic_code') or existing_sic
                entity_type = details.get('entity_type') or existing_entity
                if not company_name or company_name == 'N/A':
                    company_name = details.get('name') or company_name
                time.sleep(0.1)  # Rate limiting
            else:
                sic_code = existing_sic
                entity_type = existing_entity
            
            companies_df.loc[companies_df['cik'] == cik, 'ticker'] = ticker
            companies_df.loc[companies_df['cik'] == cik, 'name'] = company_name
            companies_df.loc[companies_df['cik'] == cik, 'sic_code'] = sic_code
            companies_df.loc[companies_df['cik'] == cik, 'entity_type'] = entity_type
            companies_df.loc[companies_df['cik'] == cik, 'updated_at'] = datetime.now().isoformat()
        else:
            # Fetch company details if not already present
            details = self._get_company_details(cik)
            sic_code = details.get('sic_code')
            entity_type = details.get('entity_type')
            if not company_name or company_name == 'N/A':
                company_name = details.get('name') or company_name
            time.sleep(0.1)  # Rate limiting
            
            # Add new company
            new_company = pd.DataFrame([{
                'cik': cik,
                'ticker': ticker,
                'name': company_name,
                'sic_code': sic_code,
                'entity_type': entity_type,
                'updated_at': datetime.now().isoformat()
            }])
            companies_df = pd.concat([companies_df, new_company], ignore_index=True)
        
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
        pq.write_table(companies_table, paths['companies'])

        # Append/update history for this company (one row per update)
        try:
            history_file = paths.get('company_history')
            if history_file:
                # Load existing history (if any)
                if os.path.exists(history_file):
                    history_df = pq.read_table(history_file).to_pandas()
                else:
                    history_df = pd.DataFrame(columns=companies_df.columns)

                # Append current state of this CIK
                current_row = companies_df[companies_df['cik'] == cik]
                if not current_row.empty:
                    history_df = pd.concat([history_df, current_row], ignore_index=True)

                history_table = pa.Table.from_pandas(history_df, schema=schema)
                pq.write_table(history_table, history_file)
        except Exception as e:
            print(f"  Warning: Error updating company_history.parquet for CIK {cik}: {e}")
        
        # Update metadata
        metadata_df = pq.read_table(paths['metadata']).to_pandas()
        total_companies = len(companies_df)
        
        total_filings_query = f"SELECT COUNT(*) FROM read_parquet('{paths['filings']}')"
        total_filings = conn.execute(total_filings_query).fetchone()[0]
        
        metadata_df.loc[metadata_df['key'] == 'total_companies', 'value'] = str(total_companies)
        metadata_df.loc[metadata_df['key'] == 'total_filings', 'value'] = str(total_filings)
        metadata_df.loc[metadata_df['key'] == 'generated_at', 'value'] = datetime.now().isoformat()
        metadata_df.loc[metadata_df['key'] == 'status', 'value'] = 'in_progress'
        
        if start_year is not None:
            if 'start_year' not in metadata_df['key'].values:
                new_row = pd.DataFrame([{'key': 'start_year', 'value': str(start_year), 'updated_at': datetime.now().isoformat()}])
                metadata_df = pd.concat([metadata_df, new_row], ignore_index=True)
            else:
                metadata_df.loc[metadata_df['key'] == 'start_year', 'value'] = str(start_year)
        
        if filing_types is not None:
            filing_types_str = ','.join(filing_types) if filing_types else ''
            if 'filing_types' not in metadata_df['key'].values:
                new_row = pd.DataFrame([{'key': 'filing_types', 'value': filing_types_str, 'updated_at': datetime.now().isoformat()}])
                metadata_df = pd.concat([metadata_df, new_row], ignore_index=True)
            else:
                metadata_df.loc[metadata_df['key'] == 'filing_types', 'value'] = filing_types_str
        
        metadata_df.loc[metadata_df['key'].isin(['total_companies', 'total_filings', 'generated_at', 'status']), 'updated_at'] = datetime.now().isoformat()
        
        pq.write_table(pa.Table.from_pandas(metadata_df), paths['metadata'])
        
        conn.close()
        
        return {
            'new_filings': new_filings_count,
            'total_filings': total_filings,
            'filings': new_filings,  # Return all filings for compatibility
            'cik': cik,
            'ticker': ticker
        }
    
    def update_ticker_parquet(self, ticker: str, parquet_file: str,
                          start_year: Optional[int] = None,
                          filing_types: Optional[List[str]] = None,
                          safe_write: bool = False) -> Dict:
        """
        Update Parquet files with filings for a specific ticker, appending only new filings
        
        Args:
            ticker: Ticker symbol
            parquet_file: Path to parquet directory or file
            start_year: Start year for filings
            filing_types: List of filing types to include
        
        Returns:
            Dictionary with update statistics
        """
        
        # Get company info
        companies = self.get_all_companies()
        company = next((c for c in companies 
                      if c.get('ticker', '').upper() == ticker.upper()), None)
        
        if not company:
            print(f"Company not found with ticker: {ticker}")
            return {'new_filings': 0, 'total_filings': 0, 'cik': None, 'ticker': ticker, 'filings': []}
        
        cik = company['cik']
        
        # Use the CIK-based update method
        return self.update_company_parquet_by_cik(cik, parquet_file, start_year, filing_types, safe_write)
    
    def download_all_filings(self, start_year: Optional[int] = None,
                             output_dir: str = 'edgar_filings',
                             filing_types: Optional[List[str]] = None,
                             ticker: Optional[str] = None) -> Dict:
        """
        Download all filings from all companies
        
        Args:
            start_year: Start year for filings (None = all available filings from earliest date)
            output_dir: Directory to save filings
            filing_types: List of filing types to download (None for all)
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
            company_pbar.set_postfix({
                'downloaded': stats['total_filings_downloaded'],
                'found': stats['total_filings_found'],
                'errors': stats['errors']
            })
            
            try:
                # Get filings for this company
                filings = self.get_company_filings(cik, start_year=start_year, filing_types=filing_types)
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
                            if self.download_filing(cik, accession, output_dir, ticker=ticker, filing_type=filing_type, is_xbrl=is_xbrl, is_inline_xbrl=is_inline_xbrl, parquet_file=None):
                                stats['total_filings_downloaded'] += 1
                            
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
        print(f"  Filings found: {stats['total_filings_found']}")
        print(f"  Filings downloaded: {stats['total_filings_downloaded']}")
        print(f"  Errors: {stats['errors']}")
        print("=" * 60)
        
        return stats
    
    def get_all_companies_and_filings(self, start_year: Optional[int] = None,
                                      filing_types: Optional[List[str]] = None,
                                      parquet_dir: Optional[str] = None,
                                      write_interval: int = 10,
                                      tickers: Optional[List[str]] = None,
                                      safe_write: bool = False) -> Dict:
        """
        Get all companies and their filings, save to Parquet files
        
        Args:
            start_year: Start year for filings (None = all available filings from earliest date, default: None)
            filing_types: List of filing types to include (None for all)
            parquet_dir: Path to directory where Parquet files will be saved
            write_interval: Number of companies to process before writing to Parquet
            tickers: Optional list of ticker symbols to filter companies (None for all companies)
        
        Returns:
            Dictionary with companies and filings
        """
        print("=" * 60)
        print("EDGAR Companies and Filings Catalog Generator")
        print("=" * 60)
        if start_year:
            print(f"Start year: {start_year}")
        else:
            print("Start year: All available (from earliest date)")
        print(f"Output Parquet directory: {parquet_dir}")
        
        companies_data = []
        companies_processed = 0
        existing_companies_map = {}
        
        # Initialize DuckDB connection and Parquet files
        if parquet_dir:
            paths = get_parquet_paths(parquet_dir)
            conn = init_duckdb_tables(paths['base_dir'])
            
            # Load existing companies and their accession numbers if Parquet files exist
            if os.path.exists(paths['companies']):
                try:
                    companies_query = f"SELECT cik, ticker, name, sic_code, entity_type FROM read_parquet('{paths['companies']}')"
                    companies_result = conn.execute(companies_query).fetchall()
                    
                    load_pbar = tqdm(companies_result, desc="Loading existing Parquet", unit="company", leave=False, total=len(companies_result))
                    loaded_count = 0
                    for row in load_pbar:
                        cik = row[0]
                        if cik:
                            # Get existing accession numbers for this company
                            accessions_query = f"""
                                SELECT DISTINCT accession_number
                                FROM read_parquet('{paths['filings']}')
                                WHERE cik = '{cik}'
                            """
                            accessions_result = conn.execute(accessions_query).fetchall()
                            existing_accessions = {row[0] for row in accessions_result if row[0]}
                            
                            existing_companies_map[cik] = {
                                'company': {
                                    'cik': cik,
                                    'ticker': row[1] or cik,
                                    'name': row[2] or 'N/A',
                                    'sic_code': row[3] if len(row) > 3 else None,
                                    'entity_type': row[4] if len(row) > 4 else None
                                },
                                'accessions': existing_accessions
                            }
                            loaded_count += 1
                            load_pbar.set_postfix({'loaded': loaded_count})
                    load_pbar.close()
                    print(f"Loaded existing Parquet with {len(existing_companies_map)} companies")
                    print(f"  Existing companies will have new filings appended")
                except Exception as e:
                    print(f"Warning: Could not load existing Parquet files: {e}")
                    print(f"  Will create new files")
            else:
                print(f"Initializing new Parquet files in: {parquet_dir}")
                print(f"  All existing companies and filings will be preserved")
                print(f"  New companies will be added, existing companies will be updated with new filings only")
        
        def write_parquet_incrementally():
            """Helper function to write current state to Parquet files - always merges with existing"""
            if parquet_dir:
                try:
                    # Reload existing companies from Parquet
                    current_existing_companies = {}
                    if os.path.exists(paths['companies']):
                        try:
                            companies_query = f"SELECT cik, ticker, name, sic_code, entity_type FROM read_parquet('{paths['companies']}')"
                            companies_result = conn.execute(companies_query).fetchall()
                            for row in companies_result:
                                current_existing_companies[row[0]] = {
                                    'cik': row[0],
                                    'ticker': row[1] or row[0],
                                    'name': row[2] or 'N/A',
                                    'sic_code': row[3] if len(row) > 3 else None,
                                    'entity_type': row[4] if len(row) > 4 else None
                                }
                        except:
                            # Fall back to original existing_companies_map
                            current_existing_companies = {cik: info['company'] for cik, info in existing_companies_map.items()}
                    
                    # Merge: processed companies (updated) + unprocessed existing companies
                    processed_ciks = {c.get('cik') for c in companies_data}
                    
                    # Update companies table
                    companies_list = []
                    for company_data in companies_data:
                        ticker = company_data.get('ticker')
                        companies_list.append({
                            'cik': company_data['cik'],
                            'ticker': ticker if ticker else None,  # None if not available (ticker is nullable)
                            'name': company_data.get('name', 'N/A'),
                            'sic_code': company_data.get('sic_code'),
                            'entity_type': company_data.get('entity_type'),
                            'updated_at': datetime.now().isoformat()
                        })
                    
                    # Add existing companies that haven't been processed
                    for cik, company in current_existing_companies.items():
                        if cik not in processed_ciks:
                            ticker = company.get('ticker')
                            companies_list.append({
                                'cik': company['cik'],
                                'ticker': ticker if ticker else None,  # None if not available (ticker is nullable)
                                'name': company.get('name', 'N/A'),
                                'sic_code': company.get('sic_code'),
                                'entity_type': company.get('entity_type'),
                                'updated_at': datetime.now().isoformat()
                            })
                    
                    # Write companies (current snapshot)
                    companies_df = pd.DataFrame(companies_list)
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
                    pq.write_table(companies_table, paths['companies'])

                    # Append snapshot to company history (one row per company per save)
                    try:
                        history_file = paths.get('company_history')
                        if history_file:
                            if os.path.exists(history_file):
                                existing_history = pq.read_table(history_file).to_pandas()
                                combined_history = pd.concat([existing_history, companies_df], ignore_index=True)
                            else:
                                combined_history = companies_df.copy()
                            history_table = pa.Table.from_pandas(combined_history, schema=schema)
                            pq.write_table(history_table, history_file)
                    except Exception as e:
                        print(f"  Warning: Error updating company_history.parquet: {e}")
                    
                    # Count total filings
                    total_filings_query = f"SELECT COUNT(*) FROM read_parquet('{paths['filings']}')"
                    total_all_filings = conn.execute(total_filings_query).fetchone()[0]
                    
                    # Update metadata
                    metadata_df = pq.read_table(paths['metadata']).to_pandas()
                    metadata_df.loc[metadata_df['key'] == 'total_companies', 'value'] = str(len(companies_list))
                    metadata_df.loc[metadata_df['key'] == 'total_filings', 'value'] = str(total_all_filings)
                    metadata_df.loc[metadata_df['key'] == 'generated_at', 'value'] = datetime.now().isoformat()
                    metadata_df.loc[metadata_df['key'] == 'status', 'value'] = 'in_progress'
                    if start_year is not None:
                        if 'start_year' not in metadata_df['key'].values:
                            new_row = pd.DataFrame([{'key': 'start_year', 'value': str(start_year), 'updated_at': datetime.now().isoformat()}])
                            metadata_df = pd.concat([metadata_df, new_row], ignore_index=True)
                        else:
                            metadata_df.loc[metadata_df['key'] == 'start_year', 'value'] = str(start_year)
                    if filing_types is not None:
                        filing_types_str = ','.join(filing_types) if filing_types else ''
                        if 'filing_types' not in metadata_df['key'].values:
                            new_row = pd.DataFrame([{'key': 'filing_types', 'value': filing_types_str, 'updated_at': datetime.now().isoformat()}])
                            metadata_df = pd.concat([metadata_df, new_row], ignore_index=True)
                        else:
                            metadata_df.loc[metadata_df['key'] == 'filing_types', 'value'] = filing_types_str
                    metadata_df.loc[metadata_df['key'].isin(['total_companies', 'total_filings', 'generated_at', 'status']), 'updated_at'] = datetime.now().isoformat()
                    pq.write_table(pa.Table.from_pandas(metadata_df), paths['metadata'])
                    
                    print(f"  [{len(companies_data)} processed, {len(companies_list)} total companies, {total_all_filings} total filings] Progress saved")
                except Exception as e:
                    print(f"  Warning: Error writing Parquet files: {e}")
        
        # Get all companies
        print("\nFetching all companies from EDGAR...")
        companies = self.get_all_companies()
        
        # Filter by tickers if specified
        if tickers:
            ticker_set = set(tickers)
            companies = [c for c in companies if c.get('ticker', '').upper() in ticker_set]
            if not companies:
                print(f"Error: No companies found with ticker(s): {', '.join(tickers)}")
                return {'total_companies': 0, 'total_filings': 0}
            print(f"Filtered to {len(companies)} companies matching ticker(s): {', '.join(tickers)}")
        
        print(f"Processing {len(companies)} companies...")
        if existing_companies_map:
            print(f"  ({len(existing_companies_map)} companies already in Parquet will be updated)")
        
        total_filings = 0
        
        # Progress bar for companies
        company_pbar = tqdm(companies, desc="Cataloging companies", unit="company", leave=True, total=len(companies))
        for company in company_pbar:
            cik = company['cik']
            ticker = company.get('ticker', 'N/A')
            name = company.get('title', 'N/A')
            
            company_pbar.set_description(f"Cataloging: {ticker}")
            company_pbar.set_postfix({
                'companies': len(companies_data),
                'filings': total_filings,
                'new_filings': total_filings
            })
            
            try:
                # Get filings for this company
                new_filings = self.get_company_filings(cik, start_year=start_year, filing_types=filing_types)
                
                # Check if this company already exists in Parquet
                if cik in existing_companies_map:
                    # Merge with existing filings - only add new ones
                    existing_company = existing_companies_map[cik]['company']
                    existing_accessions = existing_companies_map[cik]['accessions']
                    
                    # Filter out filings we already have (by accession_number)
                    truly_new_filings = [
                        f for f in new_filings 
                        if f.get('accession_number') and f.get('accession_number') not in existing_accessions
                    ]
                    
                    new_filings_count = len(truly_new_filings)
                    if new_filings_count > 0:
                        total_filings += new_filings_count
                        
                        # Add new filings to Parquet
                        filings_data = []
                        for filing in truly_new_filings:
                            filings_data.append({
                                'cik': cik,
                                'accession_number': filing.get('accession_number', ''),
                                'filing_date': filing.get('filing_date', ''),
                                'filing_type': filing.get('filing_type', ''),
                                'description': filing.get('description', ''),
                                'is_xbrl': filing.get('is_xbrl', False),
                                'is_inline_xbrl': filing.get('is_inline_xbrl', False),
                                'amends_accession': filing.get('amends_accession'),
                                'amends_filing_date': filing.get('amends_filing_date'),
                                'downloaded_file_path': None,  # Will be set when filing is downloaded
                            })
                        
                        if filings_data:
                            # Write filings (atomic if safe_write=True, fast batch otherwise)
                            if safe_write:
                                add_filings_atomically(paths['filings'], filings_data)
                            else:
                                add_filings_fast(paths['filings'], filings_data)
                    
                    # Update company data
                    company_data = {
                        'cik': cik,
                        'ticker': existing_company.get('ticker', ticker),
                        'name': existing_company.get('name', name),
                        'sic_code': existing_company.get('sic_code') or company.get('sic_code'),
                        'entity_type': existing_company.get('entity_type') or company.get('entity_type'),
                        'total_filings': len(existing_accessions) + new_filings_count,
                        'filings': new_filings  # For compatibility with return value
                    }
                else:
                    # New company - add all filings
                    new_filings_count = len(new_filings)
                    total_filings += new_filings_count
                    
                    # Add filings to Parquet
                    if new_filings:
                        filings_data = []
                        for filing in new_filings:
                            filings_data.append({
                                'cik': cik,
                                'accession_number': filing.get('accession_number', ''),
                                'filing_date': filing.get('filing_date', ''),
                                'filing_type': filing.get('filing_type', ''),
                                'description': filing.get('description', ''),
                                'is_xbrl': filing.get('is_xbrl', False),
                                'is_inline_xbrl': filing.get('is_inline_xbrl', False),
                                'amends_accession': filing.get('amends_accession'),
                                'amends_filing_date': filing.get('amends_filing_date'),
                                'downloaded_file_path': None,  # Will be set when filing is downloaded
                            })
                        
                        # Write filings (atomic if safe_write=True, fast batch otherwise)
                        if safe_write:
                            add_filings_atomically(paths['filings'], filings_data)
                        else:
                            add_filings_fast(paths['filings'], filings_data)
                    
                    company_data = {
                        'cik': cik,
                        'ticker': ticker,
                        'name': name,
                        'sic_code': company.get('sic_code'),
                        'entity_type': company.get('entity_type'),
                        'total_filings': new_filings_count,
                        'filings': new_filings
                    }
                
                companies_data.append(company_data)
                companies_processed += 1
                
                # Write incrementally
                if parquet_dir and companies_processed % write_interval == 0:
                    company_pbar.write(f"  Saving progress... ({len(companies_data)} companies, {total_filings} filings)")
                    write_parquet_incrementally()
                
                time.sleep(0.1)  # Rate limiting
                
            except Exception as e:
                # Add company even if filings fetch fails
                companies_data.append({
                    'cik': cik,
                    'ticker': ticker,
                    'name': name,
                    'sic_code': company.get('sic_code'),
                    'entity_type': company.get('entity_type'),
                    'total_filings': 0,
                    'filings': [],
                    'error': str(e)
                })
                companies_processed += 1
                company_pbar.write(f"  Error processing {ticker}: {e}")
                time.sleep(0.1)
        
        company_pbar.close()
        print(f"\n  Processed {len(companies_data)} companies, added {total_filings} new filings")
        
        # Final save to Parquet if requested
        # Always reload and merge with existing - never overwrite
        if parquet_dir:
            # Reload existing companies from Parquet to ensure we have the latest
            final_existing_companies = {}
            if os.path.exists(paths['companies']):
                try:
                    companies_query = f"SELECT cik, ticker, name, sic_code, entity_type FROM read_parquet('{paths['companies']}')"
                    companies_result = conn.execute(companies_query).fetchall()
                    for row in companies_result:
                        final_existing_companies[row[0]] = {
                            'cik': row[0],
                            'ticker': row[1] or row[0],
                            'name': row[2] or 'N/A',
                            'sic_code': row[3] if len(row) > 3 else None,
                            'entity_type': row[4] if len(row) > 4 else None
                        }
                except Exception as e:
                    print(f"  Warning: Could not reload Parquet for final merge: {e}")
                    # Fall back to existing_companies_map we loaded at start
                    final_existing_companies = {cik: info['company'] for cik, info in existing_companies_map.items()}
            
            # Merge: newly processed companies + existing companies that weren't processed
            processed_ciks = {c.get('cik') for c in companies_data}
            
            # Update companies table
            companies_list = []
            for company_data in companies_data:
                ticker = company_data.get('ticker')
                companies_list.append({
                    'cik': company_data['cik'],
                    'ticker': ticker if ticker else None,  # None if not available (ticker is nullable)
                    'name': company_data.get('name', 'N/A'),
                    'sic_code': company_data.get('sic_code'),
                    'entity_type': company_data.get('entity_type'),
                    'updated_at': datetime.now().isoformat()
                })
            
            # Add existing companies that weren't processed
            for cik, company in final_existing_companies.items():
                if cik not in processed_ciks:
                    ticker = company.get('ticker')
                    companies_list.append({
                        'cik': company['cik'],
                        'ticker': ticker if ticker else None,  # None if not available (ticker is nullable)
                        'name': company.get('name', 'N/A'),
                        'sic_code': company.get('sic_code'),
                        'entity_type': company.get('entity_type'),
                        'updated_at': datetime.now().isoformat()
                    })
            
            # Write companies (final snapshot)
            companies_df = pd.DataFrame(companies_list)
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
            pq.write_table(companies_table, paths['companies'])

            # Append snapshot to company history (one row per company at end of run)
            try:
                history_file = paths.get('company_history')
                if history_file:
                    if os.path.exists(history_file):
                        existing_history = pq.read_table(history_file).to_pandas()
                        combined_history = pd.concat([existing_history, companies_df], ignore_index=True)
                    else:
                        combined_history = companies_df.copy()
                    history_table = pa.Table.from_pandas(combined_history, schema=schema)
                    pq.write_table(history_table, history_file)
            except Exception as e:
                print(f"  Warning: Error updating company_history.parquet: {e}")
            
            # Count total filings
            total_filings_query = f"SELECT COUNT(*) FROM read_parquet('{paths['filings']}')"
            total_all_filings = conn.execute(total_filings_query).fetchone()[0]
            
            # Update metadata
            metadata_df = pq.read_table(paths['metadata']).to_pandas()
            metadata_df.loc[metadata_df['key'] == 'total_companies', 'value'] = str(len(companies_list))
            metadata_df.loc[metadata_df['key'] == 'total_filings', 'value'] = str(total_all_filings)
            metadata_df.loc[metadata_df['key'] == 'generated_at', 'value'] = datetime.now().isoformat()
            metadata_df.loc[metadata_df['key'] == 'status', 'value'] = 'complete'
            if start_year is not None:
                if 'start_year' not in metadata_df['key'].values:
                    new_row = pd.DataFrame([{'key': 'start_year', 'value': str(start_year), 'updated_at': datetime.now().isoformat()}])
                    metadata_df = pd.concat([metadata_df, new_row], ignore_index=True)
                else:
                    metadata_df.loc[metadata_df['key'] == 'start_year', 'value'] = str(start_year)
            if filing_types is not None:
                filing_types_str = ','.join(filing_types) if filing_types else ''
                if 'filing_types' not in metadata_df['key'].values:
                    new_row = pd.DataFrame([{'key': 'filing_types', 'value': filing_types_str, 'updated_at': datetime.now().isoformat()}])
                    metadata_df = pd.concat([metadata_df, new_row], ignore_index=True)
                else:
                    metadata_df.loc[metadata_df['key'] == 'filing_types', 'value'] = filing_types_str
            metadata_df.loc[metadata_df['key'].isin(['total_companies', 'total_filings', 'generated_at', 'status']), 'updated_at'] = datetime.now().isoformat()
            pq.write_table(pa.Table.from_pandas(metadata_df), paths['metadata'])
            
            conn.close()
            
            print(f"\n✓ Parquet files finalized: {parquet_dir}")
            print(f"  Total companies: {len(companies_list)}")
            print(f"  Total filings: {total_all_filings}")
            print(f"  New filings added in this run: {total_filings}")
            print(f"  ✓ All existing companies and filings preserved")
        
        return {
            'companies': companies_data,
            'total_companies': len(companies_data),
            'total_filings': total_filings
        }


def main():
    """Main function to download EDGAR filings"""
    import argparse
    import traceback as tb
    
    parser = argparse.ArgumentParser(description='Download SEC EDGAR filings')
    parser.add_argument('--start-year', type=int, default=None, help='Start year (default: None = all available filings from earliest date)')
    parser.add_argument('--output-dir', type=str, default='edgar_filings', help='Output directory')
    parser.add_argument('--filing-types', type=str, nargs='+', 
                       help='Filing types to download (e.g., --filing-types 8-K 10-K 10-Q)')
    parser.add_argument('--ticker', type=str, help='Download filings for a specific ticker symbol (e.g., NVDA)')
    parser.add_argument('--user-agent', type=str, 
                       default='VittorioApicella apicellavittorio@hotmail.it',
                       help='User-Agent string for SEC requests')
    parser.add_argument('--generate-parquet', type=str, 
                       help='Generate Parquet files with all companies and filings. Specify output directory path.')
    parser.add_argument('--from-parquet', type=str,
                       help='Download filings from Parquet files (path to parquet directory)')
    parser.add_argument('--parquet-dir', type=str, default='edgar_companies',
                       help='Path to Parquet directory for company/filing catalog (default: edgar_companies)')
    parser.add_argument('--process-zips', type=str,
                       help='Process existing ZIP files in a directory. Specify directory path. Processes recursively by default.')
    parser.add_argument('--no-recursive', action='store_true',
                       help='When used with --process-zips, only process ZIPs in the specified directory (not subdirectories)')
    parser.add_argument('--safe-write', action='store_true',
                       help='Use atomic writes (one accession_number at a time). Slower but safer. Default: fast batch writes.')
    parser.add_argument('--backfill-metadata', type=str, default=None,
                       help='Backfill missing company metadata (ticker, SIC, entityType) for an existing Parquet directory')
    args = parser.parse_args()
    
    try:
        downloader = EDGARDownloader(user_agent=args.user_agent)
        
        # If backfill-metadata is requested, backfill missing metadata
        if args.backfill_metadata:
            stats = downloader.backfill_company_metadata(args.backfill_metadata)
            print(f"\nBackfill complete: {stats['updated']} updated, {stats['failed']} failed")
            return 0
        
        # If process-zips is requested, process existing ZIP files
        if args.process_zips:
            stats = downloader.process_zip_files(args.process_zips, recursive=not args.no_recursive)
            return 0
        
        # If generate-parquet is requested, generate the Parquet files
        if args.generate_parquet:
            # Support ticker filtering for parquet generation
            tickers = None
            if args.ticker:
                # Support comma-separated or space-separated tickers
                if ',' in args.ticker:
                    tickers = [t.strip().upper() for t in args.ticker.split(',')]
                else:
                    tickers = [args.ticker.upper()]
                print(f"Generating Parquet files for ticker(s): {', '.join(tickers)}")
            else:
                print("Generating Parquet files with all companies and filings...")
            
            result = downloader.get_all_companies_and_filings(
                start_year=args.start_year,
                filing_types=args.filing_types,
                parquet_dir=args.generate_parquet,
                write_interval=10,
                tickers=tickers,  # Pass tickers for filtering
                safe_write=args.safe_write
            )
            
            print(f"\nParquet files generated successfully!")
            print(f"  Companies: {result['total_companies']}")
            print(f"  Total filings: {result['total_filings']}")
            print(f"\nNote: All companies now include SIC code and entityType.")
            print(f"      To backfill existing datasets, use: python src/trading_agent/fundamentals/backfill_company_metadata.py <parquet_dir>")
            return 0
        
        # If from-parquet is specified, download from Parquet files
        if args.from_parquet:
            print("=" * 60)
            print("Downloading filings from Parquet files")
            print("=" * 60)
            print(f"Parquet directory: {args.from_parquet}")
            print(f"Output directory: {args.output_dir}")
            
            # Initialize download logger
            log_dir = os.path.dirname(args.output_dir) if os.path.dirname(args.output_dir) else args.output_dir
            log_file = os.path.join(log_dir, 'edgar_download_errors.log')
            get_download_logger('edgar_downloader', log_file=log_file)
            print(f"Download errors will be logged to: {log_file}")
            
            # Load Parquet files
            paths = get_parquet_paths(args.from_parquet)
            if not os.path.exists(paths['companies']):
                print(f"Parquet files not found in: {args.from_parquet}")
                print("Initializing empty parquet files...")
                # Initialize empty parquet files with proper schema
                init_duckdb_tables(paths['base_dir'])
                print("✓ Empty parquet files created successfully")
            
            print(f"\nLoading Parquet files...")
            conn = duckdb.connect()
            
            # Load companies
            companies_query = f"SELECT cik, ticker, name, sic_code, entity_type FROM read_parquet('{paths['companies']}') ORDER BY ticker"
            companies_result = conn.execute(companies_query).fetchall()
            companies = [{
                'cik': row[0], 
                'ticker': row[1] or row[0], 
                'name': row[2] or 'N/A',
                'sic_code': row[3] if len(row) > 3 else None,
                'entity_type': row[4] if len(row) > 4 else None
            } for row in companies_result]
            
            if not companies:
                print("Warning: No companies found in Parquet files (files are empty)")
                print("You can populate them by running: --generate-parquet <directory>")
                print("Or download filings for a specific ticker without using --from-parquet")
                return 0
            
            print(f"Found {len(companies)} companies in Parquet files")
            
            # Determine filing types to download
            filing_types_to_download = args.filing_types
            if not filing_types_to_download:
                filing_types_to_download = ['8-K', '8-K/A', '10-K', '10-K/A', '10-Q', '10-Q/A']
            
            print(f"Filing types to download: {filing_types_to_download}")
            
            # Get start_year from Parquet metadata if available
            start_year = args.start_year
            if not start_year:
                metadata_query = f"SELECT value FROM read_parquet('{paths['metadata']}') WHERE key = 'start_year'"
                start_year_result = conn.execute(metadata_query).fetchone()
                if start_year_result and start_year_result[0]:
                    try:
                        start_year = int(start_year_result[0])
                    except:
                        pass
            
            # Create a set of base forms for matching (e.g., "10-K", "10-Q", "8-K")
            allowed_base_types = set(ft.split('/')[0] for ft in filing_types_to_download)
            
            # Initialize stats
            stats = {
                'total_companies': len(companies),
                'companies_processed': 0,
                'total_filings_found': 0,
                'total_filings_downloaded': 0,
                'total_filings_skipped': 0,
                'errors': 0
            }
            
            os.makedirs(args.output_dir, exist_ok=True)
            
            # Download filings from Parquet (no updates - just download what's already there)
            print("\n" + "=" * 60)
            print("Downloading filings")
            print("=" * 60)
            
            # Process each company
            company_pbar = tqdm(companies, desc="Downloading", unit="company", leave=True, total=len(companies))
            for company in company_pbar:
                cik = company.get('cik')
                ticker = company.get('ticker', 'N/A')
                name = company.get('name', 'N/A')
                
                company_pbar.set_description(f"Downloading: {ticker}")
                company_pbar.set_postfix({
                    'downloaded': stats['total_filings_downloaded'],
                    'skipped': stats['total_filings_skipped'],
                    'errors': stats['errors']
                })
                
                if not cik:
                    logger = get_download_logger('edgar_downloader')
                    logger.error(
                        f"Missing CIK for company:\n"
                        f"  Ticker: {ticker}\n"
                        f"  Name: {name}\n"
                        f"  Company data: {company}"
                    )
                    stats['errors'] += 1
                    continue
                
                # Get filings for this company from Parquet
                filings_query = f"""
                    SELECT accession_number, filing_date, filing_type, description, 
                           is_xbrl, is_inline_xbrl, amends_accession, amends_filing_date
                    FROM read_parquet('{paths['filings']}')
                    WHERE cik = '{cik}'
                """
                filings_result = conn.execute(filings_query).fetchall()
                filings = [{
                    'accession_number': row[0],
                    'filing_date': row[1] or '',
                    'filing_type': row[2] or '',
                    'description': row[3] or '',
                    'is_xbrl': row[4] if row[4] is not None else False,
                    'is_inline_xbrl': row[5] if row[5] is not None else False,
                    'amends_accession': row[6],
                    'amends_filing_date': row[7],
                } for row in filings_result]
                
                # Filter filings by type
                filtered_filings = [
                    f for f in filings
                    if isinstance(f, dict) and 
                    f.get('filing_type', '').split('/')[0] in allowed_base_types
                ]
                
                # Show date range for this company's filings (for verification)
                if filtered_filings:
                    filing_dates = [f.get('filing_date', '') for f in filtered_filings if f.get('filing_date')]
                    if filing_dates:
                        sorted_dates = sorted([d for d in filing_dates if d])
                        if sorted_dates:
                            oldest = sorted_dates[0]
                            newest = sorted_dates[-1]
                            if oldest != newest:
                                company_pbar.write(f"  {ticker}: {len(filtered_filings)} filings, date range: {oldest} to {newest}")
                            else:
                                company_pbar.write(f"  {ticker}: {len(filtered_filings)} filings, all from {oldest}")
                
                stats['total_filings_found'] += len(filtered_filings)
                
                if len(filtered_filings) > 0:
                    # Download each filing
                    filing_pbar = tqdm(filtered_filings, desc=f"{ticker} filings", unit="filing", leave=False, total=len(filtered_filings))
                    for filing in filing_pbar:
                        accession = filing.get('accession_number')
                        filing_type = filing.get('filing_type', 'N/A')
                        
                        if not accession:
                            continue
                        
                        filing_pbar.set_description(f"{ticker}: {filing_type}")
                        
                        # Check if filing already exists
                        form_type = filing_type.split('/')[0] if filing_type else 'OTHER'
                        form_type_dir = os.path.join(args.output_dir, ticker, form_type)
                        
                        # Check if filing exists
                        already_downloaded = False
                        if filing_type and filing_type.upper() in ['144', '144/A']:
                            accession_clean = accession.replace('-', '')
                            folder_path = os.path.join(form_type_dir, accession_clean)
                            if os.path.exists(folder_path) and os.listdir(folder_path):
                                already_downloaded = True
                        else:
                            zip_path = os.path.join(form_type_dir, f"{accession}.zip")
                            xml_path = os.path.join(form_type_dir, f"{accession}.xml")
                            txt_path = os.path.join(form_type_dir, f"{accession}.txt")
                            
                            if os.path.exists(zip_path) or os.path.exists(xml_path) or os.path.exists(txt_path):
                                already_downloaded = True
                        
                        if already_downloaded:
                            stats['total_filings_skipped'] += 1
                            filing_pbar.set_postfix({
                                'downloaded': stats['total_filings_downloaded'],
                                'skipped': stats['total_filings_skipped']
                            })
                            continue
                        
                        # Download the filing
                        try:
                            is_xbrl = filing.get('is_xbrl', True)
                            is_inline_xbrl = filing.get('is_inline_xbrl', True)
                            amends_accession = filing.get('amends_accession')
                            amends_filing_date = filing.get('amends_filing_date')
                            filing_date = filing.get('filing_date')
                            
                            if downloader.download_filing(
                                cik, accession, args.output_dir, 
                                ticker=ticker, 
                                filing_type=filing_type, 
                                is_xbrl=is_xbrl, 
                                is_inline_xbrl=is_inline_xbrl,
                                amends_accession=amends_accession,
                                amends_filing_date=amends_filing_date,
                                filing_date=filing_date,
                                parquet_file=parquet_dir
                            ):
                                stats['total_filings_downloaded'] += 1
                            else:
                                # download_filing returned False - error should already be logged in download_filing method
                                # Log here as well to ensure it's recorded when stats are updated
                                logger = get_download_logger('edgar_downloader')
                                logger.error(
                                    f"Filing download failed (tracked in statistics):\n"
                                    f"  CIK: {cik}\n"
                                    f"  Ticker: {ticker}\n"
                                    f"  Accession: {accession}\n"
                                    f"  Filing Type: {filing_type}"
                                )
                                stats['errors'] += 1
                        except Exception as e:
                            logger = get_download_logger('edgar_downloader')
                            logger.error(
                                f"Error downloading filing:\n"
                                f"  CIK: {cik}\n"
                                f"  Ticker: {ticker}\n"
                                f"  Accession: {accession}\n"
                                f"  Filing Type: {filing_type}\n"
                                f"  Exception: {str(e)}\n"
                                f"  Traceback:\n{tb.format_exc()}",
                                exc_info=False
                            )
                            stats['errors'] += 1
                        
                        filing_pbar.set_postfix({
                            'downloaded': stats['total_filings_downloaded'],
                            'skipped': stats['total_filings_skipped'],
                            'errors': stats['errors']
                        })
                        
                        time.sleep(0.05)  # Rate limiting
                    filing_pbar.close()
                
                stats['companies_processed'] += 1
            
            company_pbar.close()
            conn.close()
            
            # Mark Parquet metadata as complete
            try:
                paths = get_parquet_paths(args.from_parquet)
                conn = duckdb.connect()
                metadata_df = pq.read_table(paths['metadata']).to_pandas()
                metadata_df.loc[metadata_df['key'] == 'status', 'value'] = 'complete'
                metadata_df.loc[metadata_df['key'] == 'generated_at', 'value'] = datetime.now().isoformat()
                metadata_df.loc[metadata_df['key'].isin(['status', 'generated_at']), 'updated_at'] = datetime.now().isoformat()
                pq.write_table(pa.Table.from_pandas(metadata_df), paths['metadata'])
                conn.close()
            except Exception as e:
                print(f"  Warning: Could not update Parquet status: {e}")
            
            print("\n" + "=" * 60)
            print("Complete Summary:")
            print(f"  Companies processed: {stats['companies_processed']}/{stats['total_companies']}")
            print(f"  Filings found: {stats['total_filings_found']}")
            print(f"  Filings downloaded: {stats['total_filings_downloaded']}")
            print(f"  Filings skipped (already exist): {stats['total_filings_skipped']}")
            print(f"  Errors: {stats['errors']}")
            print("=" * 60)
            
            return 0
        
        # If ticker is specified, do full update: Parquet -> Download
        if args.ticker:
            # Support comma-separated tickers
            if ',' in args.ticker:
                tickers = [t.strip().upper() for t in args.ticker.split(',')]
            else:
                tickers = [args.ticker.upper()]
            
            # Initialize download logger (log file in same directory as output)
            log_dir = os.path.dirname(args.output_dir) if os.path.dirname(args.output_dir) else args.output_dir
            log_file = os.path.join(log_dir, 'edgar_download_errors.log')
            get_download_logger('edgar_downloader', log_file=log_file)  # Initialize logger with log file path
            print(f"Download errors will be logged to: {log_file}")
            
            # Process each ticker
            for ticker in tickers:
                # Get parquet directory
                parquet_dir = args.parquet_dir if hasattr(args, 'parquet_dir') and args.parquet_dir else 'edgar_companies'
                if not os.path.isabs(parquet_dir):
                    # Place Parquet directory in the same directory as output_dir
                    output_dir_base = os.path.dirname(args.output_dir) if os.path.dirname(args.output_dir) else args.output_dir
                    parquet_dir = os.path.join(output_dir_base, parquet_dir)
                
                # Get company CIK from parquet
                paths = get_parquet_paths(parquet_dir)
                conn = duckdb.connect()
                company_query = f"""
                    SELECT DISTINCT cik
                    FROM read_parquet('{paths['companies']}')
                    WHERE UPPER(ticker) = '{ticker.upper()}'
                """
                company_result = conn.execute(company_query).fetchone()
                
                if not company_result or not company_result[0]:
                    print(f"\nNo company found for ticker {ticker} in parquet. Skipping.")
                    conn.close()
                    continue
                
                cik = company_result[0]
                
                # Step 2: Download new filings
                # Load filings from Parquet for this company
                paths = get_parquet_paths(parquet_dir)
                conn = duckdb.connect()
                filings_query = f"""
                    SELECT accession_number, filing_date, filing_type, description,
                           is_xbrl, is_inline_xbrl, amends_accession, amends_filing_date
                    FROM read_parquet('{paths['filings']}')
                    WHERE cik = '{cik}'
                """
                filings_result = conn.execute(filings_query).fetchall()
                filings_to_process = [{
                    'accession_number': row[0],
                    'filing_date': row[1] or '',
                    'filing_type': row[2] or '',
                    'description': row[3] or '',
                    'is_xbrl': row[4] if row[4] is not None else False,
                    'is_inline_xbrl': row[5] if row[5] is not None else False,
                    'amends_accession': row[6],
                    'amends_filing_date': row[7],
                } for row in filings_result]
                conn.close()
                
                # Filter filings by filing_types if specified
                if args.filing_types:
                    # Create a set of base forms for matching (e.g., "10-K", "10-Q", "8-K")
                    allowed_base_types = set(args.filing_types)
                    
                    filings_to_process = [
                        f for f in filings_to_process
                        if f.get('filing_type', '').split('/')[0] in allowed_base_types
                    ]
                    print(f"Filtered to {len(filings_to_process)} filings matching types: {args.filing_types}")
                
                filings_to_download = []
                for f in filings_to_process:
                    if not f.get('accession_number'):
                        continue
                    accession = f.get('accession_number')
                    filing_type = f.get('filing_type', '')
                    form_type = filing_type.split('/')[0] if filing_type else 'OTHER'
                    
                    form_type_dir = os.path.join(args.output_dir, ticker, form_type)
                    
                    # Check if filing exists locally
                    # For Form 144, check if folder exists
                    if filing_type and filing_type.upper() in ['144', '144/A']:
                        accession_clean = accession.replace('-', '')
                        folder_path = os.path.join(form_type_dir, accession_clean)
                        if os.path.exists(folder_path) and os.listdir(folder_path):
                            continue  # Already downloaded
                    else:
                        # Check for individual files (.zip, .xml, .txt)
                        zip_path = os.path.join(form_type_dir, f"{accession}.zip")
                        xml_path = os.path.join(form_type_dir, f"{accession}.xml")
                        txt_path = os.path.join(form_type_dir, f"{accession}.txt")
                        
                        if os.path.exists(zip_path) or os.path.exists(xml_path) or os.path.exists(txt_path):
                            continue  # Already downloaded
                    
                    # If we get here, filing doesn't exist locally - add to download list
                    filings_to_download.append(f)
                
                if len(filings_to_download) == 0:
                    continue
                
                # Download missing filings
                download_stats = {
                    'total_filings_downloaded': 0,
                    'errors': 0
                }
                
                if filings_to_download:
                    download_pbar = tqdm(filings_to_download, desc=f"Downloading {ticker}", unit="filing", leave=True, total=len(filings_to_download))
                    for filing in download_pbar:
                        accession = filing.get('accession_number')
                        filing_type = filing.get('filing_type', 'N/A')
                        if accession:
                            download_pbar.set_description(f"Downloading {ticker}: {filing_type}")
                            try:
                                is_xbrl = filing.get('is_xbrl', True)
                                is_inline_xbrl = filing.get('is_inline_xbrl', True)
                                amends_accession = filing.get('amends_accession')
                                amends_filing_date = filing.get('amends_filing_date')
                                filing_date = filing.get('filing_date')
                                if downloader.download_filing(
                                    cik, accession, args.output_dir, 
                                    ticker=ticker, 
                                    filing_type=filing_type, 
                                    is_xbrl=is_xbrl, 
                                    is_inline_xbrl=is_inline_xbrl,
                                    amends_accession=amends_accession,
                                    amends_filing_date=amends_filing_date,
                                    filing_date=filing_date,
                                    parquet_file=parquet_dir
                                ):
                                    download_stats['total_filings_downloaded'] += 1
                                else:
                                    # download_filing returned False - error should already be logged in download_filing method
                                    # Log here as well to ensure it's recorded when stats are updated
                                    logger = get_download_logger('edgar_downloader')
                                    logger.error(
                                        f"Filing download failed (tracked in statistics):\n"
                                        f"  CIK: {cik}\n"
                                        f"  Ticker: {ticker}\n"
                                        f"  Accession: {accession}\n"
                                        f"  Filing Type: {filing_type}"
                                    )
                                    download_stats['errors'] += 1
                            except Exception as e:
                                # Log error with full traceback
                                logger = get_download_logger('edgar_downloader')
                                logger.error(
                                    f"Exception during download (outer exception handler):\n"
                                    f"  CIK: {cik}\n"
                                    f"  Ticker: {ticker}\n"
                                    f"  Accession: {accession}\n"
                                    f"  Filing Type: {filing_type}\n"
                                    f"  Exception: {str(e)}\n"
                                    f"  Traceback:\n{tb.format_exc()}",
                                    exc_info=False
                                )
                                download_pbar.write(f"  Error downloading {accession}: {e}")
                                download_stats['errors'] += 1
                            
                            # Update progress bar with stats
                            download_pbar.set_postfix({
                                'downloaded': download_stats['total_filings_downloaded'],
                                'errors': download_stats['errors']
                            })
                            
                            # Reduced rate limiting for faster downloads (SEC allows 10 requests/second)
                            time.sleep(0.05)  # 20 requests/second max (well below SEC limit)
                    download_pbar.close()
                
                # Download complete for this ticker
            
            return 0
        
        # Otherwise, download all filings directly
        stats = downloader.download_all_filings(
            start_year=args.start_year,
            output_dir=args.output_dir,
            filing_types=args.filing_types,
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

