"""
iShares ETF Details Scraper

This module scrapes detailed information (Key Facts and Portfolio Characteristics)
from individual iShares ETF pages.
"""

import os
import time
import re
import requests
from typing import List, Dict, Optional, Tuple
from datetime import datetime
import pandas as pd
from pathlib import Path

try:
    import openpyxl
    HAS_OPENPYXL = True
except ImportError:
    HAS_OPENPYXL = False

try:
    import xlrd
    HAS_XLRD = True
except ImportError:
    HAS_XLRD = False

try:
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.chrome.options import Options
    HAS_SELENIUM = True
except ImportError:
    HAS_SELENIUM = False

try:
    from tqdm import tqdm
except ImportError:
    tqdm = lambda x, **kwargs: x

try:
    from .ishares_parquet import get_ishares_parquet_paths
    from .ishares_details_parquet import (
        init_ishares_details_parquet_tables,
        get_ishares_details_parquet_paths,
        add_ishares_details_fast,
        update_ishares_details_metadata,
        load_ishares_etfs_from_parquet
    )
    from .ishares_data_parquet import (
        init_ishares_data_parquet_tables,
        get_ishares_data_parquet_paths,
        add_ishares_data_fast,
        update_ishares_data_metadata
    )
except ImportError:
    from ishares_parquet import get_ishares_parquet_paths
    from ishares_details_parquet import (
        init_ishares_details_parquet_tables,
        get_ishares_details_parquet_paths,
        add_ishares_details_fast,
        update_ishares_details_metadata,
        load_ishares_etfs_from_parquet
    )
    from ishares_data_parquet import (
        init_ishares_data_parquet_tables,
        get_ishares_data_parquet_paths,
        add_ishares_data_fast,
        update_ishares_data_metadata
    )


class iSharesDetailsScraper:
    """Class to scrape detailed information from individual iShares ETF pages"""
    
    def __init__(self, headless: bool = True, delay_between_pages: float = 2.0, excel_download_dir: Optional[str] = None):
        """
        Initialize iShares details scraper
        
        Args:
            headless: Whether to run browser in headless mode
            delay_between_pages: Delay in seconds between page visits
            excel_download_dir: Directory to save downloaded Excel files (default: ishares_excel_data)
        """
        self.headless = headless
        self.delay_between_pages = delay_between_pages
        self.base_url = "https://www.ishares.com"
        self.excel_download_dir = excel_download_dir or "ishares_excel_data"
        
        # Headers for requests
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
        }
        
        # Create Excel download directory
        os.makedirs(self.excel_download_dir, exist_ok=True)
    
    def _extract_date_from_page(self, driver) -> str:
        """
        Extract date information from the ETF page (e.g., "As of 12/31/2024")
        
        Args:
            driver: Selenium WebDriver instance
            
        Returns:
            Date string found on the page, or empty string if not found
        """
        try:
            # Common patterns for dates on iShares pages
            date_patterns = [
                r'As of\s+(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})',
                r'as of\s+(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})',
                r'Date:\s*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})',
                r'(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})',  # Generic date pattern
            ]
            
            # Get page text
            page_text = driver.find_element(By.TAG_NAME, "body").text
            
            # Try to find date in specific sections first
            date_selectors = [
                "[class*='as-of']",
                "[class*='date']",
                "[class*='Date']",
                "[class*='effective']",
                "[class*='Effective']",
            ]
            
            for selector in date_selectors:
                try:
                    elements = driver.find_elements(By.CSS_SELECTOR, selector)
                    for elem in elements:
                        text = elem.text
                        for pattern in date_patterns:
                            match = re.search(pattern, text, re.IGNORECASE)
                            if match:
                                return match.group(1) if match.lastindex else match.group(0)
                except:
                    continue
            
            # Fallback: search entire page text
            for pattern in date_patterns:
                matches = re.finditer(pattern, page_text, re.IGNORECASE)
                for match in matches:
                    # Prefer more complete matches (with "As of")
                    if 'as of' in match.group(0).lower():
                        return match.group(1) if match.lastindex else match.group(0)
            
            # Return first date found if no "as of" found
            for pattern in date_patterns:
                match = re.search(pattern, page_text, re.IGNORECASE)
                if match:
                    return match.group(1) if match.lastindex else match.group(0)
            
            return ""
        except Exception as e:
            print(f"    Warning: Error extracting date: {e}")
            return ""
    
    def _find_table_section(self, driver, section_name: str) -> Optional[object]:
        """
        Find a table section on the page (Key Facts or Portfolio Characteristics)
        
        Args:
            driver: Selenium WebDriver instance
            section_name: Name of the section to find ('Key Facts' or 'Portfolio Characteristics')
            
        Returns:
            WebElement containing the section, or None
        """
        # Try various selectors to find the section
        section_selectors = [
            f"//*[contains(text(), '{section_name}')]",
            f"//*[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '{section_name.lower()}')]",
            f"//h2[contains(text(), '{section_name}')]",
            f"//h3[contains(text(), '{section_name}')]",
            f"//div[contains(@class, '{section_name.lower().replace(' ', '-')}')]",
            f"//section[contains(@class, '{section_name.lower().replace(' ', '-')}')]",
        ]
        
        for selector in section_selectors:
            try:
                if selector.startswith('//'):
                    elements = driver.find_elements(By.XPATH, selector)
                else:
                    elements = driver.find_elements(By.CSS_SELECTOR, selector)
                
                for elem in elements:
                    # Check if this element or nearby contains a table
                    # Look for table within this element or following siblings
                    try:
                        # Try to find table within this element
                        table = elem.find_element(By.XPATH, ".//table | .//div[contains(@class, 'table')] | .//div[@role='table']")
                        return elem
                    except:
                        # Try next sibling
                        try:
                            parent = elem.find_element(By.XPATH, "./..")
                            table = parent.find_element(By.XPATH, ".//table | .//div[contains(@class, 'table')] | .//div[@role='table']")
                            return parent
                        except:
                            continue
            except:
                continue
        
        return None
    
    def _parse_table(self, section_element, section_name: str) -> List[Dict[str, str]]:
        """
        Parse a table from a section element
        
        Args:
            section_element: WebElement containing the section
            section_name: Name of the section ('key_facts' or 'portfolio_characteristics')
            
        Returns:
            List of dictionaries with 'key' and 'value' pairs
        """
        data = []
        
        try:
            # Try to find table element
            tables = section_element.find_elements(By.XPATH, ".//table | .//div[contains(@class, 'table')] | .//div[@role='table']")
            
            if not tables:
                # Try to find list-based structures
                rows = section_element.find_elements(By.XPATH, ".//tr | .//div[@role='row'] | .//div[contains(@class, 'row')]")
                if rows:
                    for row in rows:
                        cells = row.find_elements(By.XPATH, ".//td | .//th | .//div[@role='cell'] | .//div[contains(@class, 'cell')]")
                        if len(cells) >= 2:
                            key = cells[0].text.strip()
                            value = cells[1].text.strip()
                            if key and value:
                                data.append({'key': key, 'value': value})
                    return data
            
            # Parse table structure
            for table in tables:
                rows = table.find_elements(By.XPATH, ".//tr | .//div[@role='row']")
                for row in rows:
                    cells = row.find_elements(By.XPATH, ".//td | .//th | .//div[@role='cell']")
                    if len(cells) >= 2:
                        key = cells[0].text.strip()
                        value = cells[1].text.strip()
                        if key and value and key.lower() not in ['', 'key', 'field']:
                            data.append({'key': key, 'value': value})
                
                if data:
                    break
        
        except Exception as e:
            print(f"    Warning: Error parsing table for {section_name}: {e}")
        
        return data
    
    def _get_benchmark_index_from_etf(self, etf: Dict) -> Optional[str]:
        """
        Extract benchmark index from ETF data
        
        Args:
            etf: ETF dictionary from parquet
            
        Returns:
            Benchmark index string, or None if not found
        """
        # Try various field names
        benchmark = etf.get('primary_benchmark') or etf.get('benchmark') or etf.get('index')
        if benchmark:
            return str(benchmark).strip()
        return None
    
    def scrape_etf_details(self, driver, ticker: str, fund_url: str, benchmark_index: Optional[str] = None, benchmark_excel_map: Optional[Dict[str, str]] = None) -> List[Dict]:
        """
        Scrape detailed information from a single ETF page
        
        Args:
            driver: Selenium WebDriver instance
            ticker: ETF ticker symbol
            fund_url: URL of the ETF page
            benchmark_index: Benchmark index for this ETF (to check if Excel already downloaded)
            benchmark_excel_map: Dictionary mapping benchmark_index -> excel_file_path
            
        Returns:
            List of detail dictionaries
        """
        details = []
        
        try:
            print(f"  Scraping {ticker}: {fund_url}")
            
            # Ensure URL is complete
            if not fund_url.startswith('http'):
                fund_url = self.base_url + fund_url if fund_url.startswith('/') else f"{self.base_url}/{fund_url}"
            
            driver.get(fund_url)
            time.sleep(3)  # Wait for page to load
            
            # Extract date from page
            data_date = self._extract_date_from_page(driver)
            if data_date:
                print(f"    Found data date: {data_date}")
            
            now = datetime.now().isoformat()
            
            # Scrape Key Facts
            key_facts_section = self._find_table_section(driver, "Key Facts")
            if key_facts_section:
                print(f"    Found Key Facts section")
                key_facts_data = self._parse_table(key_facts_section, "key_facts")
                for item in key_facts_data:
                    details.append({
                        'ticker': ticker,
                        'fund_url': fund_url,
                        'data_date': data_date,
                        'section': 'key_facts',
                        'key': item['key'],
                        'value': item['value'],
                        'created_at': now,
                        'updated_at': now,
                    })
                print(f"    Extracted {len(key_facts_data)} Key Facts items")
            else:
                print(f"    Warning: Key Facts section not found")
            
            # Scrape Portfolio Characteristics
            portfolio_section = self._find_table_section(driver, "Portfolio Characteristics")
            if portfolio_section:
                print(f"    Found Portfolio Characteristics section")
                portfolio_data = self._parse_table(portfolio_section, "portfolio_characteristics")
                for item in portfolio_data:
                    details.append({
                        'ticker': ticker,
                        'fund_url': fund_url,
                        'data_date': data_date,
                        'section': 'portfolio_characteristics',
                        'key': item['key'],
                        'value': item['value'],
                        'created_at': now,
                        'updated_at': now,
                    })
                print(f"    Extracted {len(portfolio_data)} Portfolio Characteristics items")
            else:
                print(f"    Warning: Portfolio Characteristics section not found")
            
            # Download and process Excel file
            # Check if we already have an Excel file for this benchmark index
            excel_file = None
            is_reused = False
            if benchmark_index and benchmark_excel_map and benchmark_index in benchmark_excel_map:
                excel_file = benchmark_excel_map[benchmark_index]
                # Verify file still exists
                if os.path.exists(excel_file):
                    is_reused = True
                    print(f"    Reusing Excel file for benchmark '{benchmark_index}': {excel_file}")
                else:
                    # File doesn't exist, need to download again
                    print(f"    Excel file not found, will download: {excel_file}")
                    excel_file = None
            
            if not excel_file:
                # Download new Excel file
                excel_file = self._find_and_download_excel(driver, ticker, fund_url)
                if excel_file and benchmark_index:
                    # Update the map for future use
                    if benchmark_excel_map is not None:
                        benchmark_excel_map[benchmark_index] = excel_file
                        print(f"    Mapped benchmark '{benchmark_index}' -> {excel_file}")
            
            if excel_file:
                holdings_data, historical_data = self._process_excel_file(excel_file, ticker, fund_url, data_date)
                # Store Excel data separately (will be processed in scrape_all_etf_details)
                # We'll return it as a special marker
                details.append({
                    '_type': '_excel_data',
                    '_excel_file': excel_file,
                    '_benchmark_index': benchmark_index,
                    '_is_reused': is_reused,
                    '_holdings_data': holdings_data,
                    '_historical_data': historical_data,
                })
        
        except Exception as e:
            print(f"    Error scraping {ticker}: {e}")
            import traceback
            traceback.print_exc()
        
        return details
    
    def _find_and_download_excel(self, driver, ticker: str, fund_url: str) -> Optional[str]:
        """
        Find and download Excel file from "Data Download" section
        
        Args:
            driver: Selenium WebDriver instance
            ticker: ETF ticker symbol
            fund_url: URL of the ETF page
            
        Returns:
            Path to downloaded Excel file, or None if not found
        """
        try:
            # Find "Data Download" section
            data_download_section = self._find_table_section(driver, "Data Download")
            if not data_download_section:
                # Try alternative names
                for alt_name in ["Download", "Data", "Excel", "Export"]:
                    data_download_section = self._find_table_section(driver, alt_name)
                    if data_download_section:
                        break
            
            if not data_download_section:
                print(f"    Warning: Data Download section not found")
                return None
            
            # Find Excel download link
            excel_links = driver.find_elements(By.XPATH, 
                "//a[contains(@href, '.xls') or contains(@href, '.xlsx') or contains(text(), 'Excel') or contains(text(), 'Download')]")
            
            excel_url = None
            for link in excel_links:
                href = link.get_attribute('href')
                text = link.text.lower()
                if href and ('.xls' in href.lower() or 'excel' in text or 'download' in text):
                    excel_url = href
                    break
            
            if not excel_url:
                print(f"    Warning: Excel download link not found")
                return None
            
            # Ensure URL is complete
            if not excel_url.startswith('http'):
                excel_url = self.base_url + excel_url if excel_url.startswith('/') else f"{self.base_url}/{excel_url}"
            
            print(f"    Found Excel download link: {excel_url}")
            
            # Download the file
            response = requests.get(excel_url, headers=self.headers, timeout=60, stream=True)
            response.raise_for_status()
            
            # Determine file extension
            content_type = response.headers.get('content-type', '')
            if '.xlsx' in excel_url.lower() or 'spreadsheetml' in content_type:
                ext = '.xlsx'
            else:
                ext = '.xls'
            
            # Save file
            filename = f"{ticker}_{datetime.now().strftime('%Y%m%d')}{ext}"
            filepath = os.path.join(self.excel_download_dir, filename)
            
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            print(f"    Downloaded Excel file: {filepath}")
            return filepath
            
        except Exception as e:
            print(f"    Error downloading Excel file: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def _process_excel_file(self, excel_file: str, ticker: str, fund_url: str, data_date: str) -> Tuple[List[Dict], List[Dict]]:
        """
        Process Excel file and extract Holdings and Historical sheets
        
        Args:
            excel_file: Path to Excel file
            ticker: ETF ticker symbol
            fund_url: URL of the ETF page
            data_date: Date found on the page
            
        Returns:
            Tuple of (holdings_data, historical_data) lists
        """
        holdings_data = []
        historical_data = []
        
        if not os.path.exists(excel_file):
            print(f"    Error: Excel file not found: {excel_file}")
            return holdings_data, historical_data
        
        try:
            # Try to read with openpyxl first (for .xlsx)
            if excel_file.endswith('.xlsx') and HAS_OPENPYXL:
                excel_data = pd.ExcelFile(excel_file, engine='openpyxl')
            elif excel_file.endswith('.xls') and HAS_XLRD:
                excel_data = pd.ExcelFile(excel_file, engine='xlrd')
            else:
                # Try pandas default
                excel_data = pd.ExcelFile(excel_file)
            
            sheet_names = excel_data.sheet_names
            print(f"    Found sheets: {sheet_names}")
            
            now = datetime.now().isoformat()
            
            # Process Holdings sheet
            holdings_sheet = None
            for sheet_name in sheet_names:
                if 'holding' in sheet_name.lower():
                    holdings_sheet = sheet_name
                    break
            
            if holdings_sheet:
                print(f"    Processing Holdings sheet: {holdings_sheet}")
                df_holdings = pd.read_excel(excel_data, sheet_name=holdings_sheet)
                
                # Convert DataFrame to list of dicts, preserving all columns
                for _, row in df_holdings.iterrows():
                    record = {
                        'ticker': ticker,
                        'data_date': data_date,
                        'fund_url': fund_url,
                        'excel_file': excel_file,
                        'created_at': now,
                    }
                    # Add all columns from the Excel sheet
                    for col in df_holdings.columns:
                        value = row[col]
                        # Convert to string, handling NaN
                        if pd.isna(value):
                            record[col] = None
                        else:
                            record[col] = str(value)
                    holdings_data.append(record)
                
                print(f"    Extracted {len(holdings_data)} Holdings records")
            else:
                print(f"    Warning: Holdings sheet not found")
            
            # Process Historical sheet
            historical_sheet = None
            for sheet_name in sheet_names:
                if 'historical' in sheet_name.lower():
                    historical_sheet = sheet_name
                    break
            
            if historical_sheet:
                print(f"    Processing Historical sheet: {historical_sheet}")
                df_historical = pd.read_excel(excel_data, sheet_name=historical_sheet)
                
                # Convert DataFrame to list of dicts, preserving all columns
                for _, row in df_historical.iterrows():
                    record = {
                        'ticker': ticker,
                        'data_date': data_date,
                        'fund_url': fund_url,
                        'excel_file': excel_file,
                        'created_at': now,
                    }
                    # Add all columns from the Excel sheet
                    for col in df_historical.columns:
                        value = row[col]
                        # Convert to string, handling NaN
                        if pd.isna(value):
                            record[col] = None
                        else:
                            record[col] = str(value)
                    historical_data.append(record)
                
                print(f"    Extracted {len(historical_data)} Historical records")
            else:
                print(f"    Warning: Historical sheet not found")
            
        except Exception as e:
            print(f"    Error processing Excel file: {e}")
            import traceback
            traceback.print_exc()
        
        return holdings_data, historical_data
    
    def scrape_all_etf_details(self, 
                               etfs_parquet_file: str,
                               details_parquet_file: str,
                               data_parquet_file: Optional[str] = None,
                               limit: Optional[int] = None,
                               skip_existing: bool = True) -> List[Dict]:
        """
        Scrape detailed information from all ETF pages
        
        Args:
            etfs_parquet_file: Path to ishares_etfs.parquet file
            details_parquet_file: Path to details parquet directory
            data_parquet_file: Path to data parquet directory (for Holdings/Historical)
            limit: Maximum number of ETFs to scrape (None = all)
            skip_existing: Skip ETFs that already have details in parquet
            
        Returns:
            List of all detail dictionaries
        """
        if not HAS_SELENIUM:
            raise ImportError(
                "Selenium is required for scraping JavaScript-rendered content. "
                "Install it with: pip install selenium"
            )
        
        print("=" * 60)
        print("iShares ETF Details Scraper")
        print("=" * 60)
        
        # Load ETFs from parquet
        print(f"Loading ETFs from: {etfs_parquet_file}")
        etfs = load_ishares_etfs_from_parquet(etfs_parquet_file)
        
        if not etfs:
            print("Error: No ETFs found in parquet file")
            return []
        
        print(f"Found {len(etfs)} ETFs")
        
        # Filter ETFs with URLs
        etfs_with_urls = [e for e in etfs if e.get('fund_url')]
        print(f"Found {len(etfs_with_urls)} ETFs with URLs")
        
        if not etfs_with_urls:
            print("Error: No ETFs with URLs found")
            return []
        
        # Build benchmark index to Excel file mapping from existing data
        benchmark_excel_map = {}
        if data_parquet_file:
            data_paths = get_ishares_data_parquet_paths(data_parquet_file)
            # Check holdings parquet for existing benchmark mappings
            if os.path.exists(data_paths['holdings']):
                try:
                    existing_holdings_df = pd.read_parquet(data_paths['holdings'])
                    if 'primary_benchmark' in existing_holdings_df.columns and 'excel_file' in existing_holdings_df.columns:
                        for _, row in existing_holdings_df.iterrows():
                            benchmark = row.get('primary_benchmark')
                            excel_file = row.get('excel_file')
                            if benchmark and excel_file and os.path.exists(excel_file):
                                benchmark_excel_map[benchmark] = excel_file
                        print(f"Found {len(benchmark_excel_map)} benchmark indexes with existing Excel files")
                except Exception as e:
                    print(f"Warning: Could not load existing benchmark mappings: {e}")
        
        # Also check ETFs parquet to extract benchmark indexes
        print(f"\nAnalyzing benchmark indexes...")
        benchmark_etf_map = {}  # benchmark -> list of ETFs with that benchmark
        for etf in etfs_with_urls:
            benchmark = self._get_benchmark_index_from_etf(etf)
            if benchmark:
                if benchmark not in benchmark_etf_map:
                    benchmark_etf_map[benchmark] = []
                benchmark_etf_map[benchmark].append(etf)
        
        print(f"Found {len(benchmark_etf_map)} unique benchmark indexes across {len(etfs_with_urls)} ETFs")
        print(f"  {len(benchmark_excel_map)} benchmark indexes already have Excel files")
        print(f"  {len(benchmark_etf_map) - len(benchmark_excel_map)} benchmark indexes need Excel downloads")
        
        # Check existing details if skip_existing is True
        existing_tickers = set()
        if skip_existing:
            paths = get_ishares_details_parquet_paths(details_parquet_file)
            if os.path.exists(paths['details']):
                try:
                    existing_df = pd.read_parquet(paths['details'])
                    existing_tickers = set(existing_df['ticker'].unique())
                    print(f"Found {len(existing_tickers)} ETFs with existing details")
                except:
                    pass
        
        # Filter out existing ETFs
        if skip_existing and existing_tickers:
            etfs_with_urls = [e for e in etfs_with_urls if e.get('ticker') not in existing_tickers]
            print(f"After filtering existing: {len(etfs_with_urls)} ETFs to scrape")
        
        # Apply limit
        if limit:
            etfs_with_urls = etfs_with_urls[:limit]
            print(f"Limited to {limit} ETFs")
        
        if not etfs_with_urls:
            print("No ETFs to scrape")
            return []
        
        # Initialize parquet
        paths = get_ishares_details_parquet_paths(details_parquet_file)
        init_ishares_details_parquet_tables(paths['base_dir'])
        update_ishares_details_metadata(details_parquet_file, 'status', 'in_progress')
        
        # Initialize data parquet if provided
        data_paths = None
        if data_parquet_file:
            data_paths = get_ishares_data_parquet_paths(data_parquet_file)
            init_ishares_data_parquet_tables(data_paths['base_dir'])
            update_ishares_data_metadata(data_parquet_file, 'status', 'in_progress')
        
        # Setup Selenium
        print("\nInitializing Selenium WebDriver...")
        chrome_options = Options()
        if self.headless:
            chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument(f'user-agent={self.headers["User-Agent"]}')
        chrome_options.add_argument('--window-size=1920,1080')
        
        driver = None
        all_details = []
        all_holdings = []
        all_historical = []
        successful = 0
        failed = 0
        total_records_count = 0
        total_holdings_count = 0
        total_historical_count = 0
        files_downloaded = 0
        benchmarks_processed = set(benchmark_excel_map.keys())  # Track which benchmarks we already had
        
        try:
            driver = webdriver.Chrome(options=chrome_options)
            
            print(f"\nScraping details from {len(etfs_with_urls)} ETF pages...")
            
            for i, etf in enumerate(tqdm(etfs_with_urls, desc="Scraping ETFs"), 1):
                ticker = etf.get('ticker', '')
                fund_url = etf.get('fund_url', '')
                benchmark_index = self._get_benchmark_index_from_etf(etf)
                
                if not ticker or not fund_url:
                    failed += 1
                    continue
                
                details = self.scrape_etf_details(driver, ticker, fund_url, benchmark_index, benchmark_excel_map)
                
                if details:
                    # Separate Excel data from regular details
                    regular_details = []
                    for detail in details:
                        if detail.get('_type') == '_excel_data':
                            # Extract Excel data
                            excel_file = detail.get('_excel_file')
                            benchmark_index = detail.get('_benchmark_index')
                            holdings_data = detail.get('_holdings_data', [])
                            historical_data = detail.get('_historical_data', [])
                            
                            # Only count as downloaded if it's a new file (not reused)
                            is_reused = detail.get('_is_reused', False)
                            if excel_file and not is_reused:
                                files_downloaded += 1
                            # Track that we've processed this benchmark
                            if benchmark_index:
                                benchmarks_processed.add(benchmark_index)
                                if benchmark_index not in benchmark_excel_map and excel_file:
                                    benchmark_excel_map[benchmark_index] = excel_file
                            
                            # Add benchmark index to records
                            if holdings_data:
                                for record in holdings_data:
                                    if benchmark_index:
                                        record['primary_benchmark'] = benchmark_index
                                all_holdings.extend(holdings_data)
                                total_holdings_count += len(holdings_data)
                            if historical_data:
                                for record in historical_data:
                                    if benchmark_index:
                                        record['primary_benchmark'] = benchmark_index
                                all_historical.extend(historical_data)
                                total_historical_count += len(historical_data)
                        else:
                            regular_details.append(detail)
                    
                    all_details.extend(regular_details)
                    total_records_count += len(regular_details)
                    successful += 1
                    
                    # Save incrementally every 100 records
                    if len(all_details) >= 100:
                        added = add_ishares_details_fast(paths['details'], all_details)
                        print(f"  Saved {added} detail records (incremental save)")
                        all_details = []  # Clear after saving
                    
                    # Save Excel data incrementally
                    if data_paths:
                        if len(all_holdings) >= 100:
                            added = add_ishares_data_fast(data_paths['holdings'], all_holdings)
                            print(f"  Saved {added} holdings records (incremental save)")
                            all_holdings = []
                        if len(all_historical) >= 100:
                            added = add_ishares_data_fast(data_paths['historical'], all_historical)
                            print(f"  Saved {added} historical records (incremental save)")
                            all_historical = []
                else:
                    failed += 1
                
                # Delay between pages
                if i < len(etfs_with_urls):
                    time.sleep(self.delay_between_pages)
            
            # Save remaining details
            if all_details:
                added = add_ishares_details_fast(paths['details'], all_details)
                print(f"  Saved {added} detail records (final save)")
            
            # Save remaining Excel data
            if data_paths:
                if all_holdings:
                    added = add_ishares_data_fast(data_paths['holdings'], all_holdings)
                    print(f"  Saved {added} holdings records (final save)")
                if all_historical:
                    added = add_ishares_data_fast(data_paths['historical'], all_historical)
                    print(f"  Saved {added} historical records (final save)")
                
                # Update data metadata
                update_ishares_data_metadata(data_parquet_file, 'total_holdings_records', str(total_holdings_count))
                update_ishares_data_metadata(data_parquet_file, 'total_historical_records', str(total_historical_count))
                update_ishares_data_metadata(data_parquet_file, 'total_files_downloaded', str(files_downloaded))
                update_ishares_data_metadata(data_parquet_file, 'status', 'complete')
            
            # Update metadata
            total_records = total_records_count
            update_ishares_details_metadata(details_parquet_file, 'total_records', str(total_records))
            update_ishares_details_metadata(details_parquet_file, 'total_etfs_scraped', str(successful))
            update_ishares_details_metadata(details_parquet_file, 'status', 'complete')
            
            print(f"\n{'='*60}")
            print(f"Scraping complete!")
            print(f"  Successfully scraped: {successful}/{len(etfs_with_urls)} ETFs")
            print(f"  Failed: {failed}")
            print(f"  Total detail records: {total_records}")
            print(f"  Excel files downloaded: {files_downloaded} (unique benchmark indexes)")
            total_benchmarks = len(benchmark_excel_map)
            reused_files = max(0, total_benchmarks - files_downloaded)
            if reused_files > 0:
                print(f"  Excel files reused: {reused_files}")
            print(f"  Total holdings records: {total_holdings_count}")
            print(f"  Total historical records: {total_historical_count}")
            print(f"  Details saved to: {paths['details']}")
            if data_paths:
                print(f"  Holdings saved to: {data_paths['holdings']}")
                print(f"  Historical saved to: {data_paths['historical']}")
            print(f"{'='*60}")
            
        finally:
            if driver:
                driver.quit()
        
        return all_details

