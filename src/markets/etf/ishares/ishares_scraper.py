"""
iShares ETF Data Scraper

This module scrapes ETF data from the iShares website and stores it in Parquet format.
"""

import os
import time
import json
import requests
from typing import List, Dict, Optional
from datetime import datetime
import pandas as pd

try:
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service
    HAS_SELENIUM = True
except ImportError:
    HAS_SELENIUM = False

from tqdm import tqdm

try:
    from .ishares_parquet import init_ishares_parquet_tables, get_ishares_parquet_paths, add_ishares_etfs_fast, update_ishares_metadata
except ImportError:
    from ishares_parquet import init_ishares_parquet_tables, get_ishares_parquet_paths, add_ishares_etfs_fast, update_ishares_metadata


class iSharesScraper:
    """Class to scrape iShares ETF data from their website"""
    
    def __init__(self, headless: bool = True, use_selenium: bool = True):
        """
        Initialize iShares scraper
        
        Args:
            headless: Whether to run browser in headless mode (Selenium only)
            use_selenium: Whether to use Selenium for JavaScript rendering (if False, tries API first)
        """
        self.headless = headless
        self.use_selenium = use_selenium
        self.base_url = "https://www.ishares.com"
        self.api_base_url = "https://www.ishares.com/us/products/etf-investments"
        
        # Headers for requests
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Referer': 'https://www.ishares.com/us/products/etf-investments',
        }
    
    def _try_api_endpoint(self) -> Optional[List[Dict]]:
        """
        Try to fetch data from iShares API endpoint
        
        Returns:
            List of ETF dictionaries if successful, None otherwise
        """
        # Common API endpoint patterns for iShares
        api_endpoints = [
            f"{self.api_base_url}/api/funds",
            f"{self.api_base_url}/api/products",
            f"{self.base_url}/api/us/products/etf-investments",
            f"{self.base_url}/api/v1/products/etfs",
        ]
        
        for endpoint in api_endpoints:
            try:
                response = requests.get(endpoint, headers=self.headers, timeout=30)
                if response.status_code == 200:
                    data = response.json()
                    # Try to parse different possible response formats
                    if isinstance(data, list):
                        return data
                    elif isinstance(data, dict):
                        # Try common keys
                        for key in ['funds', 'products', 'etfs', 'data', 'items']:
                            if key in data and isinstance(data[key], list):
                                return data[key]
                    return data
            except Exception as e:
                continue
        
        return None
    
    def _get_total_pages(self, driver, wait) -> int:
        """
        Detect total number of pages from pagination controls
        
        Args:
            driver: Selenium WebDriver instance
            wait: WebDriverWait instance
            
        Returns:
            Total number of pages, or 1 if cannot detect
        """
        try:
            # Try various pagination selectors
            pagination_selectors = [
                "[aria-label*='pagination']",
                ".pagination",
                "[class*='pagination']",
                "[class*='Pagination']",
                "nav[aria-label*='page']",
                "button[aria-label*='page']",
            ]
            
            for selector in pagination_selectors:
                try:
                    pagination = driver.find_elements(By.CSS_SELECTOR, selector)
                    if pagination:
                        # Try to find all page number buttons/links
                        page_buttons = driver.find_elements(By.CSS_SELECTOR, f"{selector} button, {selector} a")
                        page_numbers = []
                        for btn in page_buttons:
                            text = btn.text.strip()
                            if text.isdigit():
                                page_numbers.append(int(text))
                        if page_numbers:
                            max_page = max(page_numbers)
                            print(f"  Detected {max_page} pages from pagination")
                            return max_page
                except:
                    continue
            
            # Try to find "of X" or "Page X of Y" text
            page_text = driver.find_elements(By.XPATH, "//*[contains(text(), 'of') or contains(text(), 'Page')]")
            for elem in page_text:
                text = elem.text
                import re
                # Look for patterns like "Page 1 of 16" or "1 of 16"
                match = re.search(r'(?:page\s+)?\d+\s+of\s+(\d+)', text, re.IGNORECASE)
                if match:
                    max_page = int(match.group(1))
                    print(f"  Detected {max_page} pages from text: {text}")
                    return max_page
            
            print("  Could not detect total pages, defaulting to 1")
            return 1
        except Exception as e:
            print(f"  Warning: Error detecting total pages: {e}")
            return 1
    
    def _scrape_page(self, driver, wait, page_num: int = 1) -> List[Dict]:
        """
        Scrape a single page of ETFs
        
        Args:
            driver: Selenium WebDriver instance
            wait: WebDriverWait instance
            page_num: Page number to scrape (1-indexed)
            
        Returns:
            List of ETF dictionaries from this page
        """
        try:
            print(f"  Scraping page {page_num}...")
            
            # Wait for the table to load (common selectors)
            # Try to find the table - common class names for ETF tables
            table_selectors = [
                "table",
                "[data-testid='fund-table']",
                ".fund-table",
                ".etf-table",
                ".products-table",
                "tbody",
            ]
            
            table_element = None
            for selector in table_selectors:
                try:
                    table_element = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, selector)))
                    if table_element:
                        print(f"    Found table with selector: {selector}")
                        break
                except:
                    continue
            
            if not table_element:
                # Try to find any table on the page
                tables = driver.find_elements(By.TAG_NAME, "table")
                if tables:
                    table_element = tables[0]
                    print(f"    Found table by tag name")
            
            if not table_element:
                # Wait a bit more and try to get page source to debug
                time.sleep(5)
                print(f"    Warning: Could not find table element on page {page_num}")
                print(f"    Page title: {driver.title}")
                return []
            
            # Wait a bit more for content to fully load
            time.sleep(2)
            
            # Parse table data
            etfs = []
            
            # Try to find all rows in the table
            rows = table_element.find_elements(By.TAG_NAME, "tr")
            if not rows:
                # Try tbody > tr
                tbody = table_element.find_elements(By.TAG_NAME, "tbody")
                if tbody:
                    rows = tbody[0].find_elements(By.TAG_NAME, "tr")
            
            print(f"    Found {len(rows)} rows in table")
            
            # Get header row if available (only on first page)
            headers = []
            if page_num == 1 and rows:
                header_row = rows[0]
                header_cells = header_row.find_elements(By.TAG_NAME, "th")
                if not header_cells:
                    header_cells = header_row.find_elements(By.TAG_NAME, "td")
                headers = [cell.text.strip() for cell in header_cells]
                if headers:
                    print(f"    Table headers: {headers}")
            
            # Parse data rows (skip header if present)
            start_idx = 1 if (page_num == 1 and headers) else 0
            for i, row in enumerate(rows[start_idx:], start=start_idx):
                try:
                    cells = row.find_elements(By.TAG_NAME, "td")
                    if not cells:
                        # Also try th tags (some tables use th for data)
                        cells = row.find_elements(By.TAG_NAME, "th")
                    if not cells:
                        continue
                    
                    row_data = [cell.text.strip() for cell in cells]
                    # Filter out empty rows
                    if not any(row_data):
                        continue
                    
                    # Try to map columns to our schema
                    etf_data = self._parse_row_data(row_data, headers, row)
                    
                    # Only add if we have at least a ticker or name
                    if etf_data.get('ticker') or etf_data.get('name'):
                        etfs.append(etf_data)
                
                except Exception as e:
                    print(f"    Warning: Error parsing row {i}: {e}")
                    continue
            
            print(f"    Extracted {len(etfs)} ETFs from page {page_num}")
            return etfs
        except Exception as e:
            print(f"    Error scraping page {page_num}: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    def _scrape_with_selenium(self, url: str, max_pages: Optional[int] = None) -> List[Dict]:
        """
        Scrape iShares website using Selenium for JavaScript rendering
        
        Args:
            url: URL to scrape (should include pageNumber parameter)
            max_pages: Maximum number of pages to scrape (None = auto-detect)
            
        Returns:
            List of ETF dictionaries from all pages
        """
        if not HAS_SELENIUM:
            raise ImportError(
                "Selenium is required for scraping JavaScript-rendered content. "
                "Install it with: pip install selenium"
            )
        
        print("Initializing Selenium WebDriver...")
        
        # Setup Chrome options
        chrome_options = Options()
        if self.headless:
            chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument(f'user-agent={self.headers["User-Agent"]}')
        chrome_options.add_argument('--window-size=1920,1080')
        
        driver = None
        all_etfs = []
        
        try:
            # Try to create Chrome driver
            driver = webdriver.Chrome(options=chrome_options)
            wait = WebDriverWait(driver, 30)
            
            # Load first page to detect total pages
            print(f"Loading first page: {url}")
            driver.get(url)
            time.sleep(3)  # Wait for initial page load
            
            # Detect total pages if not specified
            if max_pages is None:
                total_pages = self._get_total_pages(driver, wait)
            else:
                total_pages = max_pages
                print(f"  Using specified max pages: {total_pages}")
            
            # Scrape all pages
            base_url = url
            for page_num in range(1, total_pages + 1):
                if page_num > 1:
                    # Construct URL for this page
                    if 'pageNumber=' in base_url:
                        # Replace existing pageNumber parameter
                        import re
                        page_url = re.sub(r'pageNumber=\d+', f'pageNumber={page_num}', base_url)
                    else:
                        # Add pageNumber parameter
                        separator = '&' if '&' in base_url or '?' in base_url.split('#')[0] else '?'
                        if '#' in base_url:
                            # Insert before the hash
                            parts = base_url.split('#', 1)
                            page_url = f"{parts[0]}{separator}pageNumber={page_num}#{parts[1]}"
                        else:
                            page_url = f"{base_url}{separator}pageNumber={page_num}"
                    
                    print(f"  Navigating to page {page_num}...")
                    driver.get(page_url)
                    time.sleep(3)  # Wait for page to load
                
                # Scrape this page
                page_etfs = self._scrape_page(driver, wait, page_num)
                all_etfs.extend(page_etfs)
                
                # Small delay between pages
                if page_num < total_pages:
                    time.sleep(1)
            
            print(f"\n  Total ETFs scraped across {total_pages} pages: {len(all_etfs)}")
            return all_etfs
            
        finally:
            if driver:
                driver.quit()
    
    def _parse_row_data(self, row_data: List[str], headers: List[str], row_element) -> Dict:
        """
        Parse a table row into ETF dictionary
        
        Args:
            row_data: List of cell text values
            headers: List of header names (if available)
            row_element: Selenium row element (for finding links)
            
        Returns:
            Dictionary with ETF data
        """
        etf = {
            'ticker': '',
            'name': '',
            'asset_class': None,
            'expense_ratio': None,
            'total_net_assets': None,
            'ytd_return': None,
            'one_year_return': None,
            'three_year_return': None,
            'five_year_return': None,
            'ten_year_return': None,
            'inception_date': None,
            'primary_benchmark': None,
            'fund_url': None,
            'created_at': datetime.now().isoformat(),
            'updated_at': datetime.now().isoformat(),
        }
        
        # Try to find ticker symbol (usually first or second column, often all caps, 2-5 chars)
        for i, cell_text in enumerate(row_data):
            cell_text_clean = cell_text.upper().strip()
            # Ticker patterns: 2-5 uppercase letters, possibly with numbers
            if cell_text_clean and len(cell_text_clean) <= 5 and cell_text_clean.isalnum():
                etf['ticker'] = cell_text_clean
                break
        
        # Try to find name (usually contains more text, not just ticker)
        for i, cell_text in enumerate(row_data):
            if cell_text and len(cell_text) > 5 and cell_text != etf['ticker']:
                etf['name'] = cell_text
                break
        
        # Map headers to fields if available
        if headers:
            header_mapping = {
                'ticker': ['ticker', 'symbol', 'ticker symbol'],
                'name': ['name', 'fund name', 'etf name', 'product name'],
                'asset_class': ['asset class', 'asset', 'category'],
                'expense_ratio': ['expense ratio', 'exp ratio', 'er', 'fee'],
                'total_net_assets': ['total net assets', 'aum', 'assets', 'net assets', 'tna'],
                'ytd_return': ['ytd', 'ytd return', 'year to date'],
                'one_year_return': ['1 year', '1yr', 'one year', '1-year'],
                'three_year_return': ['3 year', '3yr', 'three year', '3-year'],
                'five_year_return': ['5 year', '5yr', 'five year', '5-year'],
                'ten_year_return': ['10 year', '10yr', 'ten year', '10-year'],
                'inception_date': ['inception', 'inception date', 'start date'],
                'primary_benchmark': ['benchmark', 'index', 'primary benchmark'],
            }
            
            for field, possible_names in header_mapping.items():
                for j, header in enumerate(headers):
                    header_lower = header.lower()
                    if any(name in header_lower for name in possible_names):
                        if j < len(row_data) and row_data[j]:
                            etf[field] = row_data[j]
                        break
        
        # Try to find fund URL from links in the row
        try:
            links = row_element.find_elements(By.TAG_NAME, "a")
            for link in links:
                href = link.get_attribute('href')
                if href and ('/us/products/' in href or '/funds/' in href):
                    etf['fund_url'] = href
                    break
        except:
            pass
        
        return etf
    
    def scrape_all_etfs(self, url: Optional[str] = None, parquet_file: Optional[str] = None, max_pages: Optional[int] = None) -> List[Dict]:
        """
        Scrape all ETFs from iShares website
        
        Args:
            url: URL to scrape (default: uses the iShares ETF investments page)
            parquet_file: Optional path to parquet directory to save data incrementally
            max_pages: Maximum number of pages to scrape (None = auto-detect, use 16 to scrape all)
            
        Returns:
            List of ETF dictionaries
        """
        if url is None:
            url = "https://www.ishares.com/us/products/etf-investments#/?productView=etf&pageNumber=1&sortColumn=totalNetAssets&sortDirection=desc&dataView=keyFacts&style=44342"
        
        print("=" * 60)
        print("iShares ETF Data Scraper")
        print("=" * 60)
        print(f"URL: {url}")
        print()
        
        etfs = []
        
        # Initialize Parquet if requested
        paths = None
        if parquet_file:
            paths = get_ishares_parquet_paths(parquet_file)
            init_ishares_parquet_tables(paths['base_dir'])
            update_ishares_metadata(parquet_file, 'status', 'in_progress')
            update_ishares_metadata(parquet_file, 'generated_at', datetime.now().isoformat())
            print(f"Initialized iShares Parquet database: {paths['base_dir']}")
        
        # Try API endpoint first if not forcing Selenium
        if not self.use_selenium:
            print("Attempting to fetch data via API endpoint...")
            api_data = self._try_api_endpoint()
            if api_data:
                print(f"  Successfully fetched {len(api_data)} ETFs from API")
                etfs = api_data
            else:
                print("  API endpoint not available, falling back to Selenium...")
                self.use_selenium = True
        
        # Use Selenium if needed
        if self.use_selenium:
            print("Scraping with Selenium...")
            if max_pages is None:
                max_pages = 16  # Default to 16 pages as user mentioned
                print(f"  Defaulting to {max_pages} pages (override with --max-pages)")
            etfs = self._scrape_with_selenium(url, max_pages=max_pages)
        
        if not etfs:
            print("Warning: No ETFs found")
            return []
        
        print(f"\nFound {len(etfs)} ETFs")
        
        # Save to Parquet if requested
        if parquet_file and paths:
            print(f"\nSaving to Parquet: {paths['etfs']}")
            added = add_ishares_etfs_fast(paths['etfs'], etfs)
            update_ishares_metadata(parquet_file, 'total_etfs', str(len(etfs)))
            update_ishares_metadata(parquet_file, 'status', 'complete')
            print(f"  Saved {added} ETFs to Parquet")
        
        return etfs

