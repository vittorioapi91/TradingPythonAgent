"""
FRED Economic Data Downloader

This module provides functionality to download time series data from FRED (Federal Reserve Economic Data).
"""

import os
import time
import ssl
from typing import List, Dict, Optional, Set
import pandas as pd
from fredapi import Fred
import requests
import urllib.request
import urllib.error
import warnings
from datetime import datetime

# Import FRED PostgreSQL utilities
try:
    from .fred_postgres import (
        get_postgres_connection, init_fred_postgres_tables, 
        add_fred_series_fast, load_fred_series_from_postgres, 
        update_fred_metadata, add_fred_categories_fast, 
        load_fred_categories_from_postgres, add_time_series_fast,
        load_time_series_from_postgres
    )
except ImportError:
    from fred_postgres import (
        get_postgres_connection, init_fred_postgres_tables, 
        add_fred_series_fast, load_fred_series_from_postgres, 
        update_fred_metadata, add_fred_categories_fast, 
        load_fred_categories_from_postgres, add_time_series_fast,
        load_time_series_from_postgres
    )

# Try to import certifi for proper SSL certificates
try:
    import certifi
    HAS_CERTIFI = True
except ImportError:
    HAS_CERTIFI = False
    certifi = None

warnings.filterwarnings('ignore')


class FREDDataDownloader:
    """Class to download economic time series data from FRED"""
    
    def __init__(self, api_key: Optional[str] = None, verify_ssl: bool = None,
                 user: str = "postgres", host: str = "localhost",
                 password: Optional[str] = None, port: int = 5432):
        """
        Initialize FRED data downloader
        
        Args:
            api_key: FRED API key. If not provided, will use default key or try to get from environment variable FRED_API_KEY
            verify_ssl: Whether to verify SSL certificates. If None, tries to use certifi if available, otherwise disables verification.
            user: PostgreSQL user (default: 'postgres')
            host: PostgreSQL host (default: 'localhost')
            password: PostgreSQL password (optional, can use POSTGRES_PASSWORD env var, default: '2014')
            port: PostgreSQL port (default: 5432)
        """
        # Default API key
        default_api_key = "b563d1b13b667fa632804af206064e35"
        self.api_key = api_key or os.getenv('FRED_API_KEY') or default_api_key
        
        # Handle SSL verification
        if verify_ssl is None:
            # Auto-detect: use certifi if available, otherwise disable verification
            verify_ssl = HAS_CERTIFI
        
        self.verify_ssl = verify_ssl
        
        # Set up SSL context
        if verify_ssl and HAS_CERTIFI:
            try:
                # Create SSL context with certifi certificates
                ssl_context = ssl.create_default_context(cafile=certifi.where())
                # Create HTTPS handler with the SSL context
                https_handler = urllib.request.HTTPSHandler(context=ssl_context)
                opener = urllib.request.build_opener(https_handler)
                urllib.request.install_opener(opener)
            except Exception as e:
                print(f"Warning: Could not set up SSL context with certifi: {e}")
                print("Falling back to unverified SSL context")
                verify_ssl = False
        
        if not verify_ssl:
            # Disable SSL verification by creating an unverified context
            try:
                ssl_context = ssl.create_default_context()
                ssl_context.check_hostname = False
                ssl_context.verify_mode = ssl.CERT_NONE
                https_handler = urllib.request.HTTPSHandler(context=ssl_context)
                opener = urllib.request.build_opener(https_handler)
                urllib.request.install_opener(opener)
                # Also disable SSL warnings for urllib3 (used by requests)
                import urllib3
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            except Exception as e:
                print(f"Warning: Could not set up unverified SSL context: {e}")
        
        # Initialize Fred client
        self.fred = Fred(api_key=self.api_key)
        
        # Store PostgreSQL connection parameters (dbname is fixed to 'fred')
        self.dbname = 'fred'
        self.user = user
        self.host = host
        self.password = password or os.getenv('POSTGRES_PASSWORD', '2014')
        self.port = port
        self._pg_conn = None
    
    def _get_pg_connection(self):
        """Get or create PostgreSQL connection"""
        if self._pg_conn is None or self._pg_conn.closed:
            self._pg_conn = get_postgres_connection(
                dbname=self.dbname,
                user=self.user,
                host=self.host,
                password=self.password,
                port=self.port
            )
            init_fred_postgres_tables(self._pg_conn)
        return self._pg_conn
        
    def get_all_series_ids(self, limit: Optional[int] = None, search_text: str = '') -> List[str]:
        """
        Get all available FRED series IDs
        
        Args:
            limit: Maximum number of series to return (None for all)
            search_text: Optional search text to filter series
            
        Returns:
            List of series IDs
        """
        print(f"Fetching FRED series IDs (search: '{search_text}')...")
        
        try:
            series_ids = []
            
            # Use FRED's search functionality
            if search_text:
                # Search for series with specific text
                try:
                    search_results = self.fred.search(search_text, order_by='popularity', limit=limit or 1000)
                    if isinstance(search_results, pd.DataFrame) and not search_results.empty:
                        series_ids = search_results.index.tolist()
                except Exception as e:
                    print(f"Error searching: {e}")
            else:
                # To get "all" series, we need to explore multiple approaches
                # FRED doesn't provide a direct "get all" endpoint
                # Strategy: Search with common economic terms and explore categories
                
                # Approach 1: Search with common economic keywords to discover series
                # This is the most reliable method as FRED's search API is well-supported
                search_terms = [
                    '',  # Empty search returns popular series
                    'GDP', 'unemployment', 'inflation', 'interest rate',
                    'employment', 'production', 'consumer', 'price',
                    'money', 'banking', 'trade', 'exchange rate',
                    'industrial', 'retail', 'housing', 'manufacturing',
                    'income', 'sales', 'index', 'rate', 'percent',
                    'billion', 'million', 'thousand', 'dollar'
                ]
                
                print("Searching FRED with common economic terms...")
                for i, term in enumerate(search_terms):
                    try:
                        search_limit = min(1000, limit - len(set(series_ids)) if limit else 1000)
                        if search_limit <= 0:
                            break
                            
                        results = self.fred.search(term, order_by='popularity', limit=search_limit)
                        if isinstance(results, pd.DataFrame) and not results.empty:
                            new_series = results.index.tolist()
                            series_ids.extend(new_series)
                            unique_count = len(set(series_ids))
                            print(f"  Term '{term}': +{len(new_series)} series (total: {unique_count})", end='\r')
                        
                        time.sleep(0.2)  # Rate limiting (FRED allows 120 req/min)
                    except Exception as e:
                        print(f"\n  Warning: Error searching with term '{term}': {e}")
                        continue
                
                print()  # New line after progress updates
                
                # Approach 2: Try to get popular series without search terms
                # Some fredapi versions support getting popular series directly
                try:
                    popular_results = self.fred.search('', order_by='popularity', limit=5000)
                    if isinstance(popular_results, pd.DataFrame) and not popular_results.empty:
                        series_ids.extend(popular_results.index.tolist())
                except:
                    pass  # Not all versions support this
            
            # Remove duplicates and apply limit
            series_ids = list(set(series_ids))
            if limit:
                series_ids = series_ids[:limit]
            
            print(f"Found {len(series_ids)} unique series")
            return series_ids
            
        except Exception as e:
            print(f"Error fetching series IDs: {e}")
            return []
    
    def _get_known_categories(self) -> List[int]:
        """
        Get known major FRED category IDs (fast - no API calls)
        
        Returns:
            List of known major category IDs
        """
        # FRED's major economic categories - these are well-known and don't require API calls
        major_categories = [
            1,      # National Accounts
            3,      # Production & Business Activity
            5,      # Additional category
            6,      # Additional category
            9,      # Additional category
            10,     # Money, Banking, & Finance
            11,     # Additional category
            12,     # Additional category
            13,     # Population, Employment, & Labor Markets
            15,     # Additional category
            18,     # Additional category
            22,     # Additional category
            24,     # Additional category
            31,     # Additional category
            32,     # Prices
            46,     # Additional category
            50,     # International Data
            94,     # Additional category
            95,     # Additional category
            106,    # Academic Data
            115,    # Additional category
            120,    # Additional category
            398,    # Additional category
            32217,  # Additional category
            32255,  # US Rig Count
            32263,  # Additional category
            32446,  # Additional category
            32455,  # Additional category
            33060,  # Additional category
            33061,  # Additional category
            33939,  # Additional category
            33951,  # Additional category
        ]
        return sorted(major_categories)  # Sort for easier reading
    
    def _get_all_categories(self) -> List[int]:
        """
        Recursively get all FRED category IDs starting from root category (0)
        Saves category tree to PostgreSQL database
        
        Returns:
            List of all category IDs
        """
        all_cats: Set[int] = set()
        visited: Set[int] = set()
        categories_info: List[Dict] = []
        categories_saved_count = 0
        base_url = "https://api.stlouisfed.org/fred/category/children"
        category_info_url = "https://api.stlouisfed.org/fred/category"
        batch_save_interval = 50  # Save every 50 categories
        conn = self._get_pg_connection()
        
        def save_categories_batch() -> None:
            """Helper to save categories in batch"""
            nonlocal categories_saved_count
            if categories_info:
                try:
                    # Get only new categories since last save
                    new_categories = categories_info[categories_saved_count:]
                    if new_categories:
                        added = add_fred_categories_fast(conn, new_categories)
                        categories_saved_count = len(categories_info)
                except Exception as e:
                    pass  # Will save at end
        
        def get_category_children_recursive(category_id: int, parent_id: Optional[int] = None) -> None:
            """
            Recursively fetch all child categories starting from a parent category.
            Saves the ENTIRE category tree including all parent and child categories.
            
            Args:
                category_id: The category ID to process
                parent_id: The parent category ID (None for root)
            """
            # Avoid infinite loops
            if category_id in visited:
                return
            visited.add(category_id)
            all_cats.add(category_id)
            
            try:
                # Get category info (name) - this ensures we save EVERY category we visit
                cat_params = {
                    'category_id': category_id,
                    'api_key': self.api_key,
                    'file_type': 'json'
                }
                cat_response = requests.get(category_info_url, params=cat_params, timeout=10, verify=self.verify_ssl)
                category_name = f'Category {category_id}'
                if cat_response.status_code == 200:
                    cat_data = cat_response.json()
                    if 'categories' in cat_data and cat_data['categories']:
                        category_name = cat_data['categories'][0].get('name', category_name)
                
                # Create category info dict - this saves the current category
                cat_info = {
                    'category_id': str(category_id),
                    'name': category_name,
                    'parent_id': int(parent_id) if parent_id is not None and parent_id != '' else 0,
                    'created_at': datetime.now().isoformat(),
                    'updated_at': datetime.now().isoformat(),
                }
                
                # Add to list (will be saved in batch)
                categories_info.append(cat_info)

                # Log discovered category (simple console log)
                parent_str = str(parent_id) if parent_id is not None else 'None'
                print(f"  Discovered category {category_id} (parent {parent_str}): {category_name}")
                
                # Batch save periodically
                if len(categories_info) - categories_saved_count >= batch_save_interval:
                    save_categories_batch()
                
                # Use FRED API REST endpoint to get category children
                params = {
                    'category_id': category_id,
                    'api_key': self.api_key,
                    'file_type': 'json'
                }
                
                response = requests.get(base_url, params=params, timeout=10, verify=self.verify_ssl)
                response.raise_for_status()
                
                data = response.json()
                
                # Check if we have categories in the response
                if 'categories' in data and data['categories']:
                    # For each child category, recursively process it
                    # This ensures we save ALL categories in the tree (parents and children)
                    for category in data['categories']:
                        child_id = category.get('id')
                        if child_id:
                            # Recursively get children of this category
                            # Pass current category_id as parent_id for the child
                            # This builds the complete tree structure
                            get_category_children_recursive(child_id, parent_id=category_id)
                            time.sleep(0.1)  # Rate limiting
                            
            except requests.exceptions.RequestException as e:
                print(f"  Warning: Error fetching children of category {category_id}: {e}")
            except Exception as e:
                print(f"  Warning: Unexpected error for category {category_id}: {e}")
        
        print("Fetching all FRED categories recursively...")
        print("  This will save the ENTIRE category tree (all parents and children)...")
        # Start from root category (0) which contains all top-level categories
        get_category_children_recursive(0, parent_id=None)
        print(f"Found {len(all_cats)} total categories")
        
        # Final batch save to ensure all categories are saved
        if categories_info:
            try:
                save_categories_batch()  # Save any remaining
                # Also do a final full save to ensure nothing is missed
                add_fred_categories_fast(conn, categories_info)
                print(f"  Saved {len(categories_info)} categories to PostgreSQL (complete tree)")
            except Exception as e:
                print(f"  Warning: Error saving categories to PostgreSQL: {e}")
        
        return sorted(list(all_cats))
    
    def _extract_country_from_series(self, series_id: str, series_data: Dict, geography: str = '') -> str:
        """
        Extract country information from series ID or metadata
        
        Args:
            series_id: Series ID
            series_data: Series data dictionary
            geography: Geography field from series data
            
        Returns:
            Country name
        """
        # Common country codes in FRED series IDs
        country_codes = {
            'US': 'United States',
            'USA': 'United States',
            'AUS': 'Australia',
            'CAN': 'Canada',
            'CHN': 'China',
            'DEU': 'Germany',
            'FRA': 'France',
            'GBR': 'United Kingdom',
            'ITA': 'Italy',
            'JPN': 'Japan',
            'MEX': 'Mexico',
            'BRA': 'Brazil',
            'IND': 'India',
            'KOR': 'South Korea',
            'RUS': 'Russia',
            'ZAF': 'South Africa',
        }
        
        # Try to get from geography field
        if geography:
            geography_upper = geography.upper()
            for code, country in country_codes.items():
                if code in geography_upper:
                    return country
        
        # Try to extract from series ID (common patterns: country code at end)
        series_id_upper = series_id.upper()
        for code, country in country_codes.items():
            if series_id_upper.endswith(code) or code in series_id_upper:
                return country
        
        # Check if it's a US state code (2-letter codes at end)
        if len(series_id) >= 2:
            last_two = series_id[-2:].upper()
            # Common US state codes
            us_states = ['AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
                        'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
                        'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
                        'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
                        'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY']
            if last_two in us_states:
                return 'United States'
        
        # Default to United States for most FRED series
        return 'United States'
    
    def download_series(self, series_id: str, start_date: Optional[str] = None, 
                       end_date: Optional[str] = None, save_to_db: bool = True) -> Optional[pd.Series]:
        """
        Download a single FRED time series and optionally save to PostgreSQL
        
        Args:
            series_id: FRED series ID
            start_date: Start date (YYYY-MM-DD format)
            end_date: End date (YYYY-MM-DD format)
            save_to_db: Whether to save to PostgreSQL database (default: True)
            
        Returns:
            pandas Series with the time series data
        """
        try:
            data = self.fred.get_series(series_id, start=start_date, end=end_date)
            
            if save_to_db and data is not None and not data.empty:
                # Save to PostgreSQL
                conn = self._get_pg_connection()
                time_series_list = []
                for date, value in data.items():
                    time_series_list.append({
                        'series_id': series_id,
                        'date': date.strftime('%Y-%m-%d') if hasattr(date, 'strftime') else str(date),
                        'value': float(value) if pd.notna(value) else None
                    })
                
                if time_series_list:
                    add_time_series_fast(conn, time_series_list)
                    print(f"  Saved {len(time_series_list)} data points to database for {series_id}")
            
            return data
        except Exception as e:
            print(f"Error downloading series {series_id}: {e}")
            return None
    
    def download_multiple_series(self, series_ids: List[str], 
                                 start_date: Optional[str] = None,
                                 end_date: Optional[str] = None,
                                 save_to_db: bool = True) -> pd.DataFrame:
        """
        Download multiple FRED time series and combine into a DataFrame
        
        Args:
            series_ids: List of FRED series IDs to download
            start_date: Start date (YYYY-MM-DD format)
            end_date: End date (YYYY-MM-DD format)
            save_to_db: Whether to save to PostgreSQL database (default: True)
            
        Returns:
            DataFrame with all series as columns
        """
        print(f"Downloading {len(series_ids)} series...")
        
        all_data = {}
        failed_series = []
        
        for i, series_id in enumerate(series_ids):
            try:
                print(f"Downloading {i+1}/{len(series_ids)}: {series_id}", end='\r')
                data = self.download_series(series_id, start_date, end_date, save_to_db=save_to_db)
                
                if data is not None and not data.empty:
                    all_data[series_id] = data
                else:
                    failed_series.append(series_id)
                
                # Respect API rate limits (FRED allows 120 requests per minute)
                time.sleep(0.5)
                
            except Exception as e:
                print(f"\nError downloading {series_id}: {e}")
                failed_series.append(series_id)
                continue
        
        print(f"\nSuccessfully downloaded {len(all_data)}/{len(series_ids)} series")
        if failed_series:
            print(f"Failed to download {len(failed_series)} series: {failed_series[:10]}...")
        
        # Combine into DataFrame
        if all_data:
            df = pd.DataFrame(all_data)
            df.index.name = 'date'
            
            return df
        else:
            print("No data was successfully downloaded")
            return pd.DataFrame()
    
    def download_all_available_series(self, start_date: Optional[str] = None,
                                      end_date: Optional[str] = None,
                                      limit: Optional[int] = None) -> Dict[str, pd.DataFrame]:
        """
        Download all available FRED economic variables and save to PostgreSQL
        
        Args:
            start_date: Start date for time series (YYYY-MM-DD format)
            end_date: End date for time series (YYYY-MM-DD format)
            limit: Maximum number of series to download (None for all)
            
        Returns:
            Dictionary mapping series IDs to DataFrames
        """
        print("=" * 60)
        print("FRED Economic Data Downloader")
        print("=" * 60)
        
        # Get all series IDs
        series_ids = self.get_all_series_ids(limit=limit)
        
        if not series_ids:
            print("No series found to download")
            return {}
        
        print(f"\nStarting download of {len(series_ids)} series...")
        print(f"All data will be saved to PostgreSQL database")
        
        all_data = {}
        successful = 0
        failed = 0
        
        # Download all series
        for i, series_id in enumerate(series_ids):
            try:
                print(f"Downloading {i+1}/{len(series_ids)}: {series_id}", end='\r')
                data = self.download_series(series_id, start_date=start_date, end_date=end_date, save_to_db=True)
                
                if data is not None and not data.empty:
                    df = data.to_frame(name=series_id)
                    df.index.name = 'date'
                    all_data[series_id] = df
                    successful += 1
                else:
                    failed += 1
                
                time.sleep(0.5)  # Rate limiting
                
            except Exception as e:
                print(f"\nError downloading {series_id}: {e}")
                failed += 1
                continue
        
        print(f"\n\nDownload complete!")
        print(f"  Successfully downloaded: {successful} series")
        print(f"  Failed: {failed} series")
        print(f"  All data saved to PostgreSQL database")
        
        return all_data
    
    def get_all_downloadable_series(self, limit: Optional[int] = None, 
                                     use_categories: bool = True,
                                     use_known_categories_only: bool = True,
                                     use_search_terms: bool = False,
                                     write_interval: int = 50,
                                     category_roots: Optional[List[int]] = None) -> List[Dict[str, str]]:
        """
        Retrieve all downloadable FRED series and save to PostgreSQL database incrementally
        
        Args:
            limit: Maximum number of series to retrieve (None for all)
            use_categories: If True, explore categories to find series. If False, use search terms only.
            use_known_categories_only: If True, use a limited set of root categories (fast). If False, recursively explore all categories (slow).
            use_search_terms: If True, also search using economic terms to find additional series (slower but more comprehensive).
            write_interval: Number of series to collect before writing to database
            category_roots: Optional list of FRED category IDs to use as roots when use_known_categories_only=True.
                            If provided, they replace the built-in "known categories" list.
            
        Returns:
            List of dictionaries with series information (id, title, etc.)
        """
        print("=" * 60)
        print("Retrieving all downloadable FRED series...")
        print("=" * 60)
        
        all_series_info = []
        series_ids_set: Set[str] = set()
        series_count_at_last_write = 0
        
        # Initialize PostgreSQL connection
        conn = self._get_pg_connection()
        update_fred_metadata(conn, 'status', 'in_progress')
        update_fred_metadata(conn, 'generated_at', datetime.now().isoformat())
        print(f"Connected to PostgreSQL database: {self.dbname}")
        
        def write_to_db_incrementally(show_status=True):
            """Helper function to write current state to PostgreSQL database"""
            if all_series_info:
                try:
                    # Get only new series since last write
                    new_series = all_series_info[series_count_at_last_write:]
                    if not new_series:
                        return
                    
                    # Convert series info to database format
                    db_series = []
                    for series in new_series:
                        db_series.append({
                            'series_id': series.get('id', ''),
                            'title': series.get('title', ''),
                            'description': series.get('description', ''),
                            'frequency': series.get('frequency', ''),
                            'units': series.get('units', ''),
                            'category_id': str(series.get('category_id', '')),
                            'category_name': series.get('category_name', ''),
                            'observation_start': series.get('observation_start', ''),
                            'observation_end': series.get('observation_end', ''),
                            'country': series.get('country', ''),
                            'last_updated': series.get('last_updated', ''),
                            'popularity': str(series.get('popularity', '')),
                            'created_at': datetime.now().isoformat(),
                            'updated_at': datetime.now().isoformat(),
                        })
                    
                    if db_series:
                        add_fred_series_fast(conn, db_series)
                        update_fred_metadata(conn, 'total_series', str(len(all_series_info)))
                    
                    if show_status:
                        status_msg = f"  [{len(all_series_info)} series] Progress saved to database"
                        if limit:
                            status_msg += f" ({len(all_series_info)}/{limit})"
                        print(status_msg)
                except Exception as e:
                    print(f"  Warning: Error writing to database: {e}")
        
        def print_status(message="", force=False):
            """Print status with series count"""
            if force or len(all_series_info) % 10 == 0:  # Print every 10 series
                status = f"[{len(all_series_info)} series discovered]"
                if limit:
                    status += f" ({len(all_series_info)}/{limit})"
                if message:
                    print(f"{status} {message}", end='\r', flush=True)
                else:
                    print(status, end='\r', flush=True)
        
        try:
            if use_categories:
                # Approach 1: Get series from categories
                if use_known_categories_only:
                    # If user provided explicit category roots, use those instead of the built-in list
                    if category_roots:
                        categories = sorted(set(category_roots))
                        print("\nExploring user-specified category roots to find series (fast mode)...")
                        print(f"Using {len(categories)} root categories: {categories}")
                    else:
                        print("\nExploring known major categories to find series (fast mode)...")
                        categories = self._get_known_categories()
                        print(f"Using {len(categories)} known major categories. Exploring for series...")
                else:
                    print("\nExploring ALL categories to find series (this may take a while)...")
                    categories = self._get_all_categories()
                    print(f"Found {len(categories)} categories. Exploring for series...")
                
                base_url = "https://api.stlouisfed.org/fred/category/series"
                category_info_url = "https://api.stlouisfed.org/fred/category"
                
                # Cache category names to avoid repeated API calls
                category_names_cache = {}
                
                for i, cat_id in enumerate(categories):
                    try:
                        # Get category name if not in cache
                        if cat_id not in category_names_cache:
                            try:
                                cat_params = {
                                    'category_id': cat_id,
                                    'api_key': self.api_key,
                                    'file_type': 'json'
                                }
                                cat_response = requests.get(category_info_url, params=cat_params, timeout=10, verify=self.verify_ssl)
                                if cat_response.status_code == 200:
                                    cat_data = cat_response.json()
                                    if 'categories' in cat_data and cat_data['categories']:
                                        category_names_cache[cat_id] = cat_data['categories'][0].get('name', f'Category {cat_id}')
                                    else:
                                        category_names_cache[cat_id] = f'Category {cat_id}'
                                else:
                                    category_names_cache[cat_id] = f'Category {cat_id}'
                            except:
                                category_names_cache[cat_id] = f'Category {cat_id}'
                        
                        category_name = category_names_cache[cat_id]
                        # Log which category is being processed (ID + name)
                        print(f"Exploring category {cat_id} ({category_name}) [{i+1}/{len(categories)}]")
                        
                        params = {
                            'category_id': cat_id,
                            'api_key': self.api_key,
                            'file_type': 'json',
                            'limit': 1000
                        }
                        
                        response = requests.get(base_url, params=params, timeout=10, verify=self.verify_ssl)
                        response.raise_for_status()
                        data = response.json()
                        
                        if 'seriess' in data and data['seriess']:
                            for series in data['seriess']:
                                series_id = series.get('id')
                                if series_id and series_id not in series_ids_set:
                                    series_ids_set.add(series_id)
                                    
                                    # Extract country information
                                    country = self._extract_country_from_series(series_id, series, '')
                                    
                                    # Get description if available
                                    description = series.get('notes', '') or series.get('description', '')
                                    
                                    all_series_info.append({
                                        'id': series_id,
                                        'title': series.get('title', ''),
                                        'description': description,
                                        'units': series.get('units', ''),
                                        'frequency': series.get('frequency', ''),
                                        'seasonal_adjustment': series.get('seasonal_adjustment', ''),
                                        'observation_start': series.get('observation_start', ''),
                                        'observation_end': series.get('observation_end', ''),
                                        'country': country,
                                        'last_updated': series.get('last_updated', ''),
                                        'popularity': series.get('popularity', ''),
                                        'category_id': cat_id,
                                        'category_name': category_name,
                                    })
                                    
                                    # Print status periodically
                                    print_status(f"Processing category {i+1}/{len(categories)}")
                                    
                                    # Write to database incrementally
                                    if len(all_series_info) - series_count_at_last_write >= write_interval:
                                        print()  # New line before status message
                                        write_to_db_incrementally()
                                        series_count_at_last_write = len(all_series_info)
                                    
                                    if limit and len(series_ids_set) >= limit:
                                        break
                        
                        if (i + 1) % 50 == 0:
                            print()  # New line after progress updates
                            print(f"  ✓ Processed {i + 1}/{len(categories)} categories | [{len(series_ids_set)} unique series discovered]")
                            # Also write to database at category checkpoints
                            if len(all_series_info) > series_count_at_last_write:
                                write_to_db_incrementally()
                                series_count_at_last_write = len(all_series_info)
                        
                        if limit and len(series_ids_set) >= limit:
                            break
                            
                        time.sleep(0.1)  # Rate limiting
                        
                    except Exception as e:
                        continue
                
                print(f"Found {len(series_ids_set)} series from categories")
            
            # Approach 2: Use search terms to find additional series (only if enabled)
            if use_search_terms and (not limit or len(series_ids_set) < limit):
                print("\nSearching with economic terms to find additional series...")
                search_terms = [
                    '', 'GDP', 'unemployment', 'inflation', 'interest rate',
                    'employment', 'production', 'consumer', 'price',
                    'money', 'banking', 'trade', 'exchange rate',
                    'industrial', 'retail', 'housing', 'manufacturing'
                ]
                
                remaining_limit = (limit - len(series_ids_set)) if limit else None
                
                for term in search_terms:
                    if limit and len(series_ids_set) >= limit:
                        break
                    
                    try:
                        search_limit = min(1000, remaining_limit if remaining_limit else 1000)
                        results = self.fred.search(term, order_by='popularity', limit=search_limit)
                        
                        if isinstance(results, pd.DataFrame) and not results.empty:
                            for series_id in results.index:
                                if series_id not in series_ids_set:
                                    series_ids_set.add(series_id)
                                    # Try to get series info
                                    try:
                                        info = self.fred.get_series_info(series_id)
                                        if isinstance(info, pd.Series):
                                            info_dict = info.to_dict() if hasattr(info, 'to_dict') else {}
                                            country = self._extract_country_from_series(series_id, info_dict, '')
                                            
                                            # Get category_id if available
                                            category_id = info_dict.get('category_id')
                                            category_name = 'Found via search'
                                            if category_id:
                                                # Try to get category name
                                                try:
                                                    cat_params = {
                                                        'category_id': category_id,
                                                        'api_key': self.api_key,
                                                        'file_type': 'json'
                                                    }
                                                    cat_response = requests.get("https://api.stlouisfed.org/fred/category", params=cat_params, timeout=5, verify=self.verify_ssl)
                                                    if cat_response.status_code == 200:
                                                        cat_data = cat_response.json()
                                                        if 'categories' in cat_data and cat_data['categories']:
                                                            category_name = cat_data['categories'][0].get('name', f'Category {category_id}')
                                                except:
                                                    category_name = f'Category {category_id}'
                                            
                                            description = info_dict.get('notes', '') or info_dict.get('description', '')
                                            
                                            all_series_info.append({
                                                'id': series_id,
                                                'title': info_dict.get('title', ''),
                                                'description': description,
                                                'units': info_dict.get('units', ''),
                                                'frequency': info_dict.get('frequency', ''),
                                                'seasonal_adjustment': info_dict.get('seasonal_adjustment', ''),
                                                'observation_start': info_dict.get('observation_start', ''),
                                                'observation_end': info_dict.get('observation_end', ''),
                                                'country': country,
                                                'last_updated': info_dict.get('last_updated', ''),
                                                'popularity': info_dict.get('popularity', ''),
                                                'category_id': str(category_id) if category_id else '',
                                                'category_name': category_name,
                                            })
                                    except:
                                        # Fallback with minimal info
                                        country = self._extract_country_from_series(series_id, {}, '')
                                        all_series_info.append({
                                            'id': series_id,
                                            'title': '',
                                            'description': '',
                                            'units': '',
                                            'frequency': '',
                                            'seasonal_adjustment': '',
                                            'observation_start': '',
                                            'observation_end': '',
                                            'country': country,
                                            'last_updated': '',
                                            'popularity': '',
                                            'category_id': '',
                                            'category_name': 'Found via search',
                                        })
                                    
                                    # Print status periodically
                                    print_status(f"Searching term: '{term}'")
                                    
                                    # Write to database incrementally
                                    if len(all_series_info) - series_count_at_last_write >= write_interval:
                                        print()  # New line before status message
                                        write_to_db_incrementally()
                                        series_count_at_last_write = len(all_series_info)
                                    
                                    if limit and len(series_ids_set) >= limit:
                                        break
                        
                        time.sleep(0.2)
                    except Exception as e:
                        continue
            
            print()  # Clear any in-progress status line
            print(f"\n{'='*60}")
            print(f"✓ Total unique series found: {len(all_series_info)}")
            if limit:
                print(f"  Limit: {limit} | Found: {len(all_series_info)}")
            print(f"{'='*60}")
            
            # Final save to database (mark as complete)
            # Write final batch of series
            if all_series_info:
                new_series = all_series_info[series_count_at_last_write:]
                if new_series:
                    db_series = []
                    for series in new_series:
                        db_series.append({
                            'series_id': series.get('id', ''),
                            'title': series.get('title', ''),
                            'description': series.get('description', ''),
                            'frequency': series.get('frequency', ''),
                            'units': series.get('units', ''),
                            'category_id': str(series.get('category_id', '')),
                            'category_name': series.get('category_name', ''),
                            'observation_start': series.get('observation_start', ''),
                            'observation_end': series.get('observation_end', ''),
                            'country': series.get('country', ''),
                            'last_updated': series.get('last_updated', ''),
                            'popularity': str(series.get('popularity', '')),
                            'created_at': datetime.now().isoformat(),
                            'updated_at': datetime.now().isoformat(),
                        })
                    if db_series:
                        add_fred_series_fast(conn, db_series)
            
            update_fred_metadata(conn, 'total_series', str(len(all_series_info)))
            update_fred_metadata(conn, 'status', 'complete')
            print(f"✓ FRED database finalized: {self.dbname}")
            print(f"  Total series saved: {len(all_series_info)}")
            
            return all_series_info
            
        except Exception as e:
            print(f"Error retrieving downloadable series: {e}")
            import traceback
            traceback.print_exc()
            return all_series_info
    
    def load_series_from_db(self, category_ids: Optional[List[int]] = None) -> List[str]:
        """
        Load series IDs from PostgreSQL database
        
        Args:
            category_ids: Optional list of category IDs to filter by
            
        Returns:
            List of series IDs
        """
        try:
            conn = self._get_pg_connection()
            series_list = load_fred_series_from_postgres(conn, category_ids=category_ids)
            series_ids = [s.get('series_id') for s in series_list if s.get('series_id')]
            print(f"Loaded {len(series_ids)} series IDs from PostgreSQL database")
            return series_ids
        except Exception as e:
            print(f"Error loading series from database: {e}")
            return []
    
    def download_series_from_db(self, 
                                   start_date: Optional[str] = None,
                                   end_date: Optional[str] = None,
                                   series_ids: Optional[List[str]] = None) -> Dict[str, pd.DataFrame]:
        """
        Download time series for specified series IDs and save to PostgreSQL
        
        Args:
            start_date: Start date for time series (YYYY-MM-DD format)
            end_date: End date for time series (YYYY-MM-DD format)
            series_ids: List of series IDs to download (required)
            
        Returns:
            Dictionary mapping series IDs to DataFrames
        """
        if not series_ids:
            print("Error: series_ids is required")
            return {}
        
        print(f"\nDownloading {len(series_ids)} series and saving to database...")
        
        all_data = {}
        successful_downloads = 0
        failed_downloads = 0
        
        for i, series_id in enumerate(series_ids):
            try:
                print(f"Downloading {i+1}/{len(series_ids)}: {series_id}", end='\r')
                data = self.download_series(series_id, start_date=start_date, end_date=end_date, save_to_db=True)
                
                if data is not None and not data.empty:
                    # Convert to DataFrame for return value
                    df = data.to_frame(name=series_id)
                    df.index.name = 'date'
                    all_data[series_id] = df
                    
                    successful_downloads += 1
                else:
                    failed_downloads += 1
                
                # Respect API rate limits (FRED allows 120 requests per minute)
                time.sleep(0.5)
                
            except Exception as e:
                print(f"\nError downloading {series_id}: {e}")
                failed_downloads += 1
                continue
        
        print(f"\n\nDownload complete!")
        print(f"  Successfully downloaded: {successful_downloads} series")
        print(f"  Failed: {failed_downloads} series")
        print(f"  All data saved to PostgreSQL database")
        
        return all_data

