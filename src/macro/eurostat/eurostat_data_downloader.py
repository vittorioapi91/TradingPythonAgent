"""
Eurostat Data Downloader

This module provides functionality to download time series data from Eurostat using eurostatapiclient.
"""

import os
import time
from typing import List, Dict, Optional
import pandas as pd
from datetime import datetime
import xml.etree.ElementTree as ET

# Import Eurostat PostgreSQL utilities
try:
    from .eurostat_postgres import (
        get_postgres_connection, init_eurostat_postgres_tables, 
        add_eurostat_datasets_fast, load_eurostat_datasets_from_postgres, 
        update_eurostat_metadata, add_time_series_fast,
        load_time_series_from_postgres
    )
except ImportError:
    from eurostat_postgres import (
        get_postgres_connection, init_eurostat_postgres_tables, 
        add_eurostat_datasets_fast, load_eurostat_datasets_from_postgres, 
        update_eurostat_metadata, add_time_series_fast,
        load_time_series_from_postgres
    )
import warnings
import requests

from eurostatapiclient import EurostatAPIClient


from tqdm import tqdm


warnings.filterwarnings('ignore')


class EurostatDataDownloader:
    """Class to download economic time series data from Eurostat"""
    
    def __init__(self, api_version: str = '1.0', format_type: str = 'json', language: str = 'en',
                 user: str = "tradingAgent", host: str = "localhost",
                 password: Optional[str] = None, port: int = 5432):
        """
        Initialize Eurostat data downloader
        
        Uses eurostatapiclient to connect to Eurostat's REST API
        
        Args:
            api_version: API version (default: '1.0')
            format_type: Response format (default: 'json')
            language: Language for metadata (default: 'en')
            user: PostgreSQL user (default: 'tradingAgent')
            host: PostgreSQL host (default: 'localhost')
            password: PostgreSQL password (optional, can use POSTGRES_PASSWORD env var)
            port: PostgreSQL port (default: 5432)
        """
        # Initialize Eurostat API client (will raise ImportError at import time if missing)
        self.client = EurostatAPIClient(api_version, format_type, language)
        self.api_version = api_version
        self.format_type = format_type
        self.language = language
        
        # Base URL for Eurostat API
        self.base_url = 'https://ec.europa.eu/eurostat/api/dissemination'
        
        # Store PostgreSQL connection parameters (dbname is fixed to 'eurostat')
        self.dbname = 'eurostat'
        self.user = user
        self.host = host
        self.password = password or os.getenv('POSTGRES_PASSWORD', '')
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
            init_eurostat_postgres_tables(self._pg_conn)
        return self._pg_conn
    
    def _parse_eurostat_date(self, date_val) -> Optional[str]:
        """
        Parse Eurostat date format to YYYY-MM-DD
        
        Eurostat uses formats like:
        - 2020Q1 (quarterly)
        - 2020M01 (monthly)
        - 2020 (annual)
        - 2020-W01 (weekly)
        
        Args:
            date_val: Date value (can be string, datetime, or other)
            
        Returns:
            Date string in YYYY-MM-DD format or None if parsing fails
        """
        if date_val is None:
            return None
        
        # If already a datetime object
        if isinstance(date_val, pd.Timestamp):
            return date_val.strftime('%Y-%m-%d')
        
        if isinstance(date_val, datetime):
            return date_val.strftime('%Y-%m-%d')
        
        # Convert to string
        date_str = str(date_val).strip()
        
        # Try standard date parsing first
        try:
            from dateutil.parser import parse
            date_obj = parse(date_str)
            return date_obj.strftime('%Y-%m-%d')
        except:
            pass
        
        # Handle Eurostat-specific formats
        import re
        
        # Quarterly: 2020Q1 -> 2020-01-01 (first month of quarter)
        q_match = re.match(r'(\d{4})Q([1-4])', date_str, re.IGNORECASE)
        if q_match:
            year = int(q_match.group(1))
            quarter = int(q_match.group(2))
            month = (quarter - 1) * 3 + 1
            return f"{year}-{month:02d}-01"
        
        # Monthly: 2020M01 -> 2020-01-01
        m_match = re.match(r'(\d{4})M(\d{2})', date_str, re.IGNORECASE)
        if m_match:
            year = int(m_match.group(1))
            month = int(m_match.group(2))
            return f"{year}-{month:02d}-01"
        
        # Annual: 2020 -> 2020-01-01
        y_match = re.match(r'(\d{4})$', date_str)
        if y_match:
            year = int(y_match.group(1))
            return f"{year}-01-01"
        
        # Weekly: 2020-W01 -> approximate to first day of week
        w_match = re.match(r'(\d{4})-W(\d{2})', date_str, re.IGNORECASE)
        if w_match:
            year = int(w_match.group(1))
            week = int(w_match.group(2))
            # Approximate: first week starts around Jan 1
            return f"{year}-01-01"
        
        # Try to extract year-month-day pattern
        ymd_match = re.match(r'(\d{4})-(\d{2})-(\d{2})', date_str)
        if ymd_match:
            return date_str
        
        return None
        
    def get_all_datasets(self, limit: Optional[int] = None) -> List[Dict[str, str]]:
        """
        Get all available Eurostat datasets from the XML catalogue
        
        Args:
            limit: Maximum number of datasets to return (None for all)
            
        Returns:
            List of dataset dictionaries with code, title, description, download_link, and metadata
        """
        print("Fetching Eurostat datasets from XML catalogue...")
        
        datasets: List[Dict[str, str]] = []
        
        try:
            catalog_url = "https://ec.europa.eu/eurostat/api/dissemination/catalogue/toc/xml"
            print(f"  Querying Eurostat XML catalog from: {catalog_url}")
            
            response = requests.get(catalog_url, timeout=60)
            response.raise_for_status()
            
            # Parse XML
            root = ET.fromstring(response.content)
            
            # Debug: Print root tag and first few elements to understand structure
            print(f"  Root element: {root.tag}")
            print(f"  Root attributes: {root.attrib}")
            
            # Extract namespace from root if present
            ns_map = {}
            if root.tag.startswith('{'):
                ns_uri = root.tag[1:root.tag.index('}')]
                ns_map[''] = ns_uri
                # Also register common prefixes
                ns_map['toc'] = ns_uri
                ns_map['default'] = ns_uri
                print(f"  Detected namespace: {ns_uri}")
            
            # Try to find all namespaces in the document
            for elem in root.iter():
                if elem.tag.startswith('{'):
                    ns_uri = elem.tag[1:elem.tag.index('}')]
                    if ns_uri not in ns_map.values():
                        # Try to find a prefix
                        for prefix, uri in root.attrib.items():
                            if uri == ns_uri and prefix.startswith('xmlns'):
                                prefix_name = prefix.replace('xmlns:', '') if ':' in prefix else ''
                                if prefix_name:
                                    ns_map[prefix_name] = ns_uri
            
            # Debug: Print first few child elements and their structure
            print(f"  Root has {len(root)} direct children")
            for i, child in enumerate(list(root)[:5]):
                tag_clean = child.tag.split('}')[-1] if '}' in child.tag else child.tag
                print(f"    Child {i}: {tag_clean}, attrs: {child.attrib}, children: {len(child)}")
                # Print first grandchild if exists
                if len(child) > 0:
                    grandchild = list(child)[0]
                    gc_tag = grandchild.tag.split('}')[-1] if '}' in grandchild.tag else grandchild.tag
                    print(f"      First grandchild: {gc_tag}, attrs: {grandchild.attrib}")
            
            # Debug: Print a sample of the XML structure (first 2000 chars)
            xml_sample = ET.tostring(root, encoding='unicode')[:2000]
            print(f"  XML sample (first 2000 chars):\n{xml_sample}...")
            
            # Try different approaches to find datasets
            dataset_elements = []
            
            # Approach 1: Look for common SDMX element names (case-insensitive)
            for elem in root.iter():
                tag_clean = elem.tag.split('}')[-1] if '}' in elem.tag else elem.tag
                tag_lower = tag_clean.lower()
                
                # Common SDMX/Eurostat element names
                if tag_lower in ['dataflow', 'dataset', 'series', 'item', 'code']:
                    dataset_elements.append(elem)
            
            # Approach 2: Look for elements with dataset-like attributes
            if not dataset_elements:
                for elem in root.iter():
                    attrs = elem.attrib
                    # Look for elements with id, code, or dataset-like attributes
                    if any(key.lower() in ['id', 'code', 'dataset', 'dataflow', 'series'] 
                           for key in attrs.keys()):
                        # Make sure it's not just a metadata element
                        tag_clean = elem.tag.split('}')[-1] if '}' in elem.tag else elem.tag
                        if tag_clean.lower() not in ['name', 'title', 'description', 'label']:
                            dataset_elements.append(elem)
            
            # Approach 3: If we found parent elements, look for their children
            if dataset_elements:
                # Sometimes the parent container is found, need to get actual dataset items
                actual_datasets = []
                for parent in dataset_elements:
                    # Check if this element itself looks like a dataset
                    tag_clean = parent.tag.split('}')[-1] if '}' in parent.tag else parent.tag
                    if tag_clean.lower() in ['dataflow', 'dataset', 'series']:
                        # Check if it has children that are the actual datasets
                        for child in parent:
                            child_tag = child.tag.split('}')[-1] if '}' in child.tag else child.tag
                            if child_tag.lower() in ['dataflow', 'dataset', 'series', 'item']:
                                actual_datasets.append(child)
                        # If no children, the parent itself might be the dataset
                        if not actual_datasets:
                            actual_datasets.append(parent)
                    else:
                        actual_datasets.append(parent)
                dataset_elements = actual_datasets
            
            print(f"  Found {len(dataset_elements)} potential dataset elements")
            
            # Parse each dataset element
            for elem in dataset_elements:
                # Extract dataset code (try various attribute names and child elements)
                code = ''
                
                # Try attributes first
                for attr_name in ['id', 'code', 'agencyID', 'dataflow', 'dataset']:
                    code = elem.get(attr_name, '')
                    if code:
                        break
                
                # Try child elements with various tag patterns
                if not code:
                    for child in elem:
                        tag_clean = child.tag.split('}')[-1] if '}' in child.tag else child.tag
                        tag_lower = tag_clean.lower()
                        if tag_lower in ['id', 'code', 'dataflow', 'dataset', 'series']:
                            code = child.text or child.get('value') or child.get('id') or ''
                            if code:
                                break
                
                # If still no code, try to extract from tag name or any text content
                if not code:
                    # Sometimes the code is in the element itself
                    code = elem.text and elem.text.strip() or ''
                    if not code or len(code) > 50:  # Too long, probably not a code
                        code = ''
                
                if not code:
                    continue
                
                # Extract title/name (try various patterns)
                title = ''
                # Try attributes
                for attr_name in ['name', 'title', 'label']:
                    title = elem.get(attr_name, '')
                    if title:
                        break
                
                # Try child elements
                if not title:
                    for child in elem:
                        tag_clean = child.tag.split('}')[-1] if '}' in child.tag else child.tag
                        tag_lower = tag_clean.lower()
                        if tag_lower in ['name', 'title', 'label', 'description']:
                            title = child.text or child.get('value') or ''
                            if title:
                                break
                
                # Extract description
                description = ''
                for child in elem:
                    tag_clean = child.tag.split('}')[-1] if '}' in child.tag else child.tag
                    tag_lower = tag_clean.lower()
                    if tag_lower in ['description', 'annotation', 'note']:
                        description = child.text or child.get('value') or ''
                        if description:
                            break
                
                # Extract download link
                download_link = ''
                # 1) Preferred: explicit downloadLink attribute in the XML (Eurostat TOC uses this)
                for attr_name in ['downloadLink', 'downloadlink']:
                    download_link = elem.get(attr_name, '')
                    if download_link:
                        break
                # 2) Other possible attributes
                if not download_link:
                    for attr_name in ['href', 'url', 'link', 'download', 'uri']:
                        download_link = elem.get(attr_name, '')
                        if download_link:
                            break
                # 3) Child elements that may contain the link
                if not download_link:
                    for child in elem:
                        tag_clean = child.tag.split('}')[-1] if '}' in child.tag else child.tag
                        tag_lower = tag_clean.lower()
                        if tag_lower in ['downloadlink', 'download_link', 'link', 'url', 'href', 'uri', 'download']:
                            download_link = (
                                child.text
                                or child.get('downloadLink')
                                or child.get('downloadlink')
                                or child.get('href')
                                or child.get('url')
                                or child.get('uri')
                                or ''
                            )
                            if download_link:
                                break
                
                # Extract version
                version = elem.get('version') or elem.get('ver') or ''
                
                datasets.append({
                    'code': code.strip(),
                    'title': title.strip() if title else code,
                    'description': description.strip() if description else '',
                    'version': version.strip() if version else '',
                    'download_link': download_link.strip() if download_link else '',
                })
            
            # Apply optional limit
            if limit is not None:
                datasets = datasets[:limit]
            
            print(f"Found {len(datasets)} datasets in Eurostat XML catalog")
            return datasets
            
        except Exception as e:
            print(f"Error fetching Eurostat XML catalog: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    def get_dataset_structure(self, dataset_code: str) -> Optional[Dict]:
        """
        Get structure (dimensions, codes) for a dataset
        
        Args:
            dataset_code: Dataset code (e.g., 'tps00001')
            
        Returns:
            Dictionary with dataset structure information
        """
        try:
            # Use Eurostat's structure API
            structure_url = f"{self.base_url}/statistics/1.0/dataflow/ESTAT/{dataset_code}"
            
            response = requests.get(structure_url, params={'format': 'json', 'lang': self.language}, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            dimensions = []
            
            # Parse structure response
            if 'structure' in data and 'dimensions' in data['structure']:
                dims_data = data['structure']['dimensions']
                
                for dim_id, dim_info in dims_data.items():
                    dim_dict = {
                        'id': dim_id,
                        'name': dim_info.get('label', dim_id) if isinstance(dim_info.get('label'), str) else dim_info.get('label', {}).get(self.language, dim_id),
                    }
                    # Get codes/values for this dimension
                    if 'values' in dim_info:
                        codes = []
                        for code_item in dim_info['values']:
                            if isinstance(code_item, dict):
                                codes.append({
                                    'code': code_item.get('id', ''),
                                    'name': code_item.get('label', '') if isinstance(code_item.get('label'), str) else code_item.get('label', {}).get(self.language, '')
                                })
                        dim_dict['codes'] = codes
                    dimensions.append(dim_dict)
            
            return {
                'dataset_code': dataset_code,
                'dimensions': dimensions
            }
            
        except Exception as e:
            # If structure API fails, return basic info
            print(f"Warning: Could not get full structure for {dataset_code}: {e}")
            return {
                'dataset_code': dataset_code,
                'dimensions': []
            }
    
    def download_dataset(self, dataset_code: str, 
                        filters: Optional[Dict[str, str]] = None,
                        params: Optional[Dict] = None,
                        save_to_db: bool = True) -> Optional[pd.DataFrame]:
        """
        Download a specific Eurostat dataset and optionally save to PostgreSQL
        
        Args:
            dataset_code: Dataset code (e.g., 'tps00001')
            filters: Dictionary of dimension filters (e.g., {'geo': 'DE', 'time': '2020'})
            params: Additional query parameters
            save_to_db: Whether to save to PostgreSQL database (default: True)
            
        Returns:
            DataFrame with the dataset or None if error
        """
        try:
            # Use the client to get dataset
            dataset = self.client.get_dataset(dataset_code)
            
            # Convert to pandas DataFrame
            df = dataset.to_dataframe()
            
            if df is not None and not df.empty:
                if save_to_db:
                    # Save to PostgreSQL
                    conn = self._get_pg_connection()
                    time_series_list = []
                    
                    # Eurostat DataFrames are multi-dimensional
                    # Typically: index = time, columns = other dimensions (can be multi-index)
                    # Reset index to make time a column if it's in the index
                    df_to_save = df.copy()
                    
                    # Check if index is time-based
                    time_in_index = False
                    if isinstance(df_to_save.index, pd.DatetimeIndex):
                        time_in_index = True
                        df_to_save = df_to_save.reset_index()
                        time_col = df_to_save.columns[0]
                    elif df_to_save.index.name and 'time' in str(df_to_save.index.name).lower():
                        time_in_index = True
                        df_to_save = df_to_save.reset_index()
                        time_col = df_to_save.columns[0]
                    else:
                        # Try to find time column
                        time_col = None
                        for col in df_to_save.columns:
                            if 'time' in str(col).lower():
                                time_col = col
                                break
                    
                    # Convert DataFrame to long format for easier processing
                    if time_in_index or time_col:
                        # Melt the DataFrame to long format
                        id_vars = [time_col] if time_col else []
                        df_melted = df_to_save.melt(id_vars=id_vars, var_name='dimensions', value_name='value')
                        
                        for _, row in df_melted.iterrows():
                            # Parse date
                            date_str = None
                            if time_col:
                                date_val = row[time_col]
                                date_str = self._parse_eurostat_date(date_val)
                            
                            if not date_str:
                                continue
                            
                            # Extract dimensions
                            dimensions = {}
                            dim_col = row.get('dimensions', '')
                            
                            # Handle multi-index column names (tuple)
                            if isinstance(dim_col, tuple):
                                # Try to get dimension names from DataFrame structure
                                if hasattr(df.columns, 'names') and df.columns.names:
                                    dim_names = df.columns.names
                                    for i, dim_val in enumerate(dim_col):
                                        dim_name = dim_names[i] if i < len(dim_names) else f'dim_{i}'
                                        dimensions[dim_name] = str(dim_val)
                                else:
                                    for i, dim_val in enumerate(dim_col):
                                        dimensions[f'dim_{i}'] = str(dim_val)
                            else:
                                dimensions['dimension'] = str(dim_col)
                            
                            value = row.get('value')
                            if pd.notna(value):
                                time_series_list.append({
                                    'dataset_code': dataset_code,
                                    'date': date_str,
                                    'value': float(value),
                                    'dimensions': dimensions
                                })
                    else:
                        # Fallback: treat index as time, columns as dimensions
                        for idx, row in df.iterrows():
                            date_str = self._parse_eurostat_date(idx)
                            if not date_str:
                                continue
                            
                            for col in df.columns:
                                value = row[col]
                                if pd.notna(value):
                                    dimensions = {}
                                    if isinstance(col, tuple):
                                        for i, dim_val in enumerate(col):
                                            dimensions[f'dim_{i}'] = str(dim_val)
                                    else:
                                        dimensions['dimension'] = str(col)
                                    
                                    time_series_list.append({
                                        'dataset_code': dataset_code,
                                        'date': date_str,
                                        'value': float(value),
                                        'dimensions': dimensions
                                    })
                    
                    if time_series_list:
                        add_time_series_fast(conn, time_series_list)
                        print(f"  Saved {len(time_series_list)} data points to database for {dataset_code}")
                
                return df
            else:
                print(f"No data returned for {dataset_code}")
                return None
                
        except Exception as e:
            print(f"Error downloading {dataset_code}: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def get_all_downloadable_series(self, limit: Optional[int] = None,
                                    write_interval: int = 50) -> List[Dict[str, str]]:
        """
        Retrieve all downloadable Eurostat datasets and save to PostgreSQL database incrementally
        
        Args:
            limit: Maximum number of datasets to retrieve (None for all)
            write_interval: Number of datasets to collect before writing to PostgreSQL database
            
        Returns:
            List of dictionaries with dataset information
        """
        print("=" * 60)
        print("Retrieving all downloadable Eurostat datasets...")
        print("=" * 60)
        
        all_datasets_info = []
        dataset_count_at_last_write = 0
        
        # Initialize PostgreSQL connection
        conn = self._get_pg_connection()
        update_eurostat_metadata(conn, 'status', 'in_progress')
        update_eurostat_metadata(conn, 'generated_at', datetime.now().isoformat())
        print(f"Connected to PostgreSQL database: {self.dbname}")
        
        def write_to_db_incrementally(show_status=True):
            """Helper function to write current state to PostgreSQL database"""
            if all_datasets_info:
                try:
                    # Get only new datasets since last write
                    new_datasets = all_datasets_info[dataset_count_at_last_write:]
                    if not new_datasets:
                        return
                    
                    # Convert dataset info to database format
                    db_datasets = []
                    for dataset in new_datasets:
                        # Convert dimensions list to string for keywords
                        dimensions = dataset.get('dimensions', [])
                        keywords = ', '.join([d.get('id', '') for d in dimensions]) if dimensions else ''
                        
                        db_datasets.append({
                            'dataset_code': dataset.get('code', ''),
                            'title': dataset.get('title', ''),
                            'description': dataset.get('description', ''),
                            'last_update': dataset.get('version', ''),
                            'frequency': '',  # Eurostat doesn't always provide this in catalog
                            'theme': '',
                            'keywords': keywords,
                            'created_at': datetime.now().isoformat(),
                            'updated_at': datetime.now().isoformat(),
                        })
                    
                    if db_datasets:
                        add_eurostat_datasets_fast(conn, db_datasets)
                        update_eurostat_metadata(conn, 'total_datasets', str(len(all_datasets_info)))
                    
                    if show_status:
                        status_msg = f"  [{len(all_datasets_info)} datasets] Progress saved to database"
                        if limit:
                            status_msg += f" ({len(all_datasets_info)}/{limit})"
                        print(status_msg)
                except Exception as e:
                    print(f"  Warning: Error writing to database: {e}")
        
        try:
            # Get all datasets
            print("\nFetching dataset catalog from Eurostat...")
            datasets = self.get_all_datasets(limit=limit)
            
            if not datasets:
                print("No datasets found")
                return []
            
            print(f"\nProcessing {len(datasets)} datasets...")
            
            # Progress bar
            pbar = tqdm(datasets, desc="Cataloging datasets", unit="dataset", leave=True)
            for dataset in pbar:
                code = dataset.get('code', '')
                title = dataset.get('title', '')
                
                pbar.set_postfix({'code': code[:20], 'datasets': len(all_datasets_info)})
                
                # Try to get additional metadata
                try:
                    structure = self.get_dataset_structure(code)
                    dimensions = structure.get('dimensions', []) if structure else []
                except:
                    dimensions = []
                    time.sleep(0.1)  # Rate limiting
                
                dataset_info = {
                    'code': code,
                    'title': title,
                    'description': dataset.get('description', ''),
                    'version': dataset.get('version', '1.0'),
                    'dimensions': dimensions,
                }
                
                all_datasets_info.append(dataset_info)
                
                # Write to database incrementally
                if len(all_datasets_info) - dataset_count_at_last_write >= write_interval:
                    write_to_db_incrementally()
                    dataset_count_at_last_write = len(all_datasets_info)
                
                time.sleep(0.1)  # Rate limiting
            
            pbar.close()
            
            # Final save to database (mark as complete)
                # Write final batch of datasets
                if all_datasets_info:
                    new_datasets = all_datasets_info[dataset_count_at_last_write:]
                    if new_datasets:
                    db_datasets = []
                        for dataset in new_datasets:
                            dimensions = dataset.get('dimensions', [])
                            keywords = ', '.join([d.get('id', '') for d in dimensions]) if dimensions else ''
                            
                        db_datasets.append({
                                'dataset_code': dataset.get('code', ''),
                                'title': dataset.get('title', ''),
                                'description': dataset.get('description', ''),
                                'last_update': dataset.get('version', ''),
                                'frequency': '',
                                'theme': '',
                                'keywords': keywords,
                                'created_at': datetime.now().isoformat(),
                                'updated_at': datetime.now().isoformat(),
                            })
                    if db_datasets:
                        add_eurostat_datasets_fast(conn, db_datasets)
                
            update_eurostat_metadata(conn, 'total_datasets', str(len(all_datasets_info)))
            update_eurostat_metadata(conn, 'status', 'complete')
            print(f"\n✓ Eurostat database finalized: {self.dbname}")
                print(f"  Total datasets: {len(all_datasets_info)}")
            
            return all_datasets_info
            
        except Exception as e:
            print(f"Error retrieving datasets: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    def load_datasets_from_db(self) -> List[str]:
        """
        Load dataset codes from PostgreSQL database
            
        Returns:
            List of dataset codes
        """
        try:
            conn = self._get_pg_connection()
            datasets_list = load_eurostat_datasets_from_postgres(conn)
            dataset_codes = [d.get('dataset_code') for d in datasets_list if d.get('dataset_code')]
            print(f"Loaded {len(dataset_codes)} dataset codes from PostgreSQL database")
            return dataset_codes
        except Exception as e:
            print(f"Error loading datasets from database: {e}")
            return []
    
    def download_datasets_from_db(self,
                                   dataset_codes: Optional[List[str]] = None,
                                   filters: Optional[Dict[str, Dict[str, str]]] = None,
                                   save_to_db: bool = True) -> Dict[str, pd.DataFrame]:
        """
        Download datasets from PostgreSQL database
        
        Args:
            dataset_codes: Optional list of dataset codes to download (if None, downloads all from DB)
            filters: Optional dictionary mapping dataset codes to filter dictionaries
            save_to_db: Whether to save time series data to PostgreSQL (default: True)
            
        Returns:
            Dictionary mapping dataset codes to DataFrames
        """
        if dataset_codes is None:
            dataset_codes = self.load_datasets_from_db()
        
        if not dataset_codes:
            print("No dataset codes found in database")
            return {}
        
        print(f"\nDownloading {len(dataset_codes)} datasets and saving to database...")
        
        downloaded_dfs = {}
        pbar = tqdm(dataset_codes, desc="Downloading datasets", unit="dataset", leave=True)
        
        for dataset_code in pbar:
            pbar.set_description(f"Downloading {dataset_code}")
            
            # Get filters for this dataset if provided
            dataset_filters = filters.get(dataset_code) if filters else None
            
            try:
                data = self.download_dataset(dataset_code, filters=dataset_filters, save_to_db=save_to_db)
                
                if data is not None and not data.empty:
                    downloaded_dfs[dataset_code] = data
                
                time.sleep(0.5)  # Rate limiting
                
            except Exception as e:
                pbar.write(f"Error downloading {dataset_code}: {e}")
                time.sleep(0.5)
        
        pbar.close()
        
        print(f"\nSuccessfully downloaded {len(downloaded_dfs)}/{len(dataset_codes)} datasets.")
        print(f"All data saved to PostgreSQL database")
        return downloaded_dfs

