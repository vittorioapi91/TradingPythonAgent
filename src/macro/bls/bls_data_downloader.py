"""
BLS Economic Data Downloader

This module provides functionality to download time series data from BLS (Bureau of Labor Statistics).
BLS API key is optional but recommended. Get one at: https://www.bls.gov/developers/api_signature.htm
"""

import os
import time
import json
from typing import List, Dict, Optional
import pandas as pd
import requests
import warnings
from datetime import datetime

# Import BLS PostgreSQL utilities
try:
    from .bls_postgres import (
        get_postgres_connection, init_bls_postgres_tables, 
        add_bls_series_fast, load_bls_series_from_postgres, 
        update_bls_metadata, add_time_series_fast,
        load_time_series_from_postgres
    )
except ImportError:
    from bls_postgres import (
        get_postgres_connection, init_bls_postgres_tables, 
        add_bls_series_fast, load_bls_series_from_postgres, 
        update_bls_metadata, add_time_series_fast,
        load_time_series_from_postgres
    )

warnings.filterwarnings('ignore')


class BLSDataDownloader:
    """Class to download economic time series data from BLS"""
    
    def __init__(self, api_key: Optional[str] = None,
                 user: str = "tradingAgent", host: str = "localhost",
                 password: Optional[str] = None, port: int = 5432):
        """
        Initialize BLS data downloader
        
        Args:
            api_key: BLS API key (optional but recommended for higher rate limits).
                     If not provided, will use default key or try to get from environment variable BLS_API_KEY
            user: PostgreSQL user (default: 'tradingAgent')
            host: PostgreSQL host (default: 'localhost')
            password: PostgreSQL password (optional, can use POSTGRES_PASSWORD env var)
            port: PostgreSQL port (default: 5432)
        """
        # Default API key
        default_api_key = "e2b2d9ddb16a4437bc8747c59dda4eac"
        self.api_key = api_key or os.getenv('BLS_API_KEY') or default_api_key
        self.base_url = 'https://api.bls.gov/publicAPI/v2/timeseries/data/'
        self.surveys_url = 'https://api.bls.gov/publicAPI/v2/surveys/all'
        
        # Store PostgreSQL connection parameters (dbname is fixed to 'bls')
        self.dbname = 'bls'
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
            init_bls_postgres_tables(self._pg_conn)
        return self._pg_conn
        
    def download_series(self, series_ids: List[str], start_year: int, end_year: int, 
                      save_to_db: bool = True) -> Optional[pd.DataFrame]:
        """
        Download BLS time series data for given series IDs
        
        Args:
            series_ids: List of BLS series IDs (e.g., ['CUUR0000SA0', 'SUUR0000SA0'])
            start_year: Start year for data
            end_year: End year for data
            
        Returns:
            DataFrame with time series data, or None if error
        """
        try:
            headers = {'Content-Type': 'application/json'}
            data = {
                "seriesid": series_ids,
                "startyear": str(start_year),
                "endyear": str(end_year)
            }
            
            # Add API key if available
            if self.api_key:
                data["registrationkey"] = self.api_key
            
            response = requests.post(self.base_url, headers=headers, json=data, timeout=30)
            response.raise_for_status()
            
            json_data = response.json()
            
            # Check for errors in BLS response
            if json_data.get('status') == 'REQUEST_SUCCEEDED':
                df = self._parse_bls_data(json_data)
                
                if save_to_db and df is not None and not df.empty:
                    # Save to PostgreSQL
                    conn = self._get_pg_connection()
                    time_series_list = []
                    
                    for _, row in df.iterrows():
                        time_series_list.append({
                            'series_id': row['series_id'],
                            'date': row['date'].strftime('%Y-%m-%d') if hasattr(row['date'], 'strftime') else str(row['date']),
                            'value': float(row['value']) if pd.notna(row['value']) else None,
                            'year': str(row.get('year', '')),
                            'period': str(row.get('period', '')),
                            'footnotes': row.get('footnotes', [])
                        })
                    
                    if time_series_list:
                        add_time_series_fast(conn, time_series_list)
                        print(f"  Saved {len(time_series_list)} data points to database for {len(series_ids)} series")
                
                return df
            else:
                error_msg = json_data.get('message', ['Unknown error'])
                print(f"BLS API error: {error_msg}")
                return None
                
        except Exception as e:
            print(f"Error downloading BLS series {series_ids}: {e}")
            return None
    
    def _parse_bls_data(self, json_data: Dict) -> pd.DataFrame:
        """
        Parse BLS API response into DataFrame
        
        Args:
            json_data: JSON response from BLS API
            
        Returns:
            DataFrame with parsed data
        """
        all_data = []
        
        if 'Results' in json_data and 'series' in json_data['Results']:
            for series in json_data['Results']['series']:
                series_id = series.get('seriesID', '')
                
                # Get series attributes
                catalog_data = series.get('catalogData', {})
                
                for item in series.get('data', []):
                    year = item.get('year', '')
                    period = item.get('period', '')
                    value = item.get('value', '')
                    
                    # Convert period to date (e.g., "M01" -> January)
                    date_str = self._period_to_date(year, period)
                    
                    all_data.append({
                        'series_id': series_id,
                        'date': date_str,
                        'year': year,
                        'period': period,
                        'value': value,
                        'footnotes': item.get('footnotes', [])
                    })
        
        if all_data:
            df = pd.DataFrame(all_data)
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            df = df.sort_values('date').reset_index(drop=True)
            return df
        else:
            return pd.DataFrame()
    
    def _period_to_date(self, year: str, period: str) -> str:
        """
        Convert BLS period code to date string
        
        Args:
            year: Year as string
            period: Period code (e.g., "M01" for January, "M13" for annual average, "Q01" for Q1)
            
        Returns:
            Date string in YYYY-MM-DD format
        """
        try:
            year_int = int(year)
            
            if period.startswith('M'):  # Monthly
                month = int(period[1:])
                if 1 <= month <= 12:
                    return f"{year_int}-{month:02d}-01"
            elif period.startswith('Q'):  # Quarterly
                quarter = int(period[1:])
                month = (quarter - 1) * 3 + 1
                return f"{year_int}-{month:02d}-01"
            elif period == 'M13' or period == 'A00':  # Annual average
                return f"{year_int}-01-01"
            elif period.startswith('S'):  # Semi-annual
                half = int(period[1:])
                month = (half - 1) * 6 + 1
                return f"{year_int}-{month:02d}-01"
            
            # Default to start of year if unknown format
            return f"{year_int}-01-01"
        except:
            return f"{year}-01-01"
    
    def get_series_info(self, series_id: str) -> Optional[Dict]:
        """
        Get metadata for a BLS series
        
        Args:
            series_id: BLS series ID
            
        Returns:
            Dictionary with series metadata
        """
        try:
            # BLS API v2 doesn't have a direct series info endpoint
            # We can infer from series ID structure or use catalog
            # For now, return basic structure
            return {
                'series_id': series_id,
                'source': 'BLS',
                'description': f'BLS Series {series_id}'
            }
        except Exception as e:
            print(f"Error getting series info for {series_id}: {e}")
            return None
    
    def get_all_surveys(self) -> List[Dict]:
        """
        Get list of all available BLS surveys
        
        Returns:
            List of survey dictionaries
        """
        try:
            response = requests.get(self.surveys_url, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            if data.get('status') == 'REQUEST_SUCCEEDED' and 'Results' in data:
                return data['Results'].get('survey', [])
            return []
        except Exception as e:
            print(f"Error fetching surveys: {e}")
            return []
    
    def get_available_series_from_survey(self, survey_abbreviation: str) -> List[str]:
        """
        Get available series IDs from a survey
        
        Args:
            survey_abbreviation: Survey abbreviation (e.g., 'CU', 'SU', 'CE')
            
        Returns:
            List of series IDs
        """
        # Note: BLS API doesn't provide a direct endpoint for this
        # This would require additional API calls or manual catalog lookup
        # For now, return empty list - users need to know series IDs
        print(f"Note: BLS API doesn't provide direct series listing for survey {survey_abbreviation}")
        print("You'll need to specify series IDs directly or use BLS data tools to find them")
        return []
    
    def download_multiple_series(self, series_ids: List[str], 
                                 start_year: int, 
                                 end_year: int,
                                 save_to_db: bool = True) -> pd.DataFrame:
        """
        Download multiple BLS series and combine into a DataFrame
        
        Args:
            series_ids: List of BLS series IDs
            start_year: Start year
            end_year: End year
            save_to_file: Optional path to save DataFrame as CSV
            
        Returns:
            DataFrame with all series data
        """
        print(f"Downloading {len(series_ids)} BLS series from {start_year} to {end_year}...")
        
        # BLS API can handle multiple series in one request
        df = self.download_series(series_ids, start_year, end_year, save_to_db=save_to_db)
        
        if df is not None and not df.empty:
            # Pivot to have series as columns
            df_pivot = df.pivot_table(
                index='date',
                columns='series_id',
                values='value',
                aggfunc='first'
            )
            df_pivot.index.name = 'date'
            
            return df_pivot
        else:
            print("No data was successfully downloaded")
            return pd.DataFrame()
    
    def get_all_downloadable_series(self, test_patterns: bool = True,
                                     write_interval: int = 50) -> List[Dict[str, str]]:
        """
        Get list of available BLS series information and save to PostgreSQL
        Attempts to discover series by testing common patterns and known series
        
        Args:
            test_patterns: If True, test series ID patterns to discover more series (slower)
            write_interval: Number of series to collect before writing to PostgreSQL database
            
        Returns:
            List of series information dictionaries
        """
        print("=" * 60)
        print("Retrieving BLS series information...")
        print("=" * 60)
        
        all_series_info = []
        series_ids_found = set()
        series_count_at_last_write = 0
        
        # Initialize PostgreSQL connection
        conn = self._get_pg_connection()
        update_bls_metadata(conn, 'status', 'in_progress')
        update_bls_metadata(conn, 'generated_at', datetime.now().isoformat())
        print(f"Connected to PostgreSQL database: {self.dbname}")
        
        def write_to_db_incrementally():
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
                        survey_info = series.get('survey', {})
                        if isinstance(survey_info, str):
                            survey_abbrev = survey_info
                            survey_name = survey_info
                        else:
                            survey_abbrev = survey_info.get('survey_abbreviation', '')
                            survey_name = survey_info.get('survey_name', '')
                        
                        db_series.append({
                            'series_id': series.get('id', ''),
                            'survey_abbreviation': survey_abbrev,
                            'survey_name': survey_name,
                            'seasonal': series.get('seasonal', ''),
                            'area_code': series.get('area_code', ''),
                            'area_name': series.get('area_name', ''),
                            'item_code': series.get('item_code', ''),
                            'item_name': series.get('item_name', ''),
                            'created_at': datetime.now().isoformat(),
                            'updated_at': datetime.now().isoformat(),
                        })
                    
                    if db_series:
                        add_bls_series_fast(conn, db_series)
                        update_bls_metadata(conn, 'total_series', str(len(all_series_info)))
                    
                    print(f"  [{len(all_series_info)} series] Progress saved to database")
                except Exception as e:
                    print(f"  Warning: Error writing to database: {e}")
        
        # Get all surveys first
        print("\nFetching BLS surveys...")
        try:
            surveys = self.get_all_surveys()
            print(f"Found {len(surveys)} surveys")
        except:
            surveys = []
            print("Could not fetch surveys, continuing with pattern-based discovery")
        
        # Get known/common BLS series first
        print("\nAdding known/common BLS series...")
        known_series = self._get_common_bls_series_extended()
        for series_info in known_series:
            series_id = series_info['id']
            if series_id not in series_ids_found:
                series_ids_found.add(series_id)
                all_series_info.append(series_info)
        print(f"Added {len(known_series)} known series")
        
        # Generate and test series ID patterns if enabled
        if test_patterns:
            print("\nGenerating and testing series ID patterns...")
            pattern_series = self._generate_series_patterns()
            print(f"Generated {len(pattern_series)} series IDs to test")
            
            for i, series_id in enumerate(pattern_series):
                if series_id in series_ids_found:
                    continue
                
                if (i + 1) % 50 == 0:
                    print(f"  Testing {i+1}/{len(pattern_series)} patterns... [{len(all_series_info)} found]", end='\r')
                
                # Test if series exists by attempting to fetch data
                try:
                    test_data = self.download_series([series_id], 2020, 2020)
                    if test_data is not None and not test_data.empty:
                        series_ids_found.add(series_id)
                        all_series_info.append({
                            'id': series_id,
                            'title': f'BLS Series {series_id}',
                            'survey': self._infer_survey_from_id(series_id),
                        })
                        
                        # Write incrementally
                        if len(all_series_info) - series_count_at_last_write >= write_interval:
                            write_to_db_incrementally()
                            series_count_at_last_write = len(all_series_info)
                    
                    time.sleep(0.1)  # Rate limiting
                except:
                    # Series doesn't exist or error - skip it
                    time.sleep(0.05)
                    continue
        
        print(f"\n  Total series discovered: {len(all_series_info)}")
        
        # Final save to database (mark as complete)
            # Write final batch of series
            if all_series_info:
                new_series = all_series_info[series_count_at_last_write:]
                if new_series:
                db_series = []
                    for series in new_series:
                        survey_info = series.get('survey', {})
                        if isinstance(survey_info, str):
                            survey_abbrev = survey_info
                            survey_name = survey_info
                        else:
                            survey_abbrev = survey_info.get('survey_abbreviation', '')
                            survey_name = survey_info.get('survey_name', '')
                        
                    db_series.append({
                            'series_id': series.get('id', ''),
                            'survey_abbreviation': survey_abbrev,
                            'survey_name': survey_name,
                            'seasonal': series.get('seasonal', ''),
                            'area_code': series.get('area_code', ''),
                            'area_name': series.get('area_name', ''),
                            'item_code': series.get('item_code', ''),
                            'item_name': series.get('item_name', ''),
                            'created_at': datetime.now().isoformat(),
                            'updated_at': datetime.now().isoformat(),
                        })
                if db_series:
                    add_bls_series_fast(conn, db_series)
            
        update_bls_metadata(conn, 'total_series', str(len(all_series_info)))
        update_bls_metadata(conn, 'status', 'complete')
        print(f"\n✓ BLS database finalized: {self.dbname}")
            print(f"  Total series saved: {len(all_series_info)}")
        
        return all_series_info
    
    def _generate_series_patterns(self) -> List[str]:
        """
        Generate potential BLS series IDs based on known patterns
        
        Returns:
            List of potential series IDs to test
        """
        patterns = []
        
        # CPI (Consumer Price Index) patterns: CUUR + various codes
        cpi_prefixes = ['CUUR0000SA0', 'CUUR0000SAF', 'CUUR0000SAH', 'CUUR0000SAM', 
                       'CUUR0000SAA', 'CUUR0000SAS', 'CUUR0000SET']
        cpi_suffixes = ['', 'L1E', 'L12', 'L14']
        for prefix in cpi_prefixes:
            for suffix in cpi_suffixes:
                patterns.append(f"{prefix}{suffix}")
        
        # PPI patterns: WPU, PCU prefixes
        ppi_codes = ['00000000', 'FD49207', 'FD49502', 'FD49116', 'FD49208']
        for code in ppi_codes:
            patterns.append(f"WPU{code}")
            patterns.append(f"PCU{code}")
        
        # Employment patterns: CES, CEU prefixes
        employment_codes = ['0000000001', '0500000001', '0600000001', '0700000001',
                          '1000000001', '2000000001', '3000000001', '4000000001',
                          '5000000001', '6000000001']
        for code in employment_codes:
            patterns.append(f"CES{code}")
            patterns.append(f"CEU{code}")
        
        # Labor force patterns: LNS prefixes
        labor_codes = ['14000000', '12000000', '11300000', '12300000']
        for code in labor_codes:
            patterns.append(f"LNS{code}")
        
        # Earnings patterns
        earnings_codes = ['0500000003', '0500000011', '0500000002']
        for code in earnings_codes:
            patterns.append(f"CES{code}")
        
        # State-level patterns (2-letter state codes)
        states = ['AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
                 'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
                 'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
                 'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
                 'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY']
        
        # Add state-level CPI patterns
        for state in states:
            patterns.append(f"CUUR0000SA0{state}")
            patterns.append(f"CUUS{state}0000SA0")
        
        # Additional common patterns
        additional_patterns = [
            'IWP00000000', 'EWP00000000',  # Import/Export prices
            'AWHI', 'AWHA',  # Hours indices
            'SUUR0000SA0',  # C-CPI-U
        ]
        patterns.extend(additional_patterns)
        
        return patterns
    
    def _infer_survey_from_id(self, series_id: str) -> str:
        """Infer survey abbreviation from series ID"""
        if series_id.startswith('CUUR') or series_id.startswith('CUUS'):
            return 'CU'
        elif series_id.startswith('SUUR'):
            return 'SU'
        elif series_id.startswith('WPU') or series_id.startswith('PCU'):
            return 'WP'
        elif series_id.startswith('CES') or series_id.startswith('CEU'):
            return 'CE'
        elif series_id.startswith('LNS'):
            return 'LN'
        elif series_id.startswith('IWP'):
            return 'IW'
        elif series_id.startswith('EWP'):
            return 'EW'
        else:
            return 'Unknown'
    
    def _get_common_bls_series_extended(self) -> List[Dict[str, str]]:
        """
        Get extended list of common BLS series IDs with metadata
        
        Returns:
            List of series information dictionaries
        """
        common_series = [
            # Consumer Price Index (CPI) - All Urban Consumers
            {'id': 'CUUR0000SA0', 'title': 'CPI All Urban Consumers - All Items', 'survey': 'CU'},
            {'id': 'CUUR0000SA0L1E', 'title': 'CPI All Urban Consumers - All Items Less Food & Energy', 'survey': 'CU'},
            {'id': 'CUUR0000SETB01', 'title': 'CPI All Urban Consumers - Energy', 'survey': 'CU'},
            {'id': 'CUUR0000SAF1', 'title': 'CPI All Urban Consumers - Food', 'survey': 'CU'},
            {'id': 'CUUR0000SAA', 'title': 'CPI All Urban Consumers - Apparel', 'survey': 'CU'},
            {'id': 'CUUR0000SAH1', 'title': 'CPI All Urban Consumers - Shelter', 'survey': 'CU'},
            {'id': 'CUUR0000SAM', 'title': 'CPI All Urban Consumers - Medical Care', 'survey': 'CU'},
            {'id': 'CUUR0000SAS', 'title': 'CPI All Urban Consumers - Services', 'survey': 'CU'},
            
            # Consumer Price Index Research (C-CPI-U)
            {'id': 'SUUR0000SA0', 'title': 'CPI Research - All Items', 'survey': 'SU'},
            
            # Producer Price Index (PPI)
            {'id': 'PCUOMFGOMFG', 'title': 'PPI - Manufacturing', 'survey': 'PC'},
            {'id': 'WPU00000000', 'title': 'PPI - All Commodities', 'survey': 'WP'},
            {'id': 'WPUFD49207', 'title': 'PPI - Finished Goods', 'survey': 'WP'},
            {'id': 'WPUFD49502', 'title': 'PPI - Intermediate Goods', 'survey': 'WP'},
            {'id': 'WPUFD49116', 'title': 'PPI - Crude Goods', 'survey': 'WP'},
            
            # Employment - Current Employment Statistics (CES)
            {'id': 'CEU0000000001', 'title': 'Employment - Total Nonfarm', 'survey': 'CE'},
            {'id': 'CES0000000001', 'title': 'Employment - Total Nonfarm (Alternative)', 'survey': 'CE'},
            {'id': 'CEU0500000001', 'title': 'Employment - Total Private', 'survey': 'CE'},
            {'id': 'CEU0600000001', 'title': 'Employment - Goods Producing', 'survey': 'CE'},
            {'id': 'CEU0700000001', 'title': 'Employment - Service Providing', 'survey': 'CE'},
            {'id': 'CEU1000000001', 'title': 'Employment - Mining and Logging', 'survey': 'CE'},
            {'id': 'CEU2000000001', 'title': 'Employment - Construction', 'survey': 'CE'},
            {'id': 'CEU3000000001', 'title': 'Employment - Manufacturing', 'survey': 'CE'},
            
            # Unemployment Rate
            {'id': 'LNS14000000', 'title': 'Unemployment Rate', 'survey': 'LN'},
            {'id': 'LNS12000000', 'title': 'Employment-Population Ratio', 'survey': 'LN'},
            {'id': 'LNS11300000', 'title': 'Labor Force Participation Rate', 'survey': 'LN'},
            
            # Wages and Earnings
            {'id': 'CES0500000003', 'title': 'Average Hourly Earnings - Total Private', 'survey': 'CE'},
            {'id': 'LEU0254551600', 'title': 'Average Weekly Earnings', 'survey': 'LE'},
            {'id': 'CES0500000011', 'title': 'Average Weekly Earnings - Total Private', 'survey': 'CE'},
            
            # Hours
            {'id': 'AWHI', 'title': 'Aggregate Weekly Hours Index', 'survey': 'CE'},
            {'id': 'CES0500000002', 'title': 'Average Weekly Hours - Total Private', 'survey': 'CE'},
            {'id': 'CES2000000002', 'title': 'Average Weekly Hours - Construction', 'survey': 'CE'},
            {'id': 'CES3000000002', 'title': 'Average Weekly Hours - Manufacturing', 'survey': 'CE'},
            
            # Import/Export Price Index
            {'id': 'IWP00000000', 'title': 'Import Price Index - All Imports', 'survey': 'IW'},
            {'id': 'EWP00000000', 'title': 'Export Price Index - All Exports', 'survey': 'EW'},
        ]
        
        return common_series


