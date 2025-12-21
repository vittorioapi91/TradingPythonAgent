"""
Yahoo Finance Equities Data Downloader

This module downloads equity data and metadata from Yahoo Finance.
"""

import os
import time
import requests
from typing import List, Dict, Optional, Tuple
from datetime import datetime
from io import StringIO
import pandas as pd

try:
    import yfinance as yf
    HAS_YFINANCE = True
except ImportError:
    HAS_YFINANCE = False


try:
    from tqdm import tqdm
except ImportError:
    tqdm = lambda x, **kwargs: x

try:
    from .yahoo_equities_parquet import (
        init_yahoo_equities_parquet_tables,
        get_yahoo_equities_parquet_paths,
        add_yahoo_equities_fast,
        add_yahoo_equities_essentials_fast,
        add_data_fast,
        update_yahoo_equities_metadata
    )
except ImportError:
    from yahoo_equities_parquet import (
        init_yahoo_equities_parquet_tables,
        get_yahoo_equities_parquet_paths,
        add_yahoo_equities_fast,
        add_yahoo_equities_essentials_fast,
        add_data_fast,
        update_yahoo_equities_metadata
    )


class YahooEquitiesDownloader:
    """Class to download equity data and metadata from Yahoo Finance"""
    
    def __init__(self, delay_between_requests: float = 0.1):
        """
        Initialize Yahoo Finance equities downloader
        
        Args:
            delay_between_requests: Delay in seconds between API requests (to avoid rate limiting)
        """
        self.delay_between_requests = delay_between_requests
    
    def get_all_tickers_from_exchanges(self) -> List[tuple]:
        """
        Get list of all stock tickers from major US exchanges with their exchange info
        
        Returns:
            List of tuples (ticker, exchange_name)
        """
        print("Fetching stock symbols from exchanges...")
        
        all_tickers = []  # List of (ticker, exchange) tuples
        
        # Major US exchanges
        exchanges = [
            ('NYSE', 'https://old.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=nyse&render=download'),
            ('NASDAQ', 'https://old.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=nasdaq&render=download'),
            ('AMEX', 'https://old.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=amex&render=download'),
        ]
        
        for exchange_name, url in exchanges:
            try:
                print(f"  Fetching {exchange_name} symbols...")
                response = requests.get(url, timeout=30)
                if response.status_code == 200:
                    # Parse CSV response
                    df = pd.read_csv(StringIO(response.text))
                    if 'Symbol' in df.columns:
                        tickers = df['Symbol'].dropna().unique().tolist()
                        # Filter out symbols with dots (preferred shares, etc.)
                        tickers = [t for t in tickers if '.' not in str(t) and len(str(t).strip()) > 0]
                        # Add with exchange info
                        all_tickers.extend([(t, exchange_name) for t in tickers])
                        print(f"    Found {len(tickers)} symbols from {exchange_name}")
                    time.sleep(1)  # Be polite to the server
                else:
                    print(f"    Warning: Failed to fetch {exchange_name} symbols (HTTP {response.status_code})")
            except Exception as e:
                print(f"    Warning: Error fetching {exchange_name} symbols: {e}")
        
        # Sort by ticker
        all_tickers.sort(key=lambda x: x[0])
        print(f"  Total unique symbols: {len(all_tickers)}")
        return all_tickers
    
    def get_equity_extended_data(self, ticker: str) -> Dict:
        """
        Get all extended equity data including valuation measures, EPS revisions,
        revenue estimates, and analyst recommendations
        
        Args:
            ticker: Stock ticker symbol
            
        Returns:
            Dictionary with all data sections
        """
        result = {
            'equity_info': None,
            'valuation_measures': [],
            'eps_revisions': [],
            'revenue_estimates': [],
            'analyst_recommendations': [],
            'analyst_price_targets': [],
        }
        
        try:
            stock = yf.Ticker(ticker)
            
            # Get basic info
            info = stock.info
            if info:
                result['equity_info'] = info
            
            # Get analyst recommendations (historical)
            try:
                recommendations = stock.recommendations
                if recommendations is not None and len(recommendations) > 0:
                    for date, row in recommendations.iterrows():
                        result['analyst_recommendations'].append({
                            'symbol': ticker,
                            'date': date.strftime('%Y-%m-%d') if hasattr(date, 'strftime') else str(date),
                            'firm': str(row.get('Firm', '')),
                            'to_grade': str(row.get('To Grade', '')),
                            'from_grade': str(row.get('From Grade', '')),
                            'action': str(row.get('Action', '')),
                            'created_at': datetime.now().isoformat(),
                        })
            except Exception as e:
                print(f"    Warning: Could not fetch recommendations for {ticker}: {e}")
            
            # Get upgrades/downgrades (more detailed rating changes)
            try:
                upgrades_downgrades = stock.upgrades_downgrades
                if upgrades_downgrades is not None and len(upgrades_downgrades) > 0:
                    for date, row in upgrades_downgrades.iterrows():
                        result['analyst_recommendations'].append({
                            'symbol': ticker,
                            'date': date.strftime('%Y-%m-%d') if hasattr(date, 'strftime') else str(date),
                            'firm': str(row.get('Firm', '')),
                            'to_grade': str(row.get('To Grade', '')),
                            'from_grade': str(row.get('From Grade', '')),
                            'action': str(row.get('Action', '')),
                            'created_at': datetime.now().isoformat(),
                        })
            except Exception as e:
                print(f"    Warning: Could not fetch upgrades/downgrades for {ticker}: {e}")
            
            # For valuation measures, EPS revisions, and revenue estimates,
            # we'll need to parse from the info() data or scrape the pages
            # These are often in the info dict with specific keys
            
            # Extract valuation measures from info (if available)
            valuation_keys = {
                'marketCap': 'market_cap',
                'enterpriseValue': 'enterprise_value',
                'trailingPE': 'trailing_pe',
                'forwardPE': 'forward_pe',
                'pegRatio': 'pegratio_5y',
                'priceToSalesTrailing12Months': 'price_sales',
                'priceToBook': 'price_book',
                'enterpriseToRevenue': 'ev_revenue',
                'enterpriseToEbitda': 'ev_ebitda',
            }
            
            if info:
                # Current period valuation measures
                current_valuation = {
                    'symbol': ticker,
                    'period': 'Current',
                    'created_at': datetime.now().isoformat(),
                }
                for yahoo_key, our_key in valuation_keys.items():
                    value = info.get(yahoo_key)
                    current_valuation[our_key] = str(value) if value is not None else None
                result['valuation_measures'].append(current_valuation)
                
                # Historical valuation measures are typically not in info()
                # They would need to be scraped from the statistics page
            
        except Exception as e:
            print(f"    Warning: Error fetching extended data for {ticker}: {e}")
        
        return result
    
    def get_equity_essentials(self, ticker: str, exchange: Optional[str] = None) -> Tuple[Optional[Dict], Optional[str]]:
        """
        Get only essential equity information (fast, minimal data)
        
        Args:
            ticker: Stock ticker symbol
            exchange: Exchange name (NYSE, NASDAQ, etc.) if known
            
        Returns:
            Tuple of (essentials_dict, error_message). If successful, returns (dict, None). 
            If error, returns (None, error_message).
        """
        try:
            stock = yf.Ticker(ticker)
            info = stock.info
            
            # Check if info is empty or invalid (yfinance returns empty dict for invalid tickers)
            if not info or len(info) == 0:
                return None, f"No data available for ticker {ticker} (possibly invalid or delisted)"
            
            # Check for common error indicators in yfinance response
            if 'regularMarketPrice' not in info and 'currentPrice' not in info and 'symbol' not in info:
                # This might be an error response
                error_msg = info.get('error', info.get('message', f"Invalid or unavailable data for {ticker}"))
                return None, str(error_msg)
            
            # Extract only essentials
            essentials = {
                'ticker': ticker,
                'name': info.get('longName') or info.get('shortName') or info.get('name') or ticker,
                'country': info.get('country') or None,
                'exchange': exchange or info.get('exchange') or info.get('fullExchangeName') or None,
                'industry': info.get('industry') or None,
                'sector': info.get('sector') or None,
                'created_at': datetime.now().isoformat(),
            }
            
            return essentials, None
            
        except Exception as e:
            error_msg = f"Error fetching essentials for {ticker}: {str(e)}"
            return None, error_msg
    
    def download_equities_essentials(self,
                                     parquet_file: str,
                                     tickers: Optional[List[str]] = None,
                                     limit: Optional[int] = None,
                                     batch_size: int = 500,
                                     update_existing: bool = True,
                                     company_history_parquet: Optional[str] = None) -> List[Dict]:
        """
        Download only essential equity data (fast cataloging)
        
        Args:
            parquet_file: Path to parquet directory
            tickers: Optional list of specific tickers to download (if None, uses company_history.parquet)
            limit: Maximum number of equities to download (None = all, only used if tickers not provided)
            batch_size: Number of equities to process before saving to parquet
            update_existing: If True, update existing records; if False, skip duplicates
            company_history_parquet: Path to company_history.parquet file (default: ../fundamentals/edgar_companies/company_history.parquet)
            
        Returns:
            List of essential equity dictionaries
        """
        if not HAS_YFINANCE:
            raise ImportError(
                "yfinance is required. Install it with: pip install yfinance"
            )
        
        print("=" * 60)
        print("Yahoo Finance Equities Essentials Downloader")
        print("=" * 60)
        print("Downloading only: Ticker, Name, Country, Exchange, Industry, Sector, created_at")
        print()
        
        # Initialize parquet
        paths = get_yahoo_equities_parquet_paths(parquet_file)
        init_yahoo_equities_parquet_tables(paths['base_dir'])
        update_yahoo_equities_metadata(parquet_file, 'status', 'in_progress')
        update_yahoo_equities_metadata(parquet_file, 'last_update', datetime.now().isoformat())
        
        # Get tickers
        if tickers:
            # Use provided ticker list (convert to tuples with None exchange)
            all_tickers = [(t, None) for t in tickers]
            print(f"Using provided ticker list: {len(all_tickers)} tickers")
        else:
            # Load tickers from company_history.parquet
            if company_history_parquet is None:
                # Default path: assume edgar_companies is in fundamentals folder
                script_dir = os.path.dirname(os.path.abspath(__file__))
                default_path = os.path.join(script_dir, '..', '..', 'fundamentals', 'edgar_companies', 'company_history.parquet')
                company_history_parquet = os.path.normpath(default_path)
            
            print(f"Step 1: Loading tickers from {company_history_parquet}...")
            
            if not os.path.exists(company_history_parquet):
                raise FileNotFoundError(
                    f"company_history.parquet not found at: {company_history_parquet}\n"
                    f"Please provide the correct path using --company-history-parquet argument"
                )
            
            try:
                # Read company_history.parquet
                df = pd.read_parquet(company_history_parquet)
                
                # Filter out null tickers and get unique tickers
                if 'ticker' not in df.columns:
                    raise ValueError("company_history.parquet does not contain 'ticker' column")
                
                # Filter out null/empty tickers
                valid_tickers = df[df['ticker'].notna() & (df['ticker'].str.strip() != '')]['ticker'].unique()
                
                if len(valid_tickers) == 0:
                    print("Warning: No valid tickers found in company_history.parquet")
                    return []
                
                # Convert to list of tuples (ticker, None) since we don't have exchange info from EDGAR
                all_tickers = [(str(t), None) for t in valid_tickers]
                
                print(f"  Found {len(all_tickers)} unique tickers (skipped null tickers)")
                
                # Apply limit if provided
                if limit:
                    all_tickers = all_tickers[:limit]
                    print(f"  Limited to {limit} tickers")
                    
            except Exception as e:
                raise Exception(f"Error reading company_history.parquet: {e}")
        
        # Load existing tickers to skip if update_existing is False
        existing_tickers = set()
        if not update_existing and os.path.exists(paths['equities_essentials']):
            try:
                existing_df = pd.read_parquet(paths['equities_essentials'])
                if 'ticker' in existing_df.columns:
                    existing_tickers = set(existing_df['ticker'].dropna().unique())
                    print(f"  Found {len(existing_tickers)} existing tickers in essentials parquet")
            except Exception as e:
                print(f"  Warning: Could not load existing essentials: {e}")
        
        # Filter out existing tickers if update_existing is False
        skipped_count = 0
        if not update_existing and existing_tickers:
            original_count = len(all_tickers)
            all_tickers = [
                ticker_data for ticker_data in all_tickers
                if (ticker_data[0] if isinstance(ticker_data, tuple) else ticker_data) not in existing_tickers
            ]
            skipped_count = original_count - len(all_tickers)
            if skipped_count > 0:
                print(f"  Skipping {skipped_count} tickers that already exist (use --update-existing to re-download)")
        
        if not all_tickers:
            print("\n  All tickers already exist. Nothing to download.")
            return []
        
        print(f"\nStep 2: Downloading essentials for {len(all_tickers)} tickers...")
        print(f"  Batch size: {batch_size}")
        print(f"  Delay between requests: {self.delay_between_requests}s")
        print(f"  Update existing: {update_existing}")
        
        all_essentials = []
        successful = 0
        failed = 0
        
        for i, ticker_data in enumerate(tqdm(all_tickers, desc="Downloading essentials"), 1):
            try:
                # Handle both tuple (ticker, exchange) and string ticker
                if isinstance(ticker_data, tuple):
                    ticker, exchange = ticker_data
                else:
                    ticker = ticker_data
                    exchange = None
                
                essentials, error_msg = self.get_equity_essentials(ticker, exchange)
                
                if essentials:
                    all_essentials.append(essentials)
                    successful += 1
                    
                    # Save in batches
                    if len(all_essentials) >= batch_size:
                        added = add_yahoo_equities_essentials_fast(
                            paths['equities_essentials'], 
                            all_essentials, 
                            update_existing=update_existing
                        )
                        print(f"\n  Saved batch of {added} essentials (incremental save)")
                        all_essentials = []  # Clear after saving
                else:
                    failed += 1
                    if error_msg:
                        # Only print error for first few failures to avoid spam
                        if failed <= 10:
                            print(f"  Failed {ticker}: {error_msg}")
                        elif failed == 11:
                            print(f"  ... (suppressing further error messages)")
                
                # Rate limiting
                if i < len(all_tickers):
                    time.sleep(self.delay_between_requests)
                    
            except KeyboardInterrupt:
                print("\n\nInterrupted by user")
                break
            except Exception as e:
                failed += 1
                print(f"\n  Error processing {ticker}: {e}")
                continue
        
        # Save remaining essentials
        if all_essentials:
            added = add_yahoo_equities_essentials_fast(
                paths['equities_essentials'], 
                all_essentials, 
                update_existing=update_existing
            )
            print(f"\n  Saved {added} essentials (final save)")
        
        # Update metadata
        update_yahoo_equities_metadata(parquet_file, 'total_equities', str(successful))
        update_yahoo_equities_metadata(parquet_file, 'status', 'complete')
        
        print(f"\n{'='*60}")
        print(f"Download complete!")
        print(f"  Successfully downloaded: {successful} equities")
        if skipped_count > 0:
            print(f"  Skipped (already exist): {skipped_count} tickers")
        print(f"  Failed: {failed}")
        print(f"  Data saved to: {paths['equities_essentials']}")
        print(f"{'='*60}")
        
        return all_essentials
    
    def get_equity_info(self, ticker: str) -> Optional[Dict]:
        """
        Get equity information and metadata for a single ticker
        
        Args:
            ticker: Stock ticker symbol
            
        Returns:
            Dictionary with equity information, or None if error
        """
        try:
            stock = yf.Ticker(ticker)
            info = stock.info
            
            if not info:
                return None
            
            # Add core fields - ensure Name, Sector, Industry, Country are explicitly captured
            equity_data = {
                'symbol': ticker,
                'name': info.get('longName') or info.get('shortName') or info.get('name') or info.get('symbol', ticker),
                'sector': info.get('sector') or None,
                'industry': info.get('industry') or None,
                'country': info.get('country') or None,
                'created_at': datetime.now().isoformat(),
                'updated_at': datetime.now().isoformat(),
            }
            
            # Add all other fields from info (metadata, ratios, etc.)
            # Convert all values to strings for parquet compatibility
            for key, value in info.items():
                if key not in equity_data:  # Don't overwrite core fields
                    if value is None:
                        equity_data[key] = None
                    else:
                        # Convert to string, handling various types
                        try:
                            if isinstance(value, (dict, list)):
                                # For complex types, convert to string representation
                                equity_data[key] = str(value)
                            else:
                                equity_data[key] = str(value)
                        except:
                            equity_data[key] = None
            
            # Ensure core metadata fields are always present (even if None)
            # This ensures they're in the schema
            if 'sector' not in equity_data:
                equity_data['sector'] = None
            if 'industry' not in equity_data:
                equity_data['industry'] = None
            if 'country' not in equity_data:
                equity_data['country'] = None
            
            return equity_data
            
        except Exception as e:
            print(f"    Warning: Error fetching info for {ticker}: {e}")
            return None
    
    def download_time_series(self, 
                             tickers: List[str],
                             parquet_file: str,
                             period: str = "max",
                             interval: str = "1d",
                             start_date: Optional[str] = None,
                             end_date: Optional[str] = None) -> List[Dict]:
        """
        Download historical time series data for one or more tickers
        
        Args:
            tickers: List of ticker symbols
            parquet_file: Path to parquet directory
            period: Valid periods: 1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max
            interval: Valid intervals: 1m,2m,5m,15m,30m,60m,90m,1h,1d,5d,1wk,1mo,3mo
            start_date: Start date (YYYY-MM-DD format). If provided, overrides period
            end_date: End date (YYYY-MM-DD format)
            
        Returns:
            List of time series records
        """
        if not HAS_YFINANCE:
            raise ImportError(
                "yfinance is required. Install it with: pip install yfinance"
            )
        
        print("=" * 60)
        print("Yahoo Finance Time Series Downloader")
        print("=" * 60)
        print(f"Downloading time series for {len(tickers)} ticker(s)")
        print(f"  Period: {period}")
        print(f"  Interval: {interval}")
        if start_date:
            print(f"  Start date: {start_date}")
        if end_date:
            print(f"  End date: {end_date}")
        print()
        
        # Initialize parquet
        paths = get_yahoo_equities_parquet_paths(parquet_file)
        init_yahoo_equities_parquet_tables(paths['base_dir'])
        
        # Load existing time series data to find max dates per ticker
        existing_max_dates = {}
        if os.path.exists(paths['time_series']):
            try:
                existing_df = pd.read_parquet(paths['time_series'])
                if 'symbol' in existing_df.columns and 'date' in existing_df.columns:
                    # Convert date to datetime for comparison
                    existing_df['date_parsed'] = pd.to_datetime(existing_df['date'], errors='coerce')
                    # Get max date for each ticker
                    max_dates = existing_df.groupby('symbol')['date_parsed'].max()
                    for symbol, max_date in max_dates.items():
                        if pd.notna(max_date):
                            # Add one day to avoid re-downloading the last date
                            next_date = max_date + pd.Timedelta(days=1)
                            existing_max_dates[symbol] = next_date.strftime('%Y-%m-%d')
                    print(f"  Found existing data for {len(existing_max_dates)} ticker(s)")
            except Exception as e:
                print(f"  Warning: Could not load existing time series: {e}")
        
        all_time_series = []
        successful = 0
        failed = 0
        skipped = 0
        
        for ticker in tqdm(tickers, desc="Downloading time series"):
            try:
                # Check if we have existing data for this ticker
                ticker_start_date = start_date
                if ticker in existing_max_dates:
                    # Use the day after the max existing date
                    existing_max = existing_max_dates[ticker]
                    if start_date:
                        # Use the later of the two dates
                        if existing_max > start_date:
                            ticker_start_date = existing_max
                            print(f"  {ticker}: Using existing max date + 1 day: {existing_max}")
                        else:
                            print(f"  {ticker}: Using provided start_date: {start_date}")
                    else:
                        # No start_date provided, use existing max
                        ticker_start_date = existing_max
                        print(f"  {ticker}: Downloading from {existing_max} (existing max date + 1 day)")
                
                stock = yf.Ticker(ticker)
                
                # Download historical data
                if ticker_start_date:
                    # Download from the calculated start date
                    hist = stock.history(start=ticker_start_date, end=end_date, interval=interval)
                else:
                    # No start date, use period
                    hist = stock.history(period=period, interval=interval)
                
                if hist is None or len(hist) == 0:
                    if ticker in existing_max_dates:
                        print(f"  {ticker}: No new data available (already up to date)")
                        skipped += 1
                    else:
                        print(f"  Warning: No data for {ticker}")
                        failed += 1
                    continue
                
                # Convert to list of records
                now = datetime.now().isoformat()
                for date, row in hist.iterrows():
                    record = {
                        'symbol': ticker,
                        'date': date.strftime('%Y-%m-%d') if hasattr(date, 'strftime') else str(date),
                        'open': str(row.get('Open', '')) if pd.notna(row.get('Open')) else None,
                        'high': str(row.get('High', '')) if pd.notna(row.get('High')) else None,
                        'low': str(row.get('Low', '')) if pd.notna(row.get('Low')) else None,
                        'close': str(row.get('Close', '')) if pd.notna(row.get('Close')) else None,
                        'volume': str(int(row.get('Volume', 0))) if pd.notna(row.get('Volume')) else None,
                        'adj_close': str(row.get('Adj Close', '')) if pd.notna(row.get('Adj Close')) else None,
                        'created_at': now,
                    }
                    all_time_series.append(record)
                
                successful += 1
                print(f"  Downloaded {len(hist)} records for {ticker}")
                
                # Small delay between tickers
                time.sleep(self.delay_between_requests)
                
            except Exception as e:
                print(f"  Error downloading time series for {ticker}: {e}")
                failed += 1
                continue
        
        # Save to parquet with deduplication
        if all_time_series:
            added = add_data_fast(paths['time_series'], all_time_series, primary_key=['symbol', 'date'])
            print(f"\n  Saved {added} new time series records to {paths['time_series']}")
        
        print(f"\n{'='*60}")
        print(f"Download complete!")
        print(f"  Successfully downloaded: {successful}/{len(tickers)} tickers")
        if skipped > 0:
            print(f"  Skipped (already up to date): {skipped} tickers")
        print(f"  Failed: {failed}")
        print(f"  Total new records: {len(all_time_series)}")
        print(f"{'='*60}")
        
        return all_time_series
    
    def download_all_equities(self, 
                              parquet_file: str,
                              tickers: Optional[List[str]] = None,
                              limit: Optional[int] = None,
                              batch_size: int = 100,
                              update_existing: bool = True) -> Dict:
        """
        Download extended equity data (valuation measures, EPS revisions, revenue estimates, analyst recommendations)
        for tickers from yahoo_equities_essentials.parquet.
        
        Note: Essentials are assumed to already exist. This only downloads extended data.
        
        Args:
            parquet_file: Path to parquet directory
            tickers: Optional list of specific tickers to download (if None, uses yahoo_equities_essentials.parquet)
            limit: Maximum number of equities to download (None = all, only used if tickers not provided)
            batch_size: Number of equities to process before saving to parquet
            update_existing: If True, update existing records; if False, skip duplicates
            
        Returns:
            Dictionary with extended data (valuation_measures, eps_revisions, revenue_estimates, analyst_recommendations)
        """
        if not HAS_YFINANCE:
            raise ImportError(
                "yfinance is required. Install it with: pip install yfinance"
            )
        
        print("=" * 60)
        print("Yahoo Finance Equities Extended Data Downloader")
        print("=" * 60)
        
        # Initialize parquet
        paths = get_yahoo_equities_parquet_paths(parquet_file)
        init_yahoo_equities_parquet_tables(paths['base_dir'])
        update_yahoo_equities_metadata(parquet_file, 'status', 'in_progress')
        update_yahoo_equities_metadata(parquet_file, 'last_update', datetime.now().isoformat())
        
        # Get tickers
        if tickers:
            # Use provided ticker list
            all_tickers = tickers
            print(f"\nUsing provided ticker list: {len(all_tickers)} tickers")
        else:
            # Load tickers from yahoo_equities_essentials.parquet
            essentials_file = paths['equities_essentials']
            
            print(f"\nStep 1: Loading tickers from {essentials_file}...")
            
            if not os.path.exists(essentials_file):
                raise FileNotFoundError(
                    f"yahoo_equities_essentials.parquet not found at: {essentials_file}\n"
                    f"Please run --essentials-only first to create the essentials parquet file"
                )
            
            try:
                # Read essentials parquet
                df = pd.read_parquet(essentials_file)
                
                # Check required columns
                required_cols = ['ticker', 'country', 'sector', 'industry']
                missing_cols = [col for col in required_cols if col not in df.columns]
                if missing_cols:
                    raise ValueError(f"yahoo_equities_essentials.parquet is missing required columns: {missing_cols}")
                
                # Filter to only include tickers that have at least one of: country, sector, or industry (not null)
                # A ticker is valid if country OR sector OR industry is not null
                df_valid = df[
                    df['country'].notna() | 
                    df['sector'].notna() | 
                    df['industry'].notna()
                ]
                
                # Get unique tickers from valid rows
                valid_tickers = df_valid['ticker'].dropna().unique()
                
                if len(valid_tickers) == 0:
                    print("Warning: No tickers found with country, sector, or industry data in yahoo_equities_essentials.parquet")
                    return []
                
                # Convert to list of ticker strings
                all_tickers = [str(t).strip() for t in valid_tickers if t is not None]
                all_tickers = [t for t in all_tickers if t and t.strip()]  # Remove empty strings
                
                print(f"  Found {len(all_tickers)} unique tickers with country/sector/industry data from essentials parquet")
                if len(df) > len(df_valid):
                    skipped_count = len(df) - len(df_valid)
                    print(f"  Skipped {skipped_count} records without country, sector, or industry data")
                
                # Apply limit if provided
                if limit:
                    all_tickers = all_tickers[:limit]
                    print(f"  Limited to {limit} tickers")
                    
            except Exception as e:
                raise Exception(f"Error reading yahoo_equities_essentials.parquet: {e}")
        
        print(f"\nStep 2: Downloading extended equity data for {len(all_tickers)} tickers...")
        print(f"  Note: Essentials are assumed to already exist in yahoo_equities_essentials.parquet")
        print(f"  Batch size: {batch_size}")
        print(f"  Delay between requests: {self.delay_between_requests}s")
        print(f"  Using yfinance API for extended data (valuation measures, EPS revisions, revenue estimates, analyst recommendations)")
        
        all_valuation_measures = []
        all_eps_revisions = []
        all_revenue_estimates = []
        all_analyst_recommendations = []
        successful = 0
        failed = 0
        
        try:
            for i, ticker in enumerate(tqdm(all_tickers, desc="Downloading extended data"), 1):
                ticker_success = False
                try:
                    stock = yf.Ticker(ticker)
                    
                    # Valuation measures from info
                    try:
                        info = stock.info
                        if info:
                            # Extract valuation measures
                            valuation_record = {
                                'symbol': ticker,
                                'period': 'Current',
                                'market_cap': str(info.get('marketCap', '')) if info.get('marketCap') else None,
                                'enterprise_value': str(info.get('enterpriseValue', '')) if info.get('enterpriseValue') else None,
                                'trailing_pe': str(info.get('trailingPE', '')) if info.get('trailingPE') else None,
                                'forward_pe': str(info.get('forwardPE', '')) if info.get('forwardPE') else None,
                                'pegratio_5y': str(info.get('pegRatio', '')) if info.get('pegRatio') else None,
                                'price_sales': str(info.get('priceToSalesTrailing12Months', '')) if info.get('priceToSalesTrailing12Months') else None,
                                'price_book': str(info.get('priceToBook', '')) if info.get('priceToBook') else None,
                                'ev_revenue': str(info.get('enterpriseToRevenue', '')) if info.get('enterpriseToRevenue') else None,
                                'ev_ebitda': str(info.get('enterpriseToEbitda', '')) if info.get('enterpriseToEbitda') else None,
                                'created_at': datetime.now().date().isoformat(),
                            }
                            # Only add if we have at least one valuation metric
                            if any(v for k, v in valuation_record.items() if k not in ['symbol', 'period', 'created_at']):
                                all_valuation_measures.append(valuation_record)
                                print(f"    Valuation measures retrieval successful for {ticker}")
                                ticker_success = True
                    except Exception as e:
                        if failed <= 10:
                            print(f"    Warning: Could not fetch valuation measures for {ticker}: {e}")
                    
                    # EPS revisions
                    try:
                        eps_revisions_df = stock.eps_revisions
                        if eps_revisions_df is not None and len(eps_revisions_df) > 0:
                            eps_count_before = len(all_eps_revisions)
                            # Parse EPS revisions DataFrame
                            # yfinance returns DataFrame with columns like 'currentQtr', 'nextQtr', 'currentYear', 'nextYear'
                            # and rows like 'upLast7Days', 'upLast30Days', 'downLast7Days', 'downLast30Days'
                            for col in eps_revisions_df.columns:
                                period_type = str(col)
                                # Extract period from column name or use column as period_type
                                period = period_type  # yfinance may have dates in column names
                                
                                record = {
                                    'symbol': ticker,
                                    'period_type': period_type,
                                    'period': period,
                                    'up_last_7_days': None,
                                    'up_last_30_days': None,
                                    'down_last_7_days': None,
                                    'down_last_30_days': None,
                                    'created_at': datetime.now().isoformat(),
                                }
                                
                                # Map row indices to fields
                                for idx, row in eps_revisions_df.iterrows():
                                    value = str(row[col]) if pd.notna(row[col]) else None
                                    idx_lower = str(idx).lower()
                                    if 'up' in idx_lower and ('7' in idx_lower or 'seven' in idx_lower):
                                        record['up_last_7_days'] = value
                                    elif 'up' in idx_lower and ('30' in idx_lower or 'thirty' in idx_lower):
                                        record['up_last_30_days'] = value
                                    elif 'down' in idx_lower and ('7' in idx_lower or 'seven' in idx_lower):
                                        record['down_last_7_days'] = value
                                    elif 'down' in idx_lower and ('30' in idx_lower or 'thirty' in idx_lower):
                                        record['down_last_30_days'] = value
                                
                                all_eps_revisions.append(record)
                            
                            if len(all_eps_revisions) > eps_count_before:
                                print(f"    EPS revisions retrieval successful for {ticker}")
                                ticker_success = True
                    except Exception as e:
                        if failed <= 10:
                            print(f"    Warning: Could not fetch EPS revisions for {ticker}: {e}")
                    
                    # Revenue estimates
                    try:
                        revenue_estimates_df = stock.revenue_estimate
                        if revenue_estimates_df is not None and len(revenue_estimates_df) > 0:
                            revenue_count_before = len(all_revenue_estimates)
                            # Parse revenue estimates DataFrame
                            # yfinance returns DataFrame with columns like 'currentQtr', 'nextQtr', 'currentYear', 'nextYear'
                            # and rows like 'numberOfAnalysts', 'avgEstimate', 'lowEstimate', 'highEstimate', 'yearAgoSales', 'salesGrowth'
                            for col in revenue_estimates_df.columns:
                                period_type = str(col)
                                period = period_type
                                
                                record = {
                                    'symbol': ticker,
                                    'period_type': period_type,
                                    'period': period,
                                    'num_analysts': None,
                                    'avg_estimate': None,
                                    'low_estimate': None,
                                    'high_estimate': None,
                                    'year_ago_sales': None,
                                    'sales_growth': None,
                                    'created_at': datetime.now().isoformat(),
                                }
                                
                                # Map row indices to fields
                                for idx, row in revenue_estimates_df.iterrows():
                                    value = str(row[col]) if pd.notna(row[col]) else None
                                    idx_lower = str(idx).lower()
                                    if 'number' in idx_lower and 'analyst' in idx_lower:
                                        record['num_analysts'] = value
                                    elif 'avg' in idx_lower or 'average' in idx_lower or 'mean' in idx_lower:
                                        record['avg_estimate'] = value
                                    elif 'low' in idx_lower or 'min' in idx_lower:
                                        record['low_estimate'] = value
                                    elif 'high' in idx_lower or 'max' in idx_lower:
                                        record['high_estimate'] = value
                                    elif 'year' in idx_lower and 'ago' in idx_lower:
                                        record['year_ago_sales'] = value
                                    elif 'growth' in idx_lower:
                                        record['sales_growth'] = value
                                
                                all_revenue_estimates.append(record)
                            
                            if len(all_revenue_estimates) > revenue_count_before:
                                print(f"    Revenue estimates retrieval successful for {ticker}")
                                ticker_success = True
                    except Exception as e:
                        if failed <= 10:
                            print(f"    Warning: Could not fetch revenue estimates for {ticker}: {e}")
                    
                    # Analyst recommendations
                    try:
                        recommendations = stock.recommendations
                        if recommendations is not None and len(recommendations) > 0:
                            for date, row in recommendations.iterrows():
                                all_analyst_recommendations.append({
                                    'symbol': ticker,
                                    'date': date.strftime('%Y-%m-%d') if hasattr(date, 'strftime') else str(date),
                                    'firm': str(row.get('Firm', '')) if 'Firm' in row else '',
                                    'to_grade': str(row.get('To Grade', '')) if 'To Grade' in row else '',
                                    'from_grade': str(row.get('From Grade', '')) if 'From Grade' in row else '',
                                    'action': str(row.get('Action', '')) if 'Action' in row else '',
                                    'created_at': datetime.now().isoformat(),
                                })
                            print(f"    Analyst recommendations retrieval successful for {ticker}")
                            ticker_success = True
                    except Exception as e:
                        if failed <= 10:
                            print(f"    Warning: Could not fetch recommendations for {ticker}: {e}")
                    
                    # Consider successful if we got at least one type of data
                    if ticker_success:
                        successful += 1
                    else:
                        failed += 1
                        if failed <= 10:
                            print(f"    Warning: No extended data found for {ticker}")
                        elif failed == 11:
                            print(f"    ... (suppressing further warnings)")
                    
                    # Small delay between requests
                    time.sleep(self.delay_between_requests)
                    
                    # Save extended data in batches (only if we had success)
                    if ticker_success:
                        total_records = len(all_valuation_measures) + len(all_eps_revisions) + len(all_revenue_estimates) + len(all_analyst_recommendations)
                        if total_records >= batch_size * 4:  # Approximate batch size across all data types
                            if all_valuation_measures:
                                add_data_fast(paths['valuation_measures'], all_valuation_measures, primary_key=['symbol', 'period', 'created_at'])
                                all_valuation_measures = []
                            if all_eps_revisions:
                                add_data_fast(paths['eps_revisions'], all_eps_revisions)
                                all_eps_revisions = []
                            if all_revenue_estimates:
                                add_data_fast(paths['revenue_estimates'], all_revenue_estimates)
                                all_revenue_estimates = []
                            if all_analyst_recommendations:
                                add_data_fast(paths['analyst_recommendations'], all_analyst_recommendations)
                                all_analyst_recommendations = []
                            print(f"\n  Saved batch of extended data (incremental save)")
                    
                except KeyboardInterrupt:
                    print("\n\nInterrupted by user")
                    break
                except Exception as e:
                    failed += 1
                    if failed <= 10:
                        print(f"\n  Error processing {ticker}: {e}")
                    elif failed == 11:
                        print(f"\n  ... (suppressing further error messages)")
                    continue
            
            # Save remaining extended data
            if all_valuation_measures:
                add_data_fast(paths['valuation_measures'], all_valuation_measures, primary_key=['symbol', 'period', 'created_at'])
                print(f"  Saved {len(all_valuation_measures)} valuation measures records")
            if all_eps_revisions:
                add_data_fast(paths['eps_revisions'], all_eps_revisions)
                print(f"  Saved {len(all_eps_revisions)} EPS revision records")
            if all_revenue_estimates:
                add_data_fast(paths['revenue_estimates'], all_revenue_estimates)
                print(f"  Saved {len(all_revenue_estimates)} revenue estimate records")
            if all_analyst_recommendations:
                add_data_fast(paths['analyst_recommendations'], all_analyst_recommendations)
                print(f"  Saved {len(all_analyst_recommendations)} analyst recommendation records")
        
        finally:
            pass  # No scraper to close when using yfinance API
        
        # Update metadata
        update_yahoo_equities_metadata(parquet_file, 'total_equities', str(successful))
        update_yahoo_equities_metadata(parquet_file, 'status', 'complete')
        
        print(f"\n{'='*60}")
        print(f"Download complete!")
        print(f"  Successfully downloaded extended data for: {successful}/{len(all_tickers)} tickers")
        print(f"  Failed: {failed}")
        print(f"  Extended data saved to:")
        print(f"    - Valuation measures: {paths['valuation_measures']}")
        print(f"    - EPS revisions: {paths['eps_revisions']}")
        print(f"    - Revenue estimates: {paths['revenue_estimates']}")
        print(f"    - Analyst recommendations: {paths['analyst_recommendations']}")
        print(f"{'='*60}")
        
        return {
            'valuation_measures': all_valuation_measures,
            'eps_revisions': all_eps_revisions,
            'revenue_estimates': all_revenue_estimates,
            'analyst_recommendations': all_analyst_recommendations,
        }

