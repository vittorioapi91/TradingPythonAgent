"""
Main entry point for Yahoo Finance Equities Data Downloader

This module provides a command-line interface to download equity data from Yahoo Finance.
"""

import os
from yahoo_equities_downloader import YahooEquitiesDownloader


def main():
    """Main function to download Yahoo Finance equity data"""
    import argparse
    
    # Get the directory where this script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))
    default_parquet_dir = os.path.join(script_dir, 'yahoo_equities_parquet')
    
    parser = argparse.ArgumentParser(description='Download equity data and metadata from Yahoo Finance')
    parser.add_argument('--parquet-dir', type=str, default=default_parquet_dir,
                       help='Directory to save Parquet files (default: yahoo_equities_parquet)')
    parser.add_argument('--tickers', type=str, nargs='+',
                       help='Specific ticker symbols to download (e.g., --tickers AAPL MSFT GOOGL). If not provided, downloads all.')
    parser.add_argument('--limit', type=int, default=None,
                       help='Maximum number of equities to download (default: all). Only used if --tickers not provided.')
    parser.add_argument('--batch-size', type=int, default=100,
                       help='Number of equities to process before saving to parquet (default: 100)')
    parser.add_argument('--delay', type=float, default=0.1,
                       help='Delay in seconds between API requests (default: 0.1)')
    parser.add_argument('--no-update-existing', action='store_true',
                       help='Skip updating existing equities (only add new ones). By default, existing tickers are skipped.')
    parser.add_argument('--update-existing', action='store_true',
                       help='Force re-update all tickers, including those that already exist in the essentials parquet')
    parser.add_argument('--essentials-only', action='store_true',
                       help='Download only essentials (Ticker, Name, Country, Exchange, Industry, Sector, created_at) - fast cataloging')
    parser.add_argument('--company-history-parquet', type=str, default=None,
                       help='Path to company_history.parquet file (default: ../fundamentals/edgar_companies/company_history.parquet)')
    parser.add_argument('--extended-data', action='store_true',
                       help='Download extended data (valuation measures, EPS revisions, revenue estimates, analyst recommendations)')
    
    # Time series download arguments
    parser.add_argument('--time-series', type=str, nargs='+',
                       help='Download time series history for specified ticker(s) (e.g., --time-series AAPL MSFT)')
    parser.add_argument('--period', type=str, default='max',
                       help='Time period for time series: 1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max (default: max)')
    parser.add_argument('--interval', type=str, default='1d',
                       help='Data interval: 1m,2m,5m,15m,30m,60m,90m,1h,1d,5d,1wk,1mo,3mo (default: 1d)')
    parser.add_argument('--start-date', type=str, default=None,
                       help='Start date for time series (YYYY-MM-DD). Overrides period if provided')
    parser.add_argument('--end-date', type=str, default=None,
                       help='End date for time series (YYYY-MM-DD)')
    
    args = parser.parse_args()
    
    try:
        downloader = YahooEquitiesDownloader(
            delay_between_requests=args.delay
        )
        
        # If time series download is requested, do that instead
        if args.time_series:
            time_series = downloader.download_time_series(
                tickers=args.time_series,
                parquet_file=args.parquet_dir,
                period=args.period,
                interval=args.interval,
                start_date=args.start_date,
                end_date=args.end_date
            )
            return 0
        
        # If essentials-only is requested, download just essentials
        if args.essentials_only:
            # If --update-existing is explicitly set, use it; otherwise respect --no-update-existing
            update_existing = args.update_existing if args.update_existing else (not args.no_update_existing)
            essentials = downloader.download_equities_essentials(
                parquet_file=args.parquet_dir,
                tickers=args.tickers,
                limit=args.limit,
                batch_size=args.batch_size,
                update_existing=update_existing,
                company_history_parquet=args.company_history_parquet
            )
            return 0
        
        # If extended-data is requested, download extended data only
        if args.extended_data:
            equities = downloader.download_all_equities(
                parquet_file=args.parquet_dir,
                tickers=args.tickers,
                limit=args.limit,
                batch_size=args.batch_size,
                update_existing=not args.no_update_existing
            )
            return 0
        
        # Default: download both essentials and extended data
        # First essentials (fast)
        print("Downloading essentials first...")
        # If --update-existing is explicitly set, use it; otherwise respect --no-update-existing
        update_existing = args.update_existing if args.update_existing else (not args.no_update_existing)
        essentials = downloader.download_equities_essentials(
            parquet_file=args.parquet_dir,
            tickers=args.tickers,
            limit=args.limit,
            batch_size=args.batch_size,
            update_existing=update_existing,
            company_history_parquet=args.company_history_parquet
        )
        
        # Then extended data (slower)
        print("\nDownloading extended data...")
        equities = downloader.download_all_equities(
            parquet_file=args.parquet_dir,
            tickers=args.tickers,
            limit=args.limit,
            batch_size=args.batch_size,
            update_existing=not args.no_update_existing
        )
        
        return 0
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit(main())

