"""
Main entry point for iShares ETF Details Scraper

This module provides a command-line interface to scrape detailed information
from individual iShares ETF pages.
"""

import os
from ishares_details_scraper import iSharesDetailsScraper
from ishares_parquet import get_ishares_parquet_paths


def main():
    """Main function to scrape iShares ETF details"""
    import argparse
    
    # Get the directory where this script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))
    default_etfs_parquet = os.path.join(script_dir, 'ishares_parquet', 'ishares_etfs.parquet')
    default_details_parquet_dir = os.path.join(script_dir, 'ishares_parquet')
    
    parser = argparse.ArgumentParser(description='Scrape detailed information from iShares ETF pages')
    parser.add_argument('--etfs-parquet', type=str, default=default_etfs_parquet,
                       help='Path to ishares_etfs.parquet file (default: ishares_parquet/ishares_etfs.parquet)')
    parser.add_argument('--details-parquet-dir', type=str, default=default_details_parquet_dir,
                       help='Directory to save details Parquet files (default: ishares_parquet)')
    parser.add_argument('--limit', type=int, default=None,
                       help='Maximum number of ETFs to scrape (default: all)')
    parser.add_argument('--no-headless', action='store_true',
                       help='Run browser in visible mode')
    parser.add_argument('--no-skip-existing', action='store_true',
                       help='Re-scrape ETFs that already have details')
    parser.add_argument('--delay', type=float, default=2.0,
                       help='Delay in seconds between page visits (default: 2.0)')
    parser.add_argument('--data-parquet-dir', type=str, default=default_details_parquet_dir,
                       help='Directory to save Holdings/Historical Parquet files (default: same as details)')
    parser.add_argument('--excel-download-dir', type=str, default=None,
                       help='Directory to save downloaded Excel files (default: ishares_excel_data)')
    
    args = parser.parse_args()
    
    try:
        scraper = iSharesDetailsScraper(
            headless=not args.no_headless,
            delay_between_pages=args.delay,
            excel_download_dir=args.excel_download_dir
        )
        
        details = scraper.scrape_all_etf_details(
            etfs_parquet_file=args.etfs_parquet,
            details_parquet_file=args.details_parquet_dir,
            data_parquet_file=args.data_parquet_dir,
            limit=args.limit,
            skip_existing=not args.no_skip_existing
        )
        
        return 0
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit(main())

