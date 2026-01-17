"""
Main entry point for iShares ETF Data Scraper

This module provides a command-line interface to scrape ETF data from iShares.
"""

import os
from ishares_scraper import iSharesScraper


def main():
    """Main function to scrape iShares ETF data"""
    import argparse
    
    # Get the directory where this script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))
    default_parquet_dir = os.path.join(script_dir, 'ishares_parquet')
    
    parser = argparse.ArgumentParser(description='Scrape iShares ETF data from their website')
    parser.add_argument('--url', type=str, 
                       default='https://www.ishares.com/us/products/etf-investments#/?productView=etf&pageNumber=1&sortColumn=totalNetAssets&sortDirection=desc&dataView=keyFacts&style=44342',
                       help='URL to scrape (default: iShares ETF investments page)')
    parser.add_argument('--parquet-dir', type=str, default=default_parquet_dir,
                       help='Directory to save Parquet files (default: ishares_parquet)')
    parser.add_argument('--no-headless', action='store_true',
                       help='Run browser in visible mode (Selenium only)')
    parser.add_argument('--no-selenium', action='store_true',
                       help='Try API endpoint only, do not use Selenium')
    parser.add_argument('--max-pages', type=int, default=None,
                       help='Maximum number of pages to scrape (default: 16, or auto-detect)')
    
    args = parser.parse_args()
    
    try:
        scraper = iSharesScraper(
            headless=not args.no_headless,
            use_selenium=not args.no_selenium
        )
        
        etfs = scraper.scrape_all_etfs(
            url=args.url,
            parquet_file=args.parquet_dir,
            max_pages=args.max_pages
        )
        
        print(f"\n{'='*60}")
        print(f"Scraping complete!")
        print(f"  Total ETFs found: {len(etfs)}")
        print(f"  Parquet directory: {args.parquet_dir}")
        print(f"{'='*60}")
        
        return 0
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit(main())

