"""
Main entry point for BLS Economic Data Downloader

This module provides a command-line interface to download time series data from BLS.
"""

import os
import sys
# Add current directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from bls_data_downloader import BLSDataDownloader


def main():
    """Main function to download BLS economic data"""
    import argparse
    from datetime import datetime
    
    # Get the directory where this script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))
    default_data_dir = os.path.join(script_dir, 'data')
    
    parser = argparse.ArgumentParser(description='Download BLS economic time series data')
    parser.add_argument('--api-key', type=str, help='BLS API key (or set BLS_API_KEY env var)')
    parser.add_argument('--series', type=str, nargs='+',
                       help='BLS series IDs to download (e.g., CUUR0000SA0 SUUR0000SA0). Required unless --generate-db or --list-surveys is used.')
    parser.add_argument('--start-year', type=int, default=2020,
                       help='Start year (default: 2020)')
    parser.add_argument('--end-year', type=int, default=datetime.now().year,
                       help=f'End year (default: {datetime.now().year})')
    parser.add_argument('--list-surveys', action='store_true',
                       help='List all available BLS surveys and exit')
    parser.add_argument('--generate-db', action='store_true',
                       help='Generate PostgreSQL database with all downloadable series')
    
    # Database connection parameters
    parser.add_argument('--dbuser', type=str, default='tradingAgent',
                       help='PostgreSQL user (default: tradingAgent)')
    parser.add_argument('--dbhost', type=str, default='localhost',
                       help='PostgreSQL host (default: localhost)')
    parser.add_argument('--dbport', type=int, default=5432,
                       help='PostgreSQL port (default: 5432)')
    parser.add_argument('--dbpassword', type=str, default=None,
                       help='PostgreSQL password (or set POSTGRES_PASSWORD env var)')
    
    args = parser.parse_args()
    
    try:
        # Get password from args or environment
        password = args.dbpassword or os.getenv('POSTGRES_PASSWORD', '')
        
        # Database name is fixed to 'bls' in BLSDataDownloader.__init__
        downloader = BLSDataDownloader(
            api_key=args.api_key,
            user=args.dbuser,
            host=args.dbhost,
            password=password,
            port=args.dbport
        )
        
        # If generate-db is requested, generate the database
        if args.generate_db:
            print("Generating PostgreSQL database with all downloadable BLS series...")
            series_list = downloader.get_all_downloadable_series()
            print(f"\nDatabase generated successfully with {len(series_list)} series!")
            print(f"Database: bls")
            return 0
        
        # If listing surveys, do that and exit
        if args.list_surveys:
            surveys = downloader.get_all_surveys()
            print(f"\nFound {len(surveys)} BLS surveys:")
            for survey in surveys:
                print(f"  {survey.get('surveyAbbreviation', 'N/A'):<10} - {survey.get('surveyName', 'N/A')}")
            return 0
        
        # Series download requires --series argument
        if not args.series:
            parser.error("--series is required unless --generate-db or --list-surveys is used")
        
        # Download series
        print(f"Downloading {len(args.series)} BLS series...")
        print(f"Series IDs: {', '.join(args.series)}")
        print(f"Date range: {args.start_year} to {args.end_year}")
        
        df = downloader.download_multiple_series(
            args.series,
            args.start_year,
            args.end_year,
            save_to_db=True
        )
        
        if df.empty:
            print("No data was downloaded")
            return 1
        
        print(f"\nData saved to PostgreSQL database: bls")
        print(f"\nDownload complete! Downloaded {len(df)} data points across {len(df.columns)} series")
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())

