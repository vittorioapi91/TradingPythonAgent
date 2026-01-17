"""
Main entry point for Eurostat Data Downloader

This module provides a command-line interface to download time series data from Eurostat.
"""

import os
from eurostat_data_downloader import EurostatDataDownloader


def main():
    """Main function to download Eurostat data"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Download Eurostat economic time series data')
    parser.add_argument('--limit', type=int, help='Maximum number of datasets to process')
    
    parser.add_argument('--generate-db', action='store_true',
                       help='Generate database with all downloadable datasets (saves to PostgreSQL)')
    parser.add_argument('--from-db', action='store_true',
                       help='Download datasets from the database')
    parser.add_argument('--dataset', type=str,
                       help='Download a specific dataset by code (e.g., tps00001)')
    
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
        
        # Database name is fixed to 'eurostat' in EurostatDataDownloader.__init__
        downloader = EurostatDataDownloader(
            user=args.dbuser,
            host=args.dbhost,
            password=password,
            port=args.dbport
        )
        
        # If generate-db is requested, generate the database
        if args.generate_db:
            print("Generating database with all downloadable Eurostat datasets...")
            datasets_list = downloader.get_all_downloadable_series(
                limit=args.limit
            )
            print(f"\nDatabase generated successfully with {len(datasets_list)} datasets!")
            print(f"Use --from-db to download these datasets")
            return 0
        
        # If from-db is specified, download from database
        if args.from_db:
            downloader.download_datasets_from_db()
            print("\nDownload complete!")
            return 0
        
        # If specific dataset is requested
        if args.dataset:
            print(f"Downloading dataset: {args.dataset}")
            data = downloader.download_dataset(args.dataset, save_to_db=True)
            
            if data is not None and not data.empty:
                print(f"Saved to database: {args.dataset}")
                
                print(f"\nDataset shape: {data.shape}")
                print(data.head())
            else:
                print("No data downloaded")
                return 1
            
            return 0
        
        # If no action specified, show help
        parser.print_help()
        return 0
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    main()

