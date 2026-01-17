"""
Main entry point for IMF Economic Data Downloader

This module provides a command-line interface to download time series data from IMF.
"""

import os
import sys
# Add current directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from imf_data_downloader import IMFDataDownloader


def main():
    """Main function to download IMF economic data"""
    import argparse
    from datetime import datetime
    
    # Get the directory where this script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))
    default_data_dir = os.path.join(script_dir, 'data')
    
    parser = argparse.ArgumentParser(description='Download IMF economic time series data')
    parser.add_argument('--database', type=str, default='IFS',
                       help='IMF database ID (default: IFS). Common: IFS, BOP, WEO, GFS, DOT')
    parser.add_argument('--series', type=str, nargs='+',
                       help='Series codes to download (e.g., NGDP_RPCH). Required unless --generate-parquet, --list-databases, --list-indicators, or --list-countries is used.')
    parser.add_argument('--start-date', type=str,
                       help='Start date (YYYY-MM-DD format)')
    parser.add_argument('--end-date', type=str,
                       help=f'End date (YYYY-MM-DD format, default: today)')
    parser.add_argument('--countries', type=str, nargs='+',
                       help='Country codes (ISO 3-letter codes, e.g., USA GBR JPN)')
    parser.add_argument('--output-dir', type=str, default=default_data_dir, help='Output directory')
    parser.add_argument('--list-databases', action='store_true',
                       help='List all available IMF databases and exit')
    parser.add_argument('--list-indicators', action='store_true',
                       help='List all available indicators for the specified database and exit')
    parser.add_argument('--list-countries', action='store_true',
                       help='List all available countries for the specified database and exit')
    parser.add_argument('--generate-db', action='store_true',
                       help='Generate PostgreSQL database with all downloadable indicators')
    parser.add_argument('--from-db', action='store_true',
                       help='Download indicators from the database')
    parser.add_argument('--search', type=str,
                       help='Search for series by search term')
    
    args = parser.parse_args()
    
    try:
        downloader = IMFDataDownloader()
        
        # If listing databases, do that and exit
        if args.list_databases:
            databases_df = downloader.get_databases()
            print(f"\nFound {len(databases_df)} IMF databases:")
            if not databases_df.empty:
                for _, row in databases_df.iterrows():
                    db_id = row.get('database_id', 'N/A')
                    db_desc = row.get('description', 'N/A')
                    print(f"  {db_id:<10} - {db_desc}")
            return 0
        
        # If listing indicators, do that and exit
        if args.list_indicators:
            indicators = downloader.get_indicators(args.database)
            print(f"\nFound {len(indicators)} indicators in {args.database}:")
            for ind in indicators[:50]:  # Show first 50
                ind_code = ind.get('code', 'N/A')
                ind_name = ind.get('name', 'N/A')
                print(f"  {ind_code:<15} - {ind_name}")
            if len(indicators) > 50:
                print(f"  ... and {len(indicators) - 50} more")
            return 0
        
        # If listing countries, do that and exit
        if args.list_countries:
            countries = downloader.get_countries(args.database)
            print(f"\nFound {len(countries)} countries in {args.database}:")
            for country in countries[:50]:  # Show first 50
                country_code = country.get('code', 'N/A')
                country_name = country.get('name', 'N/A')
                print(f"  {country_code:<5} - {country_name}")
            if len(countries) > 50:
                print(f"  ... and {len(countries) - 50} more")
            return 0
        
        # If generate-db is requested, generate the database
        if args.generate_db:
            print(f"Generating PostgreSQL database with all downloadable IMF indicators from {args.database}...")
            series_list = downloader.get_all_downloadable_series(
                database_id=args.database
            )
            print(f"\nDatabase generated successfully with {len(series_list)} indicators!")
            print(f"Database: imf")
            return 0
        
        # If from-db is specified, download from database
        if args.from_db:
            os.makedirs(args.output_dir, exist_ok=True)
            result = downloader.download_series_from_db(
                database_id=args.database,
                start_date=args.start_date,
                end_date=args.end_date,
                output_dir=args.output_dir
            )
            print(f"\nDownload complete: {result.get('downloaded', 0)} series, {result.get('errors', 0)} errors")
            return 0
        
        # Series download requires --series argument (unless searching)
        if not args.series and not args.search:
            parser.error("--series is required unless --generate-db, --list-databases, --list-indicators, --list-countries, or --search is used")
        
        # Ensure output directory exists
        os.makedirs(args.output_dir, exist_ok=True)
        
        # Set default end date to today if not provided
        if not args.end_date:
            args.end_date = datetime.now().strftime('%Y-%m-%d')
        
        # Download series
        if args.search:
            print(f"Searching for series matching '{args.search}' in {args.database}...")
            series_list = downloader.search_series(args.database, search_term=args.search)
            print(f"Found {len(series_list)} matching series")
            if series_list:
                print("First 10 results:")
                for s in series_list[:10]:
                    print(f"  {s}")
            return 0
        
        print(f"Downloading {len(args.series)} IMF series from {args.database}...")
        print(f"Series codes: {', '.join(args.series)}")
        if args.start_date:
            print(f"Start date: {args.start_date}")
        if args.end_date:
            print(f"End date: {args.end_date}")
        if args.countries:
            print(f"Countries: {', '.join(args.countries)}")
        
        df = downloader.download_multiple_series(
            database_id=args.database,
            indicators=args.series,
            start_date=args.start_date,
            end_date=args.end_date,
            countries=args.countries
        )
        
        if df.empty:
            print("No data was downloaded")
            return 1
        
        parquet_path = os.path.join(args.output_dir, f'imf_{args.database.lower()}_data.parquet')
        df.to_parquet(parquet_path)
        print(f"Data saved to: {parquet_path}")
        
        print(f"\nDownload complete! Downloaded {len(df)} rows across {len(df.columns)} series")
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())
