"""
Main entry point for FRED Economic Data Downloader

This module provides a command-line interface to download time series data from FRED.
"""

import os
from fred_data_downloader import FREDDataDownloader


def main():
    """Main function to download FRED economic data"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Download FRED economic time series data')
    parser.add_argument('--api-key', type=str, help='FRED API key (or set FRED_API_KEY env var)')
    parser.add_argument('--start-date', type=str, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, help='End date (YYYY-MM-DD)')
    parser.add_argument('--limit', type=int, help='Maximum number of series to download')
    parser.add_argument('--series', type=str, nargs='+', help='Specific series IDs to download (e.g., GDP)')
    parser.add_argument('--generate-db', action='store_true',
                       help='Generate database with all downloadable series (saves to PostgreSQL)')
    parser.add_argument('--from-db', action='store_true',
                       help='Download series from the database')
    parser.add_argument('--use-categories', action='store_true', default=True,
                       help='Use categories to discover series when generating database (default: True)')
    parser.add_argument('--full-category-search', action='store_true', default=False,
                       help='Recursively search ALL categories (slow). Default: only known major categories (fast)')
    parser.add_argument('--include-search-terms', action='store_true', default=False,
                       help='Also search using economic terms to find additional series (slower but more comprehensive)')
    parser.add_argument('--category-roots', type=int, nargs='+',
                       help='Optional list of FRED category IDs to use as roots when using known-categories mode')
    parser.add_argument('--series-query-file', type=str,
                       help='Path to SQL file containing a query that returns series_id values (single column named series_id).')
    # Database name is fixed to 'fred' in the code
    parser.add_argument('--dbuser', type=str, default='postgres',
                       help='PostgreSQL user (default: postgres)')
    parser.add_argument('--dbhost', type=str, default='localhost',
                       help='PostgreSQL host (default: localhost)')
    parser.add_argument('--dbport', type=int, default=5432,
                       help='PostgreSQL port (default: 5432)')
    parser.add_argument('--dbpassword', type=str, default=None,
                       help='PostgreSQL password (or set POSTGRES_PASSWORD env var, default: 2014)')
    
    args = parser.parse_args()
    
    try:
        # Get password from args or environment or default
        password = args.dbpassword or os.getenv('POSTGRES_PASSWORD', '2014')
        
        # Database name is fixed to 'fred' in FREDDataDownloader.__init__
        downloader = FREDDataDownloader(
            api_key=args.api_key,
            user=args.dbuser,
            host=args.dbhost,
            password=password,
            port=args.dbport
        )
        
        # If generate-db is requested, generate the database
        if args.generate_db:
            print("Generating database with all downloadable FRED series...")
            if args.full_category_search:
                print("  Mode: Full category search (recursive - slower)")
            else:
                print("  Mode: Known categories only (fast)")
            if args.include_search_terms:
                print("  Also searching with economic terms")
            
            series_list = downloader.get_all_downloadable_series(
                limit=args.limit,
                use_categories=args.use_categories,
                use_known_categories_only=not args.full_category_search,
                use_search_terms=args.include_search_terms,
                category_roots=args.category_roots
            )
            print(f"\nDatabase generated successfully with {len(series_list)} series!")
            print(f"Use --from-db to download these series")
            return 0
        
        # If from-db is specified, download from database
        if args.from_db:
            # Handle series query file if provided
            series_ids = None
            if args.series_query_file:
                query_file = args.series_query_file
                if not os.path.isabs(query_file):
                    # If relative path, make it relative to the script directory or workspace root
                    script_dir = os.path.dirname(os.path.abspath(__file__))
                    query_file = os.path.join(script_dir, query_file)
                
                if not os.path.exists(query_file):
                    print(f"Error: Series query file not found: {query_file}")
                    return 1
                
                print(f"Reading series query from: {query_file}")
                with open(query_file, 'r', encoding='utf-8') as f:
                    query = f.read().strip()
                
                # Remove SQL comments and empty lines for cleaner output
                query_lines = [line for line in query.split('\n') if line.strip() and not line.strip().startswith('--')]
                query_preview = ' '.join(query_lines[:3])
                if len(query_lines) > 3:
                    query_preview += '...'
                print(f"  Query: {query_preview}")
                
                from fred_postgres import get_postgres_connection
                from psycopg2.extras import RealDictCursor
                
                conn = get_postgres_connection(
                    dbname='fred',
                    user=args.dbuser,
                    host=args.dbhost,
                    password=password,
                    port=args.dbport
                )
                cur = conn.cursor(cursor_factory=RealDictCursor)
                cur.execute(query)
                results = cur.fetchall()
                cur.close()
                conn.close()
                
                # Extract series IDs from results
                if results:
                    series_ids = []
                    for row in results:
                        if isinstance(row, dict):
                            # Try common column names for series_id
                            series_id = (row.get('series_id') or 
                                       row.get('series_id::text') or 
                                       row.get('series') or
                                       list(row.values())[0])
                        elif isinstance(row, (tuple, list)):
                            series_id = row[0]
                        else:
                            series_id = row
                        
                        if series_id:
                            series_ids.append(str(series_id))
                    
                    if series_ids:
                        print(f"  Found {len(series_ids)} series from query: {series_ids[:10]}{'...' if len(series_ids) > 10 else ''}")
                    else:
                        print("  Warning: No valid series IDs found in query results")
                        series_ids = None
                else:
                    print("  Warning: Series query returned no results")
                    series_ids = None
            else:
                print("Error: --series-query-file is required when using --from-db")
                return 1
            
            downloader.download_series_from_db(
                start_date=args.start_date,
                end_date=args.end_date,
                series_ids=series_ids if series_ids else None
            )
            print("\nDownload complete!")
            return 0
        
        # If specific series are requested, download those
        if args.series:
            print(f"Downloading {len(args.series)} specified series...")
            for series_id in args.series:
                print(f"\nDownloading {series_id}...")
                data = downloader.download_series(series_id, start_date=args.start_date, end_date=args.end_date, save_to_db=True)
                
                if data is not None and not data.empty:
                    print(f"  Saved {len(data)} data points to database for {series_id}")
                else:
                    print(f"  Failed to download {series_id}")
        else:
            # Download all available series
            downloader.download_all_available_series(
                start_date=args.start_date,
                end_date=args.end_date,
                limit=args.limit
            )
        
        print("\nDownload complete!")
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == "__main__":
    main()

