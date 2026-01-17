"""
Download SEC EDGAR filing by path

This script downloads a specific filing from SEC EDGAR using its full path.
Downloads are saved to a "filings" subfolder.
"""

import os
import sys
import argparse
import logging
from pathlib import Path
import requests
from typing import Optional, List, TYPE_CHECKING
from tqdm import tqdm

if TYPE_CHECKING:
    import psycopg2

# Handle imports for both module import and direct script execution
try:
    from ..download_logger import get_download_logger
    from .edgar import EDGARDownloader
    from .edgar_postgres import get_postgres_connection
    from .filings_postgres import get_filings_filenames
except ImportError:
    # Handle direct script execution - use absolute imports
    file_path = Path(__file__).resolve()
    project_root = file_path.parent.parent.parent.parent.parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))
    from src.fundamentals.download_logger import get_download_logger
    from src.fundamentals.edgar.edgar import EDGARDownloader
    from src.fundamentals.edgar.edgar_postgres import get_postgres_connection
    from src.fundamentals.edgar.filings_postgres import get_filings_filenames

# Set up logger using download_logger utility with console output
logger = get_download_logger('edgar_filings', log_level=logging.INFO, add_console_handler=True)


class FilingDownloader(EDGARDownloader):
    """Subclass of EDGARDownloader for downloading filings by path"""
    
    def download_filing_by_path(
        self,
        filing_path: str,
        output_dir: Optional[str] = None
    ) -> Path:
        """
        Download a filing from SEC EDGAR by full path
        
        Args:
            filing_path: Full path to filing (e.g., "edgar/data/315293/0001179110-05-003398.txt")
            output_dir: Output directory for downloaded files (default: "filings" subfolder in edgar directory)
            
        Returns:
            Path to downloaded file
            
        Raises:
            FileNotFoundError: If the filing is not found (404)
            RuntimeError: If there's an error downloading the filing
        """
        # Determine output directory
        if output_dir is None:
            # Default: use "filings" subfolder in edgar directory
            edgar_dir = Path(__file__).parent
            output_dir = str(edgar_dir / "filings")
        else:
            output_dir = str(Path(output_dir).resolve())
        
        # Create output directory if it doesn't exist
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        
        # Construct URL for the filing
        filing_url = f"{self.base_url}/Archives/{filing_path}"
        
        # Download the filing
        logger.debug(f"Downloading filing: {filing_path}")
        logger.debug(f"URL: {filing_url}")
        
        try:
            response = requests.get(filing_url, headers=self.headers, timeout=30)
            response.raise_for_status()
            
            # Extract filename from path
            filename = Path(filing_path).name
            
            # Save to file
            output_file = Path(output_dir) / filename
            output_file.write_bytes(response.content)
            
            file_size = output_file.stat().st_size
            logger.debug(f"Downloaded successfully: {output_file} ({file_size:,} bytes)")
            
            return output_file
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                raise FileNotFoundError(f"Filing not found: {filing_path}. URL: {filing_url}") from e
            else:
                raise RuntimeError(f"HTTP error downloading filing {filing_path}: {e}") from e
        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"Error downloading filing {filing_path}: {e}") from e
    
    def download_filings(
        self,
        dbname: str = "edgar",
        output_dir: Optional[str] = None,
        limit: Optional[int] = None,
        sql_query: Optional[str] = None,
        **filters
    ) -> List[Path]:
        """
        Download filings from master_idx_files table based on flexible filter criteria or raw SQL query
        
        This method uses the query building functions from filings_postgres to construct
        dynamic SQL queries based on provided filters, or accepts a raw SQL query string.
        Any combination of filters can be specified, and only matching filings will be downloaded.
        
        Args:
            dbname: Database name (default: "edgar")
            output_dir: Output directory for downloaded files (default: "filings" subfolder)
            limit: Optional limit on number of filings to download (e.g., 100).
                  If not provided and running in test environment, automatically uses LIMIT 100.
                  Ignored if sql_query is provided.
            sql_query: Optional raw SQL query string. If provided, filters and limit are ignored.
                      Query should return a column named 'filename' or be a SELECT * query.
                      Example: "SELECT * FROM master_idx_files WHERE company_name LIKE '%NVIDIA%' AND year = 2019"
            **filters: Flexible filter criteria. Supported filters:
                - year: Year (e.g., 2005)
                - quarter: Quarter (e.g., 'QTR1', 'QTR2', 'QTR3', 'QTR4')
                - form_type: Form type (e.g., '10-K', '10-Q')
                - cik: CIK (Central Index Key) as string or int
                - filename: Exact filename (e.g., 'edgar/data/315293/0001179110-05-003398.txt')
                - date_filed: Filing date (DATE format: 'YYYY-MM-DD' or date object)
                - company_name: Company name (partial match with ILIKE, case-insensitive)
                
        Returns:
            List of Paths to downloaded files
            
        Examples:
            # Download all filings for year 2005
            downloader.download_filings(year=2005)
            
            # Download 10-K filings for Q1 2005, limit 100
            downloader.download_filings(year=2005, quarter='QTR1', form_type='10-K', limit=100)
            
            # Download NVIDIA filings for Q1 2019, 10-K (equivalent to SQL query)
            downloader.download_filings(company_name='NVIDIA', year=2019, quarter='QTR1', form_type='10-K')
            
            # Download using raw SQL query
            downloader.download_filings(sql_query="SELECT * FROM master_idx_files WHERE company_name LIKE '%NVIDIA%' AND year = 2019 AND quarter = 'QTR1' AND form_type = '10-K'")
            
            # Download all filings for a specific CIK
            downloader.download_filings(cik='0000315293')
            
            # Download a specific filing by filename
            downloader.download_filings(filename='edgar/data/315293/0001179110-05-003398.txt')
            
            # Download all filings for a CIK in a specific year
            downloader.download_filings(cik='0000315293', year=2005)
        """
        # Get database connection
        conn = get_postgres_connection(dbname=dbname)
        
        try:
            # Get filenames using query building functions or raw SQL
            filenames = get_filings_filenames(conn, limit=limit, sql_query=sql_query, **filters)
            
            if not filenames:
                if sql_query:
                    raise ValueError(f"No filings found for SQL query")
                else:
                    filter_str = ", ".join(f"{k}={v}" for k, v in filters.items() if v is not None)
                    raise ValueError(f"No filings found for filters: {filter_str}")
            
            logger.info(f"Found {len(filenames)} filings to download")
            
            # Download each filing with progress bar
            downloaded_files = []
            failed_downloads = []
            
            with tqdm(total=len(filenames), desc="Downloading filings", unit="file") as pbar:
                for filename in filenames:
                    try:
                        output_file = self.download_filing_by_path(filename, output_dir=output_dir)
                        downloaded_files.append(output_file)
                        pbar.update(1)
                    except Exception as e:
                        failed_downloads.append((filename, str(e)))
                        logger.warning(f"Error downloading {filename}: {e}")
                        pbar.update(1)
                        # Continue with next filing instead of stopping
                        continue
            
            if failed_downloads:
                logger.warning(f"Failed to download {len(failed_downloads)}/{len(filenames)} filings")
            else:
                logger.info(f"Successfully downloaded {len(downloaded_files)}/{len(filenames)} filings")
            
            return downloaded_files
            
        finally:
            conn.close()


def main():
    """Main function for command-line usage"""
    parser = argparse.ArgumentParser(
        description='Download SEC EDGAR filing by full path',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Download a filing by full path:
  python -m trading_agent.fundamentals.edgar.filings edgar/data/315293/0001179110-05-003398.txt
  
  # Download to custom directory:
  python -m trading_agent.fundamentals.edgar.filings edgar/data/315293/0001179110-05-003398.txt --output-dir /path/to/filings
        """
    )
    parser.add_argument(
        'filing_path',
        type=str,
        help='Full path to filing (e.g., edgar/data/315293/0001179110-05-003398.txt)'
    )
    parser.add_argument(
        '--output-dir',
        type=str,
        default=None,
        help='Output directory for downloaded files (default: filings/ subfolder in edgar directory)'
    )
    parser.add_argument(
        '--user-agent',
        type=str,
        default='VittorioApicella apicellavittorio@hotmail.it',
        help='User-Agent string for SEC EDGAR requests (required by SEC)'
    )
    
    args = parser.parse_args()
    
    try:
        downloader = FilingDownloader(user_agent=args.user_agent)
        output_file = downloader.download_filing_by_path(
            filing_path=args.filing_path,
            output_dir=args.output_dir
        )
        logger.info(f"Filing downloaded successfully: {output_file}")
        return 0
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    main()
