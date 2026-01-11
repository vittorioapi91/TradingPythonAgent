"""
EDGAR PostgreSQL Database Management

This module handles all PostgreSQL operations for EDGAR data storage.
"""

import os
from typing import Dict, Optional, List, Set
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_values, RealDictCursor
from psycopg2 import sql


def get_postgres_connection(dbname: str = "edgar", user: str = "postgres", 
                            host: str = "localhost", password: Optional[str] = None,
                            port: int = 5432) -> psycopg2.extensions.connection:
    """
    Get PostgreSQL connection
    
    Args:
        dbname: Database name (default: 'edgar')
        user: Database user (default: 'postgres')
        host: Database host (default: 'localhost')
        password: Database password (optional, can use environment variable)
        port: Database port (default: 5432)
    
    Returns:
        PostgreSQL connection
    """
    password = password or os.getenv('POSTGRES_PASSWORD', '')
    
    return psycopg2.connect(
        dbname=dbname,
        user=user,
        host=host,
        password=password,
        port=port
    )


def init_edgar_postgres_tables(conn: psycopg2.extensions.connection) -> None:
    """
    Initialize PostgreSQL tables for EDGAR data if they don't exist
    
    Args:
        conn: PostgreSQL connection
    """
    print("Initializing EDGAR PostgreSQL tables...")
    cur = conn.cursor()
    
    # Helper function to check if table exists
    def table_exists(table_name: str) -> bool:
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = %s
            )
        """, (table_name,))
        return cur.fetchone()[0]
    
    # Create companies table
    companies_existed = table_exists('companies')
    cur.execute("""
        CREATE TABLE IF NOT EXISTS companies (
            cik VARCHAR(10) PRIMARY KEY,
            ticker VARCHAR(20),
            name TEXT NOT NULL,
            sic_code VARCHAR(50),
            entity_type VARCHAR(50),
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    if not companies_existed:
        print("  ✓ Created companies table")
    
    # Create index on ticker for faster lookups
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_companies_ticker 
        ON companies(ticker)
    """)
    
    # Create index on sic_code
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_companies_sic_code 
        ON companies(sic_code)
    """)
    
    # Create filings table
    filings_existed = table_exists('filings')
    cur.execute("""
        CREATE TABLE IF NOT EXISTS filings (
            cik VARCHAR(10) NOT NULL,
            accession_number VARCHAR(50) NOT NULL,
            filing_date DATE NOT NULL,
            filing_type VARCHAR(50) NOT NULL,
            description TEXT,
            is_xbrl BOOLEAN DEFAULT FALSE,
            is_inline_xbrl BOOLEAN DEFAULT FALSE,
            amends_accession VARCHAR(50),
            amends_filing_date DATE,
            downloaded_file_path TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (cik, accession_number),
            FOREIGN KEY (cik) REFERENCES companies(cik) ON DELETE CASCADE
        )
    """)
    if not filings_existed:
        print("  ✓ Created filings table")
    
    # Create indexes on filings
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_filings_cik 
        ON filings(cik)
    """)
    
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_filings_filing_date 
        ON filings(filing_date)
    """)
    
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_filings_filing_type 
        ON filings(filing_type)
    """)
    
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_filings_accession_number 
        ON filings(accession_number)
    """)
    
    # Create company_history table (snapshot of companies at different times)
    company_history_existed = table_exists('company_history')
    cur.execute("""
        CREATE TABLE IF NOT EXISTS company_history (
            id SERIAL PRIMARY KEY,
            cik VARCHAR(10) NOT NULL,
            ticker VARCHAR(20),
            name TEXT NOT NULL,
            sic_code VARCHAR(50),
            entity_type VARCHAR(50),
            snapshot_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (cik) REFERENCES companies(cik) ON DELETE CASCADE
        )
    """)
    if not company_history_existed:
        print("  ✓ Created company_history table")
    
    # Create index on company_history
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_company_history_cik 
        ON company_history(cik)
    """)
    
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_company_history_snapshot_at 
        ON company_history(snapshot_at)
    """)
    
    # Create metadata table
    metadata_existed = table_exists('metadata')
    cur.execute("""
        CREATE TABLE IF NOT EXISTS metadata (
            key VARCHAR(255) PRIMARY KEY,
            value TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    if not metadata_existed:
        print("  ✓ Created metadata table")
    
    # Create year_completion_ledger table (ledger pattern for tracking year completion)
    year_completion_ledger_existed = table_exists('year_completion_ledger')
    cur.execute("""
        CREATE TABLE IF NOT EXISTS year_completion_ledger (
            year INTEGER NOT NULL,
            filing_type VARCHAR(50) NOT NULL,  -- 'TOTAL' for total, or specific type like '10-K', '10-Q', etc.
            sec_index_count INTEGER DEFAULT 0,
            db_count INTEGER DEFAULT 0,
            is_complete BOOLEAN DEFAULT FALSE,
            last_checked TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (year, filing_type)
        )
    """)
    if not year_completion_ledger_existed:
        print("  ✓ Created year_completion_ledger table")
    
    # Create index on year_completion_ledger
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_year_completion_ledger_year 
        ON year_completion_ledger(year)
    """)
    
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_year_completion_ledger_is_complete 
        ON year_completion_ledger(is_complete)
    """)
    
    # Initialize metadata if empty
    cur.execute("SELECT COUNT(*) FROM metadata")
    if cur.fetchone()[0] == 0:
        now = datetime.now()
        initial_metadata = [
            ('generated_at', now.isoformat(), now),
            ('total_companies', '0', now),
            ('total_filings', '0', now),
            ('status', 'in_progress', now),
            ('source', 'SEC EDGAR API', now),
        ]
        execute_values(
            cur,
            "INSERT INTO metadata (key, value, updated_at) VALUES %s",
            initial_metadata
        )
    
    # Verify all tables were created before committing
    cur.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name IN ('companies', 'filings', 'company_history', 'metadata', 'year_completion_ledger')
        ORDER BY table_name
    """)
    created_tables = [row[0] for row in cur.fetchall()]
    print(f"  ✓ Database tables initialized: {', '.join(created_tables)}")
    if 'year_completion_ledger' not in created_tables:
        print("  ERROR: year_completion_ledger table was not created! Attempting to create it manually...")
        try:
            cur.execute("""
                CREATE TABLE year_completion_ledger (
                    year INTEGER NOT NULL,
                    filing_type VARCHAR(50) NOT NULL,
                    sec_index_count INTEGER DEFAULT 0,
                    db_count INTEGER DEFAULT 0,
                    is_complete BOOLEAN DEFAULT FALSE,
                    last_checked TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (year, filing_type)
                )
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_year_completion_ledger_year 
                ON year_completion_ledger(year)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_year_completion_ledger_is_complete 
                ON year_completion_ledger(is_complete)
            """)
            print("  ✓ year_completion_ledger table created manually")
        except Exception as e:
            print(f"  ERROR: Failed to create year_completion_ledger table manually: {e}")
            import traceback
            traceback.print_exc()
            raise
    
    conn.commit()
    cur.close()
    print("  ✓ Database initialization complete")


def add_companies_fast(conn: psycopg2.extensions.connection, companies: List[Dict]) -> int:
    """
    Add companies to PostgreSQL database (upsert - insert or update if exists)
    
    Args:
        conn: PostgreSQL connection
        companies: List of company dictionaries with keys: cik, ticker, name, sic_code, entity_type
    
    Returns:
        Number of companies added/updated
    """
    if not companies:
        return 0
    
    cur = conn.cursor()
    
    # Prepare data for batch insert
    company_values = []
    for company in companies:
        company_values.append((
            company.get('cik'),
            company.get('ticker'),
            company.get('name', 'N/A'),
            company.get('sic_code'),
            company.get('entity_type'),
            datetime.now()
        ))
    
    # Use ON CONFLICT to upsert
    query = """
        INSERT INTO companies (cik, ticker, name, sic_code, entity_type, updated_at)
        VALUES %s
        ON CONFLICT (cik) DO UPDATE SET
            ticker = EXCLUDED.ticker,
            name = EXCLUDED.name,
            sic_code = EXCLUDED.sic_code,
            entity_type = EXCLUDED.entity_type,
            updated_at = EXCLUDED.updated_at
    """
    
    execute_values(cur, query, company_values)
    
    conn.commit()
    count = len(companies)
    cur.close()
    
    return count


def add_filings_fast(conn: psycopg2.extensions.connection, filings: List[Dict]) -> int:
    """
    Add filings to PostgreSQL database (insert only, skip duplicates)
    
    Args:
        conn: PostgreSQL connection
        filings: List of filing dictionaries with keys: cik, accession_number, filing_date, 
                 filing_type, description, is_xbrl, is_inline_xbrl, amends_accession, 
                 amends_filing_date, downloaded_file_path
    
    Returns:
        Number of filings added (excluding duplicates)
    """
    if not filings:
        return 0
    
    cur = conn.cursor()
    
    # Prepare data for batch insert
    filing_values = []
    for filing in filings:
        filing_date = filing.get('filing_date')
        # Convert string date to date object if needed
        if isinstance(filing_date, str):
            try:
                filing_date = datetime.strptime(filing_date, '%Y-%m-%d').date()
            except:
                filing_date = None
        
        amends_filing_date = filing.get('amends_filing_date')
        if isinstance(amends_filing_date, str):
            try:
                amends_filing_date = datetime.strptime(amends_filing_date, '%Y-%m-%d').date()
            except:
                amends_filing_date = None
        
        filing_values.append((
            filing.get('cik'),
            filing.get('accession_number'),
            filing_date,
            filing.get('filing_type', ''),
            filing.get('description'),
            filing.get('is_xbrl', False),
            filing.get('is_inline_xbrl', False),
            filing.get('amends_accession'),
            amends_filing_date,
            filing.get('downloaded_file_path'),
            datetime.now()
        ))
    
    # Use ON CONFLICT to skip duplicates
    query = """
        INSERT INTO filings (cik, accession_number, filing_date, filing_type, description,
                           is_xbrl, is_inline_xbrl, amends_accession, amends_filing_date,
                           downloaded_file_path, created_at)
        VALUES %s
        ON CONFLICT (cik, accession_number) DO NOTHING
    """
    
    execute_values(cur, query, filing_values)
    
    conn.commit()
    count = cur.rowcount
    cur.close()
    
    return count


def load_companies_from_postgres(conn: psycopg2.extensions.connection,
                                 tickers: Optional[List[str]] = None) -> List[Dict]:
    """
    Load companies from PostgreSQL database
    
    Args:
        conn: PostgreSQL connection
        tickers: Optional list of ticker symbols to filter by
    
    Returns:
        List of company dictionaries
    """
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    if tickers:
        placeholders = ','.join(['%s'] * len(tickers))
        query = f"""
            SELECT cik, ticker, name, sic_code, entity_type, updated_at
            FROM companies
            WHERE ticker IN ({placeholders})
            ORDER BY ticker
        """
        cur.execute(query, tickers)
    else:
        query = """
            SELECT cik, ticker, name, sic_code, entity_type, updated_at
            FROM companies
            ORDER BY ticker
        """
        cur.execute(query)
    
    companies = [dict(row) for row in cur.fetchall()]
    cur.close()
    
    return companies


def load_filings_from_postgres(conn: psycopg2.extensions.connection,
                               cik: Optional[str] = None,
                               filing_types: Optional[List[str]] = None,
                               start_date: Optional[str] = None,
                               end_date: Optional[str] = None) -> List[Dict]:
    """
    Load filings from PostgreSQL database
    
    Args:
        conn: PostgreSQL connection
        cik: Optional CIK to filter by
        filing_types: Optional list of filing types to filter by
        start_date: Optional start date (YYYY-MM-DD)
        end_date: Optional end date (YYYY-MM-DD)
    
    Returns:
        List of filing dictionaries
    """
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    conditions = []
    params = []
    
    if cik:
        conditions.append("cik = %s")
        params.append(cik)
    
    if filing_types:
        placeholders = ','.join(['%s'] * len(filing_types))
        conditions.append(f"filing_type IN ({placeholders})")
        params.extend(filing_types)
    
    if start_date:
        conditions.append("filing_date >= %s")
        params.append(start_date)
    
    if end_date:
        conditions.append("filing_date <= %s")
        params.append(end_date)
    
    where_clause = " AND ".join(conditions) if conditions else "1=1"
    
    query = f"""
        SELECT cik, accession_number, filing_date, filing_type, description,
               is_xbrl, is_inline_xbrl, amends_accession, amends_filing_date,
               downloaded_file_path, created_at
        FROM filings
        WHERE {where_clause}
        ORDER BY filing_date DESC, accession_number
    """
    
    cur.execute(query, params)
    filings = [dict(row) for row in cur.fetchall()]
    cur.close()
    
    return filings


def get_existing_accessions(conn: psycopg2.extensions.connection, cik: str) -> set:
    """
    Get set of existing accession numbers for a company
    
    Args:
        conn: PostgreSQL connection
        cik: Company CIK
    
    Returns:
        Set of accession numbers
    """
    cur = conn.cursor()
    cur.execute("SELECT accession_number FROM filings WHERE cik = %s", (cik,))
    accessions = {row[0] for row in cur.fetchall()}
    cur.close()
    return accessions


def add_company_history_snapshot(conn: psycopg2.extensions.connection) -> int:
    """
    Add current companies snapshot to company_history table
    
    Args:
        conn: PostgreSQL connection
    
    Returns:
        Number of companies added to history
    """
    cur = conn.cursor()
    
    query = """
        INSERT INTO company_history (cik, ticker, name, sic_code, entity_type, snapshot_at)
        SELECT cik, ticker, name, sic_code, entity_type, CURRENT_TIMESTAMP
        FROM companies
    """
    
    cur.execute(query)
    conn.commit()
    count = cur.rowcount
    cur.close()
    
    return count


def update_edgar_metadata(conn: psycopg2.extensions.connection, key: str, value: str) -> None:
    """
    Update metadata value in PostgreSQL database
    
    Args:
        conn: PostgreSQL connection
        key: Metadata key
        value: Metadata value
    """
    cur = conn.cursor()
    
    query = """
        INSERT INTO metadata (key, value, updated_at)
        VALUES (%s, %s, %s)
        ON CONFLICT (key) DO UPDATE SET
            value = EXCLUDED.value,
            updated_at = EXCLUDED.updated_at
    """
    
    cur.execute(query, (key, value, datetime.now()))
    conn.commit()
    cur.close()


def get_edgar_metadata(conn: psycopg2.extensions.connection, key: Optional[str] = None) -> Dict[str, str]:
    """
    Get metadata from PostgreSQL database
    
    Args:
        conn: PostgreSQL connection
        key: Optional metadata key (if None, returns all metadata)
    
    Returns:
        Dictionary of metadata key-value pairs
    """
    cur = conn.cursor()
    
    if key:
        cur.execute("SELECT key, value FROM metadata WHERE key = %s", (key,))
    else:
        cur.execute("SELECT key, value FROM metadata")
    
    metadata = {row[0]: row[1] for row in cur.fetchall()}
    cur.close()
    
    return metadata


def get_edgar_statistics(conn: psycopg2.extensions.connection) -> Dict[str, int]:
    """
    Get statistics about EDGAR data in PostgreSQL database
    
    Args:
        conn: PostgreSQL connection
    
    Returns:
        Dictionary with statistics: total_companies, total_filings
    """
    cur = conn.cursor()
    
    cur.execute("SELECT COUNT(*) FROM companies")
    total_companies = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(*) FROM filings")
    total_filings = cur.fetchone()[0]
    
    cur.close()
    
    return {
        'total_companies': total_companies,
        'total_filings': total_filings
    }


def get_enriched_ciks(conn: psycopg2.extensions.connection) -> Set[str]:
    """
    Get set of CIKs that are already enriched in the companies table
    
    A CIK is considered enriched if it exists in the companies table.
    Since companies are only added after enrichment in the current flow,
    any CIK in the table can be considered enriched.
    
    Args:
        conn: PostgreSQL connection
    
    Returns:
        Set of CIK strings (normalized to 10 digits)
    """
    cur = conn.cursor()
    cur.execute("SELECT cik FROM companies")
    ciks = {str(row[0]).zfill(10) for row in cur.fetchall()}
    cur.close()
    return ciks


def get_processed_years(conn: psycopg2.extensions.connection) -> set:
    """
    Get set of years that have already been processed
    
    Args:
        conn: PostgreSQL connection
    
    Returns:
        Set of years (as integers) that have been processed
    """
    cur = conn.cursor()
    
    cur.execute("SELECT value FROM metadata WHERE key = 'processed_years'")
    result = cur.fetchone()
    cur.close()
    
    if result and result[0]:
        try:
            # Value is comma-separated list of years
            years = {int(y.strip()) for y in result[0].split(',') if y.strip()}
            return years
        except:
            return set()
    return set()


def mark_year_processed(conn: psycopg2.extensions.connection, year: int) -> None:
    """
    Mark a year as processed in metadata
    
    Args:
        conn: PostgreSQL connection
        year: Year to mark as processed
    """
    processed_years = get_processed_years(conn)
    processed_years.add(year)
    
    # Store as comma-separated string
    years_str = ','.join(sorted(str(y) for y in processed_years))
    update_edgar_metadata(conn, 'processed_years', years_str)


def get_year_completion_status(conn: psycopg2.extensions.connection, year: int) -> Dict:
    """
    Get completion status for a year from the ledger
    
    Args:
        conn: PostgreSQL connection
        year: Year to check
    
    Returns:
        Dictionary with completion status by filing type and overall
    """
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    cur.execute("""
        SELECT filing_type, sec_index_count, db_count, is_complete, last_checked
        FROM year_completion_ledger
        WHERE year = %s
        ORDER BY CASE WHEN filing_type = 'TOTAL' THEN 0 ELSE 1 END, filing_type
    """, (year,))
    
    results = cur.fetchall()
    cur.close()
    
    status = {
        'year': year,
        'by_type': {},
        'total': {'sec_index_count': 0, 'db_count': 0, 'is_complete': True}
    }
    
    for row in results:
        filing_type = row['filing_type']
        if filing_type == 'TOTAL':
            # Total row
            status['total'] = {
                'sec_index_count': row['sec_index_count'],
                'db_count': row['db_count'],
                'is_complete': row['is_complete'],
                'last_checked': row['last_checked']
            }
        else:
            status['by_type'][filing_type] = {
                'sec_index_count': row['sec_index_count'],
                'db_count': row['db_count'],
                'is_complete': row['is_complete'],
                'last_checked': row['last_checked']
            }
    
    return status


def update_year_completion_ledger(conn: psycopg2.extensions.connection, 
                                   year: int, 
                                   filing_type: Optional[str],
                                   sec_index_count: int,
                                   db_count: int) -> None:
    """
    Update the year completion ledger with counts from SEC index and DB
    
    Args:
        conn: PostgreSQL connection
        year: Year
        filing_type: Filing type ('TOTAL' for total, or specific type like '10-K', '10-Q', or None which becomes 'TOTAL')
        sec_index_count: Count from SEC index
        db_count: Count in database
    """
    cur = conn.cursor()
    
    # Convert None to 'TOTAL' (since PRIMARY KEY doesn't allow NULL)
    if filing_type is None:
        filing_type = 'TOTAL'
    
    # Calculate if complete (counts match)
    is_complete = (sec_index_count == db_count) and (sec_index_count > 0)
    
    cur.execute("""
        INSERT INTO year_completion_ledger (year, filing_type, sec_index_count, db_count, is_complete, last_checked)
        VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (year, filing_type) DO UPDATE SET
            sec_index_count = EXCLUDED.sec_index_count,
            db_count = EXCLUDED.db_count,
            is_complete = EXCLUDED.is_complete,
            last_checked = EXCLUDED.last_checked
    """, (year, filing_type, sec_index_count, db_count, is_complete))
    
    conn.commit()
    cur.close()


def get_db_filing_counts_by_year(conn: psycopg2.extensions.connection, 
                                  year: int, 
                                  filing_types: Optional[List[str]] = None) -> Dict[Optional[str], int]:
    """
    Get filing counts from database for a specific year, grouped by filing type
    
    Args:
        conn: PostgreSQL connection
        year: Year to count
        filing_types: Optional list of filing types to filter (None for all)
    
    Returns:
        Dictionary mapping filing_type to count (None key for total)
    """
    cur = conn.cursor()
    
    counts = {}
    
    # Get counts by filing type
    if filing_types:
        placeholders = ','.join(['%s'] * len(filing_types))
        query = f"""
            SELECT filing_type, COUNT(*) as count
            FROM filings
            WHERE filing_date >= %s AND filing_date < %s
            AND filing_type IN ({placeholders})
            GROUP BY filing_type
        """
        params = (f"{year}-01-01", f"{year+1}-01-01") + tuple(filing_types)
    else:
        query = """
            SELECT filing_type, COUNT(*) as count
            FROM filings
            WHERE filing_date >= %s AND filing_date < %s
            GROUP BY filing_type
        """
        params = (f"{year}-01-01", f"{year+1}-01-01")
    
    cur.execute(query, params)
    
    total = 0
    for row in cur.fetchall():
        filing_type = row[0]
        count = row[1]
        counts[filing_type] = count
        total += count
    
    counts[None] = total  # Total count
    
    cur.close()
    return counts


def is_year_complete(conn: psycopg2.extensions.connection, year: int) -> bool:
    """
    Check if a year is complete (all filing types match SEC index counts)
    
    Args:
        conn: PostgreSQL connection
        year: Year to check
    
    Returns:
        True if year is complete, False otherwise
    """
    cur = conn.cursor()
    
    # Check if all entries for this year are complete
    cur.execute("""
        SELECT COUNT(*) as total,
               SUM(CASE WHEN is_complete THEN 1 ELSE 0 END) as complete_count
        FROM year_completion_ledger
        WHERE year = %s
    """, (year,))
    
    row = cur.fetchone()
    cur.close()
    
    if row and row[0] > 0:
        return row[1] == row[0]  # All entries are complete
    return False  # No entries found, not complete


def get_incomplete_years(conn: psycopg2.extensions.connection) -> List[int]:
    """
    Get list of years that are not complete
    
    Args:
        conn: PostgreSQL connection
    
    Returns:
        List of incomplete years (sorted in reverse chronological order - most recent first)
    """
    cur = conn.cursor()
    
    cur.execute("""
        SELECT DISTINCT year
        FROM year_completion_ledger
        WHERE is_complete = FALSE
        ORDER BY year DESC
    """)
    
    years = [row[0] for row in cur.fetchall()]
    cur.close()
    
    return years


def update_filing_downloaded_path(conn: psycopg2.extensions.connection, 
                                  cik: str, accession_number: str, 
                                  downloaded_file_path: str) -> None:
    """
    Update the downloaded_file_path for a filing in PostgreSQL
    
    Args:
        conn: PostgreSQL connection
        cik: Company CIK
        accession_number: Filing accession number
        downloaded_file_path: Path to the downloaded file
    """
    cur = conn.cursor()
    
    query = """
        UPDATE filings
        SET downloaded_file_path = %s
        WHERE cik = %s AND accession_number = %s
    """
    
    cur.execute(query, (downloaded_file_path, cik, accession_number))
    conn.commit()
    cur.close()

