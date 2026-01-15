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
import pandas as pd


def get_postgres_connection(dbname: str = "postgres", user: Optional[str] = None, 
                            host: Optional[str] = None, password: Optional[str] = None,
                            port: Optional[int] = None) -> psycopg2.extensions.connection:
    """
    Get PostgreSQL connection
    
    Args:
        dbname: Database name (default: 'edgar')
        user: Database user (optional, can use POSTGRES_USER env var, default: 'tradingAgent')
        host: Database host (optional, can use POSTGRES_HOST env var, default: 'localhost')
        password: Database password (optional, can use POSTGRES_PASSWORD env var)
        port: Database port (optional, can use POSTGRES_PORT env var, default: 5432)
    
    Returns:
        PostgreSQL connection
    """
    # Use env vars with fallbacks
    user = user or os.getenv('POSTGRES_USER', 'tradingAgent')
    host = host or os.getenv('POSTGRES_HOST', 'localhost')
    password = password or os.getenv('POSTGRES_PASSWORD', '')
    port = port if port is not None else int(os.getenv('POSTGRES_PORT', '5432'))
    
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
    
    # Create edgar schema if it doesn't exist
    cur.execute("CREATE SCHEMA IF NOT EXISTS edgar;")
    cur.execute("SET search_path TO edgar, public;")
    conn.commit()
    
    # Helper function to check if table exists
    def table_exists(table_name: str) -> bool:
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'edgar' 
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
    
    # Create master_idx_files table for storing parsed master.idx file data
    master_idx_existed = table_exists('master_idx_files')
    cur.execute("""
        CREATE TABLE IF NOT EXISTS master_idx_files (
            year INTEGER NOT NULL,
            quarter VARCHAR(10) NOT NULL,
            cik VARCHAR(10) NOT NULL,
            company_name TEXT NOT NULL,
            form_type VARCHAR(50) NOT NULL,
            date_filed DATE NOT NULL,
            filename TEXT NOT NULL,
            accession_number VARCHAR(50),
            downloaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (year, quarter, cik, form_type, date_filed, filename)
        )
    """)
    if not master_idx_existed:
        print("  ✓ Created master_idx_files table")
    
    # Create indexes for faster lookups
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_master_idx_files_year_quarter 
        ON master_idx_files(year, quarter)
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_master_idx_files_cik 
        ON master_idx_files(cik)
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_master_idx_files_form_type 
        ON master_idx_files(form_type)
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_master_idx_files_date_filed 
        ON master_idx_files(date_filed)
    """)
    
    # Create master_idx_download_ledger table for tracking download status
    ledger_existed = table_exists('master_idx_download_ledger')
    cur.execute("""
        CREATE TABLE IF NOT EXISTS master_idx_download_ledger (
            year INTEGER NOT NULL,
            quarter VARCHAR(10) NOT NULL,
            status VARCHAR(20) NOT NULL DEFAULT 'pending',
            downloaded_at TIMESTAMP,
            failed_at TIMESTAMP,
            error_message TEXT,
            retry_count INTEGER DEFAULT 0,
            last_attempt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (year, quarter),
            CHECK (status IN ('pending', 'success', 'failed'))
        )
    """)
    if not ledger_existed:
        print("  ✓ Created master_idx_download_ledger table")
    
    # Create index for faster lookups
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_master_idx_ledger_status 
        ON master_idx_download_ledger(status)
    """)
    
    # Verify all tables were created before committing
    cur.execute("""
        SELECT table_name 
        FROM information_schema.tables 
                WHERE table_schema = 'edgar'
        AND table_name IN ('companies', 'filings', 'company_history', 'metadata', 'year_completion_ledger', 'master_idx_files', 'master_idx_download_ledger')
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


def get_master_idx_download_status(conn: psycopg2.extensions.connection, year: int, quarter: str) -> Optional[Dict]:
    """
    Get download status for a specific year/quarter from the ledger
    
    Args:
        conn: PostgreSQL connection
        year: Year (e.g., 2024)
        quarter: Quarter (e.g., 'QTR1')
        
    Returns:
        Dictionary with status info or None if not found
    """
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            SELECT year, quarter, status, downloaded_at, failed_at, error_message, retry_count, last_attempt
            FROM master_idx_download_ledger
            WHERE year = %s AND quarter = %s
        """, (year, quarter))
        result = cur.fetchone()
        if result:
            return dict(result)
        return None
    finally:
        cur.close()


def mark_master_idx_download_success(conn: psycopg2.extensions.connection, year: int, quarter: str) -> None:
    """
    Mark a year/quarter download as successful in the ledger
    
    Args:
        conn: PostgreSQL connection
        year: Year (e.g., 2024)
        quarter: Quarter (e.g., 'QTR1')
    """
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO master_idx_download_ledger (year, quarter, status, downloaded_at, last_attempt)
            VALUES (%s, %s, 'success', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ON CONFLICT (year, quarter) 
            DO UPDATE SET 
                status = 'success',
                downloaded_at = CURRENT_TIMESTAMP,
                last_attempt = CURRENT_TIMESTAMP,
                retry_count = 0,
                error_message = NULL
        """, (year, quarter))
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()


def mark_master_idx_download_failed(conn: psycopg2.extensions.connection, year: int, quarter: str, 
                                     error_message: str) -> None:
    """
    Mark a year/quarter download as failed in the ledger
    
    Args:
        conn: PostgreSQL connection
        year: Year (e.g., 2024)
        quarter: Quarter (e.g., 'QTR1')
        error_message: Error message describing the failure
    """
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO master_idx_download_ledger (year, quarter, status, failed_at, error_message, retry_count, last_attempt)
            VALUES (%s, %s, 'failed', CURRENT_TIMESTAMP, %s, 1, CURRENT_TIMESTAMP)
            ON CONFLICT (year, quarter) 
            DO UPDATE SET 
                status = 'failed',
                failed_at = CURRENT_TIMESTAMP,
                error_message = %s,
                retry_count = master_idx_download_ledger.retry_count + 1,
                last_attempt = CURRENT_TIMESTAMP
        """, (year, quarter, error_message, error_message))
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()


def get_quarters_with_data(conn: psycopg2.extensions.connection, start_year: Optional[int] = None) -> List[tuple]:
    """
    Get list of quarters that already have data in master_idx_files table
    
    Args:
        conn: PostgreSQL connection
        start_year: Optional start year filter
        
    Returns:
        List of (year, quarter) tuples that have data
    """
    cur = conn.cursor()
    try:
        # Set search path to edgar schema
        cur.execute("SET search_path TO edgar, public;")
        query = """
            SELECT DISTINCT year, quarter 
            FROM master_idx_files
        """
        params = []
        if start_year:
            query += " WHERE year >= %s"
            params.append(start_year)
        query += " ORDER BY year, quarter"
        
        cur.execute(query, params)
        return [(row[0], row[1]) for row in cur.fetchall()]
    finally:
        cur.close()


def get_pending_or_failed_quarters(conn: psycopg2.extensions.connection, 
                                   start_year: Optional[int] = None) -> List[tuple]:
    """
    Get list of quarters that are pending or failed (need to be downloaded)
    
    Args:
        conn: PostgreSQL connection
        start_year: Start year to check from (default: 1993)
        
    Returns:
        List of (year, quarter) tuples that need to be downloaded
    """
    start_year = start_year or 1993
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT year, quarter
            FROM master_idx_download_ledger
            WHERE status IN ('pending', 'failed')
            AND year >= %s
            ORDER BY year, quarter
        """, (start_year,))
        return cur.fetchall()
    finally:
        cur.close()

