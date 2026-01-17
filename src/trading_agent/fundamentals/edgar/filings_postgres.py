"""
EDGAR Filings PostgreSQL Database Query Functions

This module handles PostgreSQL query building for filing downloads.
"""

from typing import List, Dict, Optional, Any
import sys
import os
import psycopg2
from psycopg2.extras import RealDictCursor


def _is_test_environment() -> bool:
    """Check if we're running in a test environment (pytest/unittest)"""
    # Check for pytest environment variable (most reliable)
    if os.environ.get('PYTEST_CURRENT_TEST'):
        return True
    # Check if pytest or unittest modules are loaded
    if 'pytest' in sys.modules or 'unittest' in sys.modules:
        return True
    # Check if we're being called from a test file by inspecting the call stack
    try:
        import inspect
        stack = inspect.stack()
        for frame_info in stack:
            filename = frame_info.filename
            if 'test_' in filename or '/tests/' in filename or '\\tests\\' in filename:
                return True
    except Exception:
        pass
    return False


def build_filings_query(**filters) -> tuple[str, List[Any]]:
    """
    Build a SQL query for fetching filings from master_idx_files table based on flexible filter criteria
    
    Args:
        **filters: Flexible filter criteria. Supported filters:
            - year: Year (e.g., 2005)
            - quarter: Quarter (e.g., 'QTR1', 'QTR2', 'QTR3', 'QTR4')
            - form_type: Form type (e.g., '10-K', '10-Q')
            - cik: CIK (Central Index Key) as string or int
            - filename: Exact filename (e.g., 'edgar/data/315293/0001179110-05-003398.txt')
            - date_filed: Filing date (DATE format: 'YYYY-MM-DD' or date object)
            - company_name: Company name (partial match with LIKE)
            
    Returns:
        Tuple of (query_string, params_list)
        
    Raises:
        ValueError: If no filters provided or invalid filter name/value
    """
    # Build dynamic WHERE clause based on provided filters
    where_conditions = []
    params = []
    
    # Map of filter names to column names and conversion functions
    filter_mappings = {
        'year': ('year', int, '='),
        'quarter': ('quarter', str, '='),
        'form_type': ('form_type', str, '='),
        'cik': ('cik', lambda x: str(x).zfill(10) if x else None, '='),  # Normalize CIK to 10 digits
        'filename': ('filename', str, '='),
        'date_filed': ('date_filed', str, '='),
        'company_name': ('company_name', str, 'ILIKE'),  # Case-insensitive partial match
    }
    
    for filter_name, filter_value in filters.items():
        if filter_value is None:
            continue
        
        if filter_name not in filter_mappings:
            raise ValueError(f"Unknown filter: {filter_name}. Supported filters: {', '.join(filter_mappings.keys())}")
        
        column_name, converter, operator = filter_mappings[filter_name]
        
        # Convert value
        try:
            converted_value = converter(filter_value)
            if converted_value is None:
                continue
        except (ValueError, TypeError) as e:
            raise ValueError(f"Invalid value for filter '{filter_name}': {filter_value}. Error: {e}")
        
        # Handle special case for company_name (ILIKE for partial match)
        if operator == 'ILIKE':
            where_conditions.append(f"{column_name} ILIKE %s")
            params.append(f"%{converted_value}%")
        else:
            where_conditions.append(f"{column_name} = %s")
            params.append(converted_value)
    
    # Build the query
    if not where_conditions:
        raise ValueError("At least one filter must be provided. Supported filters: year, quarter, form_type, cik, filename, date_filed, company_name")
    
    query = """
        SELECT DISTINCT filename
        FROM master_idx_files
        WHERE """ + " AND ".join(where_conditions) + """
        ORDER BY filename
    """
    
    return query, params


def get_filings_filenames(
    conn: psycopg2.extensions.connection,
    limit: Optional[int] = None,
    sql_query: Optional[str] = None,
    **filters
) -> List[str]:
    """
    Get list of filenames from master_idx_files table matching the provided filters
    
    Args:
        conn: PostgreSQL connection
        limit: Optional limit on number of results (e.g., 100).
              If not provided and running in test environment, automatically uses LIMIT 100.
        sql_query: Optional raw SQL query string. If provided, filters and limit are ignored.
                   Query should return a column named 'filename' or be a SELECT * query.
                   Example: "SELECT filename FROM master_idx_files WHERE company_name LIKE '%NVIDIA%' AND year = 2019"
        **filters: Flexible filter criteria (see build_filings_query for supported filters).
                   Ignored if sql_query is provided.
        
    Returns:
        List of filename strings
        
    Raises:
        ValueError: If no filters provided (and no sql_query) or invalid filter name/value
    """
    cur = conn.cursor()
    
    try:
        # Set search path to edgar schema
        cur.execute("SET search_path TO edgar, public;")
        
        if sql_query:
            # Use raw SQL query
            # If it's SELECT *, we need to extract filename column
            # Otherwise, assume it returns filename column
            if "SELECT *" in sql_query.upper():
                # Wrap query to extract filename
                query = f"""
                    WITH base_query AS (
                        {sql_query}
                    )
                    SELECT DISTINCT filename FROM base_query ORDER BY filename
                """
                params = []
            else:
                # Use query as-is, but ensure it has ORDER BY and DISTINCT if needed
                query = sql_query
                if "ORDER BY" not in query.upper():
                    query += " ORDER BY filename"
                params = []
        else:
            # Build query from filters
            query, params = build_filings_query(**filters)
            
            # Add LIMIT if explicitly provided, or automatically add LIMIT 100 in test environment
            if limit:
                # User explicitly provided limit - always use it
                query += " LIMIT %s"
                params.append(limit)
            elif _is_test_environment():
                # In test environment, automatically add LIMIT 100 if no limit was provided
                query += " LIMIT %s"
                params.append(100)
        
        cur.execute(query, params)
        filenames = [row[0] for row in cur.fetchall()]
        
        return filenames
        
    finally:
        cur.close()
