"""
FRED PostgreSQL Database Management

This module handles all PostgreSQL operations for FRED data storage.
"""

import os
from typing import Dict, Optional, List
from datetime import datetime
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values, RealDictCursor
from psycopg2 import sql


def get_postgres_connection(dbname: str = "fred", user: Optional[str] = None, 
                            host: Optional[str] = None, password: Optional[str] = None,
                            port: Optional[int] = None) -> psycopg2.extensions.connection:
    """
    Get PostgreSQL connection
    
    Args:
        dbname: Database name (default: 'fred')
        user: Database user (optional, can use POSTGRES_USER env var, default: 'tradingAgent')
        host: Database host (optional, can use POSTGRES_HOST env var, default: 'localhost')
        password: Database password (optional, can use POSTGRES_PASSWORD env var)
        port: Database port (optional, can use POSTGRES_PORT env var, default: 55432 for Docker Compose)
    
    Returns:
        PostgreSQL connection
    """
    # Use env vars with fallbacks
    user = user or os.getenv('POSTGRES_USER', 'tradingAgent')
    host = host or os.getenv('POSTGRES_HOST', 'localhost')
    password = password or os.getenv('POSTGRES_PASSWORD', '')
    port = port if port is not None else int(os.getenv('POSTGRES_PORT', '55432'))
    
    return psycopg2.connect(
        dbname=dbname,
        user=user,
        host=host,
        password=password,
        port=port
    )


def init_fred_postgres_tables(conn: psycopg2.extensions.connection) -> None:
    """
    Initialize PostgreSQL tables for FRED data if they don't exist
    
    Args:
        conn: PostgreSQL connection
    """
    cur = conn.cursor()
    
    # Create series table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS series (
            series_id VARCHAR(255) PRIMARY KEY,
            title TEXT,
            description TEXT,
            frequency VARCHAR(50),
            units VARCHAR(100),
            category_id VARCHAR(50),
            category_name VARCHAR(255),
            observation_start VARCHAR(50),
            observation_end VARCHAR(50),
            country VARCHAR(100),
            last_updated VARCHAR(50),
            popularity VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Create index on category_id for faster queries
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_series_category_id 
        ON series(category_id)
    """)
    
    # Create categories table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS categories (
            category_id VARCHAR(50) PRIMARY KEY,
            name TEXT,
            parent_id BIGINT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Create index on parent_id for tree traversal
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_categories_parent_id 
        ON categories(parent_id)
    """)
    
    # Create metadata table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS metadata (
            key VARCHAR(255) PRIMARY KEY,
            value TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Create time_series table for storing actual time series data
    cur.execute("""
        CREATE TABLE IF NOT EXISTS time_series (
            series_id VARCHAR(255) NOT NULL,
            date DATE NOT NULL,
            value DOUBLE PRECISION,
            PRIMARY KEY (series_id, date),
            FOREIGN KEY (series_id) REFERENCES series(series_id) ON DELETE CASCADE
        )
    """)
    
    # Create index on date for faster queries
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_time_series_date 
        ON time_series(date)
    """)
    
    # Create index on series_id for faster lookups
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_time_series_series_id 
        ON time_series(series_id)
    """)
    
    # Initialize metadata if empty
    cur.execute("SELECT COUNT(*) FROM metadata")
    if cur.fetchone()[0] == 0:
        now = datetime.now()
        initial_metadata = [
            ('generated_at', now.isoformat(), now),
            ('total_series', '0', now),
            ('status', 'in_progress', now),
            ('source', 'FRED API', now),
        ]
        execute_values(
            cur,
            "INSERT INTO metadata (key, value, updated_at) VALUES %s",
            initial_metadata
        )
    
    conn.commit()
    cur.close()


def add_fred_series_fast(conn: psycopg2.extensions.connection, new_series: List[Dict]) -> int:
    """
    Add FRED series to PostgreSQL database (upsert - insert or update if exists)
    
    Args:
        conn: PostgreSQL connection
        new_series: List of series dictionaries to add
    
    Returns:
        Number of series successfully added/updated
    """
    if not new_series:
        return 0
    
    cur = conn.cursor()
    
    # Prepare data for bulk insert
    columns = ['series_id', 'title', 'description', 'frequency', 'units', 'category_id',
               'category_name', 'observation_start', 'observation_end', 'country',
               'last_updated', 'popularity', 'created_at', 'updated_at']
    
    # Ensure all required fields exist
    for series in new_series:
        for field in columns:
            if field not in series:
                if field in ['created_at', 'updated_at']:
                    series[field] = datetime.now()
                else:
                    series[field] = ''
    
    # Prepare values for upsert
    values = []
    for series in new_series:
        row = tuple(
            str(series.get(col, '')) if col not in ['created_at', 'updated_at'] 
            else (datetime.fromisoformat(series[col]) if isinstance(series.get(col), str) 
                  else series.get(col, datetime.now()))
            for col in columns
        )
        values.append(row)
    
    # Use INSERT ... ON CONFLICT to upsert
    insert_query = sql.SQL("""
        INSERT INTO series ({columns})
        VALUES %s
        ON CONFLICT (series_id) 
        DO UPDATE SET
            title = EXCLUDED.title,
            description = EXCLUDED.description,
            frequency = EXCLUDED.frequency,
            units = EXCLUDED.units,
            category_id = EXCLUDED.category_id,
            category_name = EXCLUDED.category_name,
            observation_start = EXCLUDED.observation_start,
            observation_end = EXCLUDED.observation_end,
            country = EXCLUDED.country,
            last_updated = EXCLUDED.last_updated,
            popularity = EXCLUDED.popularity,
            updated_at = EXCLUDED.updated_at
    """).format(
        columns=sql.SQL(', ').join(map(sql.Identifier, columns))
    )
    
    execute_values(cur, insert_query, values)
    conn.commit()
    
    added_count = cur.rowcount
    cur.close()
    
    return added_count


def load_fred_series_from_postgres(conn: psycopg2.extensions.connection,
                                   category_ids: Optional[List[int]] = None) -> List[Dict]:
    """
    Load FRED series from PostgreSQL database
    
    Args:
        conn: PostgreSQL connection
        category_ids: Optional list of category IDs to filter by
    
    Returns:
        List of series dictionaries
    """
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    if category_ids:
        # Convert category_ids to strings since category_id column is VARCHAR
        category_ids_str = [str(cid) for cid in category_ids]
        placeholders = ','.join(['%s'] * len(category_ids_str))
        query = f"""
            SELECT * FROM series 
            WHERE category_id IN ({placeholders})
        """
        cur.execute(query, category_ids_str)
    else:
        cur.execute("SELECT * FROM series")
    
    results = cur.fetchall()
    cur.close()
    
    # Convert to list of dictionaries, converting timestamps to strings
    series_list = []
    for row in results:
        series_dict = dict(row)
        # Convert timestamps to ISO format strings
        for key in ['created_at', 'updated_at']:
            if series_dict.get(key) and isinstance(series_dict[key], datetime):
                series_dict[key] = series_dict[key].isoformat()
        series_list.append(series_dict)
    
    return series_list


def add_fred_categories_fast(conn: psycopg2.extensions.connection, 
                             new_categories: List[Dict]) -> int:
    """
    Add FRED categories to PostgreSQL database (upsert)
    
    Args:
        conn: PostgreSQL connection
        new_categories: List of category dictionaries with category_id, name, parent_id
    
    Returns:
        Number of categories successfully added/updated
    """
    if not new_categories:
        return 0
    
    cur = conn.cursor()
    
    # Ensure all required fields exist
    for category in new_categories:
        for field in ['category_id', 'name', 'parent_id', 'created_at', 'updated_at']:
            if field not in category:
                if field in ['created_at', 'updated_at']:
                    category[field] = datetime.now()
                elif field == 'parent_id':
                    category[field] = 0  # Default to 0 for root categories
                else:
                    category[field] = ''
    
    columns = ['category_id', 'name', 'parent_id', 'created_at', 'updated_at']
    values = []
    for category in new_categories:
        row = []
        for col in columns:
            if col in ['created_at', 'updated_at']:
                # Handle timestamp fields
                val = category.get(col)
                if isinstance(val, str):
                    row.append(datetime.fromisoformat(val))
                else:
                    row.append(val if val is not None else datetime.now())
            elif col == 'parent_id':
                # Handle parent_id as bigint (integer)
                val = category.get(col)
                if val is None or val == '':
                    row.append(0)  # Root category
                else:
                    row.append(int(val))
            else:
                # Handle string fields
                row.append(str(category.get(col, '')))
        values.append(tuple(row))
    
    insert_query = sql.SQL("""
        INSERT INTO categories ({columns})
        VALUES %s
        ON CONFLICT (category_id)
        DO UPDATE SET
            name = EXCLUDED.name,
            parent_id = EXCLUDED.parent_id,
            updated_at = EXCLUDED.updated_at
    """).format(
        columns=sql.SQL(', ').join(map(sql.Identifier, columns))
    )
    
    execute_values(cur, insert_query, values)
    conn.commit()
    
    added_count = cur.rowcount
    cur.close()
    
    return added_count


def load_fred_categories_from_postgres(conn: psycopg2.extensions.connection) -> List[Dict]:
    """
    Load FRED categories from PostgreSQL database
    
    Args:
        conn: PostgreSQL connection
    
    Returns:
        List of category dictionaries
    """
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT * FROM categories")
    results = cur.fetchall()
    cur.close()
    
    # Convert to list of dictionaries, converting timestamps to strings
    categories_list = []
    for row in results:
        category_dict = dict(row)
        for key in ['created_at', 'updated_at']:
            if category_dict.get(key) and isinstance(category_dict[key], datetime):
                category_dict[key] = category_dict[key].isoformat()
        categories_list.append(category_dict)
    
    return categories_list


def update_fred_metadata(conn: psycopg2.extensions.connection, key: str, value: str) -> None:
    """
    Update metadata value in PostgreSQL database
    
    Args:
        conn: PostgreSQL connection
        key: Metadata key
        value: Metadata value
    """
    cur = conn.cursor()
    
    cur.execute("""
        INSERT INTO metadata (key, value, updated_at)
        VALUES (%s, %s, %s)
        ON CONFLICT (key)
        DO UPDATE SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at
    """, (key, value, datetime.now()))
    
    conn.commit()
    cur.close()


def get_fred_metadata(conn: psycopg2.extensions.connection, key: Optional[str] = None) -> Dict[str, str]:
    """
    Get metadata from PostgreSQL database
    
    Args:
        conn: PostgreSQL connection
        key: Optional specific key to retrieve (if None, returns all metadata)
    
    Returns:
        Dictionary of metadata key-value pairs (or single value if key specified)
    """
    cur = conn.cursor()
    
    if key:
        cur.execute("SELECT value FROM metadata WHERE key = %s", (key,))
        result = cur.fetchone()
        cur.close()
        return {key: result[0]} if result else {}
    else:
        cur.execute("SELECT key, value FROM metadata")
        results = cur.fetchall()
        cur.close()
        return dict(results)


def add_time_series_fast(conn, time_series_list: List[Dict]) -> int:
    """
    Fast batch insert of time series data
    
    Args:
        conn: PostgreSQL connection
        time_series_list: List of dicts with keys: series_id, date, value
        
    Returns:
        Number of records inserted (excluding duplicates)
    """
    if not time_series_list:
        return 0
    
    cur = conn.cursor()
    
    try:
        # Prepare data for batch insert
        columns = ['series_id', 'date', 'value']
        values = []
        for ts in time_series_list:
            row = (
                str(ts.get('series_id', '')),
                ts.get('date'),  # Should be date string or date object
                ts.get('value') if ts.get('value') is not None else None
            )
            values.append(row)
        
        # Use INSERT ... ON CONFLICT to upsert (update if exists)
        insert_query = sql.SQL("""
            INSERT INTO time_series ({columns})
            VALUES %s
            ON CONFLICT (series_id, date) 
            DO UPDATE SET
                value = EXCLUDED.value
        """).format(
            columns=sql.SQL(', ').join(map(sql.Identifier, columns))
        )
        
        execute_values(cur, insert_query, values)
        conn.commit()
        
        added = len(values)
        cur.close()
        return added
        
    except Exception as e:
        conn.rollback()
        cur.close()
        raise e


def load_time_series_from_postgres(conn, series_id: Optional[str] = None, 
                                   start_date: Optional[str] = None,
                                   end_date: Optional[str] = None) -> List[Dict]:
    """
    Load time series data from PostgreSQL
    
    Args:
        conn: PostgreSQL connection
        series_id: Optional series ID to filter by
        start_date: Optional start date (YYYY-MM-DD format)
        end_date: Optional end date (YYYY-MM-DD format)
        
    Returns:
        List of dicts with keys: series_id, date, value
    """
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    query = "SELECT series_id, date, value, created_at FROM time_series WHERE 1=1"
    params = []
    
    if series_id:
        query += " AND series_id = %s"
        params.append(str(series_id))
    
    if start_date:
        query += " AND date >= %s"
        params.append(start_date)
    
    if end_date:
        query += " AND date <= %s"
        params.append(end_date)
    
    query += " ORDER BY series_id, date"
    
    cur.execute(query, params)
    results = cur.fetchall()
    cur.close()
    
    return [dict(row) for row in results]

