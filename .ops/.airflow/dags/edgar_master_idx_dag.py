"""
Airflow DAG for EDGAR master.idx files generation

This DAG orchestrates:
1. Downloading master.idx files from SEC EDGAR (only new/failed quarters)
2. Parsing and saving to CSV files
3. Loading parsed data into PostgreSQL database

The DAG uses a ledger table to track download status and only processes
quarters that are new or have previously failed.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
try:
    from airflow.sdk.timezone import datetime as tz_datetime
except ImportError:
    from airflow.utils.timezone import datetime as tz_datetime
import os
import sys

# Add project root to path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

default_args = {
    'owner': 'trading_agent',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=15),
    'start_date': tz_datetime(2024, 1, 1),
}

dag = DAG(
    'edgar_master_idx_generation',
    default_args=default_args,
    description='Download and process SEC EDGAR master.idx files',
    schedule='0 0 1 */3 *',  # Run quarterly (1st day of every 3rd month) to catch new quarters
    catchup=False,
    tags=['edgar', 'fundamentals', 'master-idx', 'data-download'],
)


def download_master_idx_files(**context):
    """Download master.idx files (only new/failed quarters)"""
    # The src directory is mounted at /opt/airflow/src and added to PYTHONPATH
    # So we can import directly. If that doesn't work, add the path explicitly.
    import sys
    import os
    
    # Ensure /opt/airflow/src is in path (where src is mounted in the container)
    src_path = '/opt/airflow/src'
    if os.path.exists(src_path) and src_path not in sys.path:
        sys.path.insert(0, src_path)
    # Also try adding from DAG directory (fallback)
    dag_dir = os.path.dirname(__file__)  # /opt/airflow/dags
    project_root_from_dag = os.path.abspath(os.path.join(dag_dir, '..', 'src'))
    if os.path.exists(project_root_from_dag) and project_root_from_dag not in sys.path:
        sys.path.insert(0, project_root_from_dag)
    
    # Import here to avoid import errors at DAG parse time
    # Note: /opt/airflow/src contains trading_agent/ directly (not src/trading_agent/)
    # because the mount is ../../src:/opt/airflow/src
    from trading_agent.fundamentals.edgar.edgar import EDGARDownloader
    from trading_agent.fundamentals.edgar.edgar_postgres import (
        get_postgres_connection,
        init_edgar_postgres_tables
    )
    
    # Get database connection from environment or use defaults
    dbname = os.getenv('POSTGRES_DB', 'edgar')
    dbuser = os.getenv('POSTGRES_USER', 'tradingAgent')
    dbhost = os.getenv('POSTGRES_HOST', 'localhost')
    dbpassword = os.getenv('POSTGRES_PASSWORD', '')
    dbport = int(os.getenv('POSTGRES_PORT', '5432'))
    
    # Get start year from context or use default
    start_year = context.get('dag_run').conf.get('start_year') if context.get('dag_run') else None
    
    # Initialize downloader
    user_agent = os.getenv('EDGAR_USER_AGENT', 'VittorioApicella apicellavittorio@hotmail.it')
    downloader = EDGARDownloader(user_agent=user_agent)
    
    # Connect to database
    conn = get_postgres_connection(
        dbname=dbname,
        user=dbuser,
        host=dbhost,
        password=dbpassword,
        port=dbport
    )
    
    try:
        # Initialize tables including ledger
        init_edgar_postgres_tables(conn)
        
        # Download only new/failed quarters
        downloader.save_master_idx_to_disk(conn, start_year=start_year)
    finally:
        conn.close()


def save_master_idx_to_database(**context):
    """Save parsed CSV files to PostgreSQL database"""
    # The src directory is mounted at /opt/airflow/src and added to PYTHONPATH
    # So we can import directly. If that doesn't work, add the path explicitly.
    import sys
    import os
    
    # Ensure /opt/airflow/src is in path (where src is mounted in the container)
    src_path = '/opt/airflow/src'
    if os.path.exists(src_path) and src_path not in sys.path:
        sys.path.insert(0, src_path)
    # Also try adding from DAG directory (fallback)
    dag_dir = os.path.dirname(__file__)  # /opt/airflow/dags
    project_root_from_dag = os.path.abspath(os.path.join(dag_dir, '..', 'src'))
    if os.path.exists(project_root_from_dag) and project_root_from_dag not in sys.path:
        sys.path.insert(0, project_root_from_dag)
    
    # Import here to avoid import errors at DAG parse time
    # Note: /opt/airflow/src contains trading_agent/ directly (not src/trading_agent/)
    # because the mount is ../../src:/opt/airflow/src
    from trading_agent.fundamentals.edgar.edgar import EDGARDownloader
    from trading_agent.fundamentals.edgar.edgar_postgres import get_postgres_connection
    
    # Get database connection from environment or use defaults
    dbname = os.getenv('POSTGRES_DB', 'edgar')
    dbuser = os.getenv('POSTGRES_USER', 'tradingAgent')
    dbhost = os.getenv('POSTGRES_HOST', 'localhost')
    dbpassword = os.getenv('POSTGRES_PASSWORD', '')
    dbport = int(os.getenv('POSTGRES_PORT', '5432'))
    
    # Initialize downloader
    user_agent = os.getenv('EDGAR_USER_AGENT', 'VittorioApicella apicellavittorio@hotmail.it')
    downloader = EDGARDownloader(user_agent=user_agent)
    
    # Connect to database
    conn = get_postgres_connection(
        dbname=dbname,
        user=dbuser,
        host=dbhost,
        password=dbpassword,
        port=dbport
    )
    
    try:
        # Save parsed CSV files to database
        downloader._save_master_idx_to_db(conn)
    finally:
        conn.close()


# Task 1: Download master.idx files (only new/failed)
download_task = PythonOperator(
    task_id='download_master_idx_files',
    python_callable=download_master_idx_files,
    dag=dag,
)

# Task 2: Save parsed data to database
save_to_db_task = PythonOperator(
    task_id='save_master_idx_to_database',
    python_callable=save_master_idx_to_database,
    dag=dag,
)

# Set task dependencies
download_task >> save_to_db_task
