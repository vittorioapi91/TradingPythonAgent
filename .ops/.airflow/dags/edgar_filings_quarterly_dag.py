"""
Airflow DAG for downloading SEC EDGAR filings by quarter

This DAG automatically downloads filings for the most recent completed quarter.
It runs monthly and downloads filings from the previous quarter.

Example: If run in January, it downloads Q4 filings from the previous year.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import os
from pathlib import Path


# Default arguments
default_args = {
    'owner': 'trading_agent',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': days_ago(1),
}

# Get environment
AIRFLOW_ENV = os.getenv('AIRFLOW_ENV', 'dev')
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
OUTPUT_DIR = PROJECT_ROOT / 'src' / 'trading_agent' / 'fundamentals' / 'edgar' / 'filings'
PYTHON_PATH = f"{PROJECT_ROOT}/src:{os.getenv('PYTHONPATH', '')}"

ENV_CONFIG = {
    'dev': {'dbname': 'edgar', 'dbuser': 'tradingAgent', 'schedule': '0 2 1 * *'},  # 2 AM on 1st of month
    'staging': {'dbname': 'edgar', 'dbuser': 'tradingAgent', 'schedule': '0 2 1 * *'},
    'prod': {'dbname': 'edgar', 'dbuser': 'tradingAgent', 'schedule': '0 2 1 * *'},
}

env_config = ENV_CONFIG.get(AIRFLOW_ENV, ENV_CONFIG['dev'])


def get_previous_quarter(context):
    """
    Calculate the previous quarter based on execution date.
    
    Returns:
        tuple: (year, quarter) where quarter is QTR1, QTR2, QTR3, or QTR4
    """
    execution_date = context['execution_date']
    if isinstance(execution_date, str):
        execution_date = datetime.fromisoformat(execution_date.replace('Z', '+00:00'))
    
    # Get previous month (subtract 1 month)
    if execution_date.month == 1:
        prev_month = 12
        year = execution_date.year - 1
    else:
        prev_month = execution_date.month - 1
        year = execution_date.year
    
    # Determine quarter
    if prev_month in [1, 2, 3]:
        quarter = 'QTR1'
    elif prev_month in [4, 5, 6]:
        quarter = 'QTR2'
    elif prev_month in [7, 8, 9]:
        quarter = 'QTR3'
    else:
        quarter = 'QTR4'
    
    return year, quarter


def download_quarterly_filings(**context):
    """Download filings for the previous quarter."""
    year, quarter = get_previous_quarter(context)
    
    print(f"Downloading filings for {year} {quarter}")
    
    # Build command
    cmd_parts = [
        'python', '-m', 'trading_agent.fundamentals.edgar.edgar',
        '--filings',
        '--output-dir', str(OUTPUT_DIR),
        '--dbname', env_config['dbname'],
        '--dbuser', env_config['dbuser'],
        '--year', str(year),
        '--quarter', quarter,
        '--form-type', '10-K',  # Can be parameterized
    ]
    
    if AIRFLOW_ENV != 'prod':
        cmd_parts.extend(['--limit', '1000'])  # Limit in non-prod
    
    cmd = ' '.join(cmd_parts)
    
    print(f"Executing: {cmd}")
    
    import subprocess
    result = subprocess.run(
        cmd,
        shell=True,
        cwd=str(PROJECT_ROOT),
        env={**os.environ, 'PYTHONPATH': PYTHON_PATH},
        capture_output=True,
        text=True
    )
    
    if result.returncode != 0:
        print(f"Error: {result.stderr}")
        raise Exception(f"Command failed: {result.stderr}")
    
    print(f"Success: {result.stdout}")
    return result.stdout


dag = DAG(
    'edgar_filings_quarterly',
    default_args=default_args,
    description='Download EDGAR filings for previous quarter',
    schedule_interval=env_config['schedule'],
    catchup=False,
    tags=['edgar', 'filings', 'quarterly', AIRFLOW_ENV],
    max_active_runs=1,
)

download_quarterly = PythonOperator(
    task_id='download_quarterly_filings',
    python_callable=download_quarterly_filings,
    dag=dag,
    provide_context=True,
)

download_quarterly
