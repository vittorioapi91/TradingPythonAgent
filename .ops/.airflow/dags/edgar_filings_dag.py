"""
Airflow DAG for downloading SEC EDGAR filings

This DAG downloads filings from the PostgreSQL database (master_idx table)
for all environments (dev/test/prod). The environment is determined by the
AIRFLOW_ENV environment variable set in the Airflow container.

The DAG supports configurable filters:
- year: Filter by year (e.g., 2005)
- quarter: Filter by quarter (QTR1, QTR2, QTR3, QTR4)
- form_type: Filter by form type (e.g., 10-K, 10-Q)
- company_name: Filter by company name (partial match)
- cik: Filter by CIK
- limit: Limit number of filings to download
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import os
from pathlib import Path


# Default arguments for the DAG
default_args = {
    'owner': 'trading_agent',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': days_ago(1),
}

# Get environment from Airflow environment variable (set in container)
AIRFLOW_ENV = os.getenv('AIRFLOW_ENV', 'dev')
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent

# Environment-specific configuration
ENV_CONFIG = {
    'dev': {
        'dbname': 'edgar',
        'dbuser': 'tradingAgent',
        'schedule': '@daily',  # Run daily in dev
        'default_limit': 100,  # Limit downloads in dev
    },
    'staging': {
        'dbname': 'edgar',
        'dbuser': 'tradingAgent',
        'schedule': '@daily',  # Run daily in staging
        'default_limit': 500,  # Limit downloads in staging
    },
    'prod': {
        'dbname': 'edgar',
        'dbuser': 'tradingAgent',
        'schedule': '@daily',  # Run daily in prod
        'default_limit': None,  # No limit in prod
    }
}

# Get current environment config
env_config = ENV_CONFIG.get(AIRFLOW_ENV, ENV_CONFIG['dev'])

# Output directory for filings (environment-specific)
OUTPUT_DIR = PROJECT_ROOT / 'src' / 'trading_agent' / 'fundamentals' / 'edgar' / 'filings'

# Python path - ensure we can import trading_agent
existing_pythonpath = os.getenv('PYTHONPATH', '')
PYTHON_PATH = f"{PROJECT_ROOT}/src" + (f":{existing_pythonpath}" if existing_pythonpath else "")


def get_filings_command(
    year: int = None,
    quarter: str = None,
    form_type: str = None,
    company_name: str = None,
    cik: str = None,
    limit: int = None,
    output_dir: str = None
) -> str:
    """
    Build the command to download filings.
    
    Args:
        year: Year filter (e.g., 2005)
        quarter: Quarter filter (QTR1, QTR2, QTR3, QTR4)
        form_type: Form type filter (e.g., 10-K, 10-Q)
        company_name: Company name filter
        cik: CIK filter
        limit: Limit number of filings
        output_dir: Output directory (defaults to env-specific)
    
    Returns:
        Command string to execute
    """
    if output_dir is None:
        output_dir = str(OUTPUT_DIR)
    
    # Base command
    cmd_parts = [
        'python',
        '-m',
        'trading_agent.fundamentals.edgar.edgar',
        '--filings',
        '--output-dir',
        output_dir,
        '--dbname',
        env_config['dbname'],
        '--dbuser',
        env_config['dbuser'],
    ]
    
    # Add filters
    if year is not None:
        cmd_parts.extend(['--year', str(year)])
    if quarter is not None:
        cmd_parts.extend(['--quarter', quarter])
    if form_type is not None:
        cmd_parts.extend(['--form-type', form_type])
    if company_name is not None:
        cmd_parts.extend(['--company-name', company_name])
    if cik is not None:
        cmd_parts.extend(['--cik', str(cik)])
    if limit is not None:
        cmd_parts.extend(['--limit', str(limit)])
    elif env_config['default_limit'] is not None:
        # Use environment default limit if no limit specified
        cmd_parts.extend(['--limit', str(env_config['default_limit'])])
    
    return ' '.join(cmd_parts)


# Create the DAG
dag = DAG(
    'edgar_filings_download',
    default_args=default_args,
    description='Download SEC EDGAR filings from database',
    schedule_interval=env_config['schedule'],
    catchup=False,
    tags=['edgar', 'filings', 'fundamentals', AIRFLOW_ENV],
    max_active_runs=1,
    params={
        'year': None,
        'quarter': None,
        'form_type': '10-K',  # Default to 10-K filings
        'company_name': None,
        'cik': None,
        'limit': env_config['default_limit'],
    }
)


def download_filings_task(**context):
    """
    Python task to download filings with parameters from DAG run.
    """
    params = context.get('params', {})
    
    # Get parameters (can be overridden via DAG run config)
    dag_run = context.get('dag_run')
    if dag_run and dag_run.conf:
        year = dag_run.conf.get('year', params.get('year'))
        quarter = dag_run.conf.get('quarter', params.get('quarter'))
        form_type = dag_run.conf.get('form_type', params.get('form_type'))
        company_name = dag_run.conf.get('company_name', params.get('company_name'))
        cik = dag_run.conf.get('cik', params.get('cik'))
        limit = dag_run.conf.get('limit', params.get('limit'))
    else:
        year = params.get('year')
        quarter = params.get('quarter')
        form_type = params.get('form_type')
        company_name = params.get('company_name')
        cik = params.get('cik')
        limit = params.get('limit')
    
    # Build and execute command
    cmd = get_filings_command(
        year=year,
        quarter=quarter,
        form_type=form_type,
        company_name=company_name,
        cik=cik,
        limit=limit
    )
    
    print(f"Executing command: {cmd}")
    print(f"Environment: {AIRFLOW_ENV}")
    print(f"Output directory: {OUTPUT_DIR}")
    
    # Execute the command
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
        print(f"Error output: {result.stderr}")
        raise Exception(f"Command failed with return code {result.returncode}: {result.stderr}")
    
    print(f"Success output: {result.stdout}")
    return result.stdout


# Task to download filings
download_filings = PythonOperator(
    task_id='download_filings',
    python_callable=download_filings_task,
    dag=dag,
    provide_context=True,
)


# Alternative: BashOperator version (uncomment to use instead of PythonOperator)
# download_filings_bash = BashOperator(
#     task_id='download_filings',
#     bash_command=f"""
#     cd {PROJECT_ROOT} && \
#     export PYTHONPATH="{PYTHONPATH}" && \
#     python -m trading_agent.fundamentals.edgar.edgar \
#         --filings \
#         --output-dir {OUTPUT_DIR} \
#         --dbname {env_config['dbname']} \
#         --dbuser {env_config['dbuser']} \
#         --form-type "{{{{ params.form_type }}}}" \
#         --limit {env_config['default_limit'] or ''}
#     """,
#     dag=dag,
# )


# Set task dependencies (if we add more tasks later)
download_filings
