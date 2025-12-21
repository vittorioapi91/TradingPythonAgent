"""
Airflow DAG for Macro data downloads

This DAG orchestrates data downloads for all macro modules:
- FRED
- BIS
- BLS
- Eurostat
- IMF
"""

from airflow import DAG
from datetime import datetime, timedelta
try:
    from airflow.sdk.timezone import datetime as tz_datetime
except ImportError:
    from airflow.utils.timezone import datetime as tz_datetime
import os
import sys

# Custom operators live in the dedicated 'operators' package mounted into the container
from operators import DataDownloadOperator

default_args = {
    'owner': 'trading_agent',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
    'start_date': tz_datetime(2024, 1, 1),
}

dag = DAG(
    'macro_data_downloads',
    default_args=default_args,
    description='Macro economic data downloads for all modules',
    schedule='@weekly',  # Run weekly to update data
    catchup=False,
    tags=['macro', 'data-download', 'fred', 'bis', 'bls', 'eurostat', 'imf'],
)

# FRED data download tasks
fred_generate_db = DataDownloadOperator(
    task_id='fred_generate_db',
    module_name='fred',
    command='--generate-db',
    script_args=['--dbuser', 'tradingAgent', '--dbhost', 'localhost'],
    dag=dag,
)

fred_download_data = DataDownloadOperator(
    task_id='fred_download_data',
    module_name='fred',
    command='--from-db',
    script_args=[
        '--series-query-file', 'rates_categories_query.sql',
        '--dbuser', 'tradingAgent',
        '--dbhost', 'localhost'
    ],
    dag=dag,
)

# BIS data download tasks
bis_generate_db = DataDownloadOperator(
    task_id='bis_generate_db',
    module_name='bis',
    command='--generate-db',
    script_args=['--dbuser', 'tradingAgent', '--dbhost', 'localhost'],
    dag=dag,
)

bis_download_data = DataDownloadOperator(
    task_id='bis_download_data',
    module_name='bis',
    command='--from-db',
    script_args=[
        '--dataflow', 'BIS_CBS',
        '--dbuser', 'tradingAgent',
        '--dbhost', 'localhost'
    ],
    dag=dag,
)

# BLS data download tasks
bls_generate_db = DataDownloadOperator(
    task_id='bls_generate_db',
    module_name='bls',
    command='--generate-db',
    script_args=['--dbuser', 'tradingAgent', '--dbhost', 'localhost'],
    dag=dag,
)

bls_download_data = DataDownloadOperator(
    task_id='bls_download_data',
    module_name='bls',
    command='--series',
    script_args=[
        'CUUR0000SA0', 'SUUR0000SA0',
        '--start-year', '2020',
        '--end-year', '2024',
        '--dbuser', 'tradingAgent',
        '--dbhost', 'localhost'
    ],
    dag=dag,
)

# Eurostat data download tasks
eurostat_generate_db = DataDownloadOperator(
    task_id='eurostat_generate_db',
    module_name='eurostat',
    command='--generate-db',
    script_args=['--dbuser', 'tradingAgent', '--dbhost', 'localhost'],
    dag=dag,
)

eurostat_download_data = DataDownloadOperator(
    task_id='eurostat_download_data',
    module_name='eurostat',
    command='--from-db',
    script_args=['--dbuser', 'tradingAgent', '--dbhost', 'localhost'],
    dag=dag,
)

# Define task dependencies
# Each module: generate_db -> download_data
fred_generate_db >> fred_download_data
bis_generate_db >> bis_download_data
bls_generate_db >> bls_download_data
eurostat_generate_db >> eurostat_download_data

