"""
Master DAG for all Macro workflows

This DAG orchestrates all macro data workflows:
- Data downloads for all modules
- SQL workflows and analysis
"""

from airflow import DAG
from datetime import datetime, timedelta
try:
    from airflow.sdk.timezone import datetime as tz_datetime
except ImportError:
    from airflow.utils.timezone import datetime as tz_datetime
try:
    from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
except ImportError:
    from airflow.operators.trigger_dagrun import TriggerDagRunOperator

default_args = {
    'owner': 'trading_agent',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': tz_datetime(2024, 1, 1),
}

dag = DAG(
    'macro_master_workflow',
    default_args=default_args,
    description='Master workflow for all macro data operations',
    schedule='@weekly',  # Run weekly
    catchup=False,
    tags=['macro', 'master', 'orchestration'],
)

# Trigger data downloads DAG
trigger_data_downloads = TriggerDagRunOperator(
    task_id='trigger_data_downloads',
    trigger_dag_id='macro_data_downloads',
    wait_for_completion=True,
    dag=dag,
)

# Trigger SQL workflows DAG (for all modules)
trigger_sql_workflows = TriggerDagRunOperator(
    task_id='trigger_sql_workflows',
    trigger_dag_id='macro_sql_workflows',
    wait_for_completion=True,
    dag=dag,
)

# Define workflow: downloads first, then SQL analysis
trigger_data_downloads >> trigger_sql_workflows

