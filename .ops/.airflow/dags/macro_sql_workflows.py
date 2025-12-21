"""
Airflow DAG for Macro SQL workflow management

This DAG manages SQL workflows for all macro modules:
- FRED: Category analysis, views, custom queries
- BIS: Data integrity validation, analysis
- BLS: Data integrity validation, analysis
- Eurostat: Data integrity validation, analysis
- IMF: Data integrity validation, analysis
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
from operators import (
    PostgresSQLFileOperator,
    PostgresSQLQueryOperator,
    PostgresViewOperator,
)

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
    'macro_sql_workflows',
    default_args=default_args,
    description='SQL workflow management for all macro modules',
    schedule='@daily',
    catchup=False,
    tags=['macro', 'sql', 'postgresql', 'data-analysis', 'fred', 'bis', 'bls', 'eurostat', 'imf'],
)

# ============================================================================
# FRED SQL Workflows
# ============================================================================

# FRED: Create category paths view
fred_create_category_paths_view = PostgresViewOperator(
    task_id='fred_create_category_paths_view',
    view_name='fred_category_paths',
    sql_file='categories_tree.sql',
    module_name='fred',
    dbname='fred',
    user='tradingAgent',
    host='localhost',
    dag=dag,
)

# FRED: Create category analysis view
fred_create_category_analysis_view = PostgresViewOperator(
    task_id='fred_create_category_analysis_view',
    view_name='category_analysis',
    sql_file='category_analysis.sql',
    module_name='fred',
    dbname='fred',
    user='tradingAgent',
    host='localhost',
    dag=dag,
)

# FRED: Analyze category statistics
fred_analyze_category_statistics = PostgresSQLQueryOperator(
    task_id='fred_analyze_category_statistics',
    sql="""
        SELECT 
            COUNT(*) FILTER (WHERE is_branch = true) AS total_branches,
            COUNT(*) FILTER (WHERE is_branch = false) AS total_leaves,
            COUNT(*) AS total_categories
        FROM category_analysis;
    """,
    dbname='fred',
    user='tradingAgent',
    host='localhost',
    return_results=True,
    dag=dag,
)

# FRED: Get series count by category
fred_get_series_count_by_category = PostgresSQLQueryOperator(
    task_id='fred_get_series_count_by_category',
    sql="""
        SELECT 
            c.category_id,
            c.name,
            COUNT(s.series_id) AS series_count
        FROM categories c
        LEFT JOIN series s ON c.category_id = s.category_id
        GROUP BY c.category_id, c.name
        ORDER BY series_count DESC
        LIMIT 20;
    """,
    dbname='fred',
    user='tradingAgent',
    host='localhost',
    return_results=True,
    dag=dag,
)

# FRED: Execute rates categories query
fred_execute_rates_query = PostgresSQLFileOperator(
    task_id='fred_execute_rates_query',
    sql_file='rates_categories_query.sql',
    module_name='fred',
    dbname='fred',
    user='tradingAgent',
    host='localhost',
    dag=dag,
)

# FRED: Validate data integrity
fred_validate_data_integrity = PostgresSQLQueryOperator(
    task_id='fred_validate_data_integrity',
    sql="""
        SELECT 
            'series' AS table_name,
            COUNT(*) AS row_count,
            COUNT(DISTINCT series_id) AS unique_series
        FROM series
        UNION ALL
        SELECT 
            'categories' AS table_name,
            COUNT(*) AS row_count,
            COUNT(DISTINCT category_id) AS unique_categories
        FROM categories
        UNION ALL
        SELECT 
            'time_series' AS table_name,
            COUNT(*) AS row_count,
            COUNT(DISTINCT series_id) AS unique_series
        FROM time_series;
    """,
    dbname='fred',
    user='tradingAgent',
    host='localhost',
    return_results=True,
    dag=dag,
)

# ============================================================================
# BIS SQL Workflows
# ============================================================================

# BIS: Validate data integrity
bis_validate_data_integrity = PostgresSQLQueryOperator(
    task_id='bis_validate_data_integrity',
    sql="""
        SELECT 
            'dataflows' AS table_name,
            COUNT(*) AS row_count,
            COUNT(DISTINCT dataflow_id) AS unique_dataflows
        FROM dataflows
        UNION ALL
        SELECT 
            'time_series' AS table_name,
            COUNT(*) AS row_count,
            COUNT(DISTINCT dataflow_id) AS unique_dataflows
        FROM time_series;
    """,
    dbname='bis',
    user='tradingAgent',
    host='localhost',
    return_results=True,
    dag=dag,
)

# BIS: Get dataflow statistics
bis_get_dataflow_statistics = PostgresSQLQueryOperator(
    task_id='bis_get_dataflow_statistics',
    sql="""
        SELECT 
            frequency,
            COUNT(*) AS dataflow_count
        FROM dataflows
        GROUP BY frequency
        ORDER BY dataflow_count DESC;
    """,
    dbname='bis',
    user='tradingAgent',
    host='localhost',
    return_results=True,
    dag=dag,
)

# ============================================================================
# BLS SQL Workflows
# ============================================================================

# BLS: Validate data integrity
bls_validate_data_integrity = PostgresSQLQueryOperator(
    task_id='bls_validate_data_integrity',
    sql="""
        SELECT 
            'series' AS table_name,
            COUNT(*) AS row_count,
            COUNT(DISTINCT series_id) AS unique_series
        FROM series
        UNION ALL
        SELECT 
            'time_series' AS table_name,
            COUNT(*) AS row_count,
            COUNT(DISTINCT series_id) AS unique_series
        FROM time_series;
    """,
    dbname='bls',
    user='tradingAgent',
    host='localhost',
    return_results=True,
    dag=dag,
)

# BLS: Get series by survey
bls_get_series_by_survey = PostgresSQLQueryOperator(
    task_id='bls_get_series_by_survey',
    sql="""
        SELECT 
            survey_abbreviation,
            COUNT(*) AS series_count
        FROM series
        GROUP BY survey_abbreviation
        ORDER BY series_count DESC;
    """,
    dbname='bls',
    user='tradingAgent',
    host='localhost',
    return_results=True,
    dag=dag,
)

# ============================================================================
# Eurostat SQL Workflows
# ============================================================================

# Eurostat: Validate data integrity
eurostat_validate_data_integrity = PostgresSQLQueryOperator(
    task_id='eurostat_validate_data_integrity',
    sql="""
        SELECT 
            'datasets' AS table_name,
            COUNT(*) AS row_count,
            COUNT(DISTINCT dataset_code) AS unique_datasets
        FROM datasets
        UNION ALL
        SELECT 
            'time_series' AS table_name,
            COUNT(*) AS row_count,
            COUNT(DISTINCT dataset_code) AS unique_datasets
        FROM time_series;
    """,
    dbname='eurostat',
    user='tradingAgent',
    host='localhost',
    return_results=True,
    dag=dag,
)

# Eurostat: Get dataset statistics
eurostat_get_dataset_statistics = PostgresSQLQueryOperator(
    task_id='eurostat_get_dataset_statistics',
    sql="""
        SELECT 
            theme,
            COUNT(*) AS dataset_count
        FROM datasets
        WHERE theme IS NOT NULL AND theme != ''
        GROUP BY theme
        ORDER BY dataset_count DESC
        LIMIT 20;
    """,
    dbname='eurostat',
    user='tradingAgent',
    host='localhost',
    return_results=True,
    dag=dag,
)

# ============================================================================
# IMF SQL Workflows (if implemented)
# ============================================================================

# IMF: Validate data integrity (when IMF PostgreSQL is implemented)
# imf_validate_data_integrity = PostgresSQLQueryOperator(
#     task_id='imf_validate_data_integrity',
#     sql="""
#         SELECT 
#             'datasets' AS table_name,
#             COUNT(*) AS row_count
#         FROM datasets;
#     """,
#     dbname='imf',
#     user='tradingAgent',
#     host='localhost',
#     return_results=True,
#     dag=dag,
# )

# ============================================================================
# Task Dependencies
# ============================================================================

# FRED dependencies
fred_create_category_paths_view >> fred_create_category_analysis_view
fred_create_category_analysis_view >> [fred_analyze_category_statistics, fred_get_series_count_by_category, fred_validate_data_integrity]
[fred_analyze_category_statistics, fred_get_series_count_by_category, fred_validate_data_integrity] >> fred_execute_rates_query

# All modules can run in parallel (no cross-module dependencies)
# Each module's tasks run independently

