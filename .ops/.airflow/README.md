# Trading Agent Airflow Workflow Management

This directory contains Airflow DAGs and operators for managing all Trading Agent workflows, including macro economic data operations.

## Structure

```
airflow/
├── __init__.py
├── operators.py          # Custom Airflow operators
├── config.py             # Configuration for workflows
├── dags/
│   ├── __init__.py
│   ├── macro_sql_workflows.py     # SQL workflows for ALL modules
│   ├── macro_data_downloads.py    # Data downloads for all modules
│   └── macro_master_dag.py        # Master orchestration DAG
└── README.md
```

## Custom Operators

### PostgresSQLFileOperator
Executes SQL from a file on PostgreSQL database.

**Parameters:**
- `sql_file`: Path to SQL file (relative to module or absolute)
- `module_name`: Module name for relative paths (fred, bis, bls, eurostat, imf)
- `postgres_conn_id`: Airflow connection ID (optional)
- `dbname`: Database name
- `user`: Database user (default: 'tradingAgent')
- `host`: Database host (default: 'localhost')
- `password`: Database password (optional)
- `port`: Database port (default: 5432)
- `parameters`: Optional SQL parameters

### PostgresSQLQueryOperator
Executes a SQL query string on PostgreSQL database.

**Parameters:**
- `sql`: SQL query string
- `dbname`, `user`, `host`, `password`, `port`: Database connection parameters
- `parameters`: Optional SQL parameters
- `return_results`: Whether to return query results (default: False)

### PostgresViewOperator
Creates or replaces a PostgreSQL view from a SQL file.

**Parameters:**
- `view_name`: Name of the view
- `sql_file`: Path to SQL file with view definition
- `module_name`: Module name for relative paths
- Database connection parameters

### DataDownloadOperator
Executes data download scripts for macro modules.

**Parameters:**
- `module_name`: Module name (fred, bis, bls, eurostat, imf)
- `command`: Command to execute (e.g., '--generate-db', '--from-db')
- `script_args`: Additional script arguments
- `python_path`: Python executable path

## DAGs

### 1. macro_sql_workflows
Manages SQL workflows for ALL macro modules.

**FRED Tasks:**
- Create category paths view
- Create category analysis view
- Execute rates categories query
- Analyze category statistics
- Get series count by category
- Validate data integrity

**BIS Tasks:**
- Validate data integrity
- Get dataflow statistics

**BLS Tasks:**
- Validate data integrity
- Get series by survey

**Eurostat Tasks:**
- Validate data integrity
- Get dataset statistics

**IMF Tasks:**
- (To be added when IMF PostgreSQL is implemented)

**Schedule:** Daily

### 2. macro_data_downloads
Orchestrates data downloads for all macro modules.

**Tasks:**
- FRED: Generate DB → Download data
- BIS: Generate DB → Download data
- BLS: Generate DB → Download data
- Eurostat: Generate DB → Download data
- IMF: (if implemented)

**Schedule:** Weekly

### 3. macro_master_workflow
Master DAG that orchestrates all workflows.

**Workflow:**
1. Trigger data downloads (all modules)
2. Trigger SQL workflows (all modules)

**Schedule:** Weekly

## Setup

### 1. Install Airflow

```bash
pip install apache-airflow apache-airflow-providers-postgres
```

### 2. Initialize Airflow

```bash
export AIRFLOW_HOME=~/airflow
airflow db init
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com
```

### 3. Configure DAGs Folder

Point Airflow to the DAGs folder:

```bash
# In airflow.cfg or environment variable
export AIRFLOW__CORE__DAGS_FOLDER=/path/to/.ops/.airflow/dags
```

Or symlink:
```bash
ln -s /path/to/.ops/.airflow/dags ~/airflow/dags/trading_agent
```

### 4. Set Environment Variables

```bash
export POSTGRES_PASSWORD='your_password'
export AIRFLOW_HOME=~/airflow
```

### 5. Start Airflow

```bash
# Start scheduler
airflow scheduler

# Start webserver (in another terminal)
airflow webserver
```

## Usage

### Accessing Airflow UI

1. Open `http://localhost:8080`
2. Login with admin credentials
3. Find the DAGs:
   - `macro_sql_workflows` (SQL workflows for all modules)
   - `macro_data_downloads` (Data downloads for all modules)
   - `macro_master_workflow` (Master orchestration)

### Running DAGs

- **Manual Trigger**: Click "Play" button on any DAG
- **Scheduled**: DAGs run automatically based on their schedule_interval
- **Master DAG**: Use `macro_master_workflow` to run everything in order

### Module-Specific Workflows

All modules have consistent workflows:

**FRED:**
- SQL workflows: Category analysis views, custom queries
- Data downloads with query file support

**BIS:**
- SQL workflows: Data integrity validation, dataflow statistics
- Dataflow catalog generation and dataset downloads

**BLS:**
- SQL workflows: Data integrity validation, series by survey analysis
- Series catalog generation and time series downloads

**Eurostat:**
- SQL workflows: Data integrity validation, dataset statistics
- Dataset catalog generation and multi-dimensional data downloads

**IMF:**
- SQL workflows: (To be added)
- Data downloads: (To be implemented)

## Configuration

Edit `config.py` to customize:
- Default database connection parameters
- Module database names
- SQL file paths
- View names

## Adding New Modules

1. Add module database to `MODULE_DATABASES` in `config.py`
2. Add tasks to `macro_data_downloads.py`:
   ```python
   new_module_generate_db = DataDownloadOperator(
       task_id='new_module_generate_db',
       module_name='new_module',
       command='--generate-db',
       script_args=['--dbuser', 'tradingAgent'],
       dag=dag,
   )
   ```
3. Add SQL files to `SQL_FILES` if needed
4. Create module-specific DAG if needed

## Troubleshooting

### DAG Not Appearing
- Check DAGs folder path in Airflow config
- Verify Python syntax in DAG files
- Check Airflow logs: `airflow scheduler` output

### Connection Errors
- Verify PostgreSQL is running
- Check database credentials
- Ensure databases exist (fred, bis, bls, eurostat, imf)

### Import Errors
- Verify Python path includes project root
- Check that all modules are installed
- Review Airflow logs for import errors

### Task Failures
- Check task logs in Airflow UI
- Verify script paths are correct
- Ensure database tables exist
- Review PostgreSQL logs

## Best Practices

1. **Use Master DAG**: Use `macro_master_workflow` for coordinated execution
2. **Separate Concerns**: Keep SQL workflows separate from data downloads
3. **Error Handling**: Tasks have retries configured
4. **Monitoring**: Use Airflow UI to monitor task execution
5. **Logging**: Check logs for detailed execution information

