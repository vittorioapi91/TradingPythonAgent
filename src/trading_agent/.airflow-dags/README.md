# Airflow DAGs for TradingPythonAgent

This directory contains Airflow DAGs for orchestrating data collection workflows.

## Environment Support

All DAGs are environment-aware and automatically adapt based on the `AIRFLOW_ENV` environment variable:
- **dev**: Development environment (dev/* branches)
- **staging**: Staging environment (staging branch)
- **prod**: Production environment (main branch)

Each environment has its own Airflow instance:
- **Airflow DEV**: http://localhost:8082
- **Airflow TEST**: http://localhost:8083
- **Airflow PROD**: http://localhost:8084

## DAGs

### 1. `edgar_filings_download_{env}`

Main DAG for downloading SEC EDGAR filings from the database.

**Schedule**: Daily (`@daily`)

**Parameters** (can be set via DAG run config):
- `year`: Year filter (e.g., 2005)
- `quarter`: Quarter filter (QTR1, QTR2, QTR3, QTR4)
- `form_type`: Form type filter (e.g., "10-K", "10-Q") - **Default: "10-K"**
- `company_name`: Company name filter (partial match, case-insensitive)
- `cik`: CIK (Central Index Key) filter
- `limit`: Limit number of filings to download
  - **dev**: Default limit 100
  - **staging**: Default limit 500
  - **prod**: No default limit

**Usage Examples**:

1. **Trigger with default parameters** (downloads 10-K filings):
   - Just trigger the DAG normally

2. **Trigger with custom parameters** (via DAG run config):
   ```json
   {
     "year": 2005,
     "quarter": "QTR2",
     "form_type": "10-K",
     "limit": 50
   }
   ```

3. **Download specific company filings**:
   ```json
   {
     "company_name": "NVIDIA",
     "form_type": "10-K",
     "year": 2023
   }
   ```

### 2. `edgar_filings_quarterly_{env}`

Automatically downloads filings for the most recent completed quarter.

**Schedule**: Monthly (1st of each month at 2 AM)

**Behavior**:
- Runs on the 1st of each month
- Downloads filings from the previous quarter
- Example: If run in January, downloads Q4 filings from the previous year
- Default form type: 10-K
- Limited to 1000 filings in non-prod environments

**Parameters**: None (automatically calculated)

## Configuration

### Environment-Specific Settings

Each environment has different default limits and schedules:

| Environment | Default Limit | Schedule |
|------------|---------------|----------|
| dev | 100 | Daily |
| staging | 500 | Daily |
| prod | None (unlimited) | Daily |

### Database Configuration

The DAGs use environment-aware database configuration:
- Database name: `edgar`
- Database user: `tradingAgent`
- Database host/port: From environment variables (`POSTGRES_HOST`, `POSTGRES_PORT`)

### Output Directory

Filings are downloaded to:
```
{PROJECT_ROOT}/src/trading_agent/fundamentals/edgar/filings
```

## Manual DAG Execution

### Via Airflow UI

1. Navigate to the DAG in Airflow UI
2. Click "Trigger DAG w/ config"
3. Enter JSON configuration (optional):
   ```json
   {
     "year": 2005,
     "quarter": "QTR2",
     "form_type": "10-K"
   }
   ```
4. Click "Trigger"

### Via Airflow CLI

```bash
# Trigger with default parameters
airflow dags trigger edgar_filings_download_dev

# Trigger with custom config
airflow dags trigger edgar_filings_download_dev \
  --conf '{"year": 2005, "quarter": "QTR2", "form_type": "10-K"}'
```

## Dependencies

The DAGs require:
- `trading_agent` package installed (via wheel in Airflow container)
- PostgreSQL database with `edgar` database
- `master_idx_files` table populated (via catalog generation)

## Troubleshooting

### DAG not appearing in Airflow UI

1. Check that DAG files are in `.ops/.airflow/dags/`
2. Verify Airflow can import the DAG (check for syntax errors)
3. Check Airflow logs for import errors

### Import errors

1. Ensure `trading_agent` wheel is installed in Airflow container
2. Check `PYTHONPATH` includes project `src/` directory
3. Verify all dependencies are installed in Airflow environment

### Database connection errors

1. Verify `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_PASSWORD` are set
2. Check database exists and is accessible from Airflow container
3. Verify `master_idx_files` table exists and has data

### No filings found

1. Ensure catalog has been generated (`--generate-catalog`)
2. Check filter parameters match data in database
3. Verify `master_idx_files` table has entries for the specified filters

## Adding New DAGs

To add a new DAG:

1. Create a new Python file in `.ops/.airflow/dags/`
2. Follow the pattern of existing DAGs:
   - Use `AIRFLOW_ENV` for environment detection
   - Include environment-specific configuration
   - Use appropriate retry and error handling
   - Add descriptive tags

3. Example structure:
   ```python
   from airflow import DAG
   from airflow.operators.python import PythonOperator
   import os
   
   AIRFLOW_ENV = os.getenv('AIRFLOW_ENV', 'dev')
   
   dag = DAG(
       f'my_dag_{AIRFLOW_ENV}',
       # ... configuration
   )
   ```

## Related Documentation

- [EDGAR Downloader Documentation](../../../src/trading_agent/fundamentals/edgar/README.md)
- [Environment Configuration](../../../src/trading_agent/config.py)
- [Wheel Installation](../../../WHEELS.md)
