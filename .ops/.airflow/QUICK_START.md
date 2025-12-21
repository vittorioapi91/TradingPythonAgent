# Airflow Quick Start Guide

## Initial Setup (One-time)

### 1. Set Environment Variables
Set these once in your shell session (or add to `~/.bashrc` / `~/.zshrc`):
```bash
export AIRFLOW_HOME=~/airflow
export AIRFLOW__CORE__DAGS_FOLDER=/Users/Snake91/CursorProjects/TradingPythonAgent/.ops/.airflow/dags
export POSTGRES_PASSWORD='2014'
```

**Note:** These variables must be set before running any Airflow commands. Alternatively, you can configure `dags_folder` in `~/airflow/airflow.cfg` (see step 3 below).

### 2. Activate Virtual Environment
```bash
cd /Users/Snake91/CursorProjects/TradingPythonAgent
source .venv/bin/activate
```

### 3. Initialize Airflow Database
```bash
airflow db migrate  # For Airflow 3.x (use 'airflow db init' for Airflow 2.x)
```

### 4. Create Admin User (Airflow 3.x)
**Important:** Airflow 3.x uses `SimpleAuthManager` by default and does NOT automatically create a default user. You must configure one manually.

**Method: Configure user in airflow.cfg**
1. Edit `~/airflow/airflow.cfg` and add your admin user in the `[webserver]` section:
   ```ini
   [webserver]
   simple_auth_manager_users = admin:Admin
   ```
   Replace `admin` with your desired username. The `:Admin` part sets the role to Admin.

2. Start Airflow:
   ```bash
   source .venv/bin/activate
   airflow standalone
   ```

3. When Airflow starts, it will generate a password and display it in the terminal output. Look for a message like:
   ```
   Password for user 'admin': <generated_password>
   ```
   The password is also saved to `~/airflow/simple_auth_manager_passwords.json`

4. Log in to the web UI at `http://localhost:8080` using:
   - Username: `admin` (or whatever you set in step 1)
   - Password: The generated password from step 3

**Note:** If you're getting "unauthorized" errors, make sure:
1. You've added the user to `airflow.cfg` as shown above
2. You've restarted Airflow after editing the config
3. You're using the correct password from the terminal output or `simple_auth_manager_passwords.json`

### 5. Configure DAGs Folder (Alternative to Environment Variable)
If you prefer not to use environment variables, add to `~/airflow/airflow.cfg`:
```ini
[core]
dags_folder = /Users/Snake91/CursorProjects/TradingPythonAgent/.ops/.airflow/dags
```

## Running Airflow

### Option 1: Standalone Mode (Easiest)
```bash
source .venv/bin/activate
airflow standalone
```
This starts both scheduler and API server (web UI). Access UI at `http://localhost:8080`

**Important:** You must create an admin user first (see step 3 above) before you can log in.

### Option 2: Separate Processes
```bash
# Terminal 1: Scheduler
source .venv/bin/activate
airflow scheduler

# Terminal 2: API Server (Web UI)
source .venv/bin/activate
airflow api-server --port 8080
```

## Verify DAGs

```bash
source .venv/bin/activate

# List DAGs
airflow dags list

# Check for import errors
airflow dags list-import-errors

# Test DAG syntax
python -c "import sys; sys.path.insert(0, 'src'); from trading_agent.airflow.dags import macro_sql_workflows; print('OK')"
```

## Troubleshooting

### Issue: `airflow db init` doesn't work
**Solution:** Airflow 3.x uses `airflow db migrate` instead

### Issue: DAGs not appearing
**Solutions:**
1. Check DAGs folder: `echo $AIRFLOW__CORE__DAGS_FOLDER`
2. Check for import errors: `airflow dags list-import-errors`
3. Verify DAG files are in the correct location
4. Check Airflow logs: `~/airflow/logs/`

### Issue: Import errors
**Solutions:**
1. Ensure virtual environment is activated
2. Install all dependencies: `pip install -r requirements.txt`
3. Check Python path includes project root
4. Verify module imports work: `python -c "from trading_agent.airflow.operators import PostgresSQLFileOperator"`

## Quick Test

```bash
# Test all DAGs can be imported
cd /Users/Snake91/CursorProjects/TradingPythonAgent
source .venv/bin/activate

python -c "
import sys
sys.path.insert(0, 'src')
from trading_agent.airflow.dags import macro_sql_workflows, macro_data_downloads, macro_master_dag
print('✓ All DAGs imported successfully')
print(f'  - {macro_sql_workflows.dag.dag_id}')
print(f'  - {macro_data_downloads.dag.dag_id}')
print(f'  - {macro_master_dag.dag.dag_id}')
"
```

