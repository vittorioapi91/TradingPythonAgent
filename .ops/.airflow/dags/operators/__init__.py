"""
Custom Airflow operators for Trading Agent workflows.

This `operators` package lives inside the DAGs folder so Airflow can
import it naturally when parsing DAG files:

    from operators import DataDownloadOperator
"""

from airflow.models import BaseOperator
from typing import Optional, Dict, List
import os
import psycopg2
from psycopg2.extras import RealDictCursor
import logging
import subprocess
import sys


class PostgresSQLFileOperator(BaseOperator):
    """Execute SQL from a file on PostgreSQL database."""

    template_fields = ('sql_file', 'parameters')

    def __init__(
        self,
        sql_file: str,
        module_name: Optional[str] = None,
        postgres_conn_id: Optional[str] = None,
        dbname: str = "fred",
        user: str = "tradingAgent",
        host: str = "localhost",
        password: Optional[str] = None,
        port: int = 5432,
        parameters: Optional[Dict] = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.sql_file = sql_file
        self.module_name = module_name
        self.postgres_conn_id = postgres_conn_id
        self.dbname = dbname
        self.user = user
        self.host = host
        self.password = password
        self.port = port
        self.parameters = parameters or {}

    def execute(self, context):
        log = logging.getLogger(__name__)

        # Get password from connection or environment
        if self.postgres_conn_id:
            try:
                from airflow.providers.postgres.hooks.postgres import PostgresHook
            except ImportError:
                from airflow.hooks.postgres_hook import PostgresHook
            hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
            conn = hook.get_conn()
        else:
            password = self.password or os.getenv('POSTGRES_PASSWORD', '')
            conn = psycopg2.connect(
                dbname=self.dbname,
                user=self.user,
                host=self.host,
                password=password,
                port=self.port,
            )

        # Resolve SQL file path
        if not os.path.isabs(self.sql_file):
            if self.module_name:
                # Relative to module directory: src/trading_agent/macro/{module_name}/
                base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
                macro_dir = os.path.join(base_dir, 'macro')
                module_dir = os.path.join(macro_dir, self.module_name)
                sql_file_path = os.path.join(module_dir, self.sql_file)
            else:
                # Relative to trading_agent directory
                base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
                sql_file_path = os.path.join(base_dir, self.sql_file)
        else:
            sql_file_path = self.sql_file

        if not os.path.exists(sql_file_path):
            raise FileNotFoundError(f"SQL file not found: {sql_file_path}")

        log.info("Executing SQL file: %s", sql_file_path)

        with open(sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()

        statements = [
            stmt.strip()
            for stmt in sql_content.split(';')
            if stmt.strip() and not stmt.strip().startswith('--')
        ]

        cur = conn.cursor()
        results = []

        try:
            for statement in statements:
                log.info("Executing statement: %s...", statement[:100])
                cur.execute(statement, self.parameters)

                if statement.strip().upper().startswith('SELECT'):
                    result = cur.fetchall()
                    results.append(result)
                    log.info("Query returned %d rows", len(result))

            conn.commit()
            log.info("Successfully executed %d SQL statements", len(statements))

        except Exception as e:
            conn.rollback()
            log.error("Error executing SQL: %s", e)
            raise

        finally:
            cur.close()
            if not self.postgres_conn_id:
                conn.close()

        return results


class PostgresSQLQueryOperator(BaseOperator):
    """Execute SQL query string on PostgreSQL database."""

    template_fields = ('sql', 'parameters')

    def __init__(
        self,
        sql: str,
        postgres_conn_id: Optional[str] = None,
        dbname: str = "fred",
        user: str = "tradingAgent",
        host: str = "localhost",
        password: Optional[str] = None,
        port: int = 5432,
        parameters: Optional[Dict] = None,
        return_results: bool = False,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.sql = sql
        self.postgres_conn_id = postgres_conn_id
        self.dbname = dbname
        self.user = user
        self.host = host
        self.password = password
        self.port = port
        self.parameters = parameters or {}
        self.return_results = return_results

    def execute(self, context):
        log = logging.getLogger(__name__)

        if self.postgres_conn_id:
            try:
                from airflow.providers.postgres.hooks.postgres import PostgresHook
            except ImportError:
                from airflow.hooks.postgres_hook import PostgresHook
            hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
            conn = hook.get_conn()
        else:
            password = self.password or os.getenv('POSTGRES_PASSWORD', '')
            conn = psycopg2.connect(
                dbname=self.dbname,
                user=self.user,
                host=self.host,
                password=password,
                port=self.port,
            )

        cur = conn.cursor(cursor_factory=RealDictCursor if self.return_results else None)

        try:
            log.info("Executing SQL query: %s...", self.sql[:100])
            cur.execute(self.sql, self.parameters)

            if self.return_results and self.sql.strip().upper().startswith('SELECT'):
                results = cur.fetchall()
                log.info("Query returned %d rows", len(results))
                return results
            else:
                conn.commit()
                log.info("Query executed successfully")
                return cur.rowcount

        except Exception as e:
            conn.rollback()
            log.error("Error executing SQL: %s", e)
            raise

        finally:
            cur.close()
            if not self.postgres_conn_id:
                conn.close()


class PostgresViewOperator(BaseOperator):
    """Create or replace a PostgreSQL view."""

    template_fields = ('view_name', 'sql_file')

    def __init__(
        self,
        view_name: str,
        sql_file: str,
        module_name: Optional[str] = None,
        postgres_conn_id: Optional[str] = None,
        dbname: str = "fred",
        user: str = "tradingAgent",
        host: str = "localhost",
        password: Optional[str] = None,
        port: int = 5432,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.view_name = view_name
        self.sql_file = sql_file
        self.module_name = module_name
        self.postgres_conn_id = postgres_conn_id
        self.dbname = dbname
        self.user = user
        self.host = host
        self.password = password
        self.port = port

    def execute(self, context):
        log = logging.getLogger(__name__)

        sql_operator = PostgresSQLFileOperator(
            sql_file=self.sql_file,
            module_name=self.module_name,
            postgres_conn_id=self.postgres_conn_id,
            dbname=self.dbname,
            user=self.user,
            host=self.host,
            password=self.password,
            port=self.port,
            task_id=f"create_view_{self.view_name}",
            dag=self.dag,
        )

        result = sql_operator.execute(context)
        log.info("View %s created/replaced successfully", self.view_name)

        return result


class DataDownloadOperator(BaseOperator):
    """Execute data download script for a macro module."""

    template_fields = ('command', 'script_args')

    def __init__(
        self,
        module_name: str,
        command: str,
        script_args: Optional[List[str]] = None,
        python_path: Optional[str] = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.module_name = module_name
        self.command = command
        self.script_args = script_args or []
        self.python_path = python_path or sys.executable

    def execute(self, context):
        log = logging.getLogger(__name__)

        # Get module directory: src/trading_agent/macro/{module_name}/
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        macro_dir = os.path.join(base_dir, 'macro')
        module_dir = os.path.join(macro_dir, self.module_name)
        main_script = os.path.join(module_dir, 'main.py')

        if not os.path.exists(main_script):
            raise FileNotFoundError(f"Main script not found: {main_script}")

        cmd = [self.python_path, main_script, self.command] + self.script_args

        log.info("Executing command: %s", ' '.join(cmd))

        try:
            result = subprocess.run(
                cmd,
                cwd=module_dir,
                capture_output=True,
                text=True,
                check=True,
            )
            log.info("Command output: %s", result.stdout)
            if result.stderr:
                log.warning("Command stderr: %s", result.stderr)
            return result.stdout

        except subprocess.CalledProcessError as e:
            log.error("Command failed with return code %d", e.returncode)
            log.error("Error output: %s", e.stderr)
            raise


