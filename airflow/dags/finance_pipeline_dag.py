"""
finance_pipeline_dag.py

Phase 5 - Airflow orchestration for finance-pipeline

Pipeline flow:
ingest_stocks  -+                    +- load_stocks  -+
ingest_crypto  -+-> [parallel done] -+- load_crypto  -+-> dbt_run -> dbt_test
ingest_macro   -+                    +- load_macro   -+
"""

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule

PROJECT_ROOT = "/Users/Siddik/finance-pipeline"
DBT_PROJECT_DIR = f"{PROJECT_ROOT}/dbt"
DBT_PROFILES_DIR = "/Users/siddik/.dbt"

log = logging.getLogger(__name__)

# -----------------------------
# Ingestion
# -----------------------------

def run_ingest_stocks(**context):
    import sys
    sys.path.insert(0, f"{PROJECT_ROOT}/ingestion")
    from tiingo_stocks import fetch_tiingo_stocks
    log.info("Starting Tiingo stocks ingestion...")
    fetch_tiingo_stocks()
    log.info("Tiingo stocks ingestion complete.")

def run_ingest_crypto(**context):
    import sys
    sys.path.insert(0, f"{PROJECT_ROOT}/ingestion")
    from tiingo_crypto import fetch_tiingo_crypto
    log.info("Starting Tiingo crypto ingestion...")
    fetch_tiingo_crypto()
    log.info("Tiingo crypto ingestion complete.")

def run_ingest_macro(**context):
    import sys
    sys.path.insert(0, f"{PROJECT_ROOT}/ingestion")
    from fred_macro import fetch_fred_macro
    log.info("Starting FRED macro ingestion...")
    fetch_fred_macro()
    log.info("FRED macro ingestion complete.")

# -----------------------------
# Loading
# -----------------------------

def run_load_stocks(**context):
    import sys
    sys.path.insert(0, f"{PROJECT_ROOT}/loading")
    from load_stocks import load_stocks_to_bigquery
    log.info("Starting stocks load to BigQuery...")
    load_stocks_to_bigquery()
    log.info("Stocks load complete.")

def run_load_crypto(**context):
    import sys
    sys.path.insert(0, f"{PROJECT_ROOT}/loading")
    from load_crypto import load_crypto_to_bigquery
    log.info("Starting crypto load to BigQuery...")
    load_crypto_to_bigquery()
    log.info("Crypto load complete.")

def run_load_macro(**context):
    import sys
    sys.path.insert(0, f"{PROJECT_ROOT}/loading")
    from load_macro import load_macro_to_bigquery
    log.info("Starting macro load to BigQuery...")
    load_macro_to_bigquery()
    log.info("Macro load complete.")

# -----------------------------
# Failure alert
# -----------------------------

def on_failure_alert(context):
    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    execution_date = context["execution_date"]
    log.error(
        "PIPELINE FAILURE | DAG: %s | Task: %s | Date: %s",
        dag_id, task_id, execution_date,
    )

# -----------------------------
# Default args
# -----------------------------

default_args = {
    "owner": "siddik",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "retry_exponential_backoff": True,
    "on_failure_callback": on_failure_alert,
}

# -----------------------------
# DAG
# -----------------------------

with DAG(
    dag_id="finance_pipeline",
    default_args=default_args,
    description="End-to-end finance data pipeline: ingest -> GCS -> BigQuery -> dbt",
    start_date=datetime(2025, 1, 1),
    schedule="0 7 * * 1-5",
    catchup=False,
    max_active_runs=1,
    tags=["finance", "portfolio", "dbt", "bigquery"],
) as dag:

    # Ingestion (parallel)
    ingest_stocks = PythonOperator(
        task_id="ingest_stocks",
        python_callable=run_ingest_stocks,
    )

    ingest_crypto = PythonOperator(
        task_id="ingest_crypto",
        python_callable=run_ingest_crypto,
    )

    ingest_macro = PythonOperator(
        task_id="ingest_macro",
        python_callable=run_ingest_macro,
    )

    # Loading (parallel)
    load_stocks = PythonOperator(
        task_id="load_stocks",
        python_callable=run_load_stocks,
    )

    load_crypto = PythonOperator(
        task_id="load_crypto",
        python_callable=run_load_crypto,
    )

    load_macro = PythonOperator(
        task_id="load_macro",
        python_callable=run_load_macro,
    )

    # dbt
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run --profiles-dir {DBT_PROFILES_DIR} --target dev"
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt test --profiles-dir {DBT_PROFILES_DIR} --target dev"
        ),
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # Dependencies
    ingest_stocks >> load_stocks
    ingest_crypto >> load_crypto
    ingest_macro >> load_macro

    [load_stocks, load_crypto, load_macro] >> dbt_run >> dbt_test