"""Airflow DAG - daily dbt analytics refresh.

Pipeline:
  1. wait_for_ingestion  – ExternalTaskSensor on weather_daily_ingestion.export_csv
  2. load_sources        – sync SQLite + JSON monitoring files → DuckDB
  3. dbt_run             – dbt run (all models)
  4. dbt_test            – dbt test (schema + data tests)

Scheduled at 10:00 UTC, after ingestion (06:00) and monitoring (09:00).
Monitoring output files (drift_report, monitoring_decision, last_retrain) are
loaded on a best-effort basis — load_sources.py skips missing files gracefully.
"""

import logging
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).parent.parent
ANALYTICS_DIR = ROOT / "analytics"

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dags._airflow_compat import DAG, ExternalTaskSensor, PythonOperator

logger = logging.getLogger(__name__)

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "execution_timeout": timedelta(minutes=30),
}


def _load_sources(**kwargs):
    from analytics.scripts.load_sources import main

    main()


def _dbt_run(**kwargs):
    result = subprocess.run(
        ["dbt", "run"],
        cwd=str(ANALYTICS_DIR),
        capture_output=True,
        text=True,
    )
    logger.info(result.stdout)
    if result.returncode != 0:
        logger.error(result.stderr)
        raise RuntimeError(f"dbt run failed (exit {result.returncode})")


def _dbt_test(**kwargs):
    result = subprocess.run(
        ["dbt", "test"],
        cwd=str(ANALYTICS_DIR),
        capture_output=True,
        text=True,
    )
    logger.info(result.stdout)
    if result.returncode != 0:
        logger.error(result.stderr)
        raise RuntimeError(f"dbt test failed (exit {result.returncode})")


with DAG(
    dag_id="weather_dbt_analytics",
    description="Daily dbt refresh: load DuckDB sources then run and test all models",
    schedule="0 10 * * *",
    start_date=datetime(2026, 4, 29),
    catchup=False,
    default_args=default_args,
    tags=["weather", "analytics", "dbt", "daily"],
) as dag:

    t_wait = ExternalTaskSensor(
        task_id="wait_for_ingestion",
        external_dag_id="weather_daily_ingestion",
        external_task_id="export_csv",
        timeout=3600,
        poke_interval=60,
        mode="reschedule",
        execution_timeout=timedelta(hours=1),
    )
    t_load = PythonOperator(
        task_id="load_sources",
        python_callable=_load_sources,
        execution_timeout=timedelta(minutes=10),
    )
    t_run = PythonOperator(
        task_id="dbt_run",
        python_callable=_dbt_run,
        execution_timeout=timedelta(minutes=20),
    )
    t_test = PythonOperator(
        task_id="dbt_test",
        python_callable=_dbt_test,
        execution_timeout=timedelta(minutes=10),
    )

    t_wait >> t_load >> t_run >> t_test
