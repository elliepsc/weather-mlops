"""
Airflow DAG — Daily ingestion
Schedule : every day at 06:00 UTC (data from yesterday is available by then)

Tasks:
  1. init_db      — ensure schema exists (idempotent)
  2. fetch_daily  — pull yesterday's data from Open-Meteo for all 26 cities
  3. run_predict  — regenerate predictions for new rows
  4. export_csv   — refresh CSV file for Power BI fallback
"""
import sys
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

ROOT = Path(__file__).parent.parent  # project root
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

default_args = {
    "owner":            "airflow",
    "retries":          2,
    "retry_delay_seconds": 300,
    "email_on_failure": False,
}


def step_init_db(**kwargs):
    from pipeline.run_pipeline import step_init_db as _f
    _f()

def step_ingest_daily(**kwargs):
    from pipeline.run_pipeline import step_ingest_daily as _f
    _f()

def step_predict(**kwargs):
    from pipeline.run_pipeline import step_predict as _f
    _f()

def step_export(**kwargs):
    from pipeline.run_pipeline import step_export as _f
    _f()


with DAG(
    dag_id="weather_daily_ingestion",
    description="Daily fetch from Open-Meteo API + prediction refresh",
    schedule="0 6 * * *",
    start_date=datetime(2026, 4, 22),
    catchup=False,
    default_args=default_args,
    tags=["weather", "ingestion", "daily"],
) as dag:

    t_init    = PythonOperator(task_id="init_db",          python_callable=step_init_db)
    t_fetch   = PythonOperator(task_id="fetch_daily",      python_callable=step_ingest_daily)
    t_predict = PythonOperator(task_id="run_predictions",  python_callable=step_predict)
    t_export  = PythonOperator(task_id="export_csv",       python_callable=step_export)

    t_init >> t_fetch >> t_predict >> t_export
