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
from airflow.utils.dates import days_ago

ROOT = Path(__file__).parent.parent  # project root
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.run_pipeline import (
    step_init_db,
    step_ingest_daily,
    step_predict,
    step_export,
)

default_args = {
    "owner":            "airflow",
    "retries":          2,
    "retry_delay_seconds": 300,
    "email_on_failure": False,
}

with DAG(
    dag_id="weather_daily_ingestion",
    description="Daily fetch from Open-Meteo API + prediction refresh",
    schedule_interval="0 6 * * *",
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    tags=["weather", "ingestion", "daily"],
) as dag:

    t_init = PythonOperator(
        task_id="init_db",
        python_callable=step_init_db,
    )

    t_fetch = PythonOperator(
        task_id="fetch_daily",
        python_callable=step_ingest_daily,
    )

    t_predict = PythonOperator(
        task_id="run_predictions",
        python_callable=step_predict,
    )

    t_export = PythonOperator(
        task_id="export_csv",
        python_callable=step_export,
    )

    t_init >> t_fetch >> t_predict >> t_export
