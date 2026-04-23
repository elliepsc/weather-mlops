"""
Airflow DAG — Manual backfill
Trigger type : manual only (no automatic schedule)

Use this DAG to:
  - Initialize the database for the first time (full 2008 historical fetch)
  - Re-fetch data for a specific date range after an outage
  - Force a full model retrain + prediction regeneration

Trigger via Airflow UI → DAGs → weather_backfill → Trigger DAG w/ config:
  {
    "start_date": "2008-01-01",   (optional, defaults to 2008-01-01)
    "end_date":   "2026-04-21"    (optional, defaults to yesterday)
  }

Note: delay_seconds=10 between cities to stay within Open-Meteo free-tier rate limits.
Full backfill (26 cities x 18 years) takes ~5 minutes.
"""
import sys
import logging
from pathlib import Path
from datetime import date, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

ROOT = Path(__file__).parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

logger = logging.getLogger(__name__)

default_args = {
    "owner":               "airflow",
    "retries":             1,
    "retry_delay_seconds": 300,
    "email_on_failure":    False,
}


def run_backfill_with_config(**context):
    """
    Fetch historical data for a configurable date range.
    Falls back to 2-year default if no config provided.
    """
    conf = context.get("dag_run").conf or {}
    start = conf.get("start_date", "2008-01-01")
    end   = conf.get("end_date",   (date.today() - timedelta(days=1)).isoformat())

    logger.info("Backfill requested: %s -> %s", start, end)

    from pipeline.database import init_db, upsert_weather_raw
    from pipeline.fetch_weather import fetch_all_cities

    init_db()
    df = fetch_all_cities(start, end, delay_seconds=10.0)
    if df.empty:
        raise ValueError(f"No data returned for {start} -> {end}")
    upsert_weather_raw(df)
    logger.info("Backfill stored: %d rows", len(df))


def run_train(**context):
    from pipeline.run_pipeline import step_train
    step_train()


def run_predict(**context):
    from pipeline.run_pipeline import step_predict
    step_predict()


def run_export(**context):
    from pipeline.run_pipeline import step_export
    step_export()


with DAG(
    dag_id="weather_backfill",
    description="Manual backfill: fetch historical data + retrain + predict (no schedule)",
    schedule_interval=None,       # manual trigger only
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    tags=["weather", "backfill", "manual"],
    params={
        "start_date": "2008-01-01",
        "end_date":   "2026-04-21",
    },
) as dag:

    t_fetch   = PythonOperator(task_id="fetch_historical", python_callable=run_backfill_with_config)
    t_train   = PythonOperator(task_id="retrain_models",   python_callable=run_train)
    t_predict = PythonOperator(task_id="run_predictions",  python_callable=run_predict)
    t_export  = PythonOperator(task_id="export_csv",       python_callable=run_export)

    t_fetch >> t_train >> t_predict >> t_export
