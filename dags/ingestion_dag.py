"""Airflow DAG - daily ingestion."""

import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config.settings import mlops_config
from dags._airflow_compat import DAG, PythonOperator

logger = logging.getLogger(__name__)

I = mlops_config.ingestion

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(seconds=300),
    "email_on_failure": False,
}


def step_init_db(**kwargs):
    from pipeline.run_pipeline import step_init_db as _step_init_db

    _step_init_db()


def step_ingest_daily(**context):
    from pipeline.run_pipeline import step_ingest_daily as _step_ingest_daily

    target_date = (
        datetime.fromisoformat(context["ds"]) - timedelta(days=1)
    ).date().isoformat()
    return _step_ingest_daily(target_date=target_date, delay_seconds=0.3)


def check_daily_ingestion(**context):
    """Fail when yesterday coverage is below the configured city threshold."""
    import pandas as pd
    from pipeline.database import get_connection
    from pipeline.locations import LOCATIONS

    yesterday = (
        datetime.fromisoformat(context["ds"]) - timedelta(days=1)
    ).date().isoformat()
    expected = len(LOCATIONS)

    with get_connection() as conn:
        df = pd.read_sql(
            "SELECT COUNT(DISTINCT city) AS n FROM weather_raw WHERE date = ?",
            conn,
            params=(yesterday,),
        )

    n_cities = int(df["n"].iloc[0])
    logger.info("Ingestion check: %d/%d cities for %s", n_cities, expected, yesterday)

    if n_cities < I.min_cities_threshold:
        raise ValueError(
            f"Ingestion quality FAIL: only {n_cities}/{expected} cities for {yesterday}. "
            f"Minimum required: {I.min_cities_threshold}. Predictions blocked."
        )


def step_predict(**kwargs):
    from pipeline.run_pipeline import step_predict as _step_predict

    _step_predict()


def step_export(**kwargs):
    from pipeline.run_pipeline import step_export as _step_export

    _step_export()


with DAG(
    dag_id="weather_daily_ingestion",
    description="Daily fetch from Open-Meteo API plus quality check and prediction refresh",
    schedule="0 6 * * *",
    start_date=datetime(2026, 4, 22),
    catchup=False,
    default_args=default_args,
    tags=["weather", "ingestion", "daily"],
) as dag:
    t_init = PythonOperator(task_id="init_db", python_callable=step_init_db)
    t_fetch = PythonOperator(task_id="fetch_daily", python_callable=step_ingest_daily)
    t_check = PythonOperator(
        task_id="check_daily_ingestion",
        python_callable=check_daily_ingestion,
    )
    t_predict = PythonOperator(task_id="run_predictions", python_callable=step_predict)
    t_export = PythonOperator(task_id="export_csv", python_callable=step_export)

    t_init >> t_fetch >> t_check >> t_predict >> t_export
