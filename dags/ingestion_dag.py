"""Airflow DAG - daily ingestion.

Fixes applied:
  #8  execution_timeout added to all tasks.
  #11 email_on_failure: False until SMTP is configured in airflow.cfg.
"""

import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config.settings import mlops_config, settings
from dags._airflow_compat import DAG, PythonOperator

logger = logging.getLogger(__name__)

ING = mlops_config.ingestion

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(seconds=300),
    # Set True once SMTP is configured:
    # AIRFLOW__SMTP__SMTP_HOST, AIRFLOW__SMTP__SMTP_USER, etc.
    "email_on_failure": False,
    "email": [settings.alert_email] if settings.alert_email else [],
    "execution_timeout": timedelta(minutes=20),
}


def _target_date_from_context(context):
    """Return the previous day for scheduled or manual Airflow 3 runs."""
    if context.get("ds"):
        reference = datetime.fromisoformat(context["ds"])
    else:
        dag_run = context.get("dag_run")
        reference = getattr(dag_run, "logical_date", None) or getattr(dag_run, "run_after", None)
        if reference is None:
            reference = datetime.utcnow()
    return (reference - timedelta(days=1)).date().isoformat()


def step_init_db(**kwargs):
    from pipeline.run_pipeline import step_init_db as _step_init_db
    _step_init_db()


def step_ingest_daily(**context):
    from pipeline.run_pipeline import step_ingest_daily as _step_ingest_daily

    target_date = _target_date_from_context(context)
    return _step_ingest_daily(target_date=target_date, delay_seconds=0.3)


def check_daily_ingestion(**context):
    """Fail when yesterday coverage is below the configured city threshold."""
    import pandas as pd

    from pipeline.database import get_connection
    from pipeline.locations import LOCATIONS

    yesterday = _target_date_from_context(context)
    expected = len(LOCATIONS)

    with get_connection() as conn:
        df = pd.read_sql(
            "SELECT COUNT(DISTINCT city) AS n FROM weather_raw WHERE date = ?",
            conn,
            params=(yesterday,),
        )

    n_cities = int(df["n"].iloc[0])
    logger.info("Ingestion check: %d/%d cities for %s", n_cities, expected, yesterday)

    if n_cities < ING.min_cities_threshold:
        raise ValueError(
            f"Ingestion quality FAIL: only {n_cities}/{expected} cities for {yesterday}. "
            f"Minimum required: {ING.min_cities_threshold}. Predictions blocked."
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

    t_init = PythonOperator(
        task_id="init_db",
        python_callable=step_init_db,
        execution_timeout=timedelta(minutes=5),
    )
    t_fetch = PythonOperator(
        task_id="fetch_daily",
        python_callable=step_ingest_daily,
        execution_timeout=timedelta(minutes=20),
    )
    t_check = PythonOperator(
        task_id="check_daily_ingestion",
        python_callable=check_daily_ingestion,
        execution_timeout=timedelta(minutes=5),
    )
    t_predict = PythonOperator(
        task_id="run_predictions",
        python_callable=step_predict,
        execution_timeout=timedelta(minutes=15),
    )
    t_export = PythonOperator(
        task_id="export_csv",
        python_callable=step_export,
        execution_timeout=timedelta(minutes=10),
    )

    t_init >> t_fetch >> t_check >> t_predict >> t_export
