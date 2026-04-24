"""Airflow DAG - manual historical backfill."""

import logging
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config.settings import mlops_config
from dags._airflow_compat import DAG, PythonOperator

logger = logging.getLogger(__name__)

I = mlops_config.ingestion
DEFAULT_BACKFILL_START = "2008-01-01"
DEFAULT_BACKFILL_END = (date.today() - timedelta(days=1)).isoformat()

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(seconds=300),
    "email_on_failure": False,
}


def validate_backfill_config(**context):
    """Validate start_date and end_date before fetching."""
    dag_run = context.get("dag_run")
    conf = dag_run.conf if dag_run and dag_run.conf else {}
    reference_day = date.fromisoformat(context["ds"])

    start = conf.get("start_date", DEFAULT_BACKFILL_START)
    end = conf.get("end_date", (reference_day - timedelta(days=1)).isoformat())

    try:
        start_date = date.fromisoformat(start)
        end_date = date.fromisoformat(end)
    except ValueError as exc:
        raise ValueError(f"Invalid date format - expected YYYY-MM-DD. Got: {exc}") from exc

    if start_date > end_date:
        raise ValueError(f"start_date {start} is after end_date {end}")
    if end_date >= reference_day:
        raise ValueError(
            f"end_date {end} must be before execution day ({reference_day.isoformat()})"
        )

    logger.info("Backfill config validated: %s -> %s", start, end)
    context["ti"].xcom_push(key="start_date", value=start)
    context["ti"].xcom_push(key="end_date", value=end)


def run_backfill_with_config(**context):
    """Fetch historical data incrementally, repair gaps, and fail if the range is incomplete."""
    from pipeline.database import init_db
    from pipeline.run_pipeline import backfill_and_repair_date_range

    start = context["ti"].xcom_pull(
        task_ids="validate_backfill_config",
        key="start_date",
    ) or DEFAULT_BACKFILL_START
    end = context["ti"].xcom_pull(
        task_ids="validate_backfill_config",
        key="end_date",
    ) or DEFAULT_BACKFILL_END

    logger.info("Backfill requested: %s -> %s", start, end)

    init_db()
    summary = backfill_and_repair_date_range(
        start,
        end,
        force=False,
        delay_seconds=I.backfill_delay_seconds,
    )
    context["ti"].xcom_push(key="backfill_summary", value=summary)

    if summary["failed_cities"] or summary["failed_ranges"] or summary["remaining_gap_counts"]:
        failed_cities = ", ".join(sorted(summary["failed_cities"])) or "none"
        failed_ranges = ", ".join(sorted(summary["failed_ranges"])) or "none"
        remaining = ", ".join(
            f"{city}:{count}" for city, count in sorted(summary["remaining_gap_counts"].items())
        ) or "none"
        raise RuntimeError(
            "Backfill incomplete - "
            f"failed cities: {failed_cities}; "
            f"failed repair ranges: {failed_ranges}; "
            f"remaining gap days: {remaining}. "
            "Persisted progress was kept; rerun the task to resume."
        )

    logger.info("Backfill stored: %d rows", summary["stored_rows"])


def run_train(**context):
    from pipeline.run_pipeline import step_train

    step_train()


def run_predict(**context):
    from pipeline.run_pipeline import step_predict

    step_predict()


def run_export(**context):
    from pipeline.run_pipeline import step_export

    step_export()


def trigger_monitoring_after_backfill(**_):
    logger.info(
        "Backfill complete. Run weather_daily_monitoring manually to refresh "
        "drift_report.json, model_metrics.json, and monitoring_decision.json."
    )


with DAG(
    dag_id="weather_backfill",
    description="Manual backfill: validate config -> fetch -> retrain -> predict -> export",
    schedule=None,
    start_date=datetime(2026, 4, 22),
    catchup=False,
    default_args=default_args,
    tags=["weather", "backfill", "manual"],
    params={
        "start_date": DEFAULT_BACKFILL_START,
        "end_date": DEFAULT_BACKFILL_END,
    },
) as dag:
    t_validate = PythonOperator(
        task_id="validate_backfill_config",
        python_callable=validate_backfill_config,
    )
    t_fetch = PythonOperator(
        task_id="fetch_historical",
        python_callable=run_backfill_with_config,
    )
    t_train = PythonOperator(task_id="retrain_models", python_callable=run_train)
    t_predict = PythonOperator(task_id="run_predictions", python_callable=run_predict)
    t_export = PythonOperator(task_id="export_csv", python_callable=run_export)
    t_notify = PythonOperator(
        task_id="notify_run_monitoring",
        python_callable=trigger_monitoring_after_backfill,
    )

    t_validate >> t_fetch >> t_train >> t_predict >> t_export >> t_notify
