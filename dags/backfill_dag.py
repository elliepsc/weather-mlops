"""Airflow DAG - manual historical backfill.

Fixes applied:
  #1  write_last_retrain added at end of pipeline so daily monitoring
      respects the cooldown after a backfill retrain.
  #2  max_active_runs=1 prevents concurrent backfill runs (SQLite write locks).
  #5  DEFAULT_BACKFILL_END moved inside validate_backfill_config —
      was evaluated at DAG parse time, not at task runtime.
  #8  execution_timeout on all tasks. fetch_historical overridden to 8h
      (worst case: 15+ years of data). retrain overridden to 2h.
"""

import json
import logging
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config.settings import mlops_config
from dags._airflow_compat import DAG, PythonOperator, TriggerDagRunOperator

logger = logging.getLogger(__name__)

ING = mlops_config.ingestion
DEFAULT_BACKFILL_START = "2008-01-01"
LAST_RETRAIN_PATH = ROOT / "data" / "monitoring" / "last_retrain.json"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(seconds=300),
    "email_on_failure": False,
    # Conservative default — overridden per-task where needed.
    "execution_timeout": timedelta(minutes=30),
}


def validate_backfill_config(**context):
    """Validate start_date and end_date before fetching."""
    dag_run = context.get("dag_run")
    conf = dag_run.conf if dag_run and dag_run.conf else {}
    reference_day = date.fromisoformat(context["ds"])

    # Computed here (runtime), not at module level (parse time). Fix #5.
    default_end = (reference_day - timedelta(days=1)).isoformat()

    start = conf.get("start_date") or DEFAULT_BACKFILL_START
    # Treat empty string or UI placeholder "yesterday" as "use default"
    end_raw = (conf.get("end_date") or "").strip()
    end = end_raw if (end_raw and end_raw != "yesterday") else default_end

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
    """Fetch historical data incrementally, repair gaps, and fail if incomplete.

    NOTE — retrain cooldown bypass:
    This task calls step_train() directly, bypassing the cooldown guard in
    weather_weekly_train / weather_daily_monitoring. This is intentional:
    a backfill corrects the training dataset itself, so retraining on fresh
    complete data is correct behaviour. write_last_retrain runs afterwards
    to update the cooldown file so monitoring does not re-trigger a retrain.
    The baseline snapshot + rollback gate from weather_weekly_train do NOT
    run here. For large backfills, consider triggering weather_weekly_train
    manually after completion.
    """
    from pipeline.database import init_db
    from pipeline.run_pipeline import backfill_and_repair_date_range

    dag_run = context.get("dag_run")
    conf = dag_run.conf if dag_run and dag_run.conf else {}

    start = (
        context["ti"].xcom_pull(task_ids="validate_backfill_config", key="start_date")
        or DEFAULT_BACKFILL_START
    )
    end = (
        context["ti"].xcom_pull(task_ids="validate_backfill_config", key="end_date")
        or (date.today() - timedelta(days=1)).isoformat()
    )
    force = bool(conf.get("force", False))

    logger.info("Backfill requested: %s -> %s (force=%s)", start, end, force)

    init_db()
    summary = backfill_and_repair_date_range(
        start, end, force=force, delay_seconds=ING.backfill_delay_seconds
    )
    context["ti"].xcom_push(key="backfill_summary", value=summary)

    if summary["failed_cities"] or summary["failed_ranges"] or summary["remaining_gap_counts"]:
        failed_cities = ", ".join(sorted(summary["failed_cities"])) or "none"
        failed_ranges = ", ".join(sorted(summary["failed_ranges"])) or "none"
        remaining = (
            ", ".join(
                f"{city}:{count}" for city, count in sorted(summary["remaining_gap_counts"].items())
            )
            or "none"
        )
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


def write_last_retrain(**context):
    """Update cooldown file so daily monitoring doesn't trigger another retrain.

    Fix #1: backfill retrains the model but the original code never updated
    last_retrain.json, so weather_daily_monitoring would immediately queue
    another retrain the next morning.
    """
    LAST_RETRAIN_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "last_retrain": context["ds"],
        "written_at": datetime.utcnow().isoformat(timespec="seconds"),
        "source": "weather_backfill",
    }
    LAST_RETRAIN_PATH.write_text(json.dumps(payload, indent=2))
    logger.info("Cooldown updated after backfill retrain: %s", payload["last_retrain"])


with DAG(
    dag_id="weather_backfill",
    description="Manual backfill: validate -> fetch -> retrain -> predict -> export -> monitoring",
    schedule=None,
    start_date=datetime(2026, 4, 22),
    catchup=False,
    # Fix #2: one active run at a time — prevents concurrent SQLite write locks.
    max_active_runs=1,
    default_args=default_args,
    tags=["weather", "backfill", "manual"],
    params={
        "start_date": DEFAULT_BACKFILL_START,
        "end_date": "",  # leave empty → defaults to yesterday at runtime
        "force": False,
    },
) as dag:

    t_validate = PythonOperator(
        task_id="validate_backfill_config",
        python_callable=validate_backfill_config,
        execution_timeout=timedelta(minutes=5),
    )
    t_fetch = PythonOperator(
        task_id="fetch_historical",
        python_callable=run_backfill_with_config,
        # 8h: worst case is 15+ years of multi-city data with API rate limiting.
        execution_timeout=timedelta(hours=8),
    )
    t_train = PythonOperator(
        task_id="retrain_models",
        python_callable=run_train,
        execution_timeout=timedelta(hours=2),
    )
    t_predict = PythonOperator(
        task_id="run_predictions",
        python_callable=run_predict,
        execution_timeout=timedelta(minutes=30),
    )
    t_export = PythonOperator(
        task_id="export_csv",
        python_callable=run_export,
        execution_timeout=timedelta(minutes=15),
    )
    t_cooldown = PythonOperator(
        task_id="write_last_retrain",
        python_callable=write_last_retrain,
        execution_timeout=timedelta(minutes=5),
    )
    # Triggers monitoring so drift + metrics are re-evaluated on the
    # freshly completed dataset. Replaced the former dead log-only task.
    t_monitoring = TriggerDagRunOperator(
        task_id="trigger_daily_monitoring",
        trigger_dag_id="weather_daily_monitoring",
        wait_for_completion=False,
        reset_dag_run=True,
    )

    t_validate >> t_fetch >> t_train >> t_predict >> t_export >> t_cooldown >> t_monitoring
