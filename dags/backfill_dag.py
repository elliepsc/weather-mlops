"""Airflow DAG - manual historical backfill.

Fixes applied:
  #2  max_active_runs=1 prevents concurrent backfill runs (SQLite write locks).
  #5  DEFAULT_BACKFILL_END moved inside validate_backfill_config —
      was evaluated at DAG parse time, not at task runtime.
  #8  execution_timeout on all tasks. fetch_historical overridden to 8h
      (worst case: 15+ years of data).

Design: raw data repair only. When new rows are stored, retrain is delegated to
weather_weekly_train (snapshot → retrain → compare/rollback → predict → export
→ cooldown). A single implementation of the safe retrain path; no duplication.
"""

import logging
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config.settings import mlops_config
from dags._airflow_compat import DAG, PythonOperator, ShortCircuitOperator, TriggerDagRunOperator

logger = logging.getLogger(__name__)

ING = mlops_config.ingestion
DEFAULT_BACKFILL_START = "2008-01-01"

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
    """Fetch historical data incrementally, repair gaps, and fail if incomplete."""
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
    repair_gaps = bool(conf.get("repair_gaps", True))

    logger.info(
        "Backfill requested: %s -> %s (force=%s, repair_gaps=%s)", start, end, force, repair_gaps
    )

    init_db()
    summary = backfill_and_repair_date_range(
        start, end, force=force, repair_gaps=repair_gaps, delay_seconds=ING.backfill_delay_seconds
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


def check_new_rows(**context):
    """Skip retrain/predict/export if backfill stored nothing new."""
    summary = context["ti"].xcom_pull(task_ids="fetch_historical", key="backfill_summary") or {}
    stored = summary.get("stored_rows", 0)
    if stored == 0:
        logger.info("No new rows stored — skipping retrain and downstream tasks.")
    return stored > 0


def step_sync_to_postgres(**kwargs):
    from pipeline.sync_to_postgres import sync_to_postgres

    sync_to_postgres()


with DAG(
    dag_id="weather_backfill",
    description="Manual backfill: validate -> fetch -> trigger weekly train -> monitoring",
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
        "repair_gaps": True,
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
    t_skip_check = ShortCircuitOperator(
        task_id="check_new_rows",
        python_callable=check_new_rows,
        execution_timeout=timedelta(minutes=2),
    )
    # Delegates snapshot + retrain + compare/rollback + predict + export + cooldown
    # to weather_weekly_train. wait_for_completion=True so monitoring fires only
    # after the full safe retrain path completes.
    t_trigger_train = TriggerDagRunOperator(
        task_id="trigger_weekly_train",
        trigger_dag_id="weather_weekly_train",
        wait_for_completion=True,
        conf={"source": "backfill"},
        reset_dag_run=True,
        # snapshot(15m) + retrain(2h) + compare(10m) + predict(30m) + export(15m)
        execution_timeout=timedelta(hours=4),
    )
    t_sync = PythonOperator(
        task_id="sync_to_postgres",
        python_callable=step_sync_to_postgres,
        execution_timeout=timedelta(minutes=10),
    )
    t_monitoring = TriggerDagRunOperator(
        task_id="trigger_daily_monitoring",
        trigger_dag_id="weather_daily_monitoring",
        wait_for_completion=False,
        reset_dag_run=True,
    )

    t_validate >> t_fetch >> t_skip_check >> t_trigger_train >> t_sync >> t_monitoring
