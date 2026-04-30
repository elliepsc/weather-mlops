"""Airflow DAG - daily dbt analytics refresh.

Pipeline:
  1. wait_for_ingestion  – ExternalTaskSensor on weather_daily_ingestion.export_csv
  1. wait_for_monitoring – ExternalTaskSensor on weather_daily_monitoring (full DAG)
  2. load_sources        – sync SQLite + JSON monitoring files → DuckDB
  3. dbt_run             – dbt run (all models)
  4. dbt_test            – dbt test (schema + data tests)

Both sensors run in parallel. load_sources starts only when ingestion AND monitoring
have both completed for the same execution date. This ensures mart_mlops_health,
mart_retraining_history, and related models are built from same-day monitoring data.

Scheduled at 10:30 UTC (ingestion at 06:00, monitoring at 09:00).

Manual trigger with sensor bypass:
  Airflow UI → Trigger DAG w/ config → {"skip_sensors": true}
  Or CLI: airflow dags trigger weather_dbt_analytics --conf '{"skip_sensors": true}'
"""

# for running outside of Airflow (e.g. for testing):make analytics-all
# for debugging: airflow dags trigger weather_dbt_analytics --conf '{"skip_sensors": true}'


import logging
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).parent.parent
ANALYTICS_DIR = ROOT / "analytics"

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config.settings import settings
from dags._airflow_compat import DAG, BranchPythonOperator, ExternalTaskSensor, PythonOperator

logger = logging.getLogger(__name__)

_DBT_FLAGS = [
    "--profiles-dir",
    str(ANALYTICS_DIR),
    "--project-dir",
    str(ANALYTICS_DIR),
]

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    # Set True once SMTP is configured:
    # AIRFLOW__SMTP__SMTP_HOST, AIRFLOW__SMTP__SMTP_USER, etc.
    "email_on_failure": False,
    "email": [settings.alert_email] if settings.alert_email else [],
    "execution_timeout": timedelta(minutes=30),
}


def _check_mode(**kwargs):
    dag_run = kwargs.get("dag_run")
    skip = dag_run and dag_run.conf and dag_run.conf.get("skip_sensors", False)
    if skip:
        logger.info("skip_sensors=True — bypassing ExternalTaskSensors")
        return "load_sources"
    return ["wait_for_ingestion", "wait_for_monitoring"]


def _validate_sources(**kwargs):
    import json
    from datetime import date

    today = str(kwargs.get("ds") or date.today().isoformat())

    checks = [
        ROOT / "data" / "monitoring" / "monitoring_decision.json",
        ROOT / "data" / "monitoring" / "drift_report.json",
        ROOT / "data" / "weather.db",
    ]

    for path in checks:
        if not path.exists():
            raise FileNotFoundError(f"Source manquante avant dbt : {path}")
        if path.stat().st_size == 0:
            raise ValueError(f"Source vide avant dbt : {path}")

    # Vérifier que monitoring_decision.json est du bon jour
    decision = json.loads((ROOT / "data" / "monitoring" / "monitoring_decision.json").read_text())
    if decision.get("date") != today:
        raise ValueError(
            f"monitoring_decision.json date={decision.get('date')} != expected={today}. "
            "Le monitoring n'a peut-être pas encore tourné pour aujourd'hui."
        )


def _load_sources(**kwargs):
    from analytics.scripts.load_sources import main

    dag_run = kwargs.get("dag_run")  # noqa: F841 — kept for future use
    execution_date = str(kwargs.get("logical_date") or kwargs.get("ds") or "")
    main(dag_id="weather_dbt_analytics", execution_date=execution_date)


def _dbt_run(**kwargs):
    result = subprocess.run(
        ["dbt", "run"] + _DBT_FLAGS,
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
        ["dbt", "test"] + _DBT_FLAGS,
        cwd=str(ANALYTICS_DIR),
        capture_output=True,
        text=True,
    )
    logger.info(result.stdout)
    if result.returncode != 0:
        logger.error(result.stderr)
        raise RuntimeError(f"dbt test failed (exit {result.returncode})")


def _export_analytics(**kwargs):
    from analytics.scripts.export_powerbi import main as export_main

    export_main()


with DAG(
    dag_id="weather_dbt_analytics",
    description="Daily dbt refresh: load DuckDB sources then run and test all models",
    schedule="30 10 * * *",
    start_date=datetime(2026, 4, 29),
    catchup=False,
    default_args=default_args,
    tags=["weather", "analytics", "dbt", "daily"],
) as dag:

    t_gate = BranchPythonOperator(
        task_id="check_mode",
        python_callable=_check_mode,
        execution_timeout=timedelta(seconds=30),
    )
    t_wait_ing = ExternalTaskSensor(
        task_id="wait_for_ingestion",
        external_dag_id="weather_daily_ingestion",
        external_task_id="export_csv",
        timeout=3600,
        poke_interval=60,
        mode="reschedule",
        execution_timeout=timedelta(hours=1),
    )
    # external_task_id=None waits for the full DAG run (any terminal branch counts).
    t_wait_mon = ExternalTaskSensor(
        task_id="wait_for_monitoring",
        external_dag_id="weather_daily_monitoring",
        external_task_id=None,
        timeout=7200,
        poke_interval=60,
        mode="reschedule",
        execution_timeout=timedelta(hours=2),
    )
    t_validate = PythonOperator(
        task_id="validate_sources",
        python_callable=_validate_sources,
        retries=0,
        execution_timeout=timedelta(minutes=2),
    )
    t_load = PythonOperator(
        task_id="load_sources",
        python_callable=_load_sources,
        trigger_rule="none_failed_min_one_success",
        execution_timeout=timedelta(minutes=10),
    )
    t_run = PythonOperator(
        task_id="dbt_run",
        python_callable=_dbt_run,
        execution_timeout=timedelta(minutes=20),
    )
    # retries=0: a dbt test that fails twice on the same data won't change outcome.
    t_test = PythonOperator(
        task_id="dbt_test",
        python_callable=_dbt_test,
        retries=0,
        execution_timeout=timedelta(minutes=10),
    )
    t_export = PythonOperator(
        task_id="export_analytics_csv",
        python_callable=_export_analytics,
        retries=1,
        execution_timeout=timedelta(minutes=10),
    )

    t_gate >> [t_wait_ing, t_wait_mon] >> t_validate >> t_load
    t_gate >> t_load
    t_load >> t_run >> t_test >> t_export
