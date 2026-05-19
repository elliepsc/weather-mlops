"""Airflow DAG - daily dbt analytics refresh.

Pipeline (normal schedule):
  1. check_mode          – branch: sensors si schedule, bypass si skip_sensors=True
  2. wait_for_ingestion  – ExternalTaskSensor on weather_daily_ingestion.export_csv
  2. wait_for_monitoring – ExternalTaskSensor on weather_daily_monitoring (full DAG)
  3. sensors_join        – EmptyOperator : point de convergence sensors / bypass
  4. validate_sources    – vérifie présence + fraîcheur des fichiers avant chargement
  5. load_sources        – sync SQLite + JSON monitoring files → DuckDB
  6. dbt_run             – dbt run --select <run_select from pipeline.yml>
  7. dbt_test            – dbt test --select <test_select from pipeline.yml>
  8. export_analytics_csv – export DuckDB mart tables → data/analytics/*.csv

Both sensors run in parallel. load_sources starts only when ingestion AND monitoring
have both completed for the same execution date. This ensures mart_mlops_health,
mart_retraining_history, and related models are built from same-day monitoring data.

Scheduled at 10:30 UTC (ingestion at 06:00, monitoring at 09:00).

Pipeline config lives in analytics/pipeline.yml — edit that file to control
which models are built, tested, and exported. No DAG code change needed.

── Dev / debug bypass ────────────────────────────────────────────────────────
Trigger manual sans attendre les DAGs upstream :

  Airflow UI : Trigger DAG w/ config → {"skip_sensors": true}
  CLI        : airflow dags trigger weather_dbt_analytics --conf '{"skip_sensors": true}'

NE PAS utiliser skip_sensors en production / schedule automatique.
Pour une passe locale complète préférer : make analytics-all
──────────────────────────────────────────────────────────────────────────────
"""

import json
import logging
import os
import subprocess
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import yaml

ROOT = Path(__file__).parent.parent
ANALYTICS_DIR = ROOT / "analytics"
MONITORING_DIR = ROOT / "data" / "monitoring"
PIPELINE_YML = ANALYTICS_DIR / "pipeline.yml"

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config.settings import settings
from dags._airflow_compat import (
    DAG,
    BranchPythonOperator,
    EmptyOperator,
    ExternalTaskSensor,
    PythonOperator,
    TriggerRule,
)

logger = logging.getLogger(__name__)

_DBT_FLAGS = [
    "--profiles-dir",
    str(ANALYTICS_DIR),
    "--project-dir",
    str(ANALYTICS_DIR),
    "--no-partial-parse",
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


# ── Pipeline config ───────────────────────────────────────────────────────────


def _pipeline_config() -> dict:
    """Return the active pipeline's config block from analytics/pipeline.yml."""
    pipeline = os.getenv("ANALYTICS_PIPELINE", "daily_bi")
    config = yaml.safe_load(PIPELINE_YML.read_text())
    pipelines = config.get("pipelines", {})
    if pipeline not in pipelines:
        available = ", ".join(pipelines.keys())
        raise KeyError(f"Pipeline {pipeline!r} not in pipeline.yml. Available: {available}")
    return pipelines[pipeline]


# ── Gate ──────────────────────────────────────────────────────────────────────


def _check_mode(**kwargs):
    """Branch vers les sensors (schedule normal) ou bypass direct (debug manuel)."""
    dag_run = kwargs.get("dag_run")
    skip = dag_run and dag_run.conf and dag_run.conf.get("skip_sensors", False)
    if skip:
        logger.info(
            "skip_sensors=True — bypass ExternalTaskSensors, passage direct à sensors_join"
        )
        return "sensors_join"
    return ["wait_for_ingestion", "wait_for_monitoring"]


# ── Validation ────────────────────────────────────────────────────────────────


def _validate_sources(**kwargs):
    """Vérifie que les fichiers sources sont présents et datés du bon jour avant dbt."""
    today = str(kwargs.get("ds") or date.today().isoformat())

    required = [
        ROOT / "data" / "weather.db",
        MONITORING_DIR / "monitoring_decision.json",
        MONITORING_DIR / "drift_report.json",
    ]
    for path in required:
        if not path.exists():
            raise FileNotFoundError(f"Source manquante avant dbt : {path}")
        if path.stat().st_size == 0:
            raise ValueError(f"Source vide avant dbt : {path}")

    decision_path = MONITORING_DIR / "monitoring_decision.json"
    decision = json.loads(decision_path.read_text())
    decision_date = decision.get("date")
    if decision_date != today:
        raise ValueError(
            f"monitoring_decision.json date={decision_date!r} != attendu={today!r}. "
            "Le monitoring n'a peut-être pas encore tourné pour aujourd'hui, "
            "ou un retry a écrit une décision d'un jour précédent."
        )

    logger.info("validate_sources OK — monitoring_decision date=%s", decision_date)


# ── Sources ───────────────────────────────────────────────────────────────────


def _load_sources(**kwargs):
    from analytics.scripts.load_sources import main

    dag_run = kwargs.get("dag_run")  # noqa: F841 — kept for future use
    execution_date = str(kwargs.get("logical_date") or kwargs.get("ds") or "")
    main(dag_id="weather_dbt_analytics", execution_date=execution_date)


# ── dbt ───────────────────────────────────────────────────────────────────────


def _dbt_run(**kwargs):
    cfg = _pipeline_config()
    selects = cfg.get("run_select", [])
    if isinstance(selects, str):
        selects = [selects]

    select_args: list[str] = []
    for s in selects:
        select_args += ["--select", s]

    result = subprocess.run(
        ["dbt", "run"] + select_args + _DBT_FLAGS,
        cwd=str(ANALYTICS_DIR),
        capture_output=True,
        text=True,
        env={**os.environ, "ANALYTICS_DB_PATH": str(ROOT / "data" / "analytics.duckdb")},
    )
    logger.info(result.stdout)
    if result.returncode != 0:
        logger.error(result.stderr)
        raise RuntimeError(f"dbt run failed (exit {result.returncode})")


def _dbt_test(**kwargs):
    cfg = _pipeline_config()
    selects = cfg.get("test_select", [])
    if isinstance(selects, str):
        selects = [selects]

    select_args: list[str] = []
    for s in selects:
        select_args += ["--select", s]

    result = subprocess.run(
        ["dbt", "test"] + select_args + _DBT_FLAGS,
        cwd=str(ANALYTICS_DIR),
        capture_output=True,
        text=True,
        env={**os.environ, "ANALYTICS_DB_PATH": str(ROOT / "data" / "analytics.duckdb")},
    )
    logger.info(result.stdout)
    if result.returncode != 0:
        logger.error(result.stderr)
        raise RuntimeError(f"dbt test failed (exit {result.returncode})")


# ── Export ────────────────────────────────────────────────────────────────────


def _export_analytics(**_):
    from analytics.scripts.export_powerbi import main as export_main

    export_main()


def _export_gcs(**_):
    if os.getenv("GCS_ENABLED", "false").lower() != "true":
        logger.info("GCS_ENABLED=false — skip")
        return
    from pipeline.export_to_gcs import main

    main()


# ── DAG ───────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="weather_dbt_analytics",
    description="Daily dbt refresh: load DuckDB sources, run models, test, export CSV",
    schedule="30 10 * * *",
    start_date=datetime(2026, 4, 29),
    catchup=False,
    default_args=default_args,
    tags=["weather", "analytics", "dbt", "daily"],
) as dag:

    # Entrée : schedule normal → sensors, trigger manuel → bypass
    t_gate = BranchPythonOperator(
        task_id="check_mode",
        python_callable=_check_mode,
        execution_timeout=timedelta(seconds=30),
    )

    # Sensors (chemin normal uniquement)
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

    # Point de convergence : sensors (chemin normal) ou bypass (skip_sensors=True).
    # none_failed_min_one_success : s'active dès qu'un chemin upstream a réussi,
    # même si l'autre est en état skipped.
    t_join = EmptyOperator(
        task_id="sensors_join",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
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
        execution_timeout=timedelta(minutes=10),
    )

    t_run = PythonOperator(
        task_id="dbt_run",
        python_callable=_dbt_run,
        retries=0,
        execution_timeout=timedelta(minutes=30),
    )

    t_test = PythonOperator(
        task_id="dbt_test",
        python_callable=_dbt_test,
        retries=0,
        execution_timeout=timedelta(minutes=15),
    )

    t_export = PythonOperator(
        task_id="export_analytics_csv",
        python_callable=_export_analytics,
        retries=1,
        execution_timeout=timedelta(minutes=10),
    )

    # Optional GCS export — runs regardless of t_export outcome, never blocks the DAG.
    t_export_gcs = PythonOperator(
        task_id="export_gcs_parquet",
        python_callable=_export_gcs,
        trigger_rule=TriggerRule.ALL_DONE,
        retries=1,
        execution_timeout=timedelta(minutes=20),
    )

    # ── Dépendances ───────────────────────────────────────────────────────────
    #
    # Schedule normal :
    #   check_mode → [wait_for_ingestion, wait_for_monitoring] → sensors_join
    #
    # Bypass debug (skip_sensors=True) :
    #   check_mode → sensors_join  (sensors skipped)
    #
    # Suite commune :
    #   sensors_join → validate_sources → load_sources → dbt_run → dbt_test
    #   → export_analytics_csv → export_gcs_parquet (ALL_DONE, non-blocking)

    t_gate >> [t_wait_ing, t_wait_mon] >> t_join
    t_gate >> t_join
    t_join >> t_validate >> t_load >> t_run >> t_test >> t_export >> t_export_gcs
