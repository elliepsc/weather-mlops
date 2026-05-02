"""Airflow DAG - daily dbt analytics refresh.

Pipeline (normal schedule):
  1. check_mode          – branch: sensors si schedule, bypass si skip_sensors=True
  2. wait_for_ingestion  – ExternalTaskSensor on weather_daily_ingestion.export_csv
  2. wait_for_monitoring – ExternalTaskSensor on weather_daily_monitoring (full DAG)
  3. validate_sources    – vérifie présence + fraîcheur des fichiers avant chargement
  4. load_sources        – sync SQLite + JSON monitoring files → DuckDB
  5. dbt_run             – dbt run (all models)
  6. dbt_test            – dbt test (schema + data tests)
  7. export_analytics_csv – export marts DuckDB → data/analytics/*.csv

Both sensors run in parallel. load_sources starts only when ingestion AND monitoring
have both completed for the same execution date. This ensures mart_mlops_health,
mart_retraining_history, and related models are built from same-day monitoring data.

Scheduled at 10:30 UTC (ingestion at 06:00, monitoring at 09:00).

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
from datetime import UTC, date, datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).parent.parent
ANALYTICS_DIR = ROOT / "analytics"
MONITORING_DIR = ROOT / "data" / "monitoring"

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config.settings import settings
from dags._airflow_compat import (
    DAG,
    BranchPythonOperator,
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


# ── Gate ──────────────────────────────────────────────────────────────────────


def _check_mode(**kwargs):
    """Branch vers les sensors (schedule normal) ou bypass direct (debug manuel)."""
    dag_run = kwargs.get("dag_run")
    skip = dag_run and dag_run.conf and dag_run.conf.get("skip_sensors", False)
    if skip:
        logger.info(
            "skip_sensors=True — bypass ExternalTaskSensors, passage direct à validate_sources"
        )
        return "validate_sources"
    return ["wait_for_ingestion", "wait_for_monitoring"]


# ── Validation ────────────────────────────────────────────────────────────────


def _validate_sources(**kwargs):
    """Vérifie que les fichiers sources sont présents et datés du bon jour avant dbt."""
    today = str(kwargs.get("ds") or date.today().isoformat())

    # 1. Fichiers obligatoires
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

    # 2. monitoring_decision.json doit être du bon jour
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


# ── Export ────────────────────────────────────────────────────────────────────


def _export_analytics(**kwargs):
    from analytics.scripts.export_powerbi import main as export_main

    export_main()


# ── Git push ──────────────────────────────────────────────────────────────────


MART_CSVS = [
    "mart_forecast_vs_actual_timeline.csv",
    "mart_model_performance_overview.csv",
    "mart_model_performance_by_city.csv",
    "mart_mlops_health.csv",
    "mart_retraining_history.csv",
]


def _git_push_analytics(**kwargs):
    """Commit et push les mart_*.csv de data/analytics/ vers GitHub.

    Erreurs non bloquantes : la tâche logue et retourne sans lever d'exception
    pour ne pas impacter le DAG quand le push échoue (réseau, token absent…).
    """
    now = datetime.now(UTC)

    try:
        gh_token = os.getenv("GH_TOKEN", "")

        if not gh_token:
            logger.warning(
                "git push analytics skipped — GH_TOKEN absent "
                "(définir dans .env ou variable Airflow GH_TOKEN)"
            )
            return

        analytics_dir = ROOT / "data" / "analytics"
        csv_paths = [analytics_dir / name for name in MART_CSVS if (analytics_dir / name).exists()]

        if not csv_paths:
            logger.warning(
                "git push analytics skipped — aucun mart_*.csv trouvé dans %s", analytics_dir
            )
            return

        # Stage uniquement les mart_*.csv connus
        subprocess.run(
            ["git", "add", "--"] + [str(p) for p in csv_paths],
            cwd=str(ROOT),
            check=True,
            capture_output=True,
        )

        # Vérifier si le staging a produit des changements
        cached_check = subprocess.run(
            ["git", "diff", "--quiet", "--cached"],
            cwd=str(ROOT),
            capture_output=True,
        )
        if cached_check.returncode == 0:
            logger.info("git push analytics skipped — aucun changement dans les mart_*.csv")
            return

        commit_msg = (
            f"chore: update analytics exports [skip ci] - {now.strftime('%Y-%m-%d %H:%M')} UTC"
        )
        subprocess.run(
            ["git", "commit", "-m", commit_msg],
            cwd=str(ROOT),
            check=True,
            capture_output=True,
        )

        # Construire l'URL authentifiée (token injecté, jamais loggué)
        remote_url = subprocess.run(
            ["git", "remote", "get-url", "origin"],
            cwd=str(ROOT),
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()

        if remote_url.startswith("https://"):
            authed_url = remote_url.replace("https://", f"https://{gh_token}@", 1)
        else:
            authed_url = remote_url

        push_result = subprocess.run(
            ["git", "push", authed_url, "HEAD:main"],
            cwd=str(ROOT),
            capture_output=True,
            text=True,
        )
        if push_result.returncode != 0:
            logger.error(
                "git push failed (exit %d): %s", push_result.returncode, push_result.stderr
            )
            return

        sha = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=str(ROOT),
            capture_output=True,
            text=True,
        ).stdout.strip()

        for p in csv_paths:
            size_kb = p.stat().st_size / 1024
            logger.info("  pushed %s (%.1f KB)", p.name, size_kb)

        logger.info(
            "git push analytics done — %d fichiers, SHA=%s, timestamp=%s UTC",
            len(csv_paths),
            sha,
            now.strftime("%Y-%m-%d %H:%M"),
        )

    except Exception as exc:
        logger.error("git push analytics error (non-bloquant) : %s", exc)


# ── DAG ───────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="weather_dbt_analytics",
    description="Daily dbt refresh: load DuckDB sources then run and test all models",
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

    # Validation des fichiers sources avant tout chargement.
    # trigger_rule="none_failed_min_one_success" : s'active dès qu'une tâche
    # upstream a réussi (sensors OU bypass direct depuis check_mode).
    t_validate = PythonOperator(
        task_id="validate_sources",
        python_callable=_validate_sources,
        trigger_rule="none_failed_min_one_success",
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
        execution_timeout=timedelta(minutes=20),
    )

    # retries=0 : un test dbt qui échoue deux fois sur les mêmes données
    # ne donnera pas un résultat différent.
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

    # trigger_rule=ALL_DONE : s'exécute même si t_export a échoué,
    # pour ne pas bloquer le DAG. La fonction logue les erreurs sans lever.
    t_git_push = PythonOperator(
        task_id="git_push_analytics",
        python_callable=_git_push_analytics,
        trigger_rule=TriggerRule.ALL_DONE,
        retries=0,
        execution_timeout=timedelta(minutes=5),
    )

    # ── Dépendances ───────────────────────────────────────────────────────────
    #
    # Schedule normal :
    #   check_mode → [wait_for_ingestion, wait_for_monitoring] → validate_sources
    #
    # Bypass debug (skip_sensors=True) :
    #   check_mode → validate_sources
    #
    # Suite commune :
    #   validate_sources → load_sources → dbt_run → dbt_test → export_analytics_csv
    #   → git_push_analytics

    t_gate >> [t_wait_ing, t_wait_mon] >> t_validate
    t_gate >> t_validate
    t_validate >> t_load >> t_run >> t_test >> t_export >> t_git_push
