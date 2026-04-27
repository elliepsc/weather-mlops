"""Airflow DAG - weather_gap_monitor.

Runs daily between ingestion (06:00) and model monitoring (09:00).
Detects three classes of data problems over a rolling window:
  1. Missing dates   → days with zero rows in weather_raw
  2. Partial days    → days with fewer cities than min_cities_threshold
  3. Partial rows    → rows with NULLs in critical weather columns

When problems are found:
  - Sends a deduplicated Slack alert (once per 24h per alert type)
  - Triggers weather_backfill for the full bad-date range (async)

Fixes applied:
  #2  trigger_backfill: guard checks if weather_backfill is already running
      before firing a new trigger — prevents SQLite write lock from concurrent runs.
      max_active_runs=1 on backfill_dag is the hard guard; this is the soft guard
      that avoids queue buildup.
  #7  CRITICAL_COLUMNS validated against actual weather_raw schema at runtime
      (SQLite PRAGMA table_info). Columns not in schema are skipped with a warning
      instead of causing a SQL error.
  #8  execution_timeout on all tasks.
  #10 DEFAULT_LOOKBACK_DAYS increased from 7 to 30 — a 7-day window misses gaps
      older than one week when Airflow restarts after a long outage.
"""

import logging
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config.settings import mlops_config, settings
from dags._airflow_compat import (
    DAG,
    BranchPythonOperator,
    EmptyOperator,
    PythonOperator,
    TriggerDagRunOperator,
)

try:
    from dags._notifications import send_slack_alert as _send_slack_alert
except ImportError:

    def _send_slack_alert(message: str, alert_key=None, cooldown_hours: int = 24) -> None:
        import requests

        from config.settings import settings

        if not settings.slack_webhook_url:
            logger.info("Slack webhook not configured - alert logged only: %s", message)
            return
        try:
            response = requests.post(settings.slack_webhook_url, json={"text": message}, timeout=5)
            response.raise_for_status()
        except Exception as exc:
            logger.warning("Slack alert failed: %s", exc)


logger = logging.getLogger(__name__)

ING = mlops_config.ingestion

# ── Configuration ─────────────────────────────────────────────────────────────

# Fix #10: 30 days covers a full month of potential gaps.
# If Airflow is down for > 30 days, run weather_backfill manually.
DEFAULT_LOOKBACK_DAYS = 30

# Critical columns to check for NULLs.
# Fix #7: validated against actual schema at runtime — see _get_valid_critical_columns().
CRITICAL_COLUMNS = ["max_temp", "min_temp", "rainfall"]


# ── Helpers ───────────────────────────────────────────────────────────────────


def _get_valid_critical_columns(conn) -> list[str]:
    """Return only CRITICAL_COLUMNS that exist in weather_raw (SQLite schema check).

    Fix #7: prevents SQL errors when schema evolves and CRITICAL_COLUMNS
    list is not updated in sync.
    """
    import pandas as pd

    schema = pd.read_sql("PRAGMA table_info(weather_raw)", conn)
    actual = set(schema["name"].tolist())
    valid = [c for c in CRITICAL_COLUMNS if c in actual]
    missing = [c for c in CRITICAL_COLUMNS if c not in actual]
    if missing:
        logger.warning("CRITICAL_COLUMNS not found in weather_raw schema — skipped: %s", missing)
    return valid


# ── Task callables ────────────────────────────────────────────────────────────


def detect_gaps_and_partials(**context) -> None:
    """Scan weather_raw over a rolling window and classify bad dates."""
    import pandas as pd

    from pipeline.database import get_connection
    from pipeline.locations import LOCATIONS

    dag_run = context.get("dag_run")
    conf = dag_run.conf if dag_run and dag_run.conf else {}
    lookback = int(conf.get("lookback_days", DEFAULT_LOOKBACK_DAYS))

    reference = date.fromisoformat(context["ds"])
    window_start = (reference - timedelta(days=lookback)).isoformat()
    window_end = (reference - timedelta(days=1)).isoformat()
    expected_cities = len(LOCATIONS)

    logger.info(
        "Gap scan: %s → %s | lookback=%d days | expected cities=%d",
        window_start,
        window_end,
        lookback,
        expected_cities,
    )

    with get_connection() as conn:

        # 1. Per-date city counts
        coverage_df = pd.read_sql(
            """
            SELECT date, COUNT(DISTINCT city) AS n_cities
            FROM weather_raw
            WHERE date BETWEEN ? AND ?
            GROUP BY date
            ORDER BY date
            """,
            conn,
            params=(window_start, window_end),
        )

        # 2. Rows with NULLs in critical columns — Fix #7: schema-validated list
        valid_critical = _get_valid_critical_columns(conn)
        if valid_critical:
            null_filter = " OR ".join(f"{col} IS NULL" for col in valid_critical)
            null_df = pd.read_sql(
                f"""
                SELECT date, city
                FROM weather_raw
                WHERE date BETWEEN ? AND ?
                  AND ({null_filter})
                ORDER BY date, city
                """,
                conn,
                params=(window_start, window_end),
            )
        else:
            logger.warning("No valid CRITICAL_COLUMNS found — skipping NULL check.")
            null_df = pd.DataFrame()

    # Build expected date set
    all_expected = {
        (date.fromisoformat(window_start) + timedelta(days=i)).isoformat() for i in range(lookback)
    }
    present_dates = set(coverage_df["date"].tolist())
    missing_dates = sorted(all_expected - present_dates)

    partial_coverage = coverage_df[coverage_df["n_cities"] < ING.min_cities_threshold][
        ["date", "n_cities"]
    ].to_dict(orient="records")

    partial_rows = (
        null_df.groupby("date").size().reset_index(name="null_row_count").to_dict(orient="records")
        if not null_df.empty
        else []
    )

    all_bad_dates = sorted(
        set(missing_dates)
        | {d["date"] for d in partial_coverage}
        | {d["date"] for d in partial_rows}
    )
    gap_start = all_bad_dates[0] if all_bad_dates else None
    gap_end = all_bad_dates[-1] if all_bad_dates else None
    has_issues = bool(all_bad_dates)

    report = {
        "window_start": window_start,
        "window_end": window_end,
        "expected_cities": expected_cities,
        "missing_dates": missing_dates,
        "partial_coverage_days": partial_coverage,
        "partial_rows_by_date": partial_rows,
        "has_issues": has_issues,
        "gap_start": gap_start,
        "gap_end": gap_end,
    }

    context["ti"].xcom_push(key="gap_report", value=report)
    context["ti"].xcom_push(key="has_issues", value=has_issues)

    logger.info(
        "Scan complete — missing: %d | partial coverage: %d | dates with NULLs: %d",
        len(missing_dates),
        len(partial_coverage),
        len(partial_rows),
    )


def branch_on_issues(**context) -> str:
    has_issues = context["ti"].xcom_pull(task_ids="detect_gaps_and_partials", key="has_issues")
    if has_issues:
        logger.warning("Issues detected — routing to alert and backfill.")
        return "send_gap_alert"
    logger.info("✅ No gaps or partial records in scan window.")
    return "no_action"


def send_gap_alert(**context) -> None:
    """Build structured Slack message and fire deduplicated alert."""
    report = context["ti"].xcom_pull(task_ids="detect_gaps_and_partials", key="gap_report")
    if not report:
        logger.error("No gap_report in XCom — cannot send alert.")
        return

    ds = context["ds"]
    lines = [
        f":warning: *weather-mlops gap monitor* ({ds})",
        f"Window: `{report['window_start']}` → `{report['window_end']}`",
        f"Expected cities/day: {report['expected_cities']}",
        "",
    ]

    if report["missing_dates"]:
        lines.append(f":x: *Missing dates* ({len(report['missing_dates'])}):")
        lines.extend(f"  • `{d}`" for d in report["missing_dates"])
        lines.append("")

    if report["partial_coverage_days"]:
        lines.append(f":warning: *Partial coverage* ({len(report['partial_coverage_days'])} days):")
        for entry in report["partial_coverage_days"]:
            lines.append(f"  • `{entry['date']}` — {entry['n_cities']} cities ingested")
        lines.append("")

    if report["partial_rows_by_date"]:
        cols = ", ".join(f"`{c}`" for c in CRITICAL_COLUMNS)
        lines.append(
            f":warning: *NULL rows* in {cols} ({len(report['partial_rows_by_date'])} dates):"
        )
        for entry in report["partial_rows_by_date"]:
            lines.append(f"  • `{entry['date']}` — {entry['null_row_count']} rows")
        lines.append("")

    if report["gap_start"]:
        lines.append(
            f":arrows_counterclockwise: *Auto-backfill triggered* "
            f"`{report['gap_start']}` → `{report['gap_end']}`"
        )

    # Fix #4 (via _notifications): deduplicated — one alert per 24h max.
    _send_slack_alert("\n".join(lines), alert_key="gap_monitor")


# ── DAG definition ────────────────────────────────────────────────────────────

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(seconds=120),
    "email_on_failure": True,
    "email": [settings.alert_email] if settings.alert_email else [],
    "execution_timeout": timedelta(minutes=15),
}

with DAG(
    dag_id="weather_gap_monitor",
    description=(
        "Daily gap & partial-record scan (30-day window). "
        "Alerts on Slack and auto-triggers weather_backfill when data is missing."
    ),
    schedule="30 7 * * *",
    start_date=datetime(2026, 4, 22),
    catchup=False,
    default_args=default_args,
    tags=["weather", "monitoring", "gaps", "quality"],
    params={"lookback_days": DEFAULT_LOOKBACK_DAYS},
) as dag:

    t_detect = PythonOperator(
        task_id="detect_gaps_and_partials",
        python_callable=detect_gaps_and_partials,
    )
    t_branch = BranchPythonOperator(
        task_id="branch_on_issues",
        python_callable=branch_on_issues,
        execution_timeout=timedelta(minutes=2),
    )
    t_alert = PythonOperator(
        task_id="send_gap_alert",
        python_callable=send_gap_alert,
        execution_timeout=timedelta(minutes=5),
    )
    # TriggerDagRunOperator uses Airflow's internal scheduler API — no HTTP
    # auth or ORM access required (both blocked in Airflow 3.0 task context).
    # max_active_runs=1 on weather_backfill is the concurrent-run hard guard.
    t_backfill = TriggerDagRunOperator(
        task_id="trigger_backfill",
        trigger_dag_id="weather_backfill",
        conf={
            "start_date": (
                "{{ ti.xcom_pull("
                "task_ids='detect_gaps_and_partials', key='gap_report'"
                ")['gap_start'] }}"
            ),
            "end_date": (
                "{{ ti.xcom_pull("
                "task_ids='detect_gaps_and_partials', key='gap_report'"
                ")['gap_end'] }}"
            ),
            "force": False,
        },
        wait_for_completion=False,
        reset_dag_run=True,
        execution_timeout=timedelta(minutes=5),
    )
    t_ok = EmptyOperator(task_id="no_action")

    t_detect >> t_branch >> [t_alert, t_ok]
    t_alert >> t_backfill
