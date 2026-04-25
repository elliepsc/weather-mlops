"""Airflow DAG - weather_gap_monitor.

Runs daily between ingestion (06:00) and model monitoring (09:00).
Detects three classes of data problems over a rolling window:
  1. Missing dates   → days with zero rows in weather_raw
  2. Partial days    → days with fewer cities than min_cities_threshold
  3. Partial rows    → rows with NULLs in critical weather columns

When problems are found:
  - Sends a Slack alert via dags._notifications (shared with all DAGs)
  - Triggers weather_backfill for the full bad-date range (async)

Design notes:
  - BranchPythonOperator routes to no_action when the window is clean
    (no alert fired, no backfill triggered — silent green run)
  - Backfill is triggered with force=False: dates already fully present
    are skipped by backfill_and_repair_date_range
  - trigger_dag uses the Airflow internal API for dynamic conf (gap dates
    are computed at runtime and cannot be set statically in TriggerDagRunOperator)
"""

import logging
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config.settings import mlops_config
from dags._airflow_compat import DAG, BranchPythonOperator, EmptyOperator, PythonOperator

try:
    from dags._notifications import send_slack_alert as _send_slack_alert
except ImportError:
    def _send_slack_alert(message: str) -> None:
        import requests
        from config.settings import settings

        if not settings.slack_webhook_url:
            logger.info("Slack webhook not configured - alert logged only: %s", message)
            return
        try:
            response = requests.post(
                settings.slack_webhook_url, json={"text": message}, timeout=5
            )
            response.raise_for_status()
        except Exception as exc:
            logger.warning("Slack alert failed: %s", exc)

logger = logging.getLogger(__name__)

I = mlops_config.ingestion

# ── Configuration ─────────────────────────────────────────────────────────────

# How many days back to scan. 7 covers weekend gaps + Monday failures.
# Override via DAG conf for ad-hoc full audits.
DEFAULT_LOOKBACK_DAYS = 7

# Columns that must not contain NULLs in weather_raw.
# Adjust to your actual schema.
CRITICAL_COLUMNS = ["temperature_2m_max", "temperature_2m_min", "precipitation_sum"]


# ── Task callables ────────────────────────────────────────────────────────────

def detect_gaps_and_partials(**context) -> None:
    """Scan weather_raw over a rolling window and classify bad dates.

    Pushes to XCom:
      gap_report (dict) — full structured report
      has_issues (bool) — True when any problem was found
    """
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
        window_start, window_end, lookback, expected_cities,
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

        # 2. Rows with NULLs in critical columns
        null_filter = " OR ".join(f"{col} IS NULL" for col in CRITICAL_COLUMNS)
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

    # Build expected date set
    all_expected = {
        (date.fromisoformat(window_start) + timedelta(days=i)).isoformat()
        for i in range(lookback)
    }
    present_dates = set(coverage_df["date"].tolist())
    missing_dates = sorted(all_expected - present_dates)

    partial_coverage = (
        coverage_df[coverage_df["n_cities"] < I.min_cities_threshold]
        [["date", "n_cities"]]
        .to_dict(orient="records")
    )

    partial_rows = (
        null_df.groupby("date")
        .size()
        .reset_index(name="null_row_count")
        .to_dict(orient="records")
        if not null_df.empty
        else []
    )

    # Compute consolidated bad-date range for backfill
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
    """Route to alert+backfill or no_action."""
    has_issues = context["ti"].xcom_pull(
        task_ids="detect_gaps_and_partials", key="has_issues"
    )
    if has_issues:
        logger.warning("Issues detected — routing to alert and backfill.")
        return "send_gap_alert"
    logger.info("✅ No gaps or partial records in scan window — no action needed.")
    return "no_action"


def send_gap_alert(**context) -> None:
    """Build a structured Slack message and fire it."""
    report = context["ti"].xcom_pull(
        task_ids="detect_gaps_and_partials", key="gap_report"
    )
    if not report:
        logger.error("No gap_report in XCom — cannot send alert.")
        return

    ds = context["ds"]
    lines = [
        f":warning: *weather-rain gap monitor* ({ds})",
        f"Window: `{report['window_start']}` → `{report['window_end']}`",
        f"Expected cities/day: {report['expected_cities']}",
        "",
    ]

    if report["missing_dates"]:
        lines.append(f":x: *Missing dates* ({len(report['missing_dates'])}):")
        lines.extend(f"  • `{d}`" for d in report["missing_dates"])
        lines.append("")

    if report["partial_coverage_days"]:
        lines.append(
            f":warning: *Partial city coverage* ({len(report['partial_coverage_days'])} days):"
        )
        for entry in report["partial_coverage_days"]:
            lines.append(
                f"  • `{entry['date']}` — {entry['n_cities']} cities ingested"
            )
        lines.append("")

    if report["partial_rows_by_date"]:
        cols = ", ".join(f"`{c}`" for c in CRITICAL_COLUMNS)
        lines.append(
            f":warning: *Rows with NULLs* in {cols} "
            f"({len(report['partial_rows_by_date'])} dates):"
        )
        for entry in report["partial_rows_by_date"]:
            lines.append(
                f"  • `{entry['date']}` — {entry['null_row_count']} affected rows"
            )
        lines.append("")

    if report["gap_start"]:
        lines.append(
            f":arrows_counterclockwise: *Auto-backfill triggered* "
            f"for `{report['gap_start']}` → `{report['gap_end']}`"
        )

    _send_slack_alert("\n".join(lines))


def trigger_backfill(**context) -> None:
    """Trigger weather_backfill with the detected gap range.

    Uses the Airflow internal API to pass dynamic conf (gap dates computed
    at runtime). TriggerDagRunOperator cannot be used here because conf must
    be static at parse time.
    """
    report = context["ti"].xcom_pull(
        task_ids="detect_gaps_and_partials", key="gap_report"
    )
    if not report or not report.get("gap_start"):
        logger.info("No gap range to backfill — skipping.")
        return

    gap_start = report["gap_start"]
    gap_end = report["gap_end"]
    logger.info("Triggering weather_backfill: %s → %s", gap_start, gap_end)

    try:
        from airflow.api.common.trigger_dag import trigger_dag

        trigger_dag(
            dag_id="weather_backfill",
            conf={
                "start_date": gap_start,
                "end_date": gap_end,
                "force": False,
            },
            replace_microseconds=False,
        )
        logger.info("✅ weather_backfill triggered for %s → %s", gap_start, gap_end)

    except ImportError:
        logger.warning(
            "Airflow API not available (local mode?). Trigger manually:\n"
            "  airflow dags trigger weather_backfill "
            "--conf '{\"start_date\": \"%s\", \"end_date\": \"%s\"}'",
            gap_start,
            gap_end,
        )
    except Exception as exc:  # noqa: BLE001
        # Do not fail the DAG — the alert was already sent.
        logger.error("Failed to trigger weather_backfill: %s", exc)


# ── DAG definition ────────────────────────────────────────────────────────────

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(seconds=120),
    "email_on_failure": False,
}

with DAG(
    dag_id="weather_gap_monitor",
    description=(
        "Daily gap & partial-record scan. "
        "Alerts on Slack and auto-triggers weather_backfill when data is missing."
    ),
    # 07:30 — after ingestion (06:00), before model monitoring (09:00).
    schedule="30 7 * * *",
    start_date=datetime(2026, 4, 22),
    catchup=False,
    default_args=default_args,
    tags=["weather", "monitoring", "gaps", "quality"],
    params={
        "lookback_days": DEFAULT_LOOKBACK_DAYS,
    },
) as dag:

    t_detect = PythonOperator(
        task_id="detect_gaps_and_partials",
        python_callable=detect_gaps_and_partials,
    )

    t_branch = BranchPythonOperator(
        task_id="branch_on_issues",
        python_callable=branch_on_issues,
    )

    t_alert = PythonOperator(
        task_id="send_gap_alert",
        python_callable=send_gap_alert,
    )

    t_backfill = PythonOperator(
        task_id="trigger_backfill",
        python_callable=trigger_backfill,
    )

    t_ok = EmptyOperator(task_id="no_action")

    t_detect >> t_branch >> [t_alert, t_ok]
    t_alert >> t_backfill
