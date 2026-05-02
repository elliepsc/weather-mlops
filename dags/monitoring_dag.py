"""Airflow DAG - daily monitoring.

Tasks:
  1. check_data_quality
  2. check_prediction_coverage
  3. detect_drift
  4. log_model_metrics
  5. branch_on_monitoring_decision
  6. trigger_retrain / alert_only / alert_insufficient_data / no_action

Fixes applied:
  #3  detect_drift: guard on window data completeness before KS test.
      If either window is < 70% complete (gaps present), results are flagged
      as unreliable in drift_report.json — not discarded, but annotated.
  #6  log_model_metrics: INNER JOIN → LEFT JOIN. Cities with missing
      predictions are counted and logged; metrics computed on partial coverage
      are now explicitly flagged rather than silently biased.
  #8  execution_timeout added to all tasks.
  #9  log_model_metrics: "no_data" entry written to history JSONL when
      metrics cannot be computed — no more silent holes in the timeline.
  #11 email_on_failure: True (requires SMTP configured in airflow.cfg).
  Schedule moved to 09:00 (was 08:00) to avoid race with gap_monitor + backfill.
  _send_slack_alert: tries dags._notifications, falls back to local definition.
  Dedup alert_key passed to all Slack calls.
"""

import json
import logging
import sys
from datetime import datetime, timedelta
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

M = mlops_config.monitoring
ING = mlops_config.ingestion

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(seconds=120),
    # Set True once SMTP is configured:
    # AIRFLOW__SMTP__SMTP_HOST, AIRFLOW__SMTP__SMTP_USER, etc.
    "email_on_failure": True,
    "email": [settings.alert_email] if settings.alert_email else [],
    "execution_timeout": timedelta(minutes=30),
}


def check_retrain_cooldown(ds: str) -> bool:
    """Return True when the previous successful retrain is still in cooldown."""
    from datetime import date

    cooldown_file = ROOT / "data" / "monitoring" / "last_retrain.json"
    if not cooldown_file.exists():
        return False

    data = json.loads(cooldown_file.read_text())
    last_retrain = date.fromisoformat(data.get("last_retrain", "2000-01-01"))
    current_day = date.fromisoformat(ds)
    age_days = (current_day - last_retrain).days

    if age_days < M.retrain_cooldown_days:
        logger.info("Retrain cooldown active - last retrain %d days ago", age_days)
        return True
    return False


def _write_decision(data: dict) -> None:
    out = ROOT / "data" / "monitoring" / "monitoring_decision.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(data, indent=2))


def check_data_quality(**context):
    """Fail when yesterday coverage is below the configured threshold."""
    import pandas as pd

    from pipeline.database import get_connection
    from pipeline.locations import LOCATIONS

    yesterday = (datetime.fromisoformat(context["ds"]) - timedelta(days=1)).date().isoformat()

    with get_connection() as conn:
        df = pd.read_sql(
            "SELECT city FROM weather_raw WHERE date = ?",
            conn,
            params=(yesterday,),
        )

    expected = set(LOCATIONS.keys())
    missing = expected - set(df["city"].tolist())
    available = len(expected) - len(missing)

    context["ti"].xcom_push(key="missing_cities", value=list(missing))
    logger.info("Data quality - %d/%d cities for %s", available, len(expected), yesterday)

    if available < ING.min_cities_threshold:
        raise ValueError(
            f"Data quality FAIL: only {available}/{len(expected)} cities for {yesterday}. "
            f"Minimum required: {ING.min_cities_threshold}. Missing: {missing}"
        )
    if missing:
        logger.warning("Partial data: missing %s", missing)


def check_prediction_coverage(**context):
    """Warn if predictions lag behind the latest raw data."""
    import pandas as pd

    from pipeline.database import get_connection

    with get_connection() as conn:
        raw_df = pd.read_sql(
            "SELECT city, MAX(date) AS latest FROM weather_raw GROUP BY city",
            conn,
        )
        pred_df = pd.read_sql(
            "SELECT city, MAX(date) AS latest FROM weather_predictions GROUP BY city",
            conn,
        )

    stale = raw_df.merge(pred_df, on="city", suffixes=("_raw", "_pred"), how="left")
    stale = stale[stale["latest_raw"] != stale["latest_pred"]]

    context["ti"].xcom_push(key="stale_cities", value=stale["city"].tolist())
    if not stale.empty:
        logger.warning("Stale predictions for: %s", stale["city"].tolist())
    else:
        logger.info("All predictions up to date.")


def detect_drift(**context):
    """KS test: last N days versus previous N days on configured features.

    Fix #3: before computing KS, check data completeness in both windows.
    If either window has < 70% of expected rows (cities × days), the test
    still runs but results are flagged as unreliable in drift_report.json.
    A drift signal on incomplete data is likely a data quality artefact,
    not a genuine distribution shift.
    """
    from datetime import date, timedelta

    import pandas as pd
    from scipy import stats

    from pipeline.database import get_connection
    from pipeline.locations import LOCATIONS

    today = date.fromisoformat(context["ds"])
    recent_start = (today - timedelta(days=M.drift_window_days)).isoformat()
    recent_end = (today - timedelta(days=1)).isoformat()
    baseline_start = (today - timedelta(days=M.drift_window_days * 2)).isoformat()
    baseline_end = (today - timedelta(days=M.drift_window_days + 1)).isoformat()

    with get_connection() as conn:
        recent = pd.read_sql(
            "SELECT * FROM weather_raw WHERE date BETWEEN ? AND ?",
            conn,
            params=(recent_start, recent_end),
        )
        baseline = pd.read_sql(
            "SELECT * FROM weather_raw WHERE date BETWEEN ? AND ?",
            conn,
            params=(baseline_start, baseline_end),
        )

    # --- Fix #3: completeness guard ------------------------------------------
    expected_rows = M.drift_window_days * len(LOCATIONS)
    completeness_recent = len(recent) / max(expected_rows, 1)
    completeness_baseline = len(baseline) / max(expected_rows, 1)
    data_quality_warning = completeness_recent < 0.7 or completeness_baseline < 0.7

    if data_quality_warning:
        logger.warning(
            "Drift window completeness low — KS results may be unreliable. "
            "recent: %.0f%% (%d/%d rows) | baseline: %.0f%% (%d/%d rows). "
            "Gap monitor should have triggered a backfill.",
            completeness_recent * 100,
            len(recent),
            expected_rows,
            completeness_baseline * 100,
            len(baseline),
            expected_rows,
        )
    # -------------------------------------------------------------------------

    report = {}
    drifted_features = []
    for feature in M.monitored_features:
        recent_values = recent[feature].dropna()
        baseline_values = baseline[feature].dropna()
        if len(recent_values) < 10 or len(baseline_values) < 10:
            continue

        ks_stat, p_value = stats.ks_2samp(baseline_values, recent_values)
        drifted = bool(p_value < M.drift_ks_alpha)
        report[feature] = {
            "ks_stat": round(float(ks_stat), 4),
            "p_value": round(float(p_value), 4),
            "drifted": drifted,
        }
        if drifted:
            drifted_features.append(feature)
            logger.warning("DRIFT %s: KS=%.3f p=%.4f", feature, ks_stat, p_value)

    out = ROOT / "data" / "monitoring" / "drift_report.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(
        json.dumps(
            {
                "date": today.isoformat(),
                "data_quality_warning": data_quality_warning,
                "completeness_recent_pct": round(completeness_recent * 100, 1),
                "completeness_baseline_pct": round(completeness_baseline * 100, 1),
                "features": report,
            },
            indent=2,
        )
    )

    context["ti"].xcom_push(key="drifted_features", value=drifted_features)
    context["ti"].xcom_push(key="drift_detected", value=bool(drifted_features))
    context["ti"].xcom_push(key="drift_data_quality_warning", value=data_quality_warning)


def log_model_metrics(**context):
    """Compute verifiable rain accuracy and next-day temp MAE.

    Fix #6: LEFT JOIN instead of INNER JOIN — cities without predictions are
    counted and logged. Metrics are still computed on available data, but the
    coverage gap is now visible rather than silently excluded.
    Fix #9: writes a "no_data" entry to history JSONL when metrics cannot be
    computed, so the timeline has no silent holes.
    """
    from datetime import date, timedelta

    import pandas as pd
    from sklearn.metrics import accuracy_score, mean_absolute_error

    from pipeline.database import get_connection

    today = date.fromisoformat(context["ds"])
    cutoff = (today - timedelta(days=M.drift_window_days)).isoformat()
    cutoff_7d = (today - timedelta(days=M.early_warning_window_days)).isoformat()
    monitoring_dir = ROOT / "data" / "monitoring"
    monitoring_dir.mkdir(parents=True, exist_ok=True)
    history_path = monitoring_dir / "model_metrics_history.jsonl"

    with get_connection() as conn:
        # Fix #6: LEFT JOIN — keeps all raw rows, NULLs where predictions missing.
        df = pd.read_sql(
            """
            SELECT
                r.date, r.city, r.rain_today, r.max_temp,
                p.rain_tomorrow, p.max_temp_tomorrow
            FROM weather_raw r
            LEFT JOIN weather_predictions p
                   ON r.date = p.date AND r.city = p.city
            WHERE r.date >= ?
            ORDER BY r.date, r.city
            """,
            conn,
            params=(cutoff,),
        )

    # Fix #6: explicit coverage warning.
    cities_without_preds = df[df["rain_tomorrow"].isna()]["city"].nunique()
    if cities_without_preds > 0:
        logger.warning(
            "%d cities have raw data but no predictions — "
            "metrics reflect partial coverage only.",
            cities_without_preds,
        )

    if len(df) < M.min_rows_for_metrics:
        logger.info("Not enough rows for metrics (%d < %d).", len(df), M.min_rows_for_metrics)

        # Fix #9: record the skip so history has no silent holes.
        no_data_entry = {
            "date": today.isoformat(),
            "status": "no_data",
            "reason": f"insufficient_rows ({len(df)} < {M.min_rows_for_metrics})",
        }
        with history_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(no_data_entry) + "\n")

        context["ti"].xcom_push(key="rain_accuracy_30d", value=None)
        context["ti"].xcom_push(key="temp_mae_30d", value=None)
        return

    df = df.sort_values(["city", "date"])
    df["actual_rain_tomorrow"] = df.groupby("city")["rain_today"].shift(-1)
    df["actual_max_temp_tomorrow"] = df.groupby("city")["max_temp"].shift(-1)

    metrics: dict = {"status": "computed"}

    rain_df = df.dropna(subset=["actual_rain_tomorrow", "rain_tomorrow"])
    if not rain_df.empty:
        metrics["rain_accuracy_30d"] = round(
            accuracy_score(
                rain_df["actual_rain_tomorrow"].astype(int),
                rain_df["rain_tomorrow"].astype(int),
            ),
            4,
        )

    temp_df = df.dropna(subset=["actual_max_temp_tomorrow", "max_temp_tomorrow"])
    if not temp_df.empty:
        metrics["temp_mae_30d"] = round(
            mean_absolute_error(
                temp_df["actual_max_temp_tomorrow"],
                temp_df["max_temp_tomorrow"],
            ),
            2,
        )

    logger.info("Model metrics (30d): %s", metrics)

    snapshot = {"date": today.isoformat(), **metrics}

    # Snapshot courant — écrasé à chaque run, lu par branch_on_monitoring_decision.
    (monitoring_dir / "model_metrics.json").write_text(json.dumps(snapshot, indent=2))

    # Historique — append JSONL, jamais effacé.
    with history_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(snapshot) + "\n")

    context["ti"].xcom_push(key="rain_accuracy_30d", value=metrics.get("rain_accuracy_30d"))
    context["ti"].xcom_push(key="temp_mae_30d", value=metrics.get("temp_mae_30d"))

    # 7-day early-warning window — derived from the same df, no extra query.
    df_7d = df[df["date"] >= cutoff_7d]
    metrics_7d: dict = {}
    rain_7d = df_7d.dropna(subset=["actual_rain_tomorrow", "rain_tomorrow"])
    if not rain_7d.empty:
        metrics_7d["rain_accuracy_7d"] = round(
            accuracy_score(
                rain_7d["actual_rain_tomorrow"].astype(int),
                rain_7d["rain_tomorrow"].astype(int),
            ),
            4,
        )
    temp_7d = df_7d.dropna(subset=["actual_max_temp_tomorrow", "max_temp_tomorrow"])
    if not temp_7d.empty:
        metrics_7d["temp_mae_7d"] = round(
            mean_absolute_error(
                temp_7d["actual_max_temp_tomorrow"],
                temp_7d["max_temp_tomorrow"],
            ),
            2,
        )
    logger.info("Model metrics (7d early-warning): %s", metrics_7d)
    context["ti"].xcom_push(key="rain_accuracy_7d", value=metrics_7d.get("rain_accuracy_7d"))
    context["ti"].xcom_push(key="temp_mae_7d", value=metrics_7d.get("temp_mae_7d"))


def branch_on_monitoring_decision(**context):
    """Route monitoring to retrain, alert, insufficient data, or no action."""
    features = context["ti"].xcom_pull(task_ids="detect_drift", key="drifted_features") or []
    accuracy = context["ti"].xcom_pull(task_ids="log_model_metrics", key="rain_accuracy_30d")
    temp_mae = context["ti"].xcom_pull(task_ids="log_model_metrics", key="temp_mae_30d")
    accuracy_7d = context["ti"].xcom_pull(task_ids="log_model_metrics", key="rain_accuracy_7d")
    temp_mae_7d = context["ti"].xcom_pull(task_ids="log_model_metrics", key="temp_mae_7d")
    ds = context["ds"]

    n_drifted = len(features)
    low_accuracy = accuracy is not None and accuracy < M.rain_accuracy_threshold
    high_mae = temp_mae is not None and temp_mae > M.temp_mae_threshold_celsius
    heavy_drift = n_drifted >= M.min_drift_features_retrain
    mild_drift = M.min_drift_features_alert <= n_drifted < M.min_drift_features_retrain
    early_warning = (
        accuracy_7d is not None and accuracy_7d < M.rain_accuracy_early_warning_threshold
    ) or (
        temp_mae_7d is not None and temp_mae_7d > M.temp_mae_early_warning_threshold
    )

    decision_data = {
        "date": ds,
        "rain_accuracy_30d": accuracy,
        "temp_mae_30d": temp_mae,
        "rain_accuracy_7d": accuracy_7d,
        "temp_mae_7d": temp_mae_7d,
        "drifted_features": features,
        "n_drifted": n_drifted,
        "low_accuracy": low_accuracy,
        "high_mae": high_mae,
        "heavy_drift": heavy_drift,
        "mild_drift": mild_drift,
        "early_warning": early_warning,
    }

    if accuracy is None and temp_mae is None:
        logger.warning("Insufficient data to compute metrics - alerting.")
        decision_data["action"] = "insufficient_data"
        decision_data["reason"] = "insufficient_data_for_metrics"
        _write_decision(decision_data)
        context["ti"].xcom_push(key="monitoring_action", value="insufficient_data")
        return "alert_insufficient_data"

    needs_retrain = low_accuracy or high_mae or heavy_drift
    if needs_retrain:
        if check_retrain_cooldown(ds):
            logger.warning(
                "Retrain needed but cooldown active - downgrading to alert_only. "
                "accuracy=%.3f mae=%.2f drifted=%s",
                accuracy or 0,
                temp_mae or 0,
                features,
            )
            decision_data["action"] = "alert_only"
            decision_data["reason"] = "retrain_needed_but_cooldown_active"
            _write_decision(decision_data)
            context["ti"].xcom_push(key="monitoring_action", value="alert_only")
            return "alert_only"

        reasons = []
        if low_accuracy:
            reasons.append(f"rain_accuracy {accuracy:.1%} < {M.rain_accuracy_threshold:.0%}")
        if high_mae:
            reasons.append(f"temp_mae {temp_mae:.2f}C > {M.temp_mae_threshold_celsius}C")
        if heavy_drift:
            reasons.append(f"drift on {n_drifted} features (>={M.min_drift_features_retrain})")

        logger.warning("RETRAIN triggered - %s", " | ".join(reasons))
        decision_data["action"] = "trigger_retrain"
        decision_data["reason"] = reasons
        _write_decision(decision_data)
        context["ti"].xcom_push(key="monitoring_action", value="trigger_retrain")
        return "trigger_retrain"

    if mild_drift:
        logger.warning(
            "ALERT - mild drift on %d features %s (seasonal likely). Metrics OK.",
            n_drifted,
            features,
        )
        decision_data["action"] = "alert_only"
        decision_data["reason"] = f"mild_drift_on_{n_drifted}_features"
        _write_decision(decision_data)
        context["ti"].xcom_push(key="monitoring_action", value="alert_only")
        return "alert_only"

    if early_warning:
        logger.warning(
            "EARLY WARNING - 7d metrics degrading: accuracy_7d=%s mae_7d=%s (30d still OK).",
            accuracy_7d,
            temp_mae_7d,
        )
        decision_data["action"] = "alert_only"
        decision_data["reason"] = "early_warning_7d"
        _write_decision(decision_data)
        context["ti"].xcom_push(key="monitoring_action", value="alert_only")
        return "alert_only"

    logger.info(
        "No action needed - accuracy=%.1f%% mae=%.2f drifted=%d",
        (accuracy or 0) * 100,
        temp_mae or 0,
        n_drifted,
    )
    decision_data["action"] = "no_action"
    decision_data["reason"] = "all_metrics_nominal"
    _write_decision(decision_data)
    context["ti"].xcom_push(key="monitoring_action", value="no_action")
    return "no_action"


def _format_drift_lines(drift_path: Path) -> str:
    """Return a per-feature drift summary string, or empty string if unavailable."""
    if not drift_path.exists():
        return ""
    try:
        drift = json.loads(drift_path.read_text())
    except Exception:
        return ""
    features = drift.get("features", {})
    if not features:
        return ""
    lines = []
    for feat, stats in features.items():
        flag = ":red_circle:" if stats.get("drifted") else ":white_circle:"
        lines.append(
            f"  {flag} {feat}: KS={stats.get('ks_stat')} p={stats.get('p_value')}"
        )
    dqw = drift.get("data_quality_warning")
    footer = (
        f"\n  :warning: data quality warning — completeness "
        f"recent={drift.get('completeness_recent_pct')}% "
        f"baseline={drift.get('completeness_baseline_pct')}%"
        if dqw
        else ""
    )
    return "\n" + "\n".join(lines) + footer


def send_alert(**context):
    """Send Slack alert for mild drift, cooldown-blocked retrain, or early warning."""
    action = context["ti"].xcom_pull(
        task_ids="branch_on_monitoring_decision", key="monitoring_action"
    )
    out = ROOT / "data" / "monitoring" / "monitoring_decision.json"
    details = json.loads(out.read_text()) if out.exists() else {}
    drift_lines = _format_drift_lines(ROOT / "data" / "monitoring" / "drift_report.json")
    ds = context["ds"]
    reason = details.get("reason", "unknown")

    if reason == "early_warning_7d":
        message = (
            f":eyes: *weather-mlops early warning* ({ds})\n"
            f"7-day metrics degrading — 30d still OK, watch closely.\n"
            f"accuracy_7d={details.get('rain_accuracy_7d')} "
            f"(30d={details.get('rain_accuracy_30d')}) | "
            f"mae_7d={details.get('temp_mae_7d')} "
            f"(30d={details.get('temp_mae_30d')})"
            f"{drift_lines}"
        )
    else:
        message = (
            f":warning: *weather-mlops monitoring alert* ({ds})\n"
            f"Action: `{action}` | Reason: `{reason}`\n"
            f"accuracy_30d={details.get('rain_accuracy_30d')} "
            f"mae_30d={details.get('temp_mae_30d')} | "
            f"accuracy_7d={details.get('rain_accuracy_7d')} "
            f"mae_7d={details.get('temp_mae_7d')}"
            f"{drift_lines}"
        )
    logger.warning("MONITORING ALERT: %s", message)
    _send_slack_alert(message, alert_key="monitoring_alert")


def send_insufficient_data_alert(**context):
    """Alert when metrics could not be computed."""
    ds = context["ds"]
    message = (
        f":x: *weather-mlops monitoring - insufficient data* ({ds})\n"
        f"Not enough rows (< {M.min_rows_for_metrics}) to compute metrics."
    )
    logger.warning("INSUFFICIENT DATA: %s", message)
    _send_slack_alert(message, alert_key="insufficient_data")


with DAG(
    dag_id="weather_daily_monitoring",
    description="Daily monitoring: data quality, drift, model metrics, retrain trigger",
    schedule="0 9 * * *",
    start_date=datetime(2026, 4, 22),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["weather", "monitoring", "daily"],
) as dag:

    t_quality = PythonOperator(
        task_id="check_data_quality",
        python_callable=check_data_quality,
        execution_timeout=timedelta(minutes=10),
    )
    t_coverage = PythonOperator(
        task_id="check_prediction_coverage",
        python_callable=check_prediction_coverage,
        execution_timeout=timedelta(minutes=10),
    )
    t_drift = PythonOperator(
        task_id="detect_drift",
        python_callable=detect_drift,
        execution_timeout=timedelta(minutes=30),
    )
    t_metrics = PythonOperator(
        task_id="log_model_metrics",
        python_callable=log_model_metrics,
        execution_timeout=timedelta(minutes=30),
    )
    t_branch = BranchPythonOperator(
        task_id="branch_on_monitoring_decision",
        python_callable=branch_on_monitoring_decision,
        execution_timeout=timedelta(minutes=5),
    )
    t_retrain = TriggerDagRunOperator(
        task_id="trigger_retrain",
        trigger_dag_id="weather_weekly_train",
        wait_for_completion=False,
        reset_dag_run=True,
    )
    t_alert = PythonOperator(
        task_id="alert_only",
        python_callable=send_alert,
        execution_timeout=timedelta(minutes=5),
    )
    t_alert_nodata = PythonOperator(
        task_id="alert_insufficient_data",
        python_callable=send_insufficient_data_alert,
        execution_timeout=timedelta(minutes=5),
    )
    t_ok = EmptyOperator(task_id="no_action")

    t_quality >> t_coverage >> t_drift >> t_metrics >> t_branch
    t_branch >> [t_retrain, t_alert, t_alert_nodata, t_ok]
