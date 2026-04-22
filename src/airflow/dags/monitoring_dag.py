"""
Airflow DAG — Daily monitoring
Schedule : every day at 08:00 UTC (after daily ingestion at 06:00)

Tasks:
  1. check_data_quality       — verify all 26 cities have data for yesterday
  2. check_prediction_coverage — ensure predictions exist for latest dates
  3. detect_drift              — KS test: last 30 days vs previous 30 days
  4. log_model_metrics         — verifiable accuracy on rain + temp predictions
  5. branch_on_drift           — alert if drift detected, pass otherwise
"""
import sys
import json
import logging
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago

ROOT = Path(__file__).parent.parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

logger = logging.getLogger(__name__)

default_args = {
    "owner":               "airflow",
    "retries":             1,
    "retry_delay_seconds": 120,
    "email_on_failure":    False,
}


# ─── task callables ──────────────────────────────────────────────────────────

def check_data_quality(**context):
    """Fail if more than 5 cities are missing for yesterday (>80% coverage required)."""
    from datetime import date, timedelta
    import pandas as pd
    from pipeline.database import get_connection
    from pipeline.locations import LOCATIONS

    yesterday = (date.today() - timedelta(days=1)).isoformat()
    with get_connection() as conn:
        df = pd.read_sql(
            "SELECT city FROM weather_raw WHERE date = ?", conn, params=(yesterday,)
        )

    expected = set(LOCATIONS.keys())
    missing  = expected - set(df["city"].tolist())

    context["ti"].xcom_push(key="missing_cities", value=list(missing))
    logger.info("Data quality — %d/%d cities for %s", len(expected) - len(missing), len(expected), yesterday)

    if len(missing) > 5:
        raise ValueError(f"Data quality FAIL: {len(missing)} cities missing for {yesterday}: {missing}")
    if missing:
        logger.warning("Partial data: missing %s", missing)


def check_prediction_coverage(**context):
    """Warn if predictions are not up to date with the latest raw data."""
    import pandas as pd
    from pipeline.database import get_connection

    with get_connection() as conn:
        raw_df  = pd.read_sql("SELECT city, MAX(date) AS latest FROM weather_raw GROUP BY city", conn)
        pred_df = pd.read_sql("SELECT city, MAX(date) AS latest FROM weather_predictions GROUP BY city", conn)

    stale = raw_df.merge(pred_df, on="city", suffixes=("_raw", "_pred"), how="left")
    stale = stale[stale["latest_raw"] != stale["latest_pred"]]

    context["ti"].xcom_push(key="stale_cities", value=stale["city"].tolist())
    if not stale.empty:
        logger.warning("Stale predictions for: %s", stale["city"].tolist())
    else:
        logger.info("All predictions up to date.")


def detect_drift(**context):
    """KS test: last 30 days vs previous 30 days on key weather features."""
    from datetime import date, timedelta
    from scipy import stats
    import pandas as pd
    from pipeline.database import get_connection

    today = date.today()
    recent_start   = (today - timedelta(days=30)).isoformat()
    recent_end     = (today - timedelta(days=1)).isoformat()
    baseline_start = (today - timedelta(days=60)).isoformat()
    baseline_end   = (today - timedelta(days=31)).isoformat()

    MONITORED = ["max_temp", "min_temp", "humidity_3pm",
                 "pressure_3pm", "rainfall", "wind_gust_speed"]

    with get_connection() as conn:
        recent   = pd.read_sql("SELECT * FROM weather_raw WHERE date BETWEEN ? AND ?",
                               conn, params=(recent_start, recent_end))
        baseline = pd.read_sql("SELECT * FROM weather_raw WHERE date BETWEEN ? AND ?",
                               conn, params=(baseline_start, baseline_end))

    report = {}
    drifted = []
    for feat in MONITORED:
        r, b = recent[feat].dropna(), baseline[feat].dropna()
        if len(r) < 10 or len(b) < 10:
            continue
        ks_stat, p_value = stats.ks_2samp(b, r)
        report[feat] = {"ks_stat": round(float(ks_stat), 4),
                        "p_value": round(float(p_value), 4),
                        "drifted": bool(p_value < 0.05)}
        if p_value < 0.05:
            drifted.append(feat)
            logger.warning("DRIFT '%s': KS=%.3f p=%.4f", feat, ks_stat, p_value)

    out = ROOT / "data" / "monitoring" / "drift_report.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps({"date": today.isoformat(), "features": report}, indent=2))

    context["ti"].xcom_push(key="drifted_features", value=drifted)
    context["ti"].xcom_push(key="drift_detected",   value=bool(drifted))


def log_model_metrics(**context):
    """Compute verifiable accuracy on rain + temp predictions (last 30 days)."""
    from datetime import date, timedelta
    import pandas as pd
    from sklearn.metrics import accuracy_score, mean_absolute_error
    from pipeline.database import get_connection

    cutoff = (date.today() - timedelta(days=30)).isoformat()
    with get_connection() as conn:
        df = pd.read_sql("""
            SELECT r.date, r.city, r.rain_today,
                   p.rain_tomorrow, p.max_temp_tomorrow, r.max_temp
            FROM weather_raw r
            JOIN weather_predictions p ON r.date = p.date AND r.city = p.city
            WHERE r.date >= ?
            ORDER BY r.date, r.city
        """, conn, params=(cutoff,))

    if len(df) < 30:
        logger.info("Not enough rows for metrics (%d).", len(df))
        return

    df = df.sort_values(["city", "date"])
    df["actual_rain_tomorrow"] = df.groupby("city")["rain_today"].shift(-1)

    metrics = {}
    v = df.dropna(subset=["actual_rain_tomorrow", "rain_tomorrow"])
    if not v.empty:
        metrics["rain_accuracy_30d"] = round(
            accuracy_score(v["actual_rain_tomorrow"].astype(int), v["rain_tomorrow"].astype(int)), 4)

    t = df.dropna(subset=["max_temp", "max_temp_tomorrow"])
    if not t.empty:
        metrics["temp_mae_30d"] = round(mean_absolute_error(t["max_temp"], t["max_temp_tomorrow"]), 2)

    logger.info("Model metrics (30d): %s", metrics)
    out = ROOT / "data" / "monitoring" / "model_metrics.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps({"date": date.today().isoformat(), **metrics}, indent=2))


def branch_on_drift(**context):
    drift = context["ti"].xcom_pull(task_ids="detect_drift", key="drift_detected")
    return "alert_drift" if drift else "no_drift"


def alert_drift(**context):
    features = context["ti"].xcom_pull(task_ids="detect_drift", key="drifted_features")
    logger.warning(
        "DRIFT ALERT on %s — consider triggering weather_weekly_train manually.", features
    )


# ─── DAG ─────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="weather_daily_monitoring",
    description="Daily data quality check, KS drift detection, model metrics",
    schedule_interval="0 8 * * *",
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    tags=["weather", "monitoring", "daily"],
) as dag:

    t_quality  = PythonOperator(task_id="check_data_quality",       python_callable=check_data_quality)
    t_coverage = PythonOperator(task_id="check_prediction_coverage", python_callable=check_prediction_coverage)
    t_drift    = PythonOperator(task_id="detect_drift",              python_callable=detect_drift)
    t_metrics  = PythonOperator(task_id="log_model_metrics",         python_callable=log_model_metrics)
    t_branch   = BranchPythonOperator(task_id="branch_on_drift",     python_callable=branch_on_drift)
    t_alert    = PythonOperator(task_id="alert_drift",               python_callable=alert_drift)
    t_ok       = EmptyOperator(task_id="no_drift")

    t_quality >> t_coverage >> t_drift >> t_metrics >> t_branch >> [t_alert, t_ok]
