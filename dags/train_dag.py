"""Airflow DAG - weekly model retraining.

Changes vs original:
  - _send_slack_alert : utilise dags._notifications si disponible, sinon fallback local.
    Miroir du pattern _airflow_compat.py déjà dans le projet.
"""

import json
import logging
import shutil
import sys
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config.settings import mlops_config
from dags._airflow_compat import BranchPythonOperator, DAG, PythonOperator

logger = logging.getLogger(__name__)

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

T = mlops_config.training

MODELS_DIR = ROOT / "models"
BASELINE_DIR = MODELS_DIR / "baseline"
LAST_RETRAIN_PATH = ROOT / "data" / "monitoring" / "last_retrain.json"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(seconds=600),
    "email_on_failure": False,
}


def _copy_model_artifacts(src_dir: Path, dst_dir: Path) -> list[str]:
    copied = []
    dst_dir.mkdir(parents=True, exist_ok=True)
    for path in src_dir.iterdir():
        if not path.is_file():
            continue
        if path.suffix not in {".pkl", ".json"}:
            continue
        shutil.copy2(path, dst_dir / path.name)
        copied.append(path.name)
    return copied


def snapshot_current_models(**context):
    """Copy models/*.pkl and models/*.json into models/baseline/."""
    copied = _copy_model_artifacts(MODELS_DIR, BASELINE_DIR)
    if not copied:
        logger.warning("snapshot_models: nothing to snapshot (models/ may be empty)")
    else:
        logger.info("Snapshot saved: %s", copied)
    context["ti"].xcom_push(key="snapshot_files", value=copied)


def step_train(**context):
    from pipeline.run_pipeline import step_train as _step_train

    metrics = _step_train()
    if metrics:
        context["ti"].xcom_push(key="new_metrics", value=metrics)
        logger.info("New model metrics: %s", metrics)
    return metrics


def compare_vs_baseline(**context):
    """Compare freshly trained metrics against the baseline snapshot."""
    baseline_metrics_path = BASELINE_DIR / "metrics.json"
    if not baseline_metrics_path.exists():
        logger.info("No baseline metrics found - skipping comparison (first run?)")
        context["ti"].xcom_push(key="degraded", value=False)
        context["ti"].xcom_push(key="degradation_issues", value=[])
        return

    baseline = json.loads(baseline_metrics_path.read_text())
    new_metrics = context["ti"].xcom_pull(
        task_ids="retrain_models",
        key="new_metrics",
    ) or {}

    tolerance = T.baseline_tolerance
    degraded = False
    issues = []

    old_rain = baseline.get("rain_tomorrow", {}).get("accuracy")
    new_rain = new_metrics.get("rain_tomorrow", {}).get("accuracy")
    if old_rain is not None and new_rain is not None:
        if new_rain < old_rain - tolerance.rain_accuracy_pp:
            degraded = True
            issues.append(
                f"rain_accuracy degraded: {new_rain:.3f} vs baseline {old_rain:.3f}"
            )

    old_mae = baseline.get("max_temp_tomorrow", {}).get("mae")
    new_mae = new_metrics.get("max_temp_tomorrow", {}).get("mae")
    if old_mae is not None and new_mae is not None:
        if new_mae > old_mae + tolerance.temp_mae_celsius:
            degraded = True
            issues.append(
                f"temp_mae degraded: {new_mae:.2f} vs baseline {old_mae:.2f}"
            )

    if degraded:
        logger.warning("MODEL DEGRADATION DETECTED after retrain - %s", " | ".join(issues))
    else:
        logger.info(
            "Model validation OK - rain=%.3f (was %.3f) | mae=%.2f (was %.2f)",
            new_rain or 0,
            old_rain or 0,
            new_mae or 0,
            old_mae or 0,
        )

    context["ti"].xcom_push(key="degraded", value=degraded)
    context["ti"].xcom_push(key="degradation_issues", value=issues)


def branch_after_validation(**context):
    """Use new models only when comparison against baseline succeeds."""
    degraded = bool(
        context["ti"].xcom_pull(
            task_ids="compare_vs_baseline",
            key="degraded",
        )
    )
    return "rollback_to_baseline" if degraded else "run_predictions"


def rollback_to_baseline(**context):
    """Restore the snapshot so degraded models never stay active in prod."""
    if not BASELINE_DIR.exists():
        raise FileNotFoundError(f"Baseline directory not found: {BASELINE_DIR}")

    restored = _copy_model_artifacts(BASELINE_DIR, MODELS_DIR)
    if not restored:
        raise FileNotFoundError(
            f"No baseline model artifacts found in {BASELINE_DIR} for rollback"
        )

    logger.warning("Rollback completed - restored baseline artifacts: %s", restored)
    context["ti"].xcom_push(key="rollback_files", value=restored)


def send_degradation_alert(**context):
    """Notify humans when a degraded retrain was rolled back."""
    issues = context["ti"].xcom_pull(
        task_ids="compare_vs_baseline",
        key="degradation_issues",
    ) or []
    ds = context["ds"]
    message = (
        f":x: *weather-rain retrain rolled back* ({ds})\n"
        f"Degraded model was blocked from production.\n"
        f"Issues: {' | '.join(issues) if issues else 'unknown'}"
    )
    logger.warning("RETRAIN ROLLBACK ALERT: %s", message)
    send_slack_alert(message)


def step_predict(**context):
    from pipeline.run_pipeline import step_predict as _step_predict

    _step_predict()


def step_export(**context):
    from pipeline.run_pipeline import step_export as _step_export

    _step_export()


def write_last_retrain(**context):
    """Persist cooldown only after a successful retrain and prediction refresh."""
    LAST_RETRAIN_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "last_retrain": context["ds"],
        "written_at": datetime.utcnow().isoformat(timespec="seconds"),
        "source": "weather_weekly_train",
    }
    LAST_RETRAIN_PATH.write_text(json.dumps(payload, indent=2))
    logger.info("Recorded successful retrain cooldown: %s", payload["last_retrain"])


with DAG(
    dag_id="weather_weekly_train",
    description="Weekly retraining: snapshot -> retrain -> validate -> gate -> predict -> export",
    schedule="0 2 * * 1",
    start_date=datetime(2026, 4, 16),
    catchup=False,
    default_args=default_args,
    tags=["weather", "training", "weekly"],
) as dag:
    t_snapshot = PythonOperator(
        task_id="snapshot_models",
        python_callable=snapshot_current_models,
    )
    t_train = PythonOperator(
        task_id="retrain_models",
        python_callable=step_train,
    )
    t_validate = PythonOperator(
        task_id="compare_vs_baseline",
        python_callable=compare_vs_baseline,
    )
    t_gate = BranchPythonOperator(
        task_id="branch_after_validation",
        python_callable=branch_after_validation,
    )
    t_predict = PythonOperator(
        task_id="run_predictions",
        python_callable=step_predict,
    )
    t_export = PythonOperator(
        task_id="export_csv",
        python_callable=step_export,
    )
    t_write_last_retrain = PythonOperator(
        task_id="write_last_retrain",
        python_callable=write_last_retrain,
    )
    t_rollback = PythonOperator(
        task_id="rollback_to_baseline",
        python_callable=rollback_to_baseline,
    )
    t_alert = PythonOperator(
        task_id="alert_degraded_model",
        python_callable=send_degradation_alert,
    )

    t_snapshot >> t_train >> t_validate >> t_gate
    t_gate >> t_predict >> t_export >> t_write_last_retrain
    t_gate >> t_rollback >> t_alert
