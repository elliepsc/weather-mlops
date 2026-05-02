"""
Export the latest MLflow run to models/mlflow_latest.json.

This JSON is committed to the repo so the FastAPI endpoint /api/mlflow/metrics
has a fallback when mlruns/ is absent (e.g. Render cloud deployment).

Run standalone:
    python pipeline/export_mlflow_metrics.py

From Airflow task:
    from pipeline.export_mlflow_metrics import main; main()

Set AUTO_COMMIT_MLFLOW_JSON=true to git-commit and push after writing.
"""

import json
import logging
import os
import subprocess
from datetime import UTC, datetime
from pathlib import Path

logger = logging.getLogger(__name__)

ROOT = Path(__file__).parent.parent
MODELS_DIR = ROOT / "models"
OUTPUT_PATH = MODELS_DIR / "mlflow_latest.json"
EXPERIMENT_NAME = "weather_australia"

_SUMMARY_KEYS = {
    "rain_accuracy",
    "temp_mae",
    "temp_rmse",
    "rain_f1",
    "rain_precision",
    "rain_recall",
}


def _read_json_safe(path: Path) -> dict | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text())
    except Exception as exc:
        logger.warning("Could not read %s: %s", path, exc)
        return None


def _flatten_baseline(nested: dict) -> dict:
    """Map models/metrics.json nested structure to flat summary keys."""
    flat: dict = {}
    rain = nested.get("rain_tomorrow", {})
    temp = nested.get("max_temp_tomorrow", {})
    if rain.get("accuracy") is not None:
        flat["rain_accuracy"] = rain["accuracy"]
    if temp.get("mae") is not None:
        flat["temp_mae"] = temp["mae"]
    if rain.get("auc") is not None:
        flat["rain_auc"] = rain["auc"]
    if temp.get("r2") is not None:
        flat["temp_r2"] = temp["r2"]
    return flat


def main() -> None:
    try:
        import mlflow
        from pipeline.mlflow_config import get_mlflow_tracking_uri
    except ImportError as exc:
        logger.warning("MLflow not available — skipping export: %s", exc)
        return

    try:
        uri = get_mlflow_tracking_uri(ROOT)
        mlflow.set_tracking_uri(uri)
        client = mlflow.MlflowClient()
    except Exception as exc:
        logger.warning("MLflow tracking URI inaccessible — skipping export: %s", exc)
        return

    try:
        experiment = client.get_experiment_by_name(EXPERIMENT_NAME)
    except Exception as exc:
        logger.warning("Could not connect to MLflow — skipping export: %s", exc)
        return

    if not experiment:
        logger.warning("MLflow experiment '%s' not found — skipping export", EXPERIMENT_NAME)
        return

    try:
        runs = client.search_runs(
            experiment_ids=[experiment.experiment_id],
            filter_string="attributes.status = 'FINISHED'",
            order_by=["start_time DESC"],
            max_results=1,
        )
    except Exception as exc:
        logger.warning("MLflow search_runs failed — skipping export: %s", exc)
        return

    if not runs:
        logger.warning(
            "No FINISHED run in experiment '%s' — not overwriting existing export",
            EXPERIMENT_NAME,
        )
        return

    run = runs[0]
    info = run.info

    start_dt = datetime.fromtimestamp(info.start_time / 1000, tz=UTC)
    exported_dt = datetime.now(UTC)
    duration_seconds = round((info.end_time - info.start_time) / 1000) if info.end_time else None

    flat_metrics = {
        k: round(v, 6) if isinstance(v, float) else v for k, v in run.data.metrics.items()
    }
    summary_metrics = {k: v for k, v in flat_metrics.items() if k in _SUMMARY_KEYS}

    tags = {k: v for k, v in run.data.tags.items() if not k.startswith("mlflow.")}
    model_version = tags.get("model_version") or (
        info.run_name.replace("train_", "")
        if info.run_name and info.run_name.startswith("train_")
        else info.run_id[:8]
    )

    # Baseline delta
    baseline_nested = _read_json_safe(MODELS_DIR / "baseline" / "metrics.json")
    baseline_flat = _flatten_baseline(baseline_nested) if baseline_nested else None
    delta: dict | None = None
    if baseline_flat and summary_metrics:
        delta = {
            k: round(summary_metrics[k] - baseline_flat[k], 6)
            for k in summary_metrics
            if k in baseline_flat and isinstance(summary_metrics[k], (int, float))
        }
        if not delta:
            delta = None

    # Full nested model metrics (models/metrics.json) for API backward compat
    model_metrics = _read_json_safe(MODELS_DIR / "metrics.json")

    payload = {
        "exported_at": exported_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "run_id": info.run_id,
        "run_name": info.run_name or "",
        "start_time": start_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "duration_seconds": duration_seconds,
        "status": info.status,
        "model_version": model_version,
        "metrics": summary_metrics,
        "params": dict(run.data.params),
        "tags": tags,
        "baseline_metrics": baseline_flat,
        "delta_vs_baseline": delta,
        "model_metrics": model_metrics,
    }

    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(json.dumps(payload, indent=2))

    logger.info(
        "MLflow export → %s  run_id=%s  rain_accuracy=%s  temp_mae=%s",
        OUTPUT_PATH,
        info.run_id,
        summary_metrics.get("rain_accuracy"),
        summary_metrics.get("temp_mae"),
    )

    if os.getenv("AUTO_COMMIT_MLFLOW_JSON", "false").lower() in ("1", "true", "yes"):
        _git_commit_and_push(exported_dt)


def _git_commit_and_push(ts: datetime) -> None:
    date_str = ts.strftime("%Y-%m-%d")
    cmds = [
        ["git", "add", str(OUTPUT_PATH)],
        ["git", "commit", "-m", f"chore: update mlflow metrics [skip ci] - {date_str}"],
        ["git", "push"],
    ]
    for cmd in cmds:
        try:
            subprocess.run(cmd, cwd=ROOT, check=True, capture_output=True)
        except subprocess.CalledProcessError as exc:
            logger.warning(
                "Auto-commit step failed (%s): %s",
                " ".join(cmd[:2]),
                exc.stderr.decode(errors="replace") if exc.stderr else exc,
            )
            return
    logger.info("mlflow_latest.json committed and pushed (%s)", date_str)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    main()
