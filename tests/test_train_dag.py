from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dags import train_dag as td  # noqa: E402


def test_compare_vs_baseline_flags_degradation(tmp_path, monkeypatch):
    baseline_dir = tmp_path / "baseline"
    baseline_dir.mkdir()
    (baseline_dir / "metrics.json").write_text(
        json.dumps(
            {
                "rain_tomorrow": {"accuracy": 0.80},
                "max_temp_tomorrow": {"mae": 1.50},
            }
        )
    )
    monkeypatch.setattr(td, "BASELINE_DIR", baseline_dir)

    ti = MagicMock()

    def xcom_pull(task_ids: str, key: str):
        if (task_ids, key) == ("retrain_models", "new_metrics"):
            return {
                "rain_tomorrow": {"accuracy": 0.75},
                "max_temp_tomorrow": {"mae": 1.80},
            }
        return None

    ti.xcom_pull.side_effect = xcom_pull

    td.compare_vs_baseline(ti=ti)

    pushes = {call.kwargs["key"]: call.kwargs["value"] for call in ti.xcom_push.call_args_list}
    assert pushes["degraded"] is True
    assert len(pushes["degradation_issues"]) == 2


def test_branch_after_validation_routes_to_rollback_when_degraded():
    ti = MagicMock()
    ti.xcom_pull.return_value = True

    result = td.branch_after_validation(ti=ti)

    assert result == "rollback_to_baseline"


def test_branch_after_validation_routes_to_predictions_when_valid():
    ti = MagicMock()
    ti.xcom_pull.return_value = False

    result = td.branch_after_validation(ti=ti)

    assert result == "run_predictions"


def test_rollback_to_baseline_restores_artifacts(tmp_path, monkeypatch):
    baseline_dir = tmp_path / "baseline"
    models_dir = tmp_path / "models"
    baseline_dir.mkdir()
    models_dir.mkdir()

    (baseline_dir / "rain_tomorrow.pkl").write_text("baseline-model")
    (baseline_dir / "metrics.json").write_text("{}")
    (models_dir / "rain_tomorrow.pkl").write_text("new-model")

    monkeypatch.setattr(td, "BASELINE_DIR", baseline_dir)
    monkeypatch.setattr(td, "MODELS_DIR", models_dir)

    ti = MagicMock()
    td.rollback_to_baseline(ti=ti)

    assert (models_dir / "rain_tomorrow.pkl").read_text() == "baseline-model"
    pushes = {call.kwargs["key"]: call.kwargs["value"] for call in ti.xcom_push.call_args_list}
    assert "rain_tomorrow.pkl" in pushes["rollback_files"]


def test_write_last_retrain_writes_cooldown_file(tmp_path, monkeypatch):
    last_retrain_path = tmp_path / "last_retrain.json"
    monkeypatch.setattr(td, "LAST_RETRAIN_PATH", last_retrain_path)

    td.write_last_retrain(ds="2026-04-24")

    payload = json.loads(last_retrain_path.read_text())
    assert payload["last_retrain"] == "2026-04-24"
    assert payload["source"] == "weather_weekly_train"
