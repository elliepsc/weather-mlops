from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dags import monitoring_dag as md  # noqa: E402


def make_context(
    *,
    accuracy: float | None,
    temp_mae: float | None,
    drifted_features: list[str],
    accuracy_7d: float | None = None,
    temp_mae_7d: float | None = None,
    ds: str = "2026-04-24",
) -> dict:
    ti = MagicMock()

    def xcom_pull(task_ids: str, key: str):
        mapping = {
            ("detect_drift", "drifted_features"): drifted_features,
            ("log_model_metrics", "rain_accuracy_30d"): accuracy,
            ("log_model_metrics", "temp_mae_30d"): temp_mae,
            ("log_model_metrics", "rain_accuracy_7d"): accuracy_7d,
            ("log_model_metrics", "temp_mae_7d"): temp_mae_7d,
        }
        return mapping.get((task_ids, key))

    ti.xcom_pull.side_effect = xcom_pull
    ti.xcom_push = MagicMock()
    return {"ti": ti, "ds": ds}


@pytest.fixture
def patched_io():
    with (
        patch.object(md, "_write_decision") as write_decision,
        patch.object(md, "check_retrain_cooldown") as cooldown,
    ):
        cooldown.return_value = False
        yield {
            "write_decision": write_decision,
            "cooldown": cooldown,
        }


def get_decision_payload(mock_write_decision) -> dict:
    assert mock_write_decision.called, "_write_decision was not called"
    args, _ = mock_write_decision.call_args
    return args[0]


def assert_monitoring_action_pushed(context: dict, expected: str) -> None:
    context["ti"].xcom_push.assert_called_with(key="monitoring_action", value=expected)


def test_insufficient_data_routes_to_alert_task(patched_io):
    context = make_context(accuracy=None, temp_mae=None, drifted_features=[])

    result = md.branch_on_monitoring_decision(**context)

    assert result == "alert_insufficient_data"
    payload = get_decision_payload(patched_io["write_decision"])
    assert payload["action"] == "insufficient_data"
    assert payload["reason"] == "insufficient_data_for_metrics"
    assert_monitoring_action_pushed(context, "insufficient_data")


def test_low_accuracy_triggers_retrain(patched_io):
    context = make_context(accuracy=0.70, temp_mae=2.0, drifted_features=[])

    result = md.branch_on_monitoring_decision(**context)

    assert result == "trigger_retrain"
    payload = get_decision_payload(patched_io["write_decision"])
    assert payload["action"] == "trigger_retrain"
    assert payload["low_accuracy"] is True
    assert any("rain_accuracy" in reason for reason in payload["reason"])
    assert_monitoring_action_pushed(context, "trigger_retrain")


def test_retrain_needed_but_cooldown_active_routes_to_alert(patched_io):
    patched_io["cooldown"].return_value = True
    context = make_context(accuracy=0.70, temp_mae=2.0, drifted_features=[])

    result = md.branch_on_monitoring_decision(**context)

    assert result == "alert_only"
    payload = get_decision_payload(patched_io["write_decision"])
    assert payload["action"] == "alert_only"
    assert payload["reason"] == "retrain_needed_but_cooldown_active"
    assert_monitoring_action_pushed(context, "alert_only")


def test_mild_drift_routes_to_alert_only(patched_io):
    context = make_context(
        accuracy=0.85,
        temp_mae=2.0,
        drifted_features=["max_temp", "min_temp", "humidity_3pm"],
    )

    result = md.branch_on_monitoring_decision(**context)

    assert result == "alert_only"
    payload = get_decision_payload(patched_io["write_decision"])
    assert payload["action"] == "alert_only"
    assert payload["mild_drift"] is True
    assert payload["heavy_drift"] is False
    assert payload["reason"] == "mild_drift_on_3_features"
    assert_monitoring_action_pushed(context, "alert_only")


def test_heavy_drift_triggers_retrain(patched_io):
    context = make_context(
        accuracy=0.85,
        temp_mae=2.0,
        drifted_features=["max_temp", "min_temp", "humidity_3pm", "pressure_3pm"],
    )

    result = md.branch_on_monitoring_decision(**context)

    assert result == "trigger_retrain"
    payload = get_decision_payload(patched_io["write_decision"])
    assert payload["action"] == "trigger_retrain"
    assert payload["heavy_drift"] is True
    assert any("drift on 4 features" in reason for reason in payload["reason"])
    assert_monitoring_action_pushed(context, "trigger_retrain")


def test_high_mae_alone_triggers_retrain(patched_io):
    context = make_context(accuracy=0.85, temp_mae=3.5, drifted_features=[])

    result = md.branch_on_monitoring_decision(**context)

    assert result == "trigger_retrain"
    payload = get_decision_payload(patched_io["write_decision"])
    assert payload["high_mae"] is True
    assert any("temp_mae" in reason for reason in payload["reason"])
    assert_monitoring_action_pushed(context, "trigger_retrain")


def test_all_nominal_routes_to_no_action(patched_io):
    context = make_context(accuracy=0.85, temp_mae=2.0, drifted_features=[])

    result = md.branch_on_monitoring_decision(**context)

    assert result == "no_action"
    payload = get_decision_payload(patched_io["write_decision"])
    assert payload["action"] == "no_action"
    assert payload["reason"] == "all_metrics_nominal"
    assert_monitoring_action_pushed(context, "no_action")


def test_decision_payload_schema(patched_io):
    context = make_context(accuracy=0.85, temp_mae=2.0, drifted_features=[])

    md.branch_on_monitoring_decision(**context)
    payload = get_decision_payload(patched_io["write_decision"])

    required_keys = {
        "date",
        "rain_accuracy_30d",
        "temp_mae_30d",
        "rain_accuracy_7d",
        "temp_mae_7d",
        "drifted_features",
        "n_drifted",
        "low_accuracy",
        "high_mae",
        "heavy_drift",
        "mild_drift",
        "early_warning",
        "action",
        "reason",
    }
    assert required_keys <= set(payload.keys())


def test_early_warning_accuracy_routes_to_alert_only(patched_io):
    """7d accuracy below threshold triggers alert even when 30d metrics are nominal."""
    context = make_context(
        accuracy=0.85, temp_mae=2.0, drifted_features=[], accuracy_7d=0.68
    )

    result = md.branch_on_monitoring_decision(**context)

    assert result == "alert_only"
    payload = get_decision_payload(patched_io["write_decision"])
    assert payload["action"] == "alert_only"
    assert payload["reason"] == "early_warning_7d"
    assert payload["early_warning"] is True
    assert_monitoring_action_pushed(context, "alert_only")


def test_early_warning_mae_routes_to_alert_only(patched_io):
    """7d MAE above threshold triggers alert even when 30d metrics are nominal."""
    context = make_context(
        accuracy=0.85, temp_mae=2.0, drifted_features=[], temp_mae_7d=4.5
    )

    result = md.branch_on_monitoring_decision(**context)

    assert result == "alert_only"
    payload = get_decision_payload(patched_io["write_decision"])
    assert payload["action"] == "alert_only"
    assert payload["reason"] == "early_warning_7d"
    assert payload["early_warning"] is True
    assert_monitoring_action_pushed(context, "alert_only")


def test_early_warning_does_not_fire_when_30d_triggers_retrain(patched_io):
    """Retrain path takes priority over early warning."""
    context = make_context(
        accuracy=0.70, temp_mae=2.0, drifted_features=[], accuracy_7d=0.68
    )

    result = md.branch_on_monitoring_decision(**context)

    assert result == "trigger_retrain"


def test_function_exists():
    assert hasattr(md, "branch_on_monitoring_decision")
    assert callable(md.branch_on_monitoring_decision)
