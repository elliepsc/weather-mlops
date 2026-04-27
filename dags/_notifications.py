"""Shared notification helpers for all weather DAGs.

Usage:
    from dags._notifications import send_slack_alert

    # Always send:
    send_slack_alert("something happened")

    # Deduplicated — skipped if same alert_key fired within cooldown_hours:
    send_slack_alert("gap detected", alert_key="gap_monitor", cooldown_hours=24)

Dedup state is persisted in data/monitoring/alert_dedup.json so it survives
Airflow restarts. One entry per alert_key, value = last send timestamp (ISO).
"""

import json
import logging
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

# Mirrors ROOT computation used in all DAG files (dags/ is one level deep).
_MONITORING_DIR = Path(__file__).parent.parent / "data" / "monitoring"
_DEDUP_FILE = _MONITORING_DIR / "alert_dedup.json"


def _load_dedup() -> dict:
    if _DEDUP_FILE.exists():
        try:
            return json.loads(_DEDUP_FILE.read_text())
        except Exception:
            return {}
    return {}


def _save_dedup(state: dict) -> None:
    _MONITORING_DIR.mkdir(parents=True, exist_ok=True)
    _DEDUP_FILE.write_text(json.dumps(state, indent=2))


def _is_duplicate(alert_key: str, cooldown_hours: int) -> bool:
    state = _load_dedup()
    last_sent_str = state.get(alert_key)
    if not last_sent_str:
        return False
    try:
        last_sent = datetime.fromisoformat(last_sent_str)
        # Make timezone-aware if naive
        if last_sent.tzinfo is None:
            last_sent = last_sent.replace(tzinfo=UTC)
        age = datetime.now(tz=UTC) - last_sent
        return age < timedelta(hours=cooldown_hours)
    except Exception:
        return False


def _record_send(alert_key: str) -> None:
    state = _load_dedup()
    state[alert_key] = datetime.now(tz=UTC).isoformat()
    _save_dedup(state)


def send_slack_alert(
    message: str,
    alert_key: str | None = None,
    cooldown_hours: int = 24,
) -> None:
    """Post a message to Slack via the configured webhook.

    Args:
        message:        Plain text or Slack mrkdwn-formatted string.
        alert_key:      Optional dedup key. If provided and the same key was
                        sent within cooldown_hours, the alert is skipped.
                        Pass None to always send (e.g. one-shot critical alerts).
        cooldown_hours: Dedup window in hours (default 24).
    """
    import requests

    from config.settings import settings

    if alert_key and _is_duplicate(alert_key, cooldown_hours):
        logger.info(
            "Alert '%s' already sent within %dh — skipped (dedup).",
            alert_key,
            cooldown_hours,
        )
        return

    if not settings.slack_webhook_url:
        logger.warning("Slack webhook not configured — alert logged only: %s", message)
        # Still record send so we don't log-spam either
        if alert_key:
            _record_send(alert_key)
        return

    try:
        response = requests.post(
            settings.slack_webhook_url,
            json={"text": message},
            timeout=5,
        )
        response.raise_for_status()
        logger.info("Slack alert sent (%d chars).", len(message))
        if alert_key:
            _record_send(alert_key)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Slack alert failed: %s", exc)
