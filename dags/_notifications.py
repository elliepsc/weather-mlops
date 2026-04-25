"""Shared notification helpers for all weather DAGs.

Single source of truth for Slack alerts.
Import this module instead of defining _send_slack_alert locally in each DAG.

Usage:
    from dags._notifications import send_slack_alert
"""

import logging

logger = logging.getLogger(__name__)


def send_slack_alert(message: str) -> None:
    """Post a message to Slack via the configured webhook.

    Reads slack_webhook_url from settings. If the URL is not set, the message
    is logged at WARNING level and the function returns silently — no exception
    is raised so alert failures never break a DAG run.

    Args:
        message: Plain text or Slack mrkdwn-formatted string.
    """
    import requests
    from config.settings import settings

    if not settings.slack_webhook_url:
        logger.warning("Slack webhook not configured — alert logged only: %s", message)
        return

    try:
        response = requests.post(
            settings.slack_webhook_url,
            json={"text": message},
            timeout=5,
        )
        response.raise_for_status()
        logger.info("Slack alert sent (%d chars).", len(message))
    except Exception as exc:  # noqa: BLE001
        logger.warning("Slack alert failed: %s", exc)
