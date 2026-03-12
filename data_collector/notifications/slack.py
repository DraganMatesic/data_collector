"""Slack notification channel via incoming webhooks."""

from __future__ import annotations

import logging
from typing import Any

import requests

from data_collector.enums.notifications import AlertSeverity
from data_collector.notifications.models import Notification
from data_collector.notifications.notifier import BaseNotifier

logger = logging.getLogger(__name__)

SEVERITY_COLORS: dict[AlertSeverity, str] = {
    AlertSeverity.INFO: "#36a64f",
    AlertSeverity.WARNING: "#daa038",
    AlertSeverity.ERROR: "#e01e5a",
    AlertSeverity.CRITICAL: "#cc0000",
}


class SlackNotifier(BaseNotifier):
    """Send alerts via Slack incoming webhook with Block Kit formatting.

    Messages include severity-colored attachment sidebar and structured
    fields for app context and metadata.

    Args:
        webhook_url: Slack incoming webhook URL.
        channel: Optional channel override (e.g., "#alerts").
        timeout: HTTP request timeout in seconds.
    """

    CHANNEL_NAME: str = "slack"

    def __init__(self, webhook_url: str, channel: str | None = None, *, timeout: int = 10) -> None:
        super().__init__(self.CHANNEL_NAME)
        self.webhook_url = webhook_url
        self.channel = channel
        self.timeout = timeout

    def send(self, notification: Notification) -> bool:
        """Send a notification via Slack incoming webhook.

        Args:
            notification: The notification payload to deliver.

        Returns:
            True on successful delivery, False on failure.
        """
        payload = self._build_block_kit(notification)
        try:
            response = requests.post(self.webhook_url, json=payload, timeout=self.timeout)
            if response.status_code == 200:
                return True
            logger.warning(
                "Slack webhook returned status %d: %s",
                response.status_code,
                response.text[:200],
            )
            return False
        except requests.RequestException:
            logger.exception("Failed to send Slack notification")
            return False

    def is_configured(self) -> bool:
        """Check whether Slack webhook URL is present.

        Returns:
            True if webhook_url is set.
        """
        return bool(self.webhook_url)

    def _build_block_kit(self, notification: Notification) -> dict[str, Any]:
        """Build a Block Kit payload for Slack.

        Args:
            notification: The notification to format.

        Returns:
            Slack message payload with attachments.
        """
        title = self.format_title(notification)
        color = SEVERITY_COLORS.get(notification.severity, "#808080")

        fields: list[dict[str, str | bool]] = [
            {"title": "Severity", "value": notification.severity.name, "short": True},
            {
                "title": "Time",
                "value": notification.timestamp.strftime("%Y-%m-%d %H:%M:%S UTC"),
                "short": True,
            },
        ]

        if notification.app_id:
            fields.append({"title": "App ID", "value": notification.app_id, "short": True})

        if notification.metadata:
            for key, value in notification.metadata.items():
                fields.append({"title": key, "value": value, "short": True})

        payload: dict[str, Any] = {
            "attachments": [
                {
                    "color": color,
                    "title": title,
                    "text": notification.message,
                    "fields": fields,
                    "footer": "Data Collector Notifications",
                }
            ]
        }

        if self.channel:
            payload["channel"] = self.channel

        return payload
