"""Discord notification channel via incoming webhooks."""

from __future__ import annotations

import logging
from typing import Any

import requests

from data_collector.enums.notifications import AlertSeverity
from data_collector.notifications.models import Notification
from data_collector.notifications.notifier import BaseNotifier

logger = logging.getLogger(__name__)

SEVERITY_COLORS: dict[AlertSeverity, int] = {
    AlertSeverity.INFO: 0x36A64F,
    AlertSeverity.WARNING: 0xDAA038,
    AlertSeverity.ERROR: 0xE01E5A,
    AlertSeverity.CRITICAL: 0xCC0000,
}


class DiscordNotifier(BaseNotifier):
    """Send alerts via Discord incoming webhook with rich embed formatting.

    Messages are formatted as Discord embeds with severity-based color
    coding and structured fields for app context and metadata.

    Args:
        webhook_url: Discord incoming webhook URL.
        timeout: HTTP request timeout in seconds.
    """

    CHANNEL_NAME: str = "discord"

    def __init__(self, webhook_url: str, *, timeout: int = 10) -> None:
        super().__init__(self.CHANNEL_NAME)
        self.webhook_url = webhook_url
        self.timeout = timeout

    def send(self, notification: Notification) -> bool:
        """Send a notification via Discord incoming webhook.

        Args:
            notification: The notification payload to deliver.

        Returns:
            True on successful delivery, False on failure.
        """
        payload = self._build_embed(notification)
        try:
            response = requests.post(self.webhook_url, json=payload, timeout=self.timeout)
            if response.status_code in (200, 204):
                return True
            logger.warning(
                "Discord webhook returned status %d: %s",
                response.status_code,
                response.text[:200],
            )
            return False
        except requests.RequestException:
            logger.exception("Failed to send Discord notification")
            return False

    def is_configured(self) -> bool:
        """Check whether Discord webhook URL is present.

        Returns:
            True if webhook_url is set.
        """
        return bool(self.webhook_url)

    def _build_embed(self, notification: Notification) -> dict[str, Any]:
        """Build a Discord embed payload.

        Args:
            notification: The notification to format.

        Returns:
            Discord webhook payload with embeds array.
        """
        title = self.format_title(notification)
        color = SEVERITY_COLORS.get(notification.severity, 0x808080)

        fields: list[dict[str, str | bool]] = [
            {"name": "Severity", "value": notification.severity.name, "inline": True},
        ]

        if notification.app_id:
            fields.append({"name": "App ID", "value": notification.app_id, "inline": True})

        if notification.metadata:
            for key, value in notification.metadata.items():
                fields.append({"name": key, "value": value, "inline": True})

        embed: dict[str, Any] = {
            "title": title,
            "description": notification.message,
            "color": color,
            "fields": fields,
            "timestamp": notification.timestamp.isoformat(),
            "footer": {"text": "Data Collector Notifications"},
        }

        return {"embeds": [embed]}
