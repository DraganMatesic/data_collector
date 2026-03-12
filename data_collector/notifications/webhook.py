"""Generic HTTP webhook notification channel."""

from __future__ import annotations

import json
import logging
from typing import Any, cast

import requests

from data_collector.notifications.models import Notification
from data_collector.notifications.notifier import BaseNotifier

logger = logging.getLogger(__name__)


class WebhookNotifier(BaseNotifier):
    """Send alerts via generic HTTP POST webhook.

    Sends a JSON payload with notification fields to any HTTP endpoint.
    Supports optional Bearer token authentication and custom headers.

    Args:
        url: Target webhook URL.
        custom_headers: Optional dict of custom HTTP headers.
        auth_token: Optional Bearer token for Authorization header.
        timeout: HTTP request timeout in seconds.
    """

    CHANNEL_NAME: str = "webhook"

    def __init__(
        self,
        url: str,
        custom_headers: dict[str, str] | None = None,
        auth_token: str | None = None,
        *,
        timeout: int = 10,
    ) -> None:
        super().__init__(self.CHANNEL_NAME)
        self.url = url
        self.custom_headers = custom_headers or {}
        self.auth_token = auth_token
        self.timeout = timeout

    def send(self, notification: Notification) -> bool:
        """Send a notification via HTTP POST webhook.

        Args:
            notification: The notification payload to deliver.

        Returns:
            True on successful delivery, False on failure.
        """
        payload = self._build_payload(notification)
        headers = dict(self.custom_headers)
        if self.auth_token:
            headers["Authorization"] = f"Bearer {self.auth_token}"

        try:
            response = requests.post(self.url, json=payload, headers=headers, timeout=self.timeout)
            if response.status_code in (200, 201, 202, 204):
                return True
            logger.warning(
                "Webhook returned status %d: %s",
                response.status_code,
                response.text[:200],
            )
            return False
        except requests.RequestException:
            logger.exception("Failed to send webhook notification")
            return False

    def is_configured(self) -> bool:
        """Check whether webhook URL is present.

        Returns:
            True if url is set.
        """
        return bool(self.url)

    @classmethod
    def parse_headers(cls, headers_json: str | None) -> dict[str, str] | None:
        """Parse a JSON string into a headers dictionary.

        Utility for constructing WebhookNotifier from settings where
        headers are stored as a JSON string environment variable.

        Args:
            headers_json: JSON string of headers, or None.

        Returns:
            Parsed headers dict, or None if input is None or empty.
        """
        if not headers_json:
            return None
        try:
            parsed = json.loads(headers_json)
            if isinstance(parsed, dict):
                typed_dict = cast(dict[str, object], parsed)
                return {str(key): str(value) for key, value in typed_dict.items()}
        except (json.JSONDecodeError, TypeError):
            logger.warning("Failed to parse webhook headers JSON: %s", headers_json[:100])
        return None

    def _build_payload(self, notification: Notification) -> dict[str, Any]:
        """Build a JSON payload for the webhook.

        Args:
            notification: The notification to format.

        Returns:
            Dictionary payload for JSON serialization.
        """
        payload: dict[str, Any] = {
            "severity": notification.severity.name,
            "title": notification.title,
            "message": notification.message,
            "timestamp": notification.timestamp.isoformat(),
        }

        if notification.app_id:
            payload["app_id"] = notification.app_id

        if notification.metadata:
            payload["metadata"] = notification.metadata

        return payload
