"""Tests for the Slack notification channel."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import requests

from data_collector.enums.notifications import AlertSeverity
from data_collector.notifications.models import Notification
from data_collector.notifications.slack import SlackNotifier


class TestSlackNotifier:
    """Tests for the SlackNotifier class."""

    def _make_notification(self, **kwargs: object) -> Notification:
        defaults: dict[str, object] = {
            "severity": AlertSeverity.WARNING,
            "title": "test/app",
            "message": "Something needs attention",
            "timestamp": datetime(2025, 1, 15, 10, 30, 0, tzinfo=UTC),
        }
        defaults.update(kwargs)
        return Notification(**defaults)  # type: ignore[arg-type]

    def test_send_success(self) -> None:
        notifier = SlackNotifier(webhook_url="https://hooks.slack.com/services/T00/B00/xxx")
        mock_response = MagicMock(status_code=200)

        with patch("data_collector.notifications.slack.requests.post", return_value=mock_response) as mock_post:
            result = notifier.send(self._make_notification())

        assert result is True
        mock_post.assert_called_once()

    def test_send_failure(self) -> None:
        notifier = SlackNotifier(webhook_url="https://hooks.slack.com/services/T00/B00/xxx")
        mock_response = MagicMock(status_code=403, text="Forbidden")

        with patch("data_collector.notifications.slack.requests.post", return_value=mock_response):
            result = notifier.send(self._make_notification())

        assert result is False

    def test_send_network_error(self) -> None:
        notifier = SlackNotifier(webhook_url="https://hooks.slack.com/services/T00/B00/xxx")

        with patch(
            "data_collector.notifications.slack.requests.post",
            side_effect=requests.ConnectionError("Network unreachable"),
        ):
            result = notifier.send(self._make_notification())

        assert result is False

    def test_is_configured_true(self) -> None:
        notifier = SlackNotifier(webhook_url="https://hooks.slack.com/services/T00/B00/xxx")
        assert notifier.is_configured() is True

    def test_is_configured_empty(self) -> None:
        notifier = SlackNotifier(webhook_url="")
        assert notifier.is_configured() is False

    def test_block_kit_structure(self) -> None:
        notifier = SlackNotifier(webhook_url="https://test")
        notification = self._make_notification()
        payload = notifier._build_block_kit(notification)  # pyright: ignore[reportPrivateUsage]

        assert "attachments" in payload
        attachment = payload["attachments"][0]
        assert "color" in attachment
        assert "title" in attachment
        assert "fields" in attachment

    def test_channel_override(self) -> None:
        notifier = SlackNotifier(webhook_url="https://test", channel="#alerts")
        notification = self._make_notification()
        payload = notifier._build_block_kit(notification)  # pyright: ignore[reportPrivateUsage]
        assert payload["channel"] == "#alerts"

    def test_no_channel_override(self) -> None:
        notifier = SlackNotifier(webhook_url="https://test")
        notification = self._make_notification()
        payload = notifier._build_block_kit(notification)  # pyright: ignore[reportPrivateUsage]
        assert "channel" not in payload

    def test_severity_color(self) -> None:
        notifier = SlackNotifier(webhook_url="https://test")
        notification = self._make_notification(severity=AlertSeverity.CRITICAL)
        payload = notifier._build_block_kit(notification)  # pyright: ignore[reportPrivateUsage]
        assert payload["attachments"][0]["color"] == "#cc0000"

    def test_channel_name(self) -> None:
        notifier = SlackNotifier(webhook_url="https://test")
        assert notifier.channel_name == "slack"
