"""Tests for the Discord notification channel."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import requests

from data_collector.enums.notifications import AlertSeverity
from data_collector.notifications.discord import SEVERITY_COLORS, DiscordNotifier
from data_collector.notifications.models import Notification


class TestDiscordNotifier:
    """Tests for the DiscordNotifier class."""

    def _make_notification(self, **kwargs: object) -> Notification:
        defaults: dict[str, object] = {
            "severity": AlertSeverity.ERROR,
            "title": "test/app",
            "message": "Something failed",
            "timestamp": datetime(2025, 1, 15, 10, 30, 0, tzinfo=UTC),
        }
        defaults.update(kwargs)
        return Notification(**defaults)  # type: ignore[arg-type]

    def test_send_success_200(self) -> None:
        notifier = DiscordNotifier(webhook_url="https://discord.com/api/webhooks/123/abc")
        mock_response = MagicMock(status_code=200)

        with patch("data_collector.notifications.discord.requests.post", return_value=mock_response) as mock_post:
            result = notifier.send(self._make_notification())

        assert result is True
        mock_post.assert_called_once()

    def test_send_success_204(self) -> None:
        notifier = DiscordNotifier(webhook_url="https://discord.com/api/webhooks/123/abc")
        mock_response = MagicMock(status_code=204)

        with patch("data_collector.notifications.discord.requests.post", return_value=mock_response):
            result = notifier.send(self._make_notification())

        assert result is True

    def test_send_failure(self) -> None:
        notifier = DiscordNotifier(webhook_url="https://discord.com/api/webhooks/123/abc")
        mock_response = MagicMock(status_code=429, text="Too Many Requests")

        with patch("data_collector.notifications.discord.requests.post", return_value=mock_response):
            result = notifier.send(self._make_notification())

        assert result is False

    def test_send_network_error(self) -> None:
        notifier = DiscordNotifier(webhook_url="https://discord.com/api/webhooks/123/abc")

        with patch(
            "data_collector.notifications.discord.requests.post",
            side_effect=requests.ConnectionError("Network unreachable"),
        ):
            result = notifier.send(self._make_notification())

        assert result is False

    def test_is_configured_true(self) -> None:
        notifier = DiscordNotifier(webhook_url="https://discord.com/api/webhooks/123/abc")
        assert notifier.is_configured() is True

    def test_is_configured_empty(self) -> None:
        notifier = DiscordNotifier(webhook_url="")
        assert notifier.is_configured() is False

    def test_embed_structure(self) -> None:
        notifier = DiscordNotifier(webhook_url="https://test")
        notification = self._make_notification()
        payload = notifier._build_embed(notification)  # pyright: ignore[reportPrivateUsage]

        assert "embeds" in payload
        embed = payload["embeds"][0]
        assert embed["title"] == "ERROR: test/app"
        assert embed["description"] == "Something failed"
        assert "color" in embed
        assert "fields" in embed
        assert "timestamp" in embed

    def test_severity_colors(self) -> None:
        notifier = DiscordNotifier(webhook_url="https://test")

        for severity in AlertSeverity:
            notification = self._make_notification(severity=severity)
            payload = notifier._build_embed(notification)  # pyright: ignore[reportPrivateUsage]
            embed = payload["embeds"][0]
            assert embed["color"] == SEVERITY_COLORS[severity]

    def test_embed_includes_app_id(self) -> None:
        notifier = DiscordNotifier(webhook_url="https://test")
        notification = self._make_notification(app_id="abc123")
        payload = notifier._build_embed(notification)  # pyright: ignore[reportPrivateUsage]
        embed = payload["embeds"][0]
        field_values = [field["value"] for field in embed["fields"]]
        assert "abc123" in field_values

    def test_embed_includes_metadata(self) -> None:
        notifier = DiscordNotifier(webhook_url="https://test")
        notification = self._make_notification(metadata={"runtime": "5m"})
        payload = notifier._build_embed(notification)  # pyright: ignore[reportPrivateUsage]
        embed = payload["embeds"][0]
        field_names = [field["name"] for field in embed["fields"]]
        assert "runtime" in field_names

    def test_channel_name(self) -> None:
        notifier = DiscordNotifier(webhook_url="https://test")
        assert notifier.channel_name == "discord"
