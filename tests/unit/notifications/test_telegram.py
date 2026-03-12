"""Tests for the Telegram notification channel."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import requests

from data_collector.enums.notifications import AlertSeverity
from data_collector.notifications.models import Notification
from data_collector.notifications.telegram import TelegramNotifier


class TestTelegramNotifier:
    """Tests for the TelegramNotifier class."""

    def _make_notification(self, **kwargs: object) -> Notification:
        defaults: dict[str, object] = {
            "severity": AlertSeverity.CRITICAL,
            "title": "croatia/findata/companies",
            "message": "Connection refused to registry API",
            "timestamp": datetime(2025, 1, 15, 10, 30, 0, tzinfo=UTC),
        }
        defaults.update(kwargs)
        return Notification(**defaults)  # type: ignore[arg-type]

    def test_send_success(self) -> None:
        notifier = TelegramNotifier(bot_token="123:ABC", chat_id="-100123")
        mock_response = MagicMock(status_code=200)

        with patch("data_collector.notifications.telegram.requests.post", return_value=mock_response) as mock_post:
            result = notifier.send(self._make_notification())

        assert result is True
        mock_post.assert_called_once()
        call_kwargs = mock_post.call_args
        assert "api.telegram.org/bot123:ABC/sendMessage" in call_kwargs.args[0]
        assert call_kwargs.kwargs["json"]["chat_id"] == "-100123"
        assert call_kwargs.kwargs["json"]["parse_mode"] == "MarkdownV2"

    def test_send_api_error(self) -> None:
        notifier = TelegramNotifier(bot_token="123:ABC", chat_id="-100123")
        mock_response = MagicMock(status_code=400, text="Bad Request")

        with patch("data_collector.notifications.telegram.requests.post", return_value=mock_response):
            result = notifier.send(self._make_notification())

        assert result is False

    def test_send_network_error(self) -> None:
        notifier = TelegramNotifier(bot_token="123:ABC", chat_id="-100123")

        with patch(
            "data_collector.notifications.telegram.requests.post",
            side_effect=requests.ConnectionError("Network unreachable"),
        ):
            result = notifier.send(self._make_notification())

        assert result is False

    def test_is_configured_true(self) -> None:
        notifier = TelegramNotifier(bot_token="123:ABC", chat_id="-100123")
        assert notifier.is_configured() is True

    def test_is_configured_missing_token(self) -> None:
        notifier = TelegramNotifier(bot_token="", chat_id="-100123")
        assert notifier.is_configured() is False

    def test_is_configured_missing_chat_id(self) -> None:
        notifier = TelegramNotifier(bot_token="123:ABC", chat_id="")
        assert notifier.is_configured() is False

    def test_markdown_format_includes_severity(self) -> None:
        notifier = TelegramNotifier(bot_token="t", chat_id="c")
        notification = self._make_notification()
        text = notifier._format_markdown(notification)  # pyright: ignore[reportPrivateUsage]
        assert "*CRITICAL: croatia/findata/companies*" in text

    def test_markdown_format_includes_app_id(self) -> None:
        notifier = TelegramNotifier(bot_token="t", chat_id="c")
        notification = self._make_notification(app_id="abc123")
        text = notifier._format_markdown(notification)  # pyright: ignore[reportPrivateUsage]
        assert "`abc123`" in text

    def test_markdown_format_includes_metadata(self) -> None:
        notifier = TelegramNotifier(bot_token="t", chat_id="c")
        notification = self._make_notification(metadata={"runtime": "5m"})
        text = notifier._format_markdown(notification)  # pyright: ignore[reportPrivateUsage]
        assert "runtime: 5m" in text

    def test_channel_name(self) -> None:
        notifier = TelegramNotifier(bot_token="t", chat_id="c")
        assert notifier.channel_name == "telegram"
