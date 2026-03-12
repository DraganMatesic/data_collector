"""Tests for the generic webhook notification channel."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import requests

from data_collector.enums.notifications import AlertSeverity
from data_collector.notifications.models import Notification
from data_collector.notifications.webhook import WebhookNotifier


class TestWebhookNotifier:
    """Tests for the WebhookNotifier class."""

    def _make_notification(self, **kwargs: object) -> Notification:
        defaults: dict[str, object] = {
            "severity": AlertSeverity.ERROR,
            "title": "test/app",
            "message": "Something failed",
            "timestamp": datetime(2025, 1, 15, 10, 30, 0, tzinfo=UTC),
        }
        defaults.update(kwargs)
        return Notification(**defaults)  # type: ignore[arg-type]

    def test_send_success(self) -> None:
        notifier = WebhookNotifier(url="https://webhook.site/test")
        mock_response = MagicMock(status_code=200)

        with patch("data_collector.notifications.webhook.requests.post", return_value=mock_response) as mock_post:
            result = notifier.send(self._make_notification())

        assert result is True
        mock_post.assert_called_once()

    def test_send_accepts_201(self) -> None:
        notifier = WebhookNotifier(url="https://webhook.site/test")
        mock_response = MagicMock(status_code=201)

        with patch("data_collector.notifications.webhook.requests.post", return_value=mock_response):
            result = notifier.send(self._make_notification())

        assert result is True

    def test_send_accepts_202(self) -> None:
        notifier = WebhookNotifier(url="https://webhook.site/test")
        mock_response = MagicMock(status_code=202)

        with patch("data_collector.notifications.webhook.requests.post", return_value=mock_response):
            result = notifier.send(self._make_notification())

        assert result is True

    def test_send_accepts_204(self) -> None:
        notifier = WebhookNotifier(url="https://webhook.site/test")
        mock_response = MagicMock(status_code=204)

        with patch("data_collector.notifications.webhook.requests.post", return_value=mock_response):
            result = notifier.send(self._make_notification())

        assert result is True

    def test_send_failure(self) -> None:
        notifier = WebhookNotifier(url="https://webhook.site/test")
        mock_response = MagicMock(status_code=500, text="Internal Server Error")

        with patch("data_collector.notifications.webhook.requests.post", return_value=mock_response):
            result = notifier.send(self._make_notification())

        assert result is False

    def test_send_network_error(self) -> None:
        notifier = WebhookNotifier(url="https://webhook.site/test")

        with patch(
            "data_collector.notifications.webhook.requests.post",
            side_effect=requests.ConnectionError("Network unreachable"),
        ):
            result = notifier.send(self._make_notification())

        assert result is False

    def test_auth_token_header(self) -> None:
        notifier = WebhookNotifier(url="https://test", auth_token="my-token")
        mock_response = MagicMock(status_code=200)

        with patch("data_collector.notifications.webhook.requests.post", return_value=mock_response) as mock_post:
            notifier.send(self._make_notification())

        call_kwargs = mock_post.call_args.kwargs
        assert call_kwargs["headers"]["Authorization"] == "Bearer my-token"

    def test_custom_headers(self) -> None:
        notifier = WebhookNotifier(url="https://test", custom_headers={"X-Custom": "value"})
        mock_response = MagicMock(status_code=200)

        with patch("data_collector.notifications.webhook.requests.post", return_value=mock_response) as mock_post:
            notifier.send(self._make_notification())

        call_kwargs = mock_post.call_args.kwargs
        assert call_kwargs["headers"]["X-Custom"] == "value"

    def test_is_configured_true(self) -> None:
        notifier = WebhookNotifier(url="https://test")
        assert notifier.is_configured() is True

    def test_is_configured_empty(self) -> None:
        notifier = WebhookNotifier(url="")
        assert notifier.is_configured() is False

    def test_payload_structure(self) -> None:
        notifier = WebhookNotifier(url="https://test")
        notification = self._make_notification(app_id="abc123", metadata={"key": "val"})
        payload = notifier._build_payload(notification)  # pyright: ignore[reportPrivateUsage]

        assert payload["severity"] == "ERROR"
        assert payload["title"] == "test/app"
        assert payload["message"] == "Something failed"
        assert payload["app_id"] == "abc123"
        assert payload["metadata"] == {"key": "val"}
        assert "timestamp" in payload

    def test_payload_excludes_none_fields(self) -> None:
        notifier = WebhookNotifier(url="https://test")
        notification = self._make_notification()
        payload = notifier._build_payload(notification)  # pyright: ignore[reportPrivateUsage]

        assert "app_id" not in payload
        assert "metadata" not in payload

    def test_parse_headers_valid_json(self) -> None:
        result = WebhookNotifier.parse_headers('{"X-Custom": "value"}')
        assert result == {"X-Custom": "value"}

    def test_parse_headers_none(self) -> None:
        result = WebhookNotifier.parse_headers(None)
        assert result is None

    def test_parse_headers_empty(self) -> None:
        result = WebhookNotifier.parse_headers("")
        assert result is None

    def test_parse_headers_invalid_json(self) -> None:
        result = WebhookNotifier.parse_headers("not json")
        assert result is None

    def test_channel_name(self) -> None:
        notifier = WebhookNotifier(url="https://test")
        assert notifier.channel_name == "webhook"
