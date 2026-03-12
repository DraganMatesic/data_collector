"""Tests for the email (SMTP) notification channel."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

from data_collector.enums.notifications import AlertSeverity
from data_collector.notifications.email import EmailNotifier
from data_collector.notifications.models import Notification


class TestEmailNotifier:
    """Tests for the EmailNotifier class."""

    def _make_notifier(self, **kwargs: object) -> EmailNotifier:
        defaults: dict[str, object] = {
            "host": "smtp.example.com",
            "port": 587,
            "username": "user@example.com",
            "password": "secret",
            "sender_address": "alerts@example.com",
            "recipient_addresses": "ops@example.com",
        }
        defaults.update(kwargs)
        return EmailNotifier(**defaults)  # type: ignore[arg-type]

    def _make_notification(self, **kwargs: object) -> Notification:
        defaults: dict[str, object] = {
            "severity": AlertSeverity.ERROR,
            "title": "test/app",
            "message": "Something failed",
            "timestamp": datetime(2025, 1, 15, 10, 30, 0, tzinfo=UTC),
        }
        defaults.update(kwargs)
        return Notification(**defaults)  # type: ignore[arg-type]

    def test_send_success_with_tls(self) -> None:
        notifier = self._make_notifier()
        mock_smtp = MagicMock()

        with patch("data_collector.notifications.email.smtplib.SMTP", return_value=mock_smtp):
            mock_smtp.__enter__ = MagicMock(return_value=mock_smtp)
            mock_smtp.__exit__ = MagicMock(return_value=False)
            result = notifier.send(self._make_notification())

        assert result is True
        mock_smtp.starttls.assert_called_once()
        mock_smtp.login.assert_called_once_with("user@example.com", "secret")
        mock_smtp.send_message.assert_called_once()

    def test_send_success_without_tls(self) -> None:
        notifier = self._make_notifier(use_tls=False)
        mock_smtp = MagicMock()

        with patch("data_collector.notifications.email.smtplib.SMTP", return_value=mock_smtp):
            mock_smtp.__enter__ = MagicMock(return_value=mock_smtp)
            mock_smtp.__exit__ = MagicMock(return_value=False)
            result = notifier.send(self._make_notification())

        assert result is True
        mock_smtp.starttls.assert_not_called()

    def test_send_smtp_error(self) -> None:
        notifier = self._make_notifier()

        with patch(
            "data_collector.notifications.email.smtplib.SMTP",
            side_effect=ConnectionRefusedError("Connection refused"),
        ):
            result = notifier.send(self._make_notification())

        assert result is False

    def test_is_configured_true(self) -> None:
        notifier = self._make_notifier()
        assert notifier.is_configured() is True

    def test_is_configured_missing_host(self) -> None:
        notifier = self._make_notifier(host="")
        assert notifier.is_configured() is False

    def test_is_configured_missing_password(self) -> None:
        notifier = self._make_notifier(password="")
        assert notifier.is_configured() is False

    def test_is_configured_missing_sender(self) -> None:
        notifier = self._make_notifier(sender_address="")
        assert notifier.is_configured() is False

    def test_is_configured_missing_recipients(self) -> None:
        notifier = self._make_notifier(recipient_addresses="")
        assert notifier.is_configured() is False

    def test_email_subject_contains_severity(self) -> None:
        notifier = self._make_notifier()
        notification = self._make_notification()
        message = notifier._build_email_message(notification)  # pyright: ignore[reportPrivateUsage]
        assert message["Subject"] == "[Data Collector] ERROR: test/app"

    def test_email_recipients(self) -> None:
        notifier = self._make_notifier(recipient_addresses="a@test.com, b@test.com")
        notification = self._make_notification()
        message = notifier._build_email_message(notification)  # pyright: ignore[reportPrivateUsage]
        assert message["To"] == "a@test.com, b@test.com"

    def test_email_body_includes_message(self) -> None:
        notifier = self._make_notifier()
        notification = self._make_notification()
        message = notifier._build_email_message(notification)  # pyright: ignore[reportPrivateUsage]
        body = message.get_content()
        assert "Something failed" in body

    def test_email_body_includes_app_id(self) -> None:
        notifier = self._make_notifier()
        notification = self._make_notification(app_id="abc123")
        message = notifier._build_email_message(notification)  # pyright: ignore[reportPrivateUsage]
        body = message.get_content()
        assert "abc123" in body

    def test_channel_name(self) -> None:
        notifier = self._make_notifier()
        assert notifier.channel_name == "email"
