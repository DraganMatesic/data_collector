"""Tests for the notification dispatcher."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import patch

import pytest

from data_collector.enums.notifications import AlertSeverity
from data_collector.notifications.dispatcher import NotificationDispatcher
from data_collector.notifications.models import Notification
from data_collector.notifications.notifier import BaseNotifier
from data_collector.notifications.rate_limiter import RateLimiter
from data_collector.settings.notification import NotificationSettings


class StubNotifier(BaseNotifier):
    """Stub notifier for testing dispatcher orchestration."""

    def __init__(self, channel_name: str, *, configured: bool = True, succeeds: bool = True) -> None:
        super().__init__(channel_name)
        self._configured = configured
        self._succeeds = succeeds
        self.send_count = 0

    def send(self, notification: Notification) -> bool:
        self.send_count += 1
        return self._succeeds

    def is_configured(self) -> bool:
        return self._configured


def _make_notification(**kwargs: object) -> Notification:
    defaults: dict[str, object] = {
        "severity": AlertSeverity.ERROR,
        "title": "test/app",
        "message": "Something failed",
        "timestamp": datetime(2025, 1, 15, 10, 30, 0, tzinfo=UTC),
    }
    defaults.update(kwargs)
    return Notification(**defaults)  # type: ignore[arg-type]


class TestNotificationDispatcher:
    """Tests for the NotificationDispatcher class."""

    def test_send_dispatches_to_all_channels(self) -> None:
        notifier_a = StubNotifier("channel_a")
        notifier_b = StubNotifier("channel_b")
        dispatcher = NotificationDispatcher(
            [notifier_a, notifier_b],
            min_severity=AlertSeverity.INFO,
            max_retry_attempts=1,
        )

        results = dispatcher.send(_make_notification())
        assert len(results) == 2
        assert all(result.success for result in results)

    def test_severity_filtering(self) -> None:
        notifier = StubNotifier("test")
        dispatcher = NotificationDispatcher(
            [notifier],
            min_severity=AlertSeverity.ERROR,
            max_retry_attempts=1,
        )

        results = dispatcher.send(_make_notification(severity=AlertSeverity.WARNING))
        assert len(results) == 0
        assert notifier.send_count == 0

    def test_severity_at_threshold(self) -> None:
        notifier = StubNotifier("test")
        dispatcher = NotificationDispatcher(
            [notifier],
            min_severity=AlertSeverity.ERROR,
            max_retry_attempts=1,
        )

        results = dispatcher.send(_make_notification(severity=AlertSeverity.ERROR))
        assert len(results) == 1

    def test_retry_on_failure(self) -> None:
        notifier = StubNotifier("test", succeeds=False)
        dispatcher = NotificationDispatcher(
            [notifier],
            min_severity=AlertSeverity.INFO,
            max_retry_attempts=3,
            retry_base_delay=0,
        )

        with patch("data_collector.notifications.dispatcher.time.sleep"):
            results = dispatcher.send(_make_notification())

        assert len(results) == 1
        assert results[0].success is False
        assert results[0].attempts == 3
        assert notifier.send_count == 3

    def test_retry_succeeds_on_second_attempt(self) -> None:
        notifier = StubNotifier("test")
        call_count = 0

        def alternating_send(notification: Notification) -> bool:
            nonlocal call_count
            call_count += 1
            return call_count >= 2

        notifier.send = alternating_send  # type: ignore[assignment]

        dispatcher = NotificationDispatcher(
            [notifier],
            min_severity=AlertSeverity.INFO,
            max_retry_attempts=3,
            retry_base_delay=0,
        )

        with patch("data_collector.notifications.dispatcher.time.sleep"):
            results = dispatcher.send(_make_notification())

        assert results[0].success is True
        assert results[0].attempts == 2

    def test_channel_disabled_after_consecutive_failures(self) -> None:
        notifier = StubNotifier("test", succeeds=False)
        dispatcher = NotificationDispatcher(
            [notifier],
            min_severity=AlertSeverity.INFO,
            max_retry_attempts=1,
            max_consecutive_failures=3,
        )

        for _ in range(3):
            dispatcher.send(_make_notification())

        assert "test" in dispatcher.disabled_channels
        results = dispatcher.send(_make_notification())
        assert len(results) == 0

    def test_enable_channel(self) -> None:
        notifier = StubNotifier("test", succeeds=False)
        dispatcher = NotificationDispatcher(
            [notifier],
            min_severity=AlertSeverity.INFO,
            max_retry_attempts=1,
            max_consecutive_failures=1,
        )
        dispatcher.send(_make_notification())
        assert "test" in dispatcher.disabled_channels

        dispatcher.enable_channel("test")
        assert "test" in dispatcher.enabled_channels
        assert "test" not in dispatcher.disabled_channels

    def test_enable_unknown_channel_raises(self) -> None:
        dispatcher = NotificationDispatcher([], min_severity=AlertSeverity.INFO)
        with pytest.raises(ValueError, match="Unknown channel"):
            dispatcher.enable_channel("nonexistent")

    def test_send_to_channel(self) -> None:
        notifier = StubNotifier("test")
        dispatcher = NotificationDispatcher(
            [notifier],
            min_severity=AlertSeverity.CRITICAL,
            max_retry_attempts=1,
        )

        result = dispatcher.send_to_channel("test", _make_notification(severity=AlertSeverity.INFO))
        assert result.success is True

    def test_send_to_unknown_channel_raises(self) -> None:
        dispatcher = NotificationDispatcher([], min_severity=AlertSeverity.INFO)
        with pytest.raises(ValueError, match="Unknown channel"):
            dispatcher.send_to_channel("nonexistent", _make_notification())

    def test_rate_limiting(self) -> None:
        notifier = StubNotifier("test")
        limiter = RateLimiter(min_interval_seconds=30, burst_limit=10)
        limiter.record_send()

        dispatcher = NotificationDispatcher(
            [notifier],
            min_severity=AlertSeverity.INFO,
            max_retry_attempts=1,
        )
        dispatcher._rate_limiters["test"] = limiter  # pyright: ignore[reportPrivateUsage]

        results = dispatcher.send(_make_notification(severity=AlertSeverity.WARNING))
        assert len(results) == 1
        assert results[0].success is False
        assert results[0].error_message == "Rate limited"
        assert notifier.send_count == 0

    def test_critical_bypasses_rate_limit(self) -> None:
        notifier = StubNotifier("test")
        limiter = RateLimiter(min_interval_seconds=30, burst_limit=10)
        limiter.record_send()

        dispatcher = NotificationDispatcher(
            [notifier],
            min_severity=AlertSeverity.INFO,
            max_retry_attempts=1,
        )
        dispatcher._rate_limiters["test"] = limiter  # pyright: ignore[reportPrivateUsage]

        results = dispatcher.send(_make_notification(severity=AlertSeverity.CRITICAL))
        assert len(results) == 1
        assert results[0].success is True

    def test_enabled_channels_property(self) -> None:
        dispatcher = NotificationDispatcher(
            [StubNotifier("a"), StubNotifier("b")],
            min_severity=AlertSeverity.INFO,
        )
        assert sorted(dispatcher.enabled_channels) == ["a", "b"]

    def test_disabled_channels_property_empty(self) -> None:
        dispatcher = NotificationDispatcher(
            [StubNotifier("a")],
            min_severity=AlertSeverity.INFO,
        )
        assert dispatcher.disabled_channels == []


class TestNotificationDispatcherFromSettings:
    """Tests for the from_settings factory method."""

    def test_from_settings_creates_configured_channels(self) -> None:
        settings = NotificationSettings(
            notifications_enabled=True,
            channels="telegram",
            telegram_bot_token="123:ABC",
            telegram_chat_id="-100123",
        )
        dispatcher = NotificationDispatcher.from_settings(settings)
        assert "telegram" in dispatcher.enabled_channels

    def test_from_settings_skips_unconfigured_channels(self) -> None:
        settings = NotificationSettings(
            notifications_enabled=True,
            channels="telegram",
            telegram_bot_token="",
            telegram_chat_id="",
        )
        dispatcher = NotificationDispatcher.from_settings(settings)
        assert dispatcher.enabled_channels == []

    def test_from_settings_skips_unknown_channels(self) -> None:
        settings = NotificationSettings(
            notifications_enabled=True,
            channels="nonexistent",
        )
        dispatcher = NotificationDispatcher.from_settings(settings)
        assert dispatcher.enabled_channels == []

    def test_from_settings_multiple_channels(self) -> None:
        settings = NotificationSettings(
            notifications_enabled=True,
            channels="telegram,slack",
            telegram_bot_token="123:ABC",
            telegram_chat_id="-100123",
            slack_webhook_url="https://hooks.slack.com/test",
        )
        dispatcher = NotificationDispatcher.from_settings(settings)
        assert sorted(dispatcher.enabled_channels) == ["slack", "telegram"]
