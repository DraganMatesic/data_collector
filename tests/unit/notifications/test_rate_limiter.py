"""Tests for the notification rate limiter."""

from __future__ import annotations

from unittest.mock import patch

from data_collector.enums.notifications import AlertSeverity
from data_collector.notifications.rate_limiter import RateLimiter


class TestRateLimiter:
    """Tests for the RateLimiter class."""

    def test_first_send_is_always_allowed(self) -> None:
        limiter = RateLimiter(min_interval_seconds=30, burst_limit=10)
        assert limiter.is_allowed(AlertSeverity.INFO) is True

    def test_interval_enforcement(self) -> None:
        limiter = RateLimiter(min_interval_seconds=30, burst_limit=10)
        limiter.record_send()
        assert limiter.is_allowed(AlertSeverity.INFO) is False

    def test_interval_expires(self) -> None:
        limiter = RateLimiter(min_interval_seconds=5, burst_limit=10)
        with patch("data_collector.notifications.rate_limiter.time.monotonic", return_value=100.0):
            limiter.record_send()

        with patch("data_collector.notifications.rate_limiter.time.monotonic", return_value=106.0):
            assert limiter.is_allowed(AlertSeverity.INFO) is True

    def test_burst_limit(self) -> None:
        limiter = RateLimiter(min_interval_seconds=0, burst_limit=3, window_seconds=300)
        for _ in range(3):
            limiter.record_send()
        assert limiter.is_allowed(AlertSeverity.WARNING) is False

    def test_burst_window_expiry(self) -> None:
        limiter = RateLimiter(min_interval_seconds=0, burst_limit=3, window_seconds=60)
        with patch("data_collector.notifications.rate_limiter.time.monotonic", return_value=100.0):
            for _ in range(3):
                limiter.record_send()

        with patch("data_collector.notifications.rate_limiter.time.monotonic", return_value=161.0):
            assert limiter.is_allowed(AlertSeverity.INFO) is True

    def test_critical_bypasses_interval(self) -> None:
        limiter = RateLimiter(min_interval_seconds=30, burst_limit=10)
        limiter.record_send()
        assert limiter.is_allowed(AlertSeverity.CRITICAL) is True

    def test_critical_bypasses_burst_limit(self) -> None:
        limiter = RateLimiter(min_interval_seconds=0, burst_limit=2, window_seconds=300)
        for _ in range(5):
            limiter.record_send()
        assert limiter.is_allowed(AlertSeverity.CRITICAL) is True

    def test_reset_clears_state(self) -> None:
        limiter = RateLimiter(min_interval_seconds=30, burst_limit=10)
        limiter.record_send()
        assert limiter.is_allowed(AlertSeverity.INFO) is False
        limiter.reset()
        assert limiter.is_allowed(AlertSeverity.INFO) is True

    def test_default_window_seconds(self) -> None:
        limiter = RateLimiter(min_interval_seconds=30, burst_limit=10)
        assert limiter.window_seconds == 300
