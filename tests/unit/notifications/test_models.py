"""Tests for notification data models."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from data_collector.enums.notifications import AlertSeverity
from data_collector.notifications.models import DeliveryResult, Notification


class TestNotification:
    """Tests for the Notification dataclass."""

    def test_required_fields(self) -> None:
        notification = Notification(
            severity=AlertSeverity.CRITICAL,
            title="test/app",
            message="Something failed",
        )
        assert notification.severity == AlertSeverity.CRITICAL
        assert notification.title == "test/app"
        assert notification.message == "Something failed"

    def test_optional_fields_default_to_none(self) -> None:
        notification = Notification(
            severity=AlertSeverity.INFO,
            title="test",
            message="msg",
        )
        assert notification.app_id is None
        assert notification.metadata is None

    def test_timestamp_defaults_to_now(self) -> None:
        before = datetime.now(UTC)
        notification = Notification(
            severity=AlertSeverity.INFO,
            title="test",
            message="msg",
        )
        after = datetime.now(UTC)
        assert before <= notification.timestamp <= after

    def test_custom_timestamp(self) -> None:
        custom_time = datetime(2025, 1, 15, 10, 30, 0, tzinfo=UTC)
        notification = Notification(
            severity=AlertSeverity.WARNING,
            title="test",
            message="msg",
            timestamp=custom_time,
        )
        assert notification.timestamp == custom_time

    def test_all_fields(self) -> None:
        notification = Notification(
            severity=AlertSeverity.ERROR,
            title="croatia/fina/companies",
            message="Connection refused",
            app_id="abc123",
            metadata={"runtime": "5m"},
        )
        assert notification.app_id == "abc123"
        assert notification.metadata == {"runtime": "5m"}

    def test_frozen(self) -> None:
        notification = Notification(
            severity=AlertSeverity.INFO,
            title="test",
            message="msg",
        )
        with pytest.raises(AttributeError):
            notification.title = "changed"  # type: ignore[misc]


class TestDeliveryResult:
    """Tests for the DeliveryResult dataclass."""

    def test_success_result(self) -> None:
        result = DeliveryResult(
            channel_name="telegram",
            success=True,
            attempts=1,
        )
        assert result.channel_name == "telegram"
        assert result.success is True
        assert result.attempts == 1
        assert result.error_message is None

    def test_failure_result(self) -> None:
        result = DeliveryResult(
            channel_name="slack",
            success=False,
            attempts=3,
            error_message="Connection timeout",
        )
        assert result.success is False
        assert result.attempts == 3
        assert result.error_message == "Connection timeout"

    def test_frozen(self) -> None:
        result = DeliveryResult(channel_name="test", success=True, attempts=1)
        with pytest.raises(AttributeError):
            result.success = False  # type: ignore[misc]
