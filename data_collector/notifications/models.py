"""Notification data models for the pluggable alert system."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime

from data_collector.enums.notifications import AlertSeverity


@dataclass(frozen=True)
class Notification:
    """Immutable notification payload dispatched to all enabled channels.

    Args:
        severity: Alert severity level from AlertSeverity enum.
        title: Short alert title (e.g., "croatia/findata/companies").
        message: Alert body with details.
        app_id: Optional application identifier for context.
        metadata: Optional key-value pairs for structured context.
        timestamp: When the event occurred. Defaults to current UTC time.
    """

    severity: AlertSeverity
    title: str
    message: str
    app_id: str | None = None
    metadata: dict[str, str] | None = None
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))


@dataclass(frozen=True)
class DeliveryResult:
    """Result of a single delivery attempt to one channel.

    Args:
        channel_name: Machine-readable channel identifier.
        success: Whether delivery succeeded.
        attempts: Number of attempts made (1 = no retries).
        error_message: Last error message if failed, None if succeeded.
    """

    channel_name: str
    success: bool
    attempts: int
    error_message: str | None = None
