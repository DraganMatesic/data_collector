"""Notification severity enum."""

from enum import IntEnum


class AlertSeverity(IntEnum):
    """Severity levels for alerts."""

    INFO = 1
    WARNING = 2
    ERROR = 3
    CRITICAL = 4
