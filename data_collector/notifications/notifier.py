"""Base notifier interface for all notification channels."""

from __future__ import annotations

from abc import ABC, abstractmethod

from data_collector.notifications.models import Notification


class BaseNotifier(ABC):
    """Abstract base class for notification channel implementations.

    All notification channels (Telegram, Slack, email, Discord, webhooks)
    implement this interface. Each channel's ``send()`` is a single-attempt
    method with no retry logic -- retries are handled by the
    ``NotificationDispatcher``.

    Args:
        channel_name: Machine-readable identifier (e.g., "telegram", "slack").
    """

    def __init__(self, channel_name: str) -> None:
        self.channel_name = channel_name

    @abstractmethod
    def send(self, notification: Notification) -> bool:
        """Send a notification via this channel. Single attempt, no retries.

        Args:
            notification: The notification payload to deliver.

        Returns:
            True on successful delivery, False on failure.
        """
        ...

    @abstractmethod
    def is_configured(self) -> bool:
        """Check whether this channel has valid configuration to send.

        Returns:
            True if all required settings are present and non-empty.
        """
        ...

    def format_title(self, notification: Notification) -> str:
        """Format the alert title with severity prefix.

        Args:
            notification: The notification to format.

        Returns:
            Title string like "CRITICAL: croatia/findata/companies".
        """
        return f"{notification.severity.name}: {notification.title}"
