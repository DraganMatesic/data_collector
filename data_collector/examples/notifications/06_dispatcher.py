"""Multi-channel notification dispatch with severity filtering and rate limiting.

Demonstrates:
    - NotificationDispatcher.from_settings() factory
    - Multi-channel delivery (all enabled channels receive the alert)
    - Severity filtering (only alerts at or above threshold are sent)
    - DeliveryResult inspection
    - Channel status reporting

Setup:
    1. Set DC_NOTIFICATION_NOTIFICATIONS_ENABLED=true.
    2. Set DC_NOTIFICATION_CHANNELS to a comma-separated list of channels
       to enable (e.g., "telegram,slack,discord").
    3. Set the channel-specific environment variables for each enabled
       channel (see individual examples 01-05 for per-channel setup).

Requires:
    DC_NOTIFICATION_NOTIFICATIONS_ENABLED=true
    DC_NOTIFICATION_CHANNELS (comma-separated channel list)
    Plus channel-specific env vars for each enabled channel.

Run:
    python -m data_collector.examples run notifications/06_dispatcher
"""

from __future__ import annotations

import os

from data_collector.enums.notifications import AlertSeverity
from data_collector.notifications.dispatcher import NotificationDispatcher
from data_collector.notifications.models import Notification
from data_collector.settings.notification import NotificationSettings

_REQUIRED_ENV = ("DC_NOTIFICATION_NOTIFICATIONS_ENABLED", "DC_NOTIFICATION_CHANNELS")


def main() -> None:
    """Demonstrate multi-channel notification dispatch."""
    missing = [variable for variable in _REQUIRED_ENV if not os.environ.get(variable)]
    if missing:
        print(f"Skipping: env vars not set: {', '.join(missing)}")
        return

    settings = NotificationSettings()

    if not settings.notifications_enabled:
        print("Notifications are disabled (DC_NOTIFICATION_NOTIFICATIONS_ENABLED != true)")
        return

    if not settings.notification_channels:
        print("No channels configured (DC_NOTIFICATION_NOTIFICATION_CHANNELS is empty)")
        return

    dispatcher = NotificationDispatcher.from_settings(settings)

    print(f"Enabled channels: {dispatcher.enabled_channels}")
    print(f"Severity threshold: {AlertSeverity(settings.alert_min_severity).name}")
    print()

    notification = Notification(
        severity=AlertSeverity.WARNING,
        title="data_collector/test",
        message="This is a test notification dispatched to all enabled channels.",
        app_id="example_app_id",
        metadata={"source": "06_dispatcher.py example"},
    )

    print(f"Dispatching {notification.severity.name} notification...")
    results = dispatcher.send(notification)

    if not results:
        print("No channels received the notification (severity below threshold or all rate-limited)")
    else:
        for result in results:
            status = "SUCCESS" if result.success else "FAILED"
            attempts_label = f" ({result.attempts} attempt{'s' if result.attempts != 1 else ''})"
            error_label = f" -- {result.error_message}" if result.error_message else ""
            print(f"  {result.channel_name}: {status}{attempts_label}{error_label}")

    print()
    print(f"Enabled channels: {dispatcher.enabled_channels}")
    print(f"Disabled channels: {dispatcher.disabled_channels}")


if __name__ == "__main__":
    main()
