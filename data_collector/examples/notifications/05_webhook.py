"""Send a test alert via generic HTTP webhook.

Demonstrates:
    - WebhookNotifier instantiation from environment variables
    - Sending a test notification with JSON payload
    - Optional Bearer token authentication
    - Optional custom headers
    - Checking delivery result

Setup:
    Point to any HTTP endpoint that accepts POST requests. For testing:
    1. Go to https://webhook.site and copy the unique URL.
    2. Set the URL as DC_NOTIFICATION_WEBHOOK_URL.
    3. Send the test notification and check the webhook.site dashboard.

Requires:
    DC_NOTIFICATION_WEBHOOK_URL environment variable.

Optional:
    DC_NOTIFICATION_WEBHOOK_AUTH_TOKEN -- Bearer token for Authorization header.
    DC_NOTIFICATION_WEBHOOK_HEADERS -- JSON string of custom headers,
        e.g., '{"X-Custom": "value"}'.

Run:
    python -m data_collector.examples run notifications/05_webhook
"""

from __future__ import annotations

import os

from data_collector.enums.notifications import AlertSeverity
from data_collector.notifications.models import Notification
from data_collector.notifications.webhook import WebhookNotifier

_REQUIRED_ENV = ("DC_NOTIFICATION_WEBHOOK_URL",)


def main() -> None:
    """Send a test notification via generic webhook."""
    missing = [variable for variable in _REQUIRED_ENV if not os.environ.get(variable)]
    if missing:
        print(f"Skipping: env vars not set: {', '.join(missing)}")
        return

    notifier = WebhookNotifier(
        url=os.environ["DC_NOTIFICATION_WEBHOOK_URL"],
        custom_headers=WebhookNotifier.parse_headers(os.environ.get("DC_NOTIFICATION_WEBHOOK_HEADERS")),
        auth_token=os.environ.get("DC_NOTIFICATION_WEBHOOK_AUTH_TOKEN"),
    )

    if not notifier.is_configured():
        print("Webhook notifier is not configured (empty URL)")
        return

    notification = Notification(
        severity=AlertSeverity.INFO,
        title="data_collector/test",
        message="This is a test notification from Data Collector.",
        app_id="example_app_id",
        metadata={"source": "05_webhook.py example"},
    )

    print(f"Sending test notification to {notifier.url}...")
    success = notifier.send(notification)
    print(f"Result: {'SUCCESS' if success else 'FAILED'}")


if __name__ == "__main__":
    main()
