"""Send a test alert via Slack incoming webhook.

Demonstrates:
    - SlackNotifier instantiation from environment variables
    - Sending a test notification with Block Kit formatting
    - Optional channel override
    - Checking delivery result

Setup:
    1. Go to https://api.slack.com/apps and create a new app (or use existing).
    2. Enable Incoming Webhooks under Features.
    3. Click "Add New Webhook to Workspace" and select a channel.
    4. Copy the webhook URL.

Requires:
    DC_NOTIFICATION_SLACK_WEBHOOK_URL environment variable.

Optional:
    DC_NOTIFICATION_SLACK_CHANNEL -- override the default webhook channel.

Run:
    python -m data_collector.examples run notifications/02_slack
"""

from __future__ import annotations

import os

from data_collector.enums.notifications import AlertSeverity
from data_collector.notifications.models import Notification
from data_collector.notifications.slack import SlackNotifier

_REQUIRED_ENV = ("DC_NOTIFICATION_SLACK_WEBHOOK_URL",)


def main() -> None:
    """Send a test notification via Slack."""
    missing = [variable for variable in _REQUIRED_ENV if not os.environ.get(variable)]
    if missing:
        print(f"Skipping: env vars not set: {', '.join(missing)}")
        return

    notifier = SlackNotifier(
        webhook_url=os.environ["DC_NOTIFICATION_SLACK_WEBHOOK_URL"],
        channel=os.environ.get("DC_NOTIFICATION_SLACK_CHANNEL"),
    )

    if not notifier.is_configured():
        print("Slack notifier is not configured (empty webhook URL)")
        return

    notification = Notification(
        severity=AlertSeverity.INFO,
        title="data_collector/test",
        message="This is a test notification from Data Collector.",
        metadata={"source": "02_slack.py example"},
    )

    print("Sending test notification to Slack...")
    success = notifier.send(notification)
    print(f"Result: {'SUCCESS' if success else 'FAILED'}")


if __name__ == "__main__":
    main()
