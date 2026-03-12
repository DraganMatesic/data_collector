"""Send a test alert via Discord incoming webhook.

Demonstrates:
    - DiscordNotifier instantiation from environment variables
    - Sending a test notification with rich embed formatting
    - Severity-based color coding
    - Checking delivery result

Setup:
    1. Open Discord and navigate to the target channel.
    2. Click Channel Settings (gear icon) > Integrations > Webhooks.
    3. Click "New Webhook", name it (e.g., "Data Collector Alerts").
    4. Copy the webhook URL.

Requires:
    DC_NOTIFICATION_DISCORD_WEBHOOK_URL environment variable.

Run:
    python -m data_collector.examples run notifications/04_discord
"""

from __future__ import annotations

import os

from data_collector.enums.notifications import AlertSeverity
from data_collector.notifications.discord import DiscordNotifier
from data_collector.notifications.models import Notification

_REQUIRED_ENV = ("DC_NOTIFICATION_DISCORD_WEBHOOK_URL",)


def main() -> None:
    """Send a test notification via Discord."""
    missing = [variable for variable in _REQUIRED_ENV if not os.environ.get(variable)]
    if missing:
        print(f"Skipping: env vars not set: {', '.join(missing)}")
        return

    notifier = DiscordNotifier(
        webhook_url=os.environ["DC_NOTIFICATION_DISCORD_WEBHOOK_URL"],
    )

    if not notifier.is_configured():
        print("Discord notifier is not configured (empty webhook URL)")
        return

    notification = Notification(
        severity=AlertSeverity.INFO,
        title="data_collector/test",
        message="This is a test notification from Data Collector.",
        metadata={"source": "04_discord.py example"},
    )

    print("Sending test notification to Discord channel...")
    success = notifier.send(notification)
    print(f"Result: {'SUCCESS' if success else 'FAILED'}")


if __name__ == "__main__":
    main()
