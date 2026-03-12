"""Send a test alert via Telegram Bot API.

Demonstrates:
    - TelegramNotifier instantiation from environment variables
    - Sending a test notification with Markdown formatting
    - Checking delivery result

Setup:
    1. Create a bot via @BotFather on Telegram and copy the bot token.
    2. Start a chat with your bot (or add it to a group).
    3. Get the chat_id by visiting:
       https://api.telegram.org/bot<YOUR_TOKEN>/getUpdates
       Look for "chat":{"id": <number>} in the response JSON.

Requires:
    DC_NOTIFICATION_TELEGRAM_BOT_TOKEN, DC_NOTIFICATION_TELEGRAM_CHAT_ID
    environment variables.

Run:
    python -m data_collector.examples run notifications/01_telegram
"""

from __future__ import annotations

import os

from data_collector.enums.notifications import AlertSeverity
from data_collector.notifications.models import Notification
from data_collector.notifications.telegram import TelegramNotifier

_REQUIRED_ENV = ("DC_NOTIFICATION_TELEGRAM_BOT_TOKEN", "DC_NOTIFICATION_TELEGRAM_CHAT_ID")


def main() -> None:
    """Send a test notification via Telegram."""
    missing = [variable for variable in _REQUIRED_ENV if not os.environ.get(variable)]
    if missing:
        print(f"Skipping: env vars not set: {', '.join(missing)}")
        return

    notifier = TelegramNotifier(
        bot_token=os.environ["DC_NOTIFICATION_TELEGRAM_BOT_TOKEN"],
        chat_id=os.environ["DC_NOTIFICATION_TELEGRAM_CHAT_ID"],
    )

    if not notifier.is_configured():
        print("Telegram notifier is not configured (empty token or chat_id)")
        return

    notification = Notification(
        severity=AlertSeverity.INFO,
        title="data_collector/test",
        message="This is a test notification from Data Collector.",
        metadata={"source": "01_telegram.py example"},
    )

    print(f"Sending test notification to chat_id={notifier.chat_id}...")
    success = notifier.send(notification)
    print(f"Result: {'SUCCESS' if success else 'FAILED'}")


if __name__ == "__main__":
    main()
