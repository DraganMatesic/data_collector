"""Telegram Bot API notification channel."""

from __future__ import annotations

import logging

import requests

from data_collector.notifications.models import Notification
from data_collector.notifications.notifier import BaseNotifier

logger = logging.getLogger(__name__)

_MARKDOWN_SPECIAL = ("_", "*", "[", "]", "(", ")", "~", "`", ">", "#", "+", "-", "=", "|", "{", "}", ".", "!")


def _escape_markdown(text: str) -> str:
    """Escape Telegram MarkdownV2 special characters.

    Args:
        text: Raw text to escape.

    Returns:
        Text with special characters prefixed by backslash.
    """
    for character in _MARKDOWN_SPECIAL:
        text = text.replace(character, f"\\{character}")
    return text


class TelegramNotifier(BaseNotifier):
    """Send alerts via Telegram Bot API.

    Uses the ``sendMessage`` endpoint with Markdown formatting. Messages
    include severity prefix, title, body, and optional metadata fields.

    Args:
        bot_token: Telegram Bot API token from @BotFather.
        chat_id: Target chat or group ID.
        timeout: HTTP request timeout in seconds.
    """

    CHANNEL_NAME: str = "telegram"
    API_BASE_URL: str = "https://api.telegram.org"

    def __init__(self, bot_token: str, chat_id: str, *, timeout: int = 10) -> None:
        super().__init__(self.CHANNEL_NAME)
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.timeout = timeout

    def send(self, notification: Notification) -> bool:
        """Send a notification via Telegram Bot API.

        Args:
            notification: The notification payload to deliver.

        Returns:
            True on successful delivery, False on failure.
        """
        url = f"{self.API_BASE_URL}/bot{self.bot_token}/sendMessage"
        payload = {
            "chat_id": self.chat_id,
            "text": self._format_markdown(notification),
            "parse_mode": "MarkdownV2",
        }
        try:
            response = requests.post(url, json=payload, timeout=self.timeout)
            if response.status_code == 200:
                return True
            logger.warning(
                "Telegram API returned status %d: %s",
                response.status_code,
                response.text[:200],
            )
            return False
        except requests.RequestException:
            logger.exception("Failed to send Telegram notification")
            return False

    def is_configured(self) -> bool:
        """Check whether Telegram credentials are present.

        Returns:
            True if both bot_token and chat_id are set.
        """
        return bool(self.bot_token) and bool(self.chat_id)

    def _format_markdown(self, notification: Notification) -> str:
        """Format notification as Telegram Markdown message.

        Args:
            notification: The notification to format.

        Returns:
            Markdown-formatted message string.
        """
        title = _escape_markdown(self.format_title(notification))
        message = _escape_markdown(notification.message)
        lines = [f"*{title}*", "", message]

        if notification.app_id:
            lines.append(f"\nApp: `{notification.app_id}`")

        timestamp = _escape_markdown(notification.timestamp.strftime("%Y-%m-%d %H:%M:%S UTC"))
        lines.append(f"Time: {timestamp}")

        if notification.metadata:
            for key, value in notification.metadata.items():
                lines.append(f"{_escape_markdown(key)}: {_escape_markdown(value)}")

        return "\n".join(lines)
