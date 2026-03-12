"""Notification system with pluggable alert channels."""

from data_collector.notifications.discord import DiscordNotifier
from data_collector.notifications.dispatcher import NotificationDispatcher
from data_collector.notifications.email import EmailNotifier
from data_collector.notifications.models import DeliveryResult, Notification
from data_collector.notifications.notifier import BaseNotifier
from data_collector.notifications.rate_limiter import RateLimiter
from data_collector.notifications.slack import SlackNotifier
from data_collector.notifications.telegram import TelegramNotifier
from data_collector.notifications.webhook import WebhookNotifier

__all__ = [
    "BaseNotifier",
    "DeliveryResult",
    "DiscordNotifier",
    "EmailNotifier",
    "Notification",
    "NotificationDispatcher",
    "RateLimiter",
    "SlackNotifier",
    "TelegramNotifier",
    "WebhookNotifier",
]
