"""Notification dispatcher for multi-channel delivery with retry and rate limiting."""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable
from types import MappingProxyType

from data_collector.enums.notifications import AlertSeverity
from data_collector.notifications.discord import DiscordNotifier
from data_collector.notifications.email import EmailNotifier
from data_collector.notifications.models import DeliveryResult, Notification
from data_collector.notifications.notifier import BaseNotifier
from data_collector.notifications.rate_limiter import RateLimiter
from data_collector.notifications.slack import SlackNotifier
from data_collector.notifications.telegram import TelegramNotifier
from data_collector.notifications.webhook import WebhookNotifier
from data_collector.settings.notification import NotificationSettings

logger = logging.getLogger(__name__)

CHANNEL_REGISTRY: MappingProxyType[str, type[BaseNotifier]] = MappingProxyType({
    TelegramNotifier.CHANNEL_NAME: TelegramNotifier,
    SlackNotifier.CHANNEL_NAME: SlackNotifier,
    EmailNotifier.CHANNEL_NAME: EmailNotifier,
    DiscordNotifier.CHANNEL_NAME: DiscordNotifier,
    WebhookNotifier.CHANNEL_NAME: WebhookNotifier,
})


def _build_notifier(channel_name: str, settings: NotificationSettings) -> BaseNotifier | None:
    """Instantiate a notifier from settings for the given channel name.

    Args:
        channel_name: Channel identifier (e.g., "telegram", "slack").
        settings: NotificationSettings with per-channel credentials.

    Returns:
        Configured BaseNotifier instance, or None if channel is unknown.
    """
    builders: dict[str, Callable[[], BaseNotifier]] = {
        "telegram": lambda: TelegramNotifier(
            bot_token=settings.telegram_bot_token or "",
            chat_id=settings.telegram_chat_id or "",
        ),
        "slack": lambda: SlackNotifier(
            webhook_url=settings.slack_webhook_url or "",
            channel=settings.slack_channel,
        ),
        "email": lambda: EmailNotifier(
            host=settings.smtp_host or "",
            port=settings.smtp_port,
            username=settings.smtp_username or "",
            password=settings.smtp_password or "",
            sender_address=settings.smtp_from or "",
            recipient_addresses=settings.smtp_to or "",
            use_tls=settings.smtp_use_tls,
        ),
        "discord": lambda: DiscordNotifier(
            webhook_url=settings.discord_webhook_url or "",
        ),
        "webhook": lambda: WebhookNotifier(
            url=settings.webhook_url or "",
            custom_headers=WebhookNotifier.parse_headers(settings.webhook_headers),
            auth_token=settings.webhook_auth_token,
        ),
    }

    builder = builders.get(channel_name)
    if builder is None:
        logger.warning("Unknown notification channel: %s", channel_name)
        return None
    return builder()


class NotificationDispatcher:
    """Orchestrates notification delivery across all enabled channels.

    Handles severity filtering, rate limiting, retry with exponential
    backoff, and automatic channel disabling after consecutive failures.

    Args:
        notifiers: List of BaseNotifier instances to dispatch to.
        min_severity: Minimum AlertSeverity to send (lower are dropped).
        rate_limiter_factory: Callable that creates a RateLimiter per channel.
        max_retry_attempts: Maximum delivery attempts per channel (1 = no retries).
        retry_base_delay: Base delay in seconds for exponential backoff.
        retry_multiplier: Multiplier for exponential backoff.
        max_consecutive_failures: Failures before temporarily disabling a channel.
    """

    def __init__(
        self,
        notifiers: list[BaseNotifier],
        *,
        min_severity: AlertSeverity = AlertSeverity.WARNING,
        rate_limiter_factory: Callable[[], RateLimiter] | None = None,
        max_retry_attempts: int = 3,
        retry_base_delay: int = 10,
        retry_multiplier: int = 3,
        max_consecutive_failures: int = 5,
    ) -> None:
        self._notifiers: dict[str, BaseNotifier] = {notifier.channel_name: notifier for notifier in notifiers}
        self._min_severity = min_severity
        self._max_retry_attempts = max_retry_attempts
        self._retry_base_delay = retry_base_delay
        self._retry_multiplier = retry_multiplier
        self._max_consecutive_failures = max_consecutive_failures

        factory = rate_limiter_factory or RateLimiter
        self._rate_limiters: dict[str, RateLimiter] = {
            notifier.channel_name: factory() for notifier in notifiers
        }
        self._consecutive_failures: dict[str, int] = {notifier.channel_name: 0 for notifier in notifiers}
        self._disabled_channels: set[str] = set()
        self._lock = threading.Lock()

    @classmethod
    def from_settings(cls, settings: NotificationSettings) -> NotificationDispatcher:
        """Build dispatcher from NotificationSettings.

        Instantiates only the channels listed in ``settings.notification_channels``
        whose ``is_configured()`` returns True.

        Args:
            settings: NotificationSettings instance.

        Returns:
            Configured NotificationDispatcher ready to send.
        """
        notifiers: list[BaseNotifier] = []

        for channel_name in settings.notification_channels:
            notifier = _build_notifier(channel_name, settings)
            if notifier is None:
                continue
            if not notifier.is_configured():
                logger.warning("Channel '%s' is listed but not fully configured, skipping", channel_name)
                continue
            notifiers.append(notifier)

        return cls(
            notifiers,
            min_severity=AlertSeverity(settings.alert_min_severity),
            rate_limiter_factory=lambda: RateLimiter(
                min_interval_seconds=settings.rate_limit_min_interval,
                burst_limit=settings.rate_limit_burst_limit,
            ),
            max_retry_attempts=settings.retry_max_attempts,
            retry_base_delay=settings.retry_base_delay,
            retry_multiplier=settings.retry_multiplier,
        )

    def send(self, notification: Notification) -> list[DeliveryResult]:
        """Dispatch a notification to all enabled channels.

        Applies severity filtering, rate limiting, and retry logic.

        Args:
            notification: The notification to send.

        Returns:
            List of DeliveryResult, one per channel attempted.
        """
        if notification.severity < self._min_severity:
            logger.debug(
                "Notification severity %s below threshold %s, skipping",
                notification.severity.name,
                self._min_severity.name,
            )
            return []

        results: list[DeliveryResult] = []

        for channel_name, notifier in self._notifiers.items():
            with self._lock:
                if channel_name in self._disabled_channels:
                    logger.debug("Channel '%s' is temporarily disabled, skipping", channel_name)
                    continue

                rate_limiter = self._rate_limiters.get(channel_name)
                if rate_limiter and not rate_limiter.is_allowed(notification.severity):
                    logger.debug("Channel '%s' is rate-limited, skipping", channel_name)
                    results.append(DeliveryResult(
                        channel_name=channel_name,
                        success=False,
                        attempts=0,
                        error_message="Rate limited",
                    ))
                    continue

            result = self._deliver_with_retry(notifier, notification)
            results.append(result)

            with self._lock:
                if result.success:
                    self._consecutive_failures[channel_name] = 0
                    if rate_limiter:
                        rate_limiter.record_send()
                else:
                    self._consecutive_failures[channel_name] = (
                        self._consecutive_failures.get(channel_name, 0) + 1
                    )
                    if self._consecutive_failures[channel_name] >= self._max_consecutive_failures:
                        self._disabled_channels.add(channel_name)
                        logger.error(
                            "Channel '%s' disabled after %d consecutive failures",
                            channel_name,
                            self._max_consecutive_failures,
                        )

        return results

    def send_to_channel(self, channel_name: str, notification: Notification) -> DeliveryResult:
        """Send to a specific channel by name, bypassing severity filter.

        Args:
            channel_name: Target channel name.
            notification: The notification to send.

        Returns:
            DeliveryResult for the targeted channel.

        Raises:
            ValueError: If channel_name is not registered.
        """
        notifier = self._notifiers.get(channel_name)
        if notifier is None:
            raise ValueError(f"Unknown channel: {channel_name}")
        return self._deliver_with_retry(notifier, notification)

    def _deliver_with_retry(self, notifier: BaseNotifier, notification: Notification) -> DeliveryResult:
        """Attempt delivery with exponential backoff retry.

        Backoff schedule: ``base_delay * (multiplier ** attempt_index)``.
        e.g., 10s, 30s, 90s with base=10, multiplier=3.

        Args:
            notifier: The channel notifier to send through.
            notification: The notification payload.

        Returns:
            DeliveryResult with attempt count and success/failure.
        """
        last_error: str | None = None

        for attempt in range(self._max_retry_attempts):
            try:
                success = notifier.send(notification)
                if success:
                    return DeliveryResult(
                        channel_name=notifier.channel_name,
                        success=True,
                        attempts=attempt + 1,
                    )
                last_error = "Send returned False"
            except Exception as error:
                last_error = str(error)
                logger.warning(
                    "Channel '%s' delivery attempt %d failed: %s",
                    notifier.channel_name,
                    attempt + 1,
                    last_error,
                )

            if attempt < self._max_retry_attempts - 1:
                delay = self._retry_base_delay * (self._retry_multiplier ** attempt)
                logger.info(
                    "Retrying channel '%s' in %d seconds (attempt %d/%d)",
                    notifier.channel_name,
                    delay,
                    attempt + 2,
                    self._max_retry_attempts,
                )
                time.sleep(delay)

        logger.error(
            "Channel '%s' delivery failed after %d attempts: %s",
            notifier.channel_name,
            self._max_retry_attempts,
            last_error,
        )
        return DeliveryResult(
            channel_name=notifier.channel_name,
            success=False,
            attempts=self._max_retry_attempts,
            error_message=last_error,
        )

    @property
    def enabled_channels(self) -> list[str]:
        """Return names of currently enabled (non-disabled) channels."""
        return [name for name in self._notifiers if name not in self._disabled_channels]

    @property
    def disabled_channels(self) -> list[str]:
        """Return names of channels temporarily disabled due to failures."""
        return list(self._disabled_channels)

    def enable_channel(self, channel_name: str) -> None:
        """Re-enable a previously disabled channel.

        Args:
            channel_name: Channel to re-enable.

        Raises:
            ValueError: If channel_name is not registered.
        """
        if channel_name not in self._notifiers:
            raise ValueError(f"Unknown channel: {channel_name}")
        with self._lock:
            self._disabled_channels.discard(channel_name)
            self._consecutive_failures[channel_name] = 0
        logger.info("Channel '%s' re-enabled", channel_name)
