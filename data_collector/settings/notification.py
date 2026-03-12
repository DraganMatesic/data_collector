"""Pydantic settings for the notification system."""

from __future__ import annotations

from pydantic import computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict


class NotificationSettings(BaseSettings):
    """Notification system settings loaded from environment variables.

    Environment variables follow the ``DC_NOTIFICATION_`` prefix pattern.

    Global settings control the master switch, enabled channels, and severity
    threshold. Per-channel settings provide credentials and endpoints for each
    notification channel. All channel-specific fields are optional -- only
    channels listed in ``notification_channels`` whose ``is_configured()``
    returns True are activated.

    Examples:
        From environment variables::

            settings = NotificationSettings()
            dispatcher = NotificationDispatcher.from_settings(settings)

        Direct construction (testing, overrides)::

            settings = NotificationSettings(
                notifications_enabled=True,
                channels="telegram",
                telegram_bot_token="123:ABC",
                telegram_chat_id="-100123",
            )
    """

    model_config = SettingsConfigDict(env_prefix="DC_NOTIFICATION_")

    # -- Global --
    notifications_enabled: bool = False
    channels: str = ""
    alert_min_severity: int = 2
    daily_summary_enabled: bool = False
    daily_summary_time: str = "08:00"

    # -- Rate limiting --
    rate_limit_min_interval: int = 30
    rate_limit_burst_limit: int = 10

    # -- Retry --
    retry_max_attempts: int = 3
    retry_base_delay: int = 10
    retry_multiplier: int = 3

    # -- Telegram --
    telegram_bot_token: str | None = None
    telegram_chat_id: str | None = None

    # -- Slack --
    slack_webhook_url: str | None = None
    slack_channel: str | None = None

    # -- Email (SMTP) --
    smtp_host: str | None = None
    smtp_port: int = 587
    smtp_username: str | None = None
    smtp_password: str | None = None
    smtp_from: str | None = None
    smtp_to: str | None = None
    smtp_use_tls: bool = True

    # -- Discord --
    discord_webhook_url: str | None = None

    # -- Generic Webhook --
    webhook_url: str | None = None
    webhook_headers: str | None = None
    webhook_auth_token: str | None = None

    @computed_field  # type: ignore[prop-decorator]
    @property
    def notification_channels(self) -> list[str]:
        """Parse comma-separated channel list from the raw environment variable.

        Returns:
            List of channel name strings.
        """
        if not self.channels:
            return []
        return [channel.strip() for channel in self.channels.split(",") if channel.strip()]
