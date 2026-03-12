"""Tests for notification settings."""

from __future__ import annotations

from data_collector.settings.notification import NotificationSettings


class TestNotificationSettings:
    """Tests for the NotificationSettings class."""

    def test_defaults(self) -> None:
        settings = NotificationSettings()
        assert settings.notifications_enabled is False
        assert settings.notification_channels == []
        assert settings.alert_min_severity == 2
        assert settings.daily_summary_enabled is False
        assert settings.daily_summary_time == "08:00"
        assert settings.rate_limit_min_interval == 30
        assert settings.rate_limit_burst_limit == 10
        assert settings.retry_max_attempts == 3
        assert settings.retry_base_delay == 10
        assert settings.retry_multiplier == 3

    def test_channel_fields_default_none(self) -> None:
        settings = NotificationSettings()
        assert settings.telegram_bot_token is None
        assert settings.telegram_chat_id is None
        assert settings.slack_webhook_url is None
        assert settings.slack_channel is None
        assert settings.smtp_host is None
        assert settings.smtp_username is None
        assert settings.smtp_password is None
        assert settings.smtp_from is None
        assert settings.smtp_to is None
        assert settings.discord_webhook_url is None
        assert settings.webhook_url is None
        assert settings.webhook_headers is None
        assert settings.webhook_auth_token is None

    def test_smtp_port_default(self) -> None:
        settings = NotificationSettings()
        assert settings.smtp_port == 587

    def test_smtp_use_tls_default(self) -> None:
        settings = NotificationSettings()
        assert settings.smtp_use_tls is True

    def test_parse_channels_from_comma_separated(self) -> None:
        settings = NotificationSettings(channels="telegram,slack,email")
        assert settings.notification_channels == ["telegram", "slack", "email"]

    def test_parse_channels_from_comma_separated_with_spaces(self) -> None:
        settings = NotificationSettings(channels="telegram, slack , email")
        assert settings.notification_channels == ["telegram", "slack", "email"]

    def test_parse_channels_empty_string(self) -> None:
        settings = NotificationSettings(channels="")
        assert settings.notification_channels == []

    def test_direct_construction(self) -> None:
        settings = NotificationSettings(
            notifications_enabled=True,
            channels="telegram",
            telegram_bot_token="123:ABC",
            telegram_chat_id="-100123",
            alert_min_severity=3,
        )
        assert settings.notifications_enabled is True
        assert settings.telegram_bot_token == "123:ABC"
        assert settings.alert_min_severity == 3

    def test_env_prefix(self) -> None:
        assert NotificationSettings.model_config.get("env_prefix") == "DC_NOTIFICATION_"
