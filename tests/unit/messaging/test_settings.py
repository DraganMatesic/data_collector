"""Tests for RabbitMQ settings."""

from __future__ import annotations

import pytest

from data_collector.settings.rabbitmq import RabbitMQSettings


class TestRabbitMQSettings:
    """Tests for the RabbitMQSettings class."""

    def test_defaults(self) -> None:
        settings = RabbitMQSettings()
        assert settings.host == "localhost"
        assert settings.port == 5672
        assert settings.username == "guest"
        assert settings.password == "guest"
        assert settings.queue == "dc_manager"
        assert settings.prefetch == 1

    def test_reliability_defaults(self) -> None:
        settings = RabbitMQSettings()
        assert settings.heartbeat == 600
        assert settings.connection_timeout == 10
        assert settings.reconnect_max_attempts == 5
        assert settings.reconnect_base_delay == 1
        assert settings.reconnect_max_delay == 30

    def test_env_prefix(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DC_RABBIT_HOST", "rabbitmq.internal")
        monkeypatch.setenv("DC_RABBIT_PORT", "5673")
        monkeypatch.setenv("DC_RABBIT_QUEUE", "dc_manager_eu")
        settings = RabbitMQSettings()
        assert settings.host == "rabbitmq.internal"
        assert settings.port == 5673
        assert settings.queue == "dc_manager_eu"

    def test_direct_construction(self) -> None:
        settings = RabbitMQSettings(
            host="10.0.0.5",
            port=5673,
            username="admin",
            password="secret",
            queue="dc_manager_prod",
            prefetch=10,
        )
        assert settings.host == "10.0.0.5"
        assert settings.port == 5673
        assert settings.username == "admin"
        assert settings.password == "secret"
        assert settings.queue == "dc_manager_prod"
        assert settings.prefetch == 10

    def test_custom_reconnect_settings(self) -> None:
        settings = RabbitMQSettings(
            reconnect_max_attempts=10,
            reconnect_base_delay=2,
            reconnect_max_delay=60,
        )
        assert settings.reconnect_max_attempts == 10
        assert settings.reconnect_base_delay == 2
        assert settings.reconnect_max_delay == 60
