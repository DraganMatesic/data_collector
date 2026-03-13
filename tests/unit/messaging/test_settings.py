"""Tests for RabbitMQ settings."""

from __future__ import annotations

import pytest

from data_collector.settings.rabbitmq import RabbitMQSettings


class TestRabbitMQSettings:
    """Tests for the RabbitMQSettings class."""

    def test_defaults(self) -> None:
        settings = RabbitMQSettings()
        assert settings.rabbit_host == "localhost"
        assert settings.rabbit_port == 5672
        assert settings.rabbit_username == "guest"
        assert settings.rabbit_password == "guest"
        assert settings.rabbit_queue == "dc_manager"
        assert settings.rabbit_prefetch == 1

    def test_reliability_defaults(self) -> None:
        settings = RabbitMQSettings()
        assert settings.rabbit_heartbeat == 600
        assert settings.rabbit_connection_timeout == 10
        assert settings.rabbit_reconnect_max_attempts == 5
        assert settings.rabbit_reconnect_base_delay == 1
        assert settings.rabbit_reconnect_max_delay == 30

    def test_env_prefix(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DC_RABBIT_RABBIT_HOST", "rabbitmq.internal")
        monkeypatch.setenv("DC_RABBIT_RABBIT_PORT", "5673")
        monkeypatch.setenv("DC_RABBIT_RABBIT_QUEUE", "dc_manager_eu")
        settings = RabbitMQSettings()
        assert settings.rabbit_host == "rabbitmq.internal"
        assert settings.rabbit_port == 5673
        assert settings.rabbit_queue == "dc_manager_eu"

    def test_direct_construction(self) -> None:
        settings = RabbitMQSettings(
            rabbit_host="10.0.0.5",
            rabbit_port=5673,
            rabbit_username="admin",
            rabbit_password="secret",
            rabbit_queue="dc_manager_prod",
            rabbit_prefetch=10,
        )
        assert settings.rabbit_host == "10.0.0.5"
        assert settings.rabbit_port == 5673
        assert settings.rabbit_username == "admin"
        assert settings.rabbit_password == "secret"
        assert settings.rabbit_queue == "dc_manager_prod"
        assert settings.rabbit_prefetch == 10

    def test_custom_reconnect_settings(self) -> None:
        settings = RabbitMQSettings(
            rabbit_reconnect_max_attempts=10,
            rabbit_reconnect_base_delay=2,
            rabbit_reconnect_max_delay=60,
        )
        assert settings.rabbit_reconnect_max_attempts == 10
        assert settings.rabbit_reconnect_base_delay == 2
        assert settings.rabbit_reconnect_max_delay == 60
