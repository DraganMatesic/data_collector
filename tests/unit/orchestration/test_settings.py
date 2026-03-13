"""Unit tests for ManagerSettings."""

from __future__ import annotations

import pytest

from data_collector.settings.manager import ManagerSettings


class TestManagerSettings:
    """Test ManagerSettings defaults and env var overrides."""

    def test_defaults(self) -> None:
        settings = ManagerSettings()
        assert settings.polling_interval == 10
        assert settings.command_poll_interval == 5
        assert settings.process_check_interval == 30
        assert settings.startup_grace_period == 10
        assert settings.max_start_failures == 3
        assert settings.rabbitmq_enabled is False
        assert settings.notifications_enabled is False
        assert settings.shutdown_timeout == 30
        assert settings.retention_enabled is True
        assert settings.retention_check_interval == 3600
        assert settings.retention_log_days == 90
        assert settings.retention_runtime_days == 180
        assert settings.retention_function_log_days == 90
        assert settings.retention_command_log_days == 365

    def test_env_override(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DC_MANAGER_POLLING_INTERVAL", "5")
        monkeypatch.setenv("DC_MANAGER_RABBITMQ_ENABLED", "true")
        monkeypatch.setenv("DC_MANAGER_RETENTION_LOG_DAYS", "30")

        settings = ManagerSettings()
        assert settings.polling_interval == 5
        assert settings.rabbitmq_enabled is True
        assert settings.retention_log_days == 30

    def test_direct_construction(self) -> None:
        settings = ManagerSettings(
            polling_interval=2,
            max_start_failures=5,
            notifications_enabled=True,
        )
        assert settings.polling_interval == 2
        assert settings.max_start_failures == 5
        assert settings.notifications_enabled is True
