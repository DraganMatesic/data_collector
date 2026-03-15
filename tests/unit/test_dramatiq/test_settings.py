"""Tests for pipeline settings."""

from __future__ import annotations

import pytest

from data_collector.settings.dramatiq import DramatiqSettings, TaskDispatcherSettings


class TestDramatiqSettings:
    """Tests for the DramatiqSettings class."""

    def test_defaults(self) -> None:
        settings = DramatiqSettings()
        assert settings.workers == 4
        assert settings.processes == 1
        assert settings.max_retries == 3
        assert settings.min_backoff == 1000
        assert settings.max_backoff == 300_000
        assert settings.time_limit == 600_000
        assert settings.max_age == 86_400_000

    def test_env_prefix(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DC_DRAMATIQ_WORKERS", "8")
        monkeypatch.setenv("DC_DRAMATIQ_PROCESSES", "2")
        monkeypatch.setenv("DC_DRAMATIQ_MAX_RETRIES", "5")
        monkeypatch.setenv("DC_DRAMATIQ_TIME_LIMIT", "120000")
        monkeypatch.setenv("DC_DRAMATIQ_MAX_AGE", "3600000")
        settings = DramatiqSettings()
        assert settings.workers == 8
        assert settings.processes == 2
        assert settings.max_retries == 5
        assert settings.time_limit == 120_000
        assert settings.max_age == 3_600_000

    def test_direct_construction(self) -> None:
        settings = DramatiqSettings(
            workers=16,
            processes=4,
            max_retries=10,
            min_backoff=2000,
            max_backoff=600_000,
        )
        assert settings.workers == 16
        assert settings.processes == 4
        assert settings.max_retries == 10
        assert settings.min_backoff == 2000
        assert settings.max_backoff == 600_000


class TestTaskDispatcherSettings:
    """Tests for the TaskDispatcherSettings class."""

    def test_defaults(self) -> None:
        settings = TaskDispatcherSettings()
        assert settings.batch_size == 100
        assert settings.poll_interval == 10

    def test_env_prefix(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DC_DISPATCHER_BATCH_SIZE", "50")
        monkeypatch.setenv("DC_DISPATCHER_POLL_INTERVAL", "5")
        settings = TaskDispatcherSettings()
        assert settings.batch_size == 50
        assert settings.poll_interval == 5

    def test_direct_construction(self) -> None:
        settings = TaskDispatcherSettings(batch_size=200, poll_interval=30)
        assert settings.batch_size == 200
        assert settings.poll_interval == 30
