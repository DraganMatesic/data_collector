# pyright: reportUnusedFunction=false
"""Shared fixtures for orchestration unit tests."""

from __future__ import annotations

import pytest

_MANAGER_ENV_VARS = [
    "DC_MANAGER_POLLING_INTERVAL",
    "DC_MANAGER_COMMAND_POLL_INTERVAL",
    "DC_MANAGER_PROCESS_CHECK_INTERVAL",
    "DC_MANAGER_STARTUP_GRACE_PERIOD",
    "DC_MANAGER_MAX_START_FAILURES",
    "DC_MANAGER_RABBITMQ_ENABLED",
    "DC_MANAGER_NOTIFICATIONS_ENABLED",
    "DC_MANAGER_SHUTDOWN_TIMEOUT",
    "DC_MANAGER_RETENTION_ENABLED",
    "DC_MANAGER_RETENTION_CHECK_INTERVAL",
    "DC_MANAGER_RETENTION_LOG_DAYS",
    "DC_MANAGER_RETENTION_RUNTIME_DAYS",
    "DC_MANAGER_RETENTION_FUNCTION_LOG_DAYS",
    "DC_MANAGER_RETENTION_COMMAND_LOG_DAYS",
    "DC_MANAGER_RETENTION_APP_PURGE_ENABLED",
]


@pytest.fixture(autouse=True)
def _clean_manager_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Remove DC_MANAGER_* env vars for clean defaults in every test."""
    for variable in _MANAGER_ENV_VARS:
        monkeypatch.delenv(variable, raising=False)
