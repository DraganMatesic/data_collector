"""Shared fixtures for messaging tests."""

from __future__ import annotations

import pytest

_RABBIT_ENV_VARS = (
    "DC_RABBIT_RABBIT_HOST",
    "DC_RABBIT_RABBIT_PORT",
    "DC_RABBIT_RABBIT_USERNAME",
    "DC_RABBIT_RABBIT_PASSWORD",
    "DC_RABBIT_RABBIT_QUEUE",
    "DC_RABBIT_RABBIT_PREFETCH",
    "DC_RABBIT_RABBIT_HEARTBEAT",
    "DC_RABBIT_RABBIT_CONNECTION_TIMEOUT",
    "DC_RABBIT_RABBIT_RECONNECT_MAX_ATTEMPTS",
    "DC_RABBIT_RABBIT_RECONNECT_BASE_DELAY",
    "DC_RABBIT_RABBIT_RECONNECT_MAX_DELAY",
)


@pytest.fixture(autouse=True)
def clean_rabbit_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Remove all DC_RABBIT_* env vars so BaseSettings reads defaults."""
    for variable in _RABBIT_ENV_VARS:
        monkeypatch.delenv(variable, raising=False)
