"""Shared fixtures for pipeline tests."""

from __future__ import annotations

import os

import pytest

# Prevent actors.py module-level initialization during test collection.
# Must be set before any test file imports from data_collector.dramatiq.actors.
os.environ["_DC_SKIP_ACTOR_INIT"] = "1"

_DRAMATIQ_ENV_VARS = (
    "DC_DRAMATIQ_WORKERS",
    "DC_DRAMATIQ_PROCESSES",
    "DC_DRAMATIQ_MAX_RETRIES",
    "DC_DRAMATIQ_MIN_BACKOFF",
    "DC_DRAMATIQ_MAX_BACKOFF",
    "DC_DRAMATIQ_TIME_LIMIT",
    "DC_DRAMATIQ_MAX_AGE",
)

_DISPATCHER_ENV_VARS = (
    "DC_DISPATCHER_BATCH_SIZE",
    "DC_DISPATCHER_POLL_INTERVAL",
)

_RABBIT_ENV_VARS = (
    "DC_RABBIT_HOST",
    "DC_RABBIT_PORT",
    "DC_RABBIT_MANAGEMENT_PORT",
    "DC_RABBIT_USERNAME",
    "DC_RABBIT_PASSWORD",
    "DC_RABBIT_QUEUE",
    "DC_RABBIT_PREFETCH",
    "DC_RABBIT_HEARTBEAT",
    "DC_RABBIT_CONNECTION_TIMEOUT",
    "DC_RABBIT_RECONNECT_MAX_ATTEMPTS",
    "DC_RABBIT_RECONNECT_BASE_DELAY",
    "DC_RABBIT_RECONNECT_MAX_DELAY",
)


@pytest.fixture(autouse=True)
def clean_pipeline_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Remove all DC_DRAMATIQ_*, DC_DISPATCHER_*, and DC_RABBIT_* env vars.

    Also sets ``_DC_SKIP_ACTOR_INIT`` to prevent module-level initialization
    in ``actors.py`` from running during test collection.
    """
    monkeypatch.setenv("_DC_SKIP_ACTOR_INIT", "1")
    for variable in (*_DRAMATIQ_ENV_VARS, *_DISPATCHER_ENV_VARS, *_RABBIT_ENV_VARS):
        monkeypatch.delenv(variable, raising=False)
