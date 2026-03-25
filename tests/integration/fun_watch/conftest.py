"""Fixtures for @fun_watch integration tests."""

from __future__ import annotations

from collections.abc import Generator

import pytest
import structlog  # type: ignore[import-untyped]
from sqlalchemy import delete
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from data_collector.tables.apps import (
    AppFunctions,
    AppGroups,
    AppParents,
    Apps,
    CodebookCommandFlags,
    CodebookFatalFlags,
    CodebookRunStatus,
)
from data_collector.tables.log import FunctionLog
from data_collector.tables.runtime import Runtime
from data_collector.utilities.database.main import Database
from data_collector.utilities.fun_watch import FunWatchRegistry

_TEST_APPS = [
    {"app": "integ_test_app", "group_name": "test", "parent_name": "test", "app_name": "test_app"},
    {"app": "integ_log_app", "group_name": "test", "parent_name": "test", "app_name": "log_app"},
    {"app": "integ_seen_app", "group_name": "test", "parent_name": "test", "app_name": "seen_app"},
    {"app": "integ_ctx_app", "group_name": "test", "parent_name": "test", "app_name": "ctx_app"},
]

_TEST_RUNTIMES = [
    "integ_test_runtime",
    "integ_log_runtime",
    "integ_seen_runtime",
    "integ_ctx_runtime",
]


@pytest.fixture(scope="session", autouse=True)
def seed_fun_watch_parents(db: Database, db_engine: Engine) -> Generator[None]:
    """Seed codebooks, hierarchy, and Runtime rows required by FK constraints."""
    with db.create_session() as session:
        # Codebook rows required by Apps server_defaults (run_status=0, fatal_flag=0, cmd_flag=0)
        session.merge(CodebookRunStatus(id=0, description="Not running"))
        session.merge(CodebookFatalFlags(id=0, description="No fatal"))
        session.merge(CodebookCommandFlags(id=0, description="Pending"))

        # Hierarchy: AppGroups -> AppParents -> Apps
        session.merge(AppGroups(name="test"))
        session.merge(AppParents(name="test", group_name="test", parent="integ_test_parent"))

        for app_kwargs in _TEST_APPS:
            session.merge(Apps(**app_kwargs))

        for runtime_id in _TEST_RUNTIMES:
            session.merge(Runtime(runtime=runtime_id))

        session.commit()

    yield

    with db.create_session() as session:

        session.execute(delete(FunctionLog))
        session.execute(delete(AppFunctions))
        for app_kwargs in _TEST_APPS:
            session.execute(delete(Apps).where(Apps.app == app_kwargs["app"]))
        for runtime_id in _TEST_RUNTIMES:
            session.execute(delete(Runtime).where(Runtime.runtime == runtime_id))
        session.execute(delete(AppParents).where(AppParents.parent == "integ_test_parent"))
        session.execute(delete(AppGroups).where(AppGroups.name == "test"))
        session.commit()


@pytest.fixture(autouse=True)
def reset_registry() -> None:  # noqa: PT004
    """Reset FunWatchRegistry before each test."""
    FunWatchRegistry.reset()
    structlog.contextvars.clear_contextvars()


@pytest.fixture()
def clean_fun_watch_tables(session: Session) -> None:
    """Delete all fun_watch-related rows before the test."""
    session.execute(delete(FunctionLog))
    session.execute(delete(AppFunctions))
    session.commit()
