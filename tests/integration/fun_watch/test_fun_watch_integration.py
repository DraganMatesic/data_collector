"""Integration tests for @fun_watch with a live database.

These tests require a running PostgreSQL instance. They are skipped automatically
when database environment variables are not set.
"""

from __future__ import annotations

import pytest
import structlog  # type: ignore[import-untyped]
from sqlalchemy import select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from data_collector.tables.apps import AppFunctions
from data_collector.tables.log import FunctionLog
from data_collector.utilities.fun_watch import FunWatchMixin, FunWatchRegistry, fun_watch


class IntegrationApp(FunWatchMixin):
    """Minimal app for integration tests."""

    def __init__(self, app_id: str, runtime: str) -> None:
        self.app_id = app_id
        self.runtime = runtime

    @fun_watch
    def process_items(self, items: list[str]) -> int:
        for _item in items:
            self._fun_watch.mark_solved()
        return len(items)

    @fun_watch
    def failing_method(self) -> None:
        self._fun_watch.mark_failed()
        raise ValueError("integration test failure")


@pytest.mark.integration
@pytest.mark.usefixtures("clean_fun_watch_tables")
class TestRegisterFunction:
    def test_register_function_upserts_app_functions_row(
        self, db_engine: Engine, session: Session
    ) -> None:
        app = IntegrationApp(app_id="integ_test_app", runtime="integ_test_runtime")
        app.process_items(["a", "b"])

        stmt = select(AppFunctions).where(AppFunctions.app_id == "integ_test_app")
        row = session.execute(stmt).scalar_one_or_none()
        assert row is not None
        assert row.function_name == "process_items"  # pyright: ignore[reportGeneralTypeIssues]
        assert row.app_id == "integ_test_app"  # pyright: ignore[reportGeneralTypeIssues]


@pytest.mark.integration
@pytest.mark.usefixtures("clean_fun_watch_tables")
class TestInsertFunctionLog:
    def test_insert_function_log_writes_metrics(
        self, db_engine: Engine, session: Session
    ) -> None:
        app = IntegrationApp(app_id="integ_log_app", runtime="integ_log_runtime")
        app.process_items(["x", "y", "z"])

        stmt = select(FunctionLog).where(FunctionLog.app_id == "integ_log_app")
        row = session.execute(stmt).scalar_one_or_none()
        assert row is not None
        assert row.solved == 3  # pyright: ignore[reportGeneralTypeIssues]
        assert row.failed == 0  # pyright: ignore[reportGeneralTypeIssues]
        assert row.task_size == 3  # pyright: ignore[reportGeneralTypeIssues]
        assert row.runtime == "integ_log_runtime"  # pyright: ignore[reportGeneralTypeIssues]


@pytest.mark.integration
@pytest.mark.usefixtures("clean_fun_watch_tables")
class TestUpdateLastSeen:
    def test_update_last_seen_updates_timestamp(
        self, db_engine: Engine, session: Session
    ) -> None:
        app = IntegrationApp(app_id="integ_seen_app", runtime="integ_seen_runtime")
        app.process_items(["a"])

        stmt = select(AppFunctions).where(AppFunctions.app_id == "integ_seen_app")
        row = session.execute(stmt).scalar_one()
        first_seen = row.last_seen

        FunWatchRegistry.reset()
        app.process_items(["b"])

        session.expire_all()
        row = session.execute(stmt).scalar_one()
        assert row.last_seen >= first_seen  # pyright: ignore[reportGeneralTypeIssues]


@pytest.mark.integration
@pytest.mark.usefixtures("clean_fun_watch_tables")
class TestStructlogFunctionIdBinding:
    def test_function_id_bound_during_execution(
        self, db_engine: Engine
    ) -> None:
        captured: dict[str, str] = {}

        class CapturingApp(FunWatchMixin):
            app_id = "integ_ctx_app"
            runtime = "integ_ctx_runtime"

            @fun_watch
            def do_work(self, items: list[str]) -> None:
                ctx = structlog.contextvars.get_contextvars()
                captured["function_id"] = ctx.get("function_id", "")

        app = CapturingApp()
        app.do_work(["a"])

        assert captured["function_id"] != ""
        ctx_after = structlog.contextvars.get_contextvars()
        assert "function_id" not in ctx_after
