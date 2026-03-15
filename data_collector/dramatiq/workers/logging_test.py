"""Test worker to verify logging and @fun_watch integration in Dramatiq processes.

This actor validates that:
1. LoggingService is active -- logger calls flow to Logs table and Splunk
2. @fun_watch records FunctionLog rows with timing and counters
3. Every log entry has full context (function_id, call_chain, lineno)

The ``@dramatiq.actor`` function is a thin entry point that delegates
to ``LoggingTestProcessor`` -- a class with ``@fun_watch`` on every
method.  Zero standalone logging in the actor function.

Remove this worker after verification is complete.
"""

from __future__ import annotations

import time
import uuid
from datetime import UTC, datetime

import dramatiq
import structlog  # type: ignore[import-untyped]
from sqlalchemy import select

from data_collector.settings.main import MainDatabaseSettings
from data_collector.tables.apps import AppGroups, AppParents, Apps
from data_collector.tables.runtime import Runtime
from data_collector.utilities.database.main import Database
from data_collector.utilities.fun_watch import FunWatchMixin, FunWatchRegistry, fun_watch
from data_collector.utilities.functions.runtime import AppInfo, get_app_info

_APP_INFO: AppInfo = get_app_info(__file__, depth=-3)  # type: ignore[assignment]
_APP_ID: str = _APP_INFO["app_id"]


class LoggingTestProcessor(FunWatchMixin):
    """Test processor with full @fun_watch tracking on every method."""

    def __init__(self, app_id: str, runtime: str) -> None:
        self.app_id = app_id
        self.runtime = runtime
        self.logger = structlog.get_logger(__name__).bind(app_id=app_id, runtime=runtime)

    @fun_watch
    def run(self, items: list[str]) -> None:
        """Top-level orchestrator -- call_chain root for the entire actor invocation.

        Args:
            items: List of string items to process.
        """
        count = self.process_items(items)
        self.validate_results(count)

    @fun_watch
    def process_items(self, items: list[str]) -> int:
        """Process a batch of items, marking each as solved.

        Args:
            items: List of string items to process.

        Returns:
            Number of items processed.
        """
        for item in items:
            self.logger.info("Processing item", item=item)
            time.sleep(0.1)
            self._fun_watch.mark_solved()
        return len(items)

    @fun_watch
    def validate_results(self, count: int) -> str:
        """Validate processing results.

        Args:
            count: Number of items that were processed.

        Returns:
            Validation status message.
        """
        self.logger.info("Validating processed items", count=count)
        status = f"Validated {count} items at {datetime.now(UTC).isoformat()}"
        self._fun_watch.mark_solved()
        return status


def _ensure_parent_rows(database: Database, runtime_id: str) -> None:
    """Seed AppGroups, AppParents, Apps, and Runtime rows if missing."""
    group = _APP_INFO["app_group"]
    parent = _APP_INFO["app_parent"]
    app_name = _APP_INFO["app_name"]

    with database.create_session() as session:
        if not session.execute(select(AppGroups).where(AppGroups.name == group)).scalar():
            session.add(AppGroups(name=group))
            session.flush()
        if not session.execute(
            select(AppParents).where(AppParents.name == parent, AppParents.group_name == group)
        ).scalar():
            session.add(AppParents(name=parent, group_name=group))
            session.flush()
        session.merge(Apps(
            app=_APP_ID,
            group_name=group,
            parent_name=parent,
            app_name=app_name,
        ))
        session.merge(Runtime(
            runtime=runtime_id,
            app_id=_APP_ID,
            start_time=datetime.now(UTC),
        ))
        session.commit()


@dramatiq.actor(queue_name="dc_logging_test", max_retries=0)  # pyright: ignore[reportUntypedFunctionDecorator]
def logging_test_worker(items: list[str]) -> None:
    """Thin entry point -- delegates to LoggingTestProcessor.

    Args:
        items: List of string items to process.
    """
    runtime_id = uuid.uuid4().hex
    database = Database(MainDatabaseSettings())
    _ensure_parent_rows(database, runtime_id)

    processor = LoggingTestProcessor(app_id=_APP_ID, runtime=runtime_id)
    processor.run(items)

    FunWatchRegistry.reset()
