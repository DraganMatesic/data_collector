"""Database row structure produced by @fun_watch with real DB inserts.

Demonstrates:
    - Real LoggingService integration (Logs table inserts via DatabaseHandler)
    - Real AppFunctions registration rows (function_hash, function_name, filepath, app_id)
    - Real FunctionLog rows (call_count, task_size, solved, failed, timing metrics)
    - task_size auto-detection from first arg with __len__
    - task_size=None when first arg has no __len__ or no args at all
    - main_app defaults to app_id when self.main_app is not set
    - Lifecycle logging: "Function started" / "Function completed" events in Logs table

Requires:
    DC_DB_MAIN_USERNAME, DC_DB_MAIN_PASSWORD, DC_DB_MAIN_DATABASENAME,
    DC_DB_MAIN_IP, DC_DB_MAIN_PORT environment variables.

Optional Splunk HEC (env-driven, see logging/01_splunk_hec for setup):
    DC_LOG_SPLUNK_ENABLED=true
    DC_LOG_SPLUNK_URL=https://127.0.0.1:8088/services/collector
    DC_LOG_SPLUNK_TOKEN=<your-hec-token>
    DC_LOG_SPLUNK_VERIFY_TLS=false

Run:
    python -m data_collector.examples run fun_watch/02_database_rows
"""

from __future__ import annotations

import os
import time
import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select

from data_collector.settings.main import LogSettings
from data_collector.tables.apps import AppFunctions, AppGroups, AppParents, Apps
from data_collector.tables.deploy import ExampleDeploy
from data_collector.tables.log import FunctionLog, Logs
from data_collector.tables.runtime import Runtime
from data_collector.utilities.database.main import Database
from data_collector.utilities.fun_watch import FunWatchMixin, FunWatchRegistry, fun_watch
from data_collector.utilities.functions.math import get_totalh, get_totalm, get_totals
from data_collector.utilities.functions.runtime import AppInfo, get_app_id, get_app_info
from data_collector.utilities.log.main import LoggingService

_REQUIRED_ENV = (
    "DC_DB_MAIN_USERNAME", "DC_DB_MAIN_PASSWORD", "DC_DB_MAIN_DATABASENAME",
    "DC_DB_MAIN_IP", "DC_DB_MAIN_PORT",
)


class Collector(FunWatchMixin):
    """Example app with optional main_app override."""

    def __init__(self, app_id: str, runtime: str, logger: Any, main_app: str = "") -> None:
        self.app_id = app_id
        self.runtime = runtime
        self.logger = logger
        self.main_app = main_app

    @fun_watch
    def fetch_pages(self, urls: list[str]) -> int:
        """Fetch a list of URLs (task_size = len(urls))."""
        for _url in urls:
            self._fun_watch.mark_solved()
        return len(urls)

    @fun_watch
    def compute_score(self, value: float) -> float:
        """Compute on a single float (no __len__ -> task_size=None)."""
        self._fun_watch.mark_solved()
        return value * 2.0

    @fun_watch
    def heartbeat(self) -> str:
        """No arguments at all (task_size=None)."""
        return "alive"


def _check_db_env() -> bool:
    """Return True when required DB environment variables are set."""
    missing = [v for v in _REQUIRED_ENV if not os.environ.get(v)]
    if missing:
        print(f"Skipping: DB env vars not set: {', '.join(missing)}")
        return False
    return True


def _seed_parent_rows(db: Database, app_info: AppInfo, apps: list[tuple[str, str, str]]) -> None:
    """Seed Apps and Runtime rows required by FK constraints.

    Args:
        app_info: Base application info (used for group/parent).
        apps: List of (app_id, app_name, runtime_id) tuples.
    """
    group = app_info["app_group"]
    parent = app_info["app_parent"]
    with db.create_session() as session:
        if not session.execute(select(AppGroups).where(AppGroups.name == group)).scalar():
            session.add(AppGroups(name=group))
            session.flush()
        if not session.execute(
            select(AppParents).where(AppParents.name == parent, AppParents.group_name == group)
        ).scalar():
            session.add(AppParents(name=parent, group_name=group))
            session.flush()
        for app_id, app_name, runtime_id in apps:
            session.merge(Apps(
                app=app_id,
                group_name=group,
                parent_name=parent,
                app_name=app_name,
            ))
            session.merge(Runtime(
                runtime=runtime_id,
                app_id=app_id,
                start_time=datetime.now(UTC),
            ))
        session.commit()


def _complete_runtime(db: Database, runtime_id: str, start_time: datetime, exception_count: int = 0) -> None:
    """Finalize Runtime row with end_time, duration, and exception count."""
    end_time = datetime.now(UTC)
    with db.create_session() as session:
        record = db.query(
            select(Runtime).where(Runtime.runtime == runtime_id), session,
        ).scalar_one_or_none()
        if record is not None:
            record.end_time = end_time  # type: ignore[assignment]
            record.totals = get_totals(start_time, end_time)  # type: ignore[assignment]
            record.totalm = get_totalm(start_time, end_time)  # type: ignore[assignment]
            record.totalh = get_totalh(start_time, end_time)  # type: ignore[assignment]
            record.except_cnt = exception_count  # type: ignore[assignment]
            session.commit()


def _print_results(db: Database, app_ids: list[str]) -> None:
    """Query and print all rows inserted by this example."""
    with db.create_session() as session:
        print("\n=== AppFunctions rows ===")
        af_rows = session.execute(
            select(AppFunctions).where(AppFunctions.app_id.in_(app_ids))
        ).scalars().all()
        for row in af_rows:
            print(f"  {row!r}")
        if not af_rows:
            print("  (none)")

        print("\n=== FunctionLog rows ===")
        fl_rows = session.execute(
            select(FunctionLog).where(FunctionLog.app_id.in_(app_ids))
        ).scalars().all()
        for row in fl_rows:
            print(
                f"  id={row.id} | log_role={row.log_role} | call_count={row.call_count}"
                f" | solved={row.solved} | failed={row.failed} | task_size={row.task_size}"
            )
        if not fl_rows:
            print("  (none)")

        print("\n=== Logs rows (lifecycle events) ===")
        log_rows = session.execute(
            select(Logs).where(Logs.app_id.in_(app_ids)).order_by(Logs.id)
        ).scalars().all()
        for row in log_rows:
            print(f"  app_id={row.app_id} | level={row.log_level} | msg={row.msg} | function_id={row.function_id}")
        if not log_rows:
            print("  (none)")


def main() -> None:
    """Run database row structure examples with real DB inserts."""
    if not _check_db_env():
        return

    FunWatchRegistry.reset()
    app_info = get_app_info(__file__, depth=-3)
    assert isinstance(app_info, dict)
    app_id_main = app_info["app_id"]
    app_id_child = get_app_id(app_info["app_group"], app_info["app_parent"], app_info["app_name"] + "_child")
    runtime_main = uuid.uuid4().hex
    runtime_child = uuid.uuid4().hex
    app_ids = [app_id_main, app_id_child]

    deploy = ExampleDeploy()
    deploy.create_tables()
    deploy.populate_tables()
    FunWatchRegistry.instance().set_system_db(deploy.database)

    db = deploy.database
    runtime_start = datetime.now(UTC)
    _seed_parent_rows(db, app_info, [
        (app_id_main, app_info["app_name"], runtime_main),
        (app_id_child, app_info["app_name"] + "_child", runtime_child),
    ])

    log_settings = LogSettings(log_level=10, log_error_file="error.log")
    service = LoggingService(
        logger_name="examples.fun_watch.db_rows",
        settings=log_settings,
        db_engine=db.engine,
    )

    try:
        logger = service.configure_logger()
        logger = logger.bind(app_id=app_id_main, runtime=runtime_main)

        try:
            # --- task_size from list ---
            logger.info("task_size detected from list", expected_len=3)
            app = Collector(app_id=app_id_main, runtime=runtime_main, logger=logger)
            app.fetch_pages(["https://a.com", "https://b.com", "https://c.com"])

            # --- task_size=None for non-sized arg ---
            logger.info("task_size=None for float argument")
            app.compute_score(42.5)

            # --- task_size=None for no args ---
            logger.info("task_size=None when no arguments")
            app.heartbeat()

            # --- main_app defaults to app_id ---
            logger.info("main_app defaults to app_id")
            child_logger = logger.bind(app_id=app_id_child, runtime=runtime_child)
            child = Collector(app_id=app_id_child, runtime=runtime_child, logger=child_logger)
            child.fetch_pages(["https://x.com"])

            # --- main_app explicit override ---
            logger.info("main_app explicit override")
            child2 = Collector(
                app_id=app_id_child, runtime=runtime_child, logger=child_logger, main_app=app_id_main,
            )
            child2.fetch_pages(["https://y.com"])

            # Finalize Runtime rows and let QueueListener flush
            _complete_runtime(db, runtime_main, runtime_start)
            _complete_runtime(db, runtime_child, runtime_start)
            time.sleep(0.5)
            _print_results(db, app_ids)

        except Exception:
            logger.exception("Unhandled error in main")
            raise

    finally:
        service.stop()
        FunWatchRegistry.reset()


if __name__ == "__main__":
    main()
