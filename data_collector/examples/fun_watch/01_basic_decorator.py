"""Basic @fun_watch decorator with real DB inserts and lifecycle logging.

Demonstrates:
    - Real LoggingService integration (Logs table inserts via DatabaseHandler)
    - Real FunctionLog and AppFunctions inserts via FunWatchRegistry
    - Lifecycle logging: automatic "Function started" / "Function completed" / exception events
    - self._fun_watch.mark_solved() / mark_failed() inside the method body
    - Return value passthrough (decorator is transparent)
    - Exception handling (decorator records metrics + logs exception, then re-raises)

Requires:
    DC_DB_MAIN_USERNAME, DC_DB_MAIN_PASSWORD, DC_DB_MAIN_DATABASENAME,
    DC_DB_MAIN_IP, DC_DB_MAIN_PORT environment variables.

Optional Splunk HEC (env-driven, see logging/01_splunk_hec for setup):
    DC_LOG_SPLUNK_ENABLED=true
    DC_LOG_SPLUNK_URL=https://127.0.0.1:8088/services/collector
    DC_LOG_SPLUNK_TOKEN=<your-hec-token>
    DC_LOG_SPLUNK_VERIFY_TLS=false

Run:
    python -m data_collector.examples run fun_watch/01_basic_decorator
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
from data_collector.utilities.functions.runtime import AppInfo, get_app_info
from data_collector.utilities.log.main import LoggingService

_REQUIRED_ENV = (
    "DC_DB_MAIN_USERNAME", "DC_DB_MAIN_PASSWORD", "DC_DB_MAIN_DATABASENAME",
    "DC_DB_MAIN_IP", "DC_DB_MAIN_PORT",
)


class DemoApp(FunWatchMixin):
    """Minimal application class with real LoggingService logger."""

    def __init__(self, app_id: str, runtime: str, logger: Any) -> None:
        self.app_id = app_id
        self.runtime = runtime
        self.logger = logger

    @fun_watch
    def process_records(self, records: list[str]) -> int:
        """Process a batch of records, marking each as solved."""
        for _record in records:
            self.logger.debug(f"processing record: {_record}")
            self._fun_watch.mark_solved()
        return len(records)

    @fun_watch
    def run_pipeline(self, records: list[str]) -> int:
        """Orchestrator that calls process_records -- demonstrates call_chain nesting."""
        return self.process_records(records)

    @fun_watch
    def risky_operation(self, items: list[str]) -> None:
        """Process items but fail halfway through."""
        for i, _item in enumerate(items):
            if i >= 2:
                self._fun_watch.mark_failed(
                    len(items) - i,
                    error_type="RuntimeError",
                    error_message="Simulated failure after 2 items",
                )
                raise RuntimeError("Simulated failure after 2 items")
            self._fun_watch.mark_solved()


def _check_db_env() -> bool:
    """Return True when required DB environment variables are set."""
    missing = [v for v in _REQUIRED_ENV if not os.environ.get(v)]
    if missing:
        print(f"Skipping: DB env vars not set: {', '.join(missing)}")
        return False
    return True


def _seed_parent_rows(db: Database, app_info: AppInfo, runtime_id: str) -> None:
    """Seed AppGroups, AppParents, Apps, and Runtime rows required by FK constraints."""
    group = app_info["app_group"]
    parent = app_info["app_parent"]
    app_id = app_info["app_id"]
    with db.create_session() as session:
        if not session.execute(select(AppGroups).where(AppGroups.name == group)).scalar():
            session.add(AppGroups(name=group))
            session.flush()
        if not session.execute(
            select(AppParents).where(AppParents.name == parent, AppParents.group_name == group)
        ).scalar():
            session.add(AppParents(name=parent, group_name=group))
            session.flush()
        session.merge(Apps(
            app=app_id,
            group_name=group,
            parent_name=parent,
            app_name=app_info["app_name"],
        ))
        session.merge(Runtime(
            runtime=runtime_id,
            app_id=app_id,
            start_time=datetime.now(UTC),
        ))
        session.commit()


def _print_results(db: Database, app_id: str) -> None:
    """Query and print all rows inserted by this example."""
    with db.create_session() as session:
        print("\n=== AppFunctions rows ===")
        rows = session.execute(select(AppFunctions).where(AppFunctions.app_id == app_id)).scalars().all()
        for row in rows:
            print(f"  {row!r}")
        if not rows:
            print("  (none)")

        print("\n=== FunctionLog rows ===")
        rows = session.execute(select(FunctionLog).where(FunctionLog.app_id == app_id)).scalars().all()
        for row in rows:
            print(
                f"  id={row.id} | log_role={row.log_role} | parent_log_id={row.parent_log_id}"
                f" | solved={row.solved} | failed={row.failed} | task_size={row.task_size}"
            )
        if not rows:
            print("  (none)")

        print("\n=== Logs rows (lifecycle + exception events) ===")
        rows = session.execute(
            select(Logs).where(Logs.app_id == app_id).order_by(Logs.id)
        ).scalars().all()
        for row in rows:
            print(
                f"  level={row.log_level} | msg={row.msg}"
                f" | module_name={row.module_name} | call_chain={row.call_chain}"
            )
        if not rows:
            print("  (none)")


def main() -> None:
    """Run basic @fun_watch example with real DB inserts."""
    if not _check_db_env():
        return

    FunWatchRegistry.reset()
    app_info = get_app_info(__file__, depth=-3)
    assert isinstance(app_info, dict)
    app_id = app_info["app_id"]
    runtime_id = uuid.uuid4().hex

    deploy = ExampleDeploy()
    deploy.create_tables()
    deploy.populate_tables()
    FunWatchRegistry.instance().set_system_db(deploy.database)

    db = deploy.database
    _seed_parent_rows(db, app_info, runtime_id)

    log_settings = LogSettings(log_level=10, log_error_file="error.log")
    service = LoggingService(
        logger_name="examples.fun_watch.basic",
        settings=log_settings,
        db_engine=db.engine,
    )

    try:
        logger = service.configure_logger()
        logger = logger.bind(app_id=app_id, runtime=runtime_id)

        try:
            app = DemoApp(app_id=app_id, runtime=runtime_id, logger=logger)

            # --- Success case ---
            logger.info("Success: process 5 records")
            result = app.process_records(["a", "b", "c", "d", "e"])
            logger.info("Return value", result=result)

            # --- Return value passthrough ---
            logger.info("Return value passthrough")
            count = app.process_records(["x", "y"])
            logger.info("Decorator returns original result", result=count)

            # --- Nested call chain ---
            logger.info("Nested call chain: run_pipeline -> process_records")
            result = app.run_pipeline(["n1", "n2", "n3"])
            logger.info("Return value", result=result)

            # --- Exception handling ---
            logger.info("Exception: failure after 2 items")
            try:
                app.risky_operation(["r1", "r2", "r3", "r4", "r5"])
            except RuntimeError as exc:
                logger.warning("Exception re-raised", error=str(exc))

            # Let QueueListener flush pending log records to DB
            time.sleep(0.5)
            _print_results(db, app_id)

        except Exception:
            logger.exception("Unhandled error in main")
            raise

    finally:
        service.stop()
        FunWatchRegistry.reset()


if __name__ == "__main__":
    main()
