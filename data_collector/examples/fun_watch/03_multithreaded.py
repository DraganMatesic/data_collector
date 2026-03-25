"""Multi-threaded @fun_watch usage with real DB inserts.

Demonstrates:
    - Real LoggingService integration (Logs table inserts via DatabaseHandler)
    - ThreadPoolExecutor with multiple workers calling the same decorated method
    - All threads share one aggregate FunWatchContext per function per runtime
    - Thread-safe counter updates via _counter_lock (no data corruption)
    - One FunctionLog row per function per runtime (aggregate model)
    - Partial failure: mark_solved() + mark_failed() with exception re-raise

Requires:
    DC_DB_MAIN_USERNAME, DC_DB_MAIN_PASSWORD, DC_DB_MAIN_DATABASENAME,
    DC_DB_MAIN_IP, DC_DB_MAIN_PORT environment variables.

Optional Splunk HEC (env-driven, see logging/01_splunk_hec for setup):
    DC_LOG_SPLUNK_ENABLED=true
    DC_LOG_SPLUNK_URL=https://127.0.0.1:8088/services/collector
    DC_LOG_SPLUNK_TOKEN=<your-hec-token>
    DC_LOG_SPLUNK_VERIFY_TLS=false

Run:
    python -m data_collector.examples run fun_watch/03_multithreaded
"""

from __future__ import annotations

import os
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
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
from data_collector.utilities.functions.runtime import AppInfo, get_app_info
from data_collector.utilities.log.main import LoggingService

_REQUIRED_ENV = (
    "DC_DB_MAIN_USERNAME", "DC_DB_MAIN_PASSWORD", "DC_DB_MAIN_DATABASENAME",
    "DC_DB_MAIN_IP", "DC_DB_MAIN_PORT",
)


class BatchProcessor(FunWatchMixin):
    """Application that processes batches across multiple threads."""

    def __init__(self, app_id: str, runtime: str, logger: Any) -> None:
        self.app_id = app_id
        self.runtime = runtime
        self.logger = logger

    @fun_watch
    def process_batch(self, items: list[int]) -> int:
        """Process a batch of integers, marking each as solved."""
        total = 0
        for item in items:
            self._fun_watch.mark_solved()
            total += item
        return total

    @fun_watch
    def process_batch_partial(self, items: list[int], fail_after: int) -> int:
        """Process a batch but fail after a given number of items.

        Items up to ``fail_after`` are marked solved; the remainder are marked
        failed and a ``RuntimeError`` is raised.
        """
        total = 0
        for i, item in enumerate(items):
            if i >= fail_after:
                self._fun_watch.mark_failed(
                    len(items) - i,
                    error_type="RuntimeError",
                    error_message=f"Simulated failure after {fail_after} items",
                )
                raise RuntimeError(f"Simulated failure after {fail_after} items")
            self._fun_watch.mark_solved()
            total += item
        return total


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


def _print_results(db: Database, app_id: str) -> None:
    """Query and print all rows inserted by this example."""
    with db.create_session() as session:
        print("\n=== AppFunctions rows ===")
        rows = session.execute(
            select(AppFunctions).where(AppFunctions.app_id == app_id)
        ).scalars().all()
        for row in rows:
            print(f"  {row!r}")
        if not rows:
            print("  (none)")

        print("\n=== FunctionLog rows ===")
        log_rows = session.execute(
            select(FunctionLog).where(FunctionLog.app_id == app_id)
        ).scalars().all()
        for row in log_rows:
            print(
                f"  id={row.id} | log_role={row.log_role} | call_count={row.call_count}"
                f" | solved={row.solved} | failed={row.failed} | task_size={row.task_size}"
                f" | is_success={row.is_success}"
            )
        if not log_rows:
            print("  (none)")

        # --- Aggregate summary ---
        if log_rows:
            print("\n=== Aggregate summary ===")
            print(f"  {'log_role':>12}  {'call_count':>12}  {'task_size':>10}  {'solved':>7}")
            print(f"  {'-' * 12}  {'-' * 12}  {'-' * 10}  {'-' * 7}")
            for row in sorted(log_rows, key=lambda r: r.id or 0):
                print(
                    f"  {row.log_role or '':>12}  "
                    f"{row.call_count or '':>12}  "
                    f"{row.task_size or '':>10}  "
                    f"{row.solved or '':>7}"
                )

        # --- Aggregate verification ---
        print("\n=== Aggregate verification ===")
        total_solved = sum(row.solved or 0 for row in log_rows)
        total_rows = len(log_rows)
        print(f"  Total FunctionLog rows: {total_rows}")
        print(f"  Total solved across all aggregates: {total_solved}")

        print("\n=== Logs rows (lifecycle events) ===")
        logs_rows = session.execute(
            select(Logs).where(Logs.app_id == app_id).order_by(Logs.id)
        ).scalars().all()
        for row in logs_rows:
            print(f"  level={row.log_level} | msg={row.msg} | function_id={row.function_id}")
        if not logs_rows:
            print("  (none)")


def main() -> None:
    """Run multi-threaded @fun_watch example with real DB inserts."""
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
    runtime_start = datetime.now(UTC)
    _seed_parent_rows(db, app_info, runtime_id)

    log_settings = LogSettings(log_level=10, log_error_file="error.log")
    service = LoggingService(
        logger_name="examples.fun_watch.multithreaded",
        settings=log_settings,
        db_engine=db.engine,
    )

    try:
        logger = service.configure_logger()
        logger = logger.bind(app_id=app_id, runtime=runtime_id)

        try:
            # --- Launch 4 threads, each processing a different batch ---
            logger.info("Launching threads with different batch sizes", worker_count=4)
            batches: list[list[int]] = [
                list(range(10)),
                list(range(20)),
                list(range(15)),
                list(range(5)),
            ]

            def run_batch(batch: list[int]) -> tuple[int, int]:
                worker_app = BatchProcessor(
                    app_id=app_id, runtime=runtime_id, logger=logger,
                )
                result = worker_app.process_batch(batch)
                return result, threading.get_ident()

            with ThreadPoolExecutor(max_workers=4) as executor:
                futures = {
                    executor.submit(run_batch, batch): i
                    for i, batch in enumerate(batches)
                }
                for future in as_completed(futures):
                    worker_idx = futures[future]
                    result, worker_thread_id = future.result()
                    logger.info(
                        "Worker completed",
                        worker_idx=worker_idx,
                        batch_size=len(batches[worker_idx]),
                        total_sum=result,
                        thread_id=worker_thread_id,
                    )

            # --- Partial failure: some items fail in each batch ---
            logger.info("Launching threads with partial failures", worker_count=3)
            fail_specs: list[tuple[list[int], int]] = [
                (list(range(10)), 7),   # 7 solved, 3 failed
                (list(range(8)), 3),    # 3 solved, 5 failed
                (list(range(12)), 12),  # all 12 solved, no failure
            ]

            def run_batch_partial(items: list[int], cutoff: int) -> tuple[int, int]:
                worker_app = BatchProcessor(
                    app_id=app_id, runtime=runtime_id, logger=logger,
                )
                result = worker_app.process_batch_partial(items, cutoff)
                return result, threading.get_ident()

            with ThreadPoolExecutor(max_workers=3) as executor:
                futures_partial = {
                    executor.submit(run_batch_partial, items, cutoff): idx
                    for idx, (items, cutoff) in enumerate(fail_specs)
                }
                for future in as_completed(futures_partial):
                    worker_idx = futures_partial[future]
                    items, cutoff = fail_specs[worker_idx]
                    try:
                        result, worker_thread_id = future.result()
                        logger.info(
                            "Worker completed",
                            worker_idx=worker_idx,
                            batch_size=len(items),
                            total_sum=result,
                            thread_id=worker_thread_id,
                        )
                    except RuntimeError as exc:
                        logger.warning(
                            "Worker failed",
                            worker_idx=worker_idx,
                            batch_size=len(items),
                            fail_after=cutoff,
                            error=str(exc),
                        )

            # Finalize Runtime row and let QueueListener flush
            _complete_runtime(db, runtime_id, runtime_start, exception_count=2)
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
