"""Exception handling demo: handled vs unhandled exceptions in thread callbacks.

Demonstrates two exception patterns side by side:

1. Handled exception (try/except in app code):
   - _scrape_page catches ValueError, calls self.logger.exception() at the
     app call site (correct lineno, module, traceback), then increment_failed().
   - Logs record shows traceback from the app's _scrape_page method.

2. Unhandled exception (propagates to framework):
   - _scrape_page raises RuntimeError with no try/except.
   - wrap_with_thread_context catches it, calls logger.exception() with
     the thread's function_id and full traceback.
   - process_batch's except handler calls increment_failed() (counter only).
   - Logs record shows traceback from the framework wrapper.

Both tracebacks are stored in Logs.context_json under the "exception" key.
Query: SELECT * FROM logs WHERE log_level >= 40 to inspect.

Requires:
    DC_DB_MAIN_USERNAME, DC_DB_MAIN_PASSWORD, DC_DB_MAIN_DATABASENAME,
    DC_DB_MAIN_IP, DC_DB_MAIN_PORT environment variables.

Run:
    python -m data_collector.examples run scraping/abort_exception/main
"""

from __future__ import annotations

import logging
import os
import threading
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import select

from data_collector.enums import ErrorCategory, FatalFlag, RunStatus
from data_collector.examples.scraping.books.parser import Parser
from data_collector.scraping import DEFAULT_CATEGORY_THRESHOLDS, ThreadedScraper
from data_collector.scraping.base import update_app_status
from data_collector.settings.main import LogSettings
from data_collector.tables.apps import AppGroups, AppParents, Apps
from data_collector.tables.deploy import ExampleDeploy
from data_collector.tables.runtime import Runtime
from data_collector.utilities.database.main import Database
from data_collector.utilities.fun_watch import FunWatchRegistry, fun_watch
from data_collector.utilities.functions.math import get_totalh, get_totalm, get_totals
from data_collector.utilities.functions.runtime import AppInfo, bulk_hash, get_app_info
from data_collector.utilities.log.main import LoggingService
from data_collector.utilities.request import Request, RequestMetrics

_REQUIRED_ENV = (
    "DC_DB_MAIN_USERNAME", "DC_DB_MAIN_PASSWORD", "DC_DB_MAIN_DATABASENAME",
    "DC_DB_MAIN_IP", "DC_DB_MAIN_PORT",
)

# Item indices at which to trigger each exception type
_SIMULATE_HANDLED_AT = 2
_SIMULATE_UNHANDLED_AT = 4


def _parse_books(content: bytes) -> list[Any]:
    """Simulate parsing that raises ValueError on bad content."""
    raise ValueError("Malformed HTML: unexpected <script> tag in catalogue row 3")


class AbortException(ThreadedScraper):
    """Multi-threaded scraper demonstrating handled and unhandled exception patterns.

    After a few successful items:
    - Item N triggers a handled ValueError (caught in try/except, logged by app).
    - Item M triggers an unhandled RuntimeError (caught by wrap_with_thread_context).
    """

    base_url = "https://books.toscrape.com"

    def __init__(self, database: Database, **kwargs: Any) -> None:
        super().__init__(database, **kwargs)
        self.parser = Parser()
        self._handled_triggered = False
        self._unhandled_triggered = False
        self._simulate_lock = threading.Lock()
        self._call_counter = 0

    @fun_watch
    def prepare_list(self) -> None:
        """Generate 10 valid catalogue page URLs."""
        self.work_list = [
            f"{self.base_url}/catalogue/page-{page}.html"
            for page in range(1, 11)
        ]
        self.list_size = len(self.work_list)
        self.logger.info("Work list prepared", extra={"list_size": self.list_size})
        print(f"  Prepared {self.list_size} URLs")

    @fun_watch
    def collect(self) -> None:
        """Distribute pages across 2 worker threads."""
        self._start_collect_timer()
        self._fun_watch.set_task_size(self.list_size)
        self.process_batch(self.work_list, self._scrape_page, max_workers=2)

    def _scrape_page(self, item: Any, instance_id: int) -> None:
        """Fetch one page; simulate handled and unhandled exceptions."""
        if self.should_abort:
            return

        request = self.create_worker_request()
        response = request.get(item)
        if response is None:
            self.logger.error(f"Connection failed: {item}")
            self.increment_failed(error_category=request.get_error_category())
            return

        # Decide which failure mode to simulate based on call order
        with self._simulate_lock:
            call_number = self._call_counter
            self._call_counter += 1
            trigger_handled = not self._handled_triggered and call_number == _SIMULATE_HANDLED_AT
            trigger_unhandled = not self._unhandled_triggered and call_number == _SIMULATE_UNHANDLED_AT
            if trigger_handled:
                self._handled_triggered = True
            if trigger_unhandled:
                self._unhandled_triggered = True

        # --- Pattern 1: Handled exception (app catches, logs, increments counter) ---
        if trigger_handled:
            print(f"  HANDLED EXCEPTION [{instance_id}]: {item}")
            try:
                _parse_books(response.content)
            except ValueError:
                self.logger.exception(f"Parse error on {item}")
                self.increment_failed(error_category=ErrorCategory.PARSE)
            return

        # --- Pattern 2: Unhandled exception (propagates to framework) ---
        if trigger_unhandled:
            print(f"  UNHANDLED EXCEPTION [{instance_id}]: {item}")
            raise RuntimeError(f"Unexpected response format from {item}")

        # Normal processing
        books = self.parser.parse_catalogue(response.content)
        if books:
            self.store(books)
        print(f"  OK [{instance_id}]: {item} ({len(books)} books)")
        self.increment_solved()

    def create_worker_request(self) -> Request:
        """Create per-thread Request with shared metrics."""
        return Request(timeout=30, retries=2, metrics=self.metrics)

    @fun_watch(log_lifecycle=False)
    def store(self, records: list[Any]) -> None:
        """Hash and merge book records into database."""
        bulk_hash(records)
        with self.database.create_session() as session:
            self.database.merge(records, session, logger=self.logger)
        self._fun_watch.mark_solved(len(records))

    def set_next_run(self) -> None:
        """Schedule next run in 1 day."""
        self.next_run = datetime.now(UTC) + timedelta(days=1)


def _register_app(database: Database, app_info: AppInfo) -> None:
    """Register AppGroups, AppParents, and Apps rows using idempotent update_insert."""
    with database.create_session() as session:
        database.update_insert(AppGroups(name=app_info["app_group"]), session, filter_cols=["name"])

    with database.create_session() as session:
        database.update_insert(
            AppParents(name=app_info["app_parent"], group_name=app_info["app_group"], parent=app_info["parent_id"]),
            session,
            filter_cols=["name", "group_name"],
        )

    with database.create_session() as session:
        database.update_insert(
            Apps(
                app=app_info["app_id"],
                group_name=app_info["app_group"],
                parent_name=app_info["app_parent"],
                app_name=app_info["app_name"],
                parent_id=app_info["parent_id"],
                run_status=RunStatus.NOT_RUNNING,
                fatal_flag=FatalFlag.NONE,
                disable=True,
            ),
            session,
            filter_cols=["group_name", "parent_name", "app_name"],
        )


def _update_runtime(
    database: Database, runtime: str, start_time: datetime, scraper: AbortException | None,
) -> None:
    """Finalize the Runtime record with end time and counters."""
    end_time = datetime.now(UTC)
    with database.create_session() as session:
        runtime_record = session.execute(
            select(Runtime).where(Runtime.runtime == runtime)
        ).scalar_one_or_none()
        if runtime_record is not None:
            session.merge(Runtime(
                id=runtime_record.id,
                runtime=runtime,
                app_id=runtime_record.app_id,
                start_time=runtime_record.start_time,
                end_time=end_time,
                task_size=scraper.list_size if scraper is not None else None,
                except_cnt=scraper.failed if scraper is not None else 0,
                totals=get_totals(start_time, end_time),
                totalm=get_totalm(start_time, end_time),
                totalh=get_totalh(start_time, end_time),
            ))
            session.commit()


def main() -> None:
    """Exception handling demo: handled vs unhandled exceptions in thread callbacks."""
    missing = [v for v in _REQUIRED_ENV if not os.environ.get(v)]
    if missing:
        print(f"Skipping: DB env vars not set: {', '.join(missing)}")
        return

    FunWatchRegistry.reset()

    deploy = ExampleDeploy()
    deploy.create_tables()
    deploy.populate_tables()
    FunWatchRegistry.instance().set_system_db(deploy.database)

    database = deploy.database
    app_info: AppInfo = get_app_info(__file__)  # type: ignore[assignment]
    app_id = app_info["app_id"]
    app_group = app_info["app_group"]
    app_parent = app_info["app_parent"]
    app_name = app_info["app_name"]

    _register_app(database, app_info)
    database.app_id = app_id

    log_settings = LogSettings(log_level=10, log_error_file="error.log")
    FunWatchRegistry.instance().set_default_lifecycle_log_level(log_settings.log_level)
    service = LoggingService(
        f"{app_group}.{app_parent}.{app_name}", settings=log_settings, db_engine=database.engine,
    )
    logger = service.configure_logger()

    runtime = uuid.uuid4().hex
    logger = logger.bind(app_id=app_id, runtime=runtime)

    update_app_status(database, app_id, run_status=RunStatus.RUNNING, runtime_id=runtime)

    start_time = datetime.now(UTC)
    with database.create_session() as session:
        session.merge(Runtime(runtime=runtime, app_id=app_id, start_time=start_time))
        session.commit()

    print("\n--- Exception Handling Demo: Handled vs Unhandled ---")
    print(f"  Item {_SIMULATE_HANDLED_AT}: handled ValueError (app catches, logs traceback)")
    print(f"  Item {_SIMULATE_UNHANDLED_AT}: unhandled RuntimeError (framework catches, logs traceback)\n")

    scraper: AbortException | None = None
    try:
        metrics = RequestMetrics()
        scraper = AbortException(
            database, logger=logger, runtime=runtime, app_id=app_id, metrics=metrics, max_workers=2,
            category_thresholds=DEFAULT_CATEGORY_THRESHOLDS,
        )
        scraper.prepare_list()
        scraper.collect()
        scraper.fatal_check()

        if scraper.should_abort:
            scraper.next_run = scraper.get_retry_next_run()
            if scraper.next_run is not None:
                print(f"\n  NON-BLOCKER: retry scheduled at {scraper.next_run}")
            else:
                print("\n  BLOCKER: no retry, manual intervention required")
        else:
            scraper.set_next_run()

        print(f"\n  Results: solved={scraper.solved}, failed={scraper.failed}")
        print(f"  fatal_flag={scraper.fatal_flag}")
        print("  Check Logs table: SELECT * FROM dc_example.logs WHERE log_level >= 40")
        print("  Both records should have traceback in context_json")

        scraper.update_progress(force=True)
        update_app_status(
            database, app_id,
            run_status=RunStatus.NOT_RUNNING,
            next_run=scraper.next_run,
            solved=scraper.solved,
            failed=scraper.failed,
            task_size=scraper.list_size,
            fatal_flag=scraper.fatal_flag,
            fatal_msg=scraper.fatal_msg or None,
            fatal_time=scraper.fatal_time,
            disable=True if scraper.should_abort and scraper.next_run is None else None,
        )

        scraper.metrics.log_stats(logging.getLogger(__name__))
    finally:
        _update_runtime(database, runtime, start_time, scraper)
        FunWatchRegistry.reset()
        service.stop()
