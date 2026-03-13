"""Abort signal demo: blocker DATABASE error with permanent stop.

Demonstrates:
    - ThreadedScraper with _abort_event cancelling pending futures
    - Blocker DATABASE error (simulated) triggering immediate abort
    - get_retry_next_run() returning None (no auto-retry for blockers)
    - Workers cooperatively checking should_abort before work
    - Corrected post-fatal pattern in main()

Fetches valid book pages but simulates a DATABASE write failure on the
3rd processed item. The DATABASE threshold (max_count=1, is_blocker=True)
triggers immediately, sets _abort_event, and cancels remaining futures.

Requires:
    DC_DB_MAIN_USERNAME, DC_DB_MAIN_PASSWORD, DC_DB_MAIN_DATABASENAME,
    DC_DB_MAIN_IP, DC_DB_MAIN_PORT environment variables.

Run:
    python -m data_collector.examples run scraping/abort_blocker/main
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
from data_collector.examples.scraping import SCHEMA
from data_collector.examples.scraping.books.parser import Parser
from data_collector.scraping import DEFAULT_CATEGORY_THRESHOLDS, ThreadedScraper
from data_collector.scraping.base import update_app_status
from data_collector.settings.main import LogSettings
from data_collector.tables.apps import AppGroups, AppParents, Apps
from data_collector.tables.deploy import Deploy
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

# Item index at which to simulate a DATABASE error
_SIMULATE_DB_ERROR_AT = 2


class AbortBlocker(ThreadedScraper):
    """Multi-threaded scraper demonstrating blocker abort via _abort_event.

    Fetches valid book pages but simulates a DATABASE write failure after
    processing a few items. The DATABASE threshold triggers immediately
    (max_count=1, is_blocker=True), sets _abort_event, and process_batch()
    cancels remaining futures with executor.shutdown(cancel_futures=True).
    """

    base_url = "https://books.toscrape.com"

    def __init__(self, database: Database, **kwargs: Any) -> None:
        super().__init__(database, **kwargs)
        self.parser = Parser()
        self._simulated_error = False
        self._simulate_lock = threading.Lock()

    @fun_watch
    def prepare_list(self) -> None:
        """Generate 10 valid catalogue page URLs."""
        self.work_list = [
            f"{self.base_url}/catalogue/page-{page}.html"
            for page in range(1, 11)
        ]
        self.list_size = len(self.work_list)
        self.logger.info("Work list prepared", extra={"list_size": self.list_size})
        print(f"  Prepared {self.list_size} URLs (all valid)")

    @fun_watch
    def collect(self) -> None:
        """Distribute pages across 2 worker threads."""
        self._start_collect_timer()
        self._fun_watch.set_task_size(self.list_size)
        self.process_batch(self.work_list, self._scrape_page, max_workers=2)

    def _scrape_page(self, item: Any, instance_id: int) -> None:
        """Fetch one page; simulate DATABASE error on the 3rd processed item."""
        if self.should_abort:
            return

        request = self.create_worker_request()
        response = request.get(item)
        if response is None:
            print(f"  FAILED [{instance_id}]: {item}")
            self.increment_failed(error_category=request.get_error_category())
            return

        books = self.parser.parse_catalogue(response.content)

        # Simulate DATABASE error after a few successful items
        with self._simulate_lock:
            processed = self.solved + self.failed
            should_simulate = not self._simulated_error and processed >= _SIMULATE_DB_ERROR_AT
            if should_simulate:
                self._simulated_error = True

        if should_simulate:
            print(f"  SIMULATING DATABASE ERROR [{instance_id}]: {item}")
            self.increment_failed(error_category=ErrorCategory.DATABASE)
            return

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
                managed=False,
            ),
            session,
            filter_cols=["group_name", "parent_name", "app_name"],
        )


def _update_runtime(
    database: Database, runtime: str, start_time: datetime, scraper: AbortBlocker | None,
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
    """Blocker abort demo: simulated DATABASE error triggers permanent stop."""
    missing = [v for v in _REQUIRED_ENV if not os.environ.get(v)]
    if missing:
        print(f"Skipping: DB env vars not set: {', '.join(missing)}")
        return

    FunWatchRegistry.reset()

    deploy = Deploy()
    deploy.database.ensure_schema(SCHEMA)
    deploy.create_tables()
    deploy.populate_tables()

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

    print("\n--- Abort Blocker Demo: DATABASE Error ---")
    print(f"Expected: ~{_SIMULATE_DB_ERROR_AT} OK, then simulated DATABASE error triggers blocker abort\n")

    scraper: AbortBlocker | None = None
    try:
        metrics = RequestMetrics()
        scraper = AbortBlocker(
            database, logger=logger, runtime=runtime, app_id=app_id, metrics=metrics, max_workers=2,
            category_thresholds=DEFAULT_CATEGORY_THRESHOLDS,
        )
        scraper.prepare_list()
        scraper.collect()
        scraper.fatal_check()

        # Corrected post-fatal pattern: use get_retry_next_run() when fatal
        if scraper.should_abort:
            scraper.next_run = scraper.get_retry_next_run()
            if scraper.next_run is not None:
                print(f"\n  NON-BLOCKER: retry scheduled at {scraper.next_run}")
            else:
                print("\n  BLOCKER: no retry, manual intervention required")
        else:
            scraper.set_next_run()
            print(f"\n  Normal completion: next_run={scraper.next_run}")

        print(f"\n  Results: solved={scraper.solved}, failed={scraper.failed}")
        print(f"  list_size={scraper.list_size}, max_workers={scraper.max_workers}")
        print(f"  fatal_flag={scraper.fatal_flag}")
        print(f"  fatal_msg={scraper.fatal_msg or 'none'}")

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
