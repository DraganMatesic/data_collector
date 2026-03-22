"""Abort signal demo: non-blocker HTTP errors with retry scheduling.

Demonstrates:
    - should_abort breaking a single-threaded collection loop
    - Non-blocker HTTP consecutive threshold triggering abort
    - get_retry_next_run() returning a retry datetime (now + 30 min)
    - Corrected post-fatal pattern in main()

Mixes valid and invalid URLs to trigger HTTP consecutive failures.
After 5 consecutive HTTP errors, should_abort becomes True and the loop
breaks. The main() function uses get_retry_next_run() to schedule a retry
instead of disabling the app permanently.

Requires:
    DC_DB_MAIN_USERNAME, DC_DB_MAIN_PASSWORD, DC_DB_MAIN_DATABASENAME,
    DC_DB_MAIN_IP, DC_DB_MAIN_PORT environment variables.

Run:
    python -m data_collector.examples run scraping/abort_http/main
"""

from __future__ import annotations

import logging
import os
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import select

from data_collector.enums import ErrorCategory, FatalFlag, RunStatus
from data_collector.examples.scraping.books.parser import Parser
from data_collector.scraping.base import BaseScraper, CategoryThreshold, update_app_status
from data_collector.settings.main import LogSettings
from data_collector.tables.apps import AppGroups, AppParents, Apps
from data_collector.tables.deploy import ExampleDeploy
from data_collector.tables.runtime import Runtime
from data_collector.utilities.database.main import Database
from data_collector.utilities.fun_watch import FunWatchRegistry, fun_watch
from data_collector.utilities.functions.math import get_totalh, get_totalm, get_totals
from data_collector.utilities.functions.runtime import AppInfo, bulk_hash, get_app_info
from data_collector.utilities.log.main import LoggingService
from data_collector.utilities.request import RequestMetrics

_REQUIRED_ENV = (
    "DC_DB_MAIN_USERNAME", "DC_DB_MAIN_PASSWORD", "DC_DB_MAIN_DATABASENAME",
    "DC_DB_MAIN_IP", "DC_DB_MAIN_PORT",
)

# Custom threshold: 5 consecutive HTTP errors trigger non-blocker abort
_HTTP_THRESHOLD = (
    CategoryThreshold(ErrorCategory.HTTP, max_count=0, max_rate=0.0, min_sample=0, max_consecutive=5),
)


class AbortHttp(BaseScraper):
    """Single-threaded scraper demonstrating non-blocker abort and retry.

    Mixes valid book catalogue pages with invalid URLs. After 5 consecutive
    HTTP failures, should_abort triggers and the collection loop breaks.
    get_retry_next_run() returns a retry datetime because HTTP is non-blocker.
    """

    base_url = "https://books.toscrape.com"

    def __init__(self, database: Database, **kwargs: Any) -> None:
        super().__init__(database, **kwargs)
        self.parser = Parser()

    @fun_watch
    def prepare_list(self) -> None:
        """Generate mixed valid/invalid URLs to trigger consecutive HTTP failures."""
        self.work_list = [
            f"{self.base_url}/catalogue/page-1.html",
            f"{self.base_url}/catalogue/page-2.html",
            f"{self.base_url}/catalogue/page-3.html",
            f"{self.base_url}/catalogue/page-9991.html",
            f"{self.base_url}/catalogue/page-9992.html",
            f"{self.base_url}/catalogue/page-9993.html",
            f"{self.base_url}/catalogue/page-9994.html",
            f"{self.base_url}/catalogue/page-9995.html",
            f"{self.base_url}/catalogue/page-4.html",
            f"{self.base_url}/catalogue/page-5.html",
        ]
        self.list_size = len(self.work_list)
        self.logger.info("Work list prepared", extra={"list_size": self.list_size})
        print(f"  Prepared {self.list_size} URLs (3 valid, 5 invalid, 2 valid)")

    @fun_watch
    def collect(self) -> None:
        """Fetch each URL, detecting non-2xx as HTTP errors."""
        self._start_collect_timer()
        for url in self.work_list:
            if self.should_abort:
                print("\n  ABORT: should_abort=True, stopping collection loop")
                print(f"  Reason: {self.fatal_msg}")
                print(f"  Category: {self._fatal_category}, is_blocker={self._fatal_is_blocker}")
                break

            response = self.request.get(url)
            if response is None:
                print(f"  FAILED (connection): {url}")
                self.increment_failed(error_category=self.request.get_error_category())
                self.update_progress()
                continue

            if response.status_code < 200 or response.status_code >= 300:
                print(f"  FAILED (HTTP {response.status_code}): {url}")
                self.increment_failed(error_category=ErrorCategory.HTTP)
                self.update_progress()
                continue

            books = self.parser.parse_catalogue(response.content)
            if books:
                self.store(books)
            print(f"  OK: {url} ({len(books)} books)")
            self.increment_solved()
            self.update_progress()

        self.logger.info("Collection complete", extra={"solved": self.solved, "failed": self.failed})

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
    database: Database, runtime: str, start_time: datetime, scraper: AbortHttp | None,
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
    """Non-blocker abort demo: HTTP consecutive errors trigger retry scheduling."""
    missing = [v for v in _REQUIRED_ENV if not os.environ.get(v)]
    if missing:
        print(f"Skipping: DB env vars not set: {', '.join(missing)}")
        return

    FunWatchRegistry.reset()

    # Deploy all tables into dc_example schema (non-destructive) + codebook seed data
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

    print("\n--- Abort HTTP Demo: Non-Blocker ---")
    print("Expected: 3 OK, then 5 consecutive HTTP failures trigger abort, 2 URLs never reached\n")

    scraper: AbortHttp | None = None
    try:
        metrics = RequestMetrics()
        scraper = AbortHttp(
            database, logger=logger, runtime=runtime, app_id=app_id, metrics=metrics,
            category_thresholds=_HTTP_THRESHOLD,
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
        print(f"  list_size={scraper.list_size}, fatal_flag={scraper.fatal_flag}")
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
