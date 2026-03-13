"""Single-threaded proxy scraper: books.toscrape.com via rotating proxy.

Demonstrates:
    - ProxySettings.from_zone() for zone-parameterized proxy credentials
    - BrightDataProvider for proxy URL construction
    - ProxyManager.acquire() / release() / report_failure() lifecycle
    - Request.set_proxy() integration with BaseScraper
    - Blacklist cleanup on completion

Requires:
    DC_DB_MAIN_USERNAME, DC_DB_MAIN_PASSWORD, DC_DB_MAIN_DATABASENAME,
    DC_DB_MAIN_IP, DC_DB_MAIN_PORT,
    DC_PROXY_HOST_SCRAPING, DC_PROXY_PORT_SCRAPING,
    DC_PROXY_USERNAME_SCRAPING, DC_PROXY_PASSWORD_SCRAPING
    environment variables.

Run:
    python -m data_collector.examples run scraping/proxy_single/main
"""

from __future__ import annotations

import logging
import os
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import select

from data_collector.enums import ErrorCategory, FatalFlag, RunStatus
from data_collector.examples.scraping import SCHEMA
from data_collector.examples.scraping.books.parser import Parser
from data_collector.examples.scraping.books.tables import ExampleBook as ExampleBook
from data_collector.proxy import BrightDataProvider, ProxyManager
from data_collector.proxy.models import Proxy
from data_collector.scraping import DEFAULT_CATEGORY_THRESHOLDS, BaseScraper
from data_collector.scraping.base import update_app_status
from data_collector.settings.main import LogSettings
from data_collector.settings.proxy import ProxySettings
from data_collector.tables.apps import AppGroups, AppParents, Apps
from data_collector.tables.deploy import Deploy
from data_collector.tables.runtime import Runtime
from data_collector.utilities.database.main import Database
from data_collector.utilities.fun_watch import FunWatchRegistry, fun_watch
from data_collector.utilities.functions.math import get_totalh, get_totalm, get_totals
from data_collector.utilities.functions.runtime import AppInfo, bulk_hash, get_app_info
from data_collector.utilities.log.main import LoggingService
from data_collector.utilities.request import RequestMetrics

_REQUIRED_DB_ENV = (
    "DC_DB_MAIN_USERNAME", "DC_DB_MAIN_PASSWORD", "DC_DB_MAIN_DATABASENAME",
    "DC_DB_MAIN_IP", "DC_DB_MAIN_PORT",
)
_REQUIRED_PROXY_ENV = (
    "DC_PROXY_HOST_SCRAPING", "DC_PROXY_PORT_SCRAPING",
    "DC_PROXY_USERNAME_SCRAPING", "DC_PROXY_PASSWORD_SCRAPING",
)


class ProxyBooks(BaseScraper):
    """Single-threaded scraper for books.toscrape.com using a proxy.

    Acquires one proxy at the start of collect(), uses it for all page
    requests, and releases it in cleanup(). Proxy failures are reported
    to the blacklist checker for lockout escalation.
    """

    base_url = "https://books.toscrape.com"

    def __init__(self, database: Database, proxy_manager: ProxyManager, **kwargs: Any) -> None:
        super().__init__(database, **kwargs)
        self.proxy_manager = proxy_manager
        self.parser = Parser()
        self.acquired_proxy: Proxy | None = None

    @fun_watch
    def prepare_list(self) -> None:
        """Generate catalogue page URLs (pages 1-5)."""
        self.work_list = [
            f"{self.base_url}/catalogue/page-{page}.html"
            for page in range(1, 6)
        ]
        self.list_size = len(self.work_list)
        self.logger.info("Work list prepared", extra={"list_size": self.list_size})

    @fun_watch
    def collect(self) -> None:
        """Fetch each catalogue page through a proxy and parse book listings."""
        self._start_collect_timer()
        self.acquired_proxy = self.proxy_manager.acquire(self.logger)
        self.request.set_proxy(self.acquired_proxy.url)

        for url in self.work_list:
            if self.should_abort:
                break
            response = self.request.get(url)
            if response is None:
                error_category = self.request.get_error_category()
                self.increment_failed(error_category=error_category)
                if error_category == ErrorCategory.PROXY:
                    self.proxy_manager.report_failure(self.acquired_proxy.ip_address)
                self.update_progress()
                continue

            books = self.parser.parse_catalogue(response.content)
            if books:
                self.store(books)
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
        self.logger.debug("Records stored", extra={"record_count": len(records)})

    def cleanup(self) -> None:
        """Release the proxy reservation and clean up expired blacklist entries."""
        if self.acquired_proxy is not None:
            self.proxy_manager.release(self.acquired_proxy.ip_address)
            self.logger.info(
                "Proxy released",
                extra={"ip_address": self.acquired_proxy.ip_address},
            )
        self.proxy_manager.blacklist_checker.cleanup_expired()
        self.proxy_manager.shutdown()

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
    database: Database, runtime: str, start_time: datetime, scraper: ProxyBooks | None,
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
    """End-to-end single-threaded proxy scraper example."""
    missing = [v for v in (*_REQUIRED_DB_ENV, *_REQUIRED_PROXY_ENV) if not os.environ.get(v)]
    if missing:
        print(f"Skipping: env vars not set: {', '.join(missing)}")
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

    proxy_data = ProxySettings.from_zone(app_parent).to_proxy_data()
    provider = BrightDataProvider(proxy_data)
    proxy_manager = ProxyManager(
        provider=provider,
        database=database,
        target_domain="books.toscrape.com",
        app_id=app_id,
    )

    scraper: ProxyBooks | None = None
    try:
        metrics = RequestMetrics()
        scraper = ProxyBooks(
            database, proxy_manager,
            logger=logger, runtime=runtime, app_id=app_id, metrics=metrics,
            category_thresholds=DEFAULT_CATEGORY_THRESHOLDS,
        )
        scraper.prepare_list()
        scraper.collect()
        scraper.fatal_check()

        if scraper.should_abort:
            scraper.next_run = scraper.get_retry_next_run()
        else:
            scraper.set_next_run()

        print(f"\nProxy Books scraper completed: solved={scraper.solved}, failed={scraper.failed}")
        print(f"  list_size={scraper.list_size}, next_run={scraper.next_run}")

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
        if scraper is not None:
            scraper.cleanup()
        _update_runtime(database, runtime, start_time, scraper)
        FunWatchRegistry.reset()
        service.stop()
