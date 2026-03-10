"""Multi-threaded proxy scraper: quotes.toscrape.com with per-item proxy rotation.

Demonstrates:
    - ThreadedScraper with process_batch() for parallel processing
    - Per-item proxy rotation: each request acquires a fresh proxy IP
    - ProxyManager atomic reservation prevents IP collision across threads
    - Per-thread Request via create_worker_request() with proxy
    - Blacklist cleanup on completion

Per-item rotation rationale:
    Sequential requests from a single IP are the primary bot detection signal.
    Rotating per request ensures diverse IPs across the target domain. The
    ProxyManager's database-backed reservation prevents two concurrent workers
    from receiving the same IP. After release, the IP returns to the pool with
    TTL + blacklist cooldowns preventing immediate reuse.

Requires:
    DC_DB_MAIN_USERNAME, DC_DB_MAIN_PASSWORD, DC_DB_MAIN_DATABASENAME,
    DC_DB_MAIN_IP, DC_DB_MAIN_PORT,
    DC_PROXY_HOST_SCRAPING, DC_PROXY_PORT_SCRAPING,
    DC_PROXY_USERNAME_SCRAPING, DC_PROXY_PASSWORD_SCRAPING
    environment variables.

Run:
    python -m data_collector.examples run scraping/proxy_threaded/main
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
from data_collector.examples.scraping.quotes_authors.parser import Parser
from data_collector.examples.scraping.quotes_authors.tables import ExampleQuoteAuthor as ExampleQuoteAuthor
from data_collector.proxy import BrightDataProvider, ProxyManager
from data_collector.scraping import DEFAULT_CATEGORY_THRESHOLDS, ThreadedScraper
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
from data_collector.utilities.request import Request, RequestMetrics

_REQUIRED_DB_ENV = (
    "DC_DB_MAIN_USERNAME", "DC_DB_MAIN_PASSWORD", "DC_DB_MAIN_DATABASENAME",
    "DC_DB_MAIN_IP", "DC_DB_MAIN_PORT",
)
_REQUIRED_PROXY_ENV = (
    "DC_PROXY_HOST_SCRAPING", "DC_PROXY_PORT_SCRAPING",
    "DC_PROXY_USERNAME_SCRAPING", "DC_PROXY_PASSWORD_SCRAPING",
)


class ProxyQuotes(ThreadedScraper):
    """Multi-threaded scraper for quotes.toscrape.com with per-item proxy rotation.

    Each worker thread acquires a fresh proxy IP, makes one request, then
    releases the proxy. This prevents IP reuse across sequential requests
    and minimizes bot detection risk.
    """

    base_url = "https://quotes.toscrape.com"

    def __init__(self, database: Database, proxy_manager: ProxyManager, **kwargs: Any) -> None:
        super().__init__(database, **kwargs)
        self.proxy_manager = proxy_manager
        self.parser = Parser()
        self.pending_quotes: list[ExampleQuoteAuthor] = []
        self._data_lock = threading.Lock()

    @fun_watch
    def prepare_list(self) -> None:
        """Generate quote page URLs (pages 1-10)."""
        self.work_list = [
            f"{self.base_url}/page/{page}/"
            for page in range(1, 11)
        ]
        self.list_size = len(self.work_list)
        self.logger.info("Work list prepared", extra={"list_size": self.list_size})

    @fun_watch
    def collect(self) -> None:
        """Fetch quote pages in parallel with per-item proxy rotation."""
        self._start_collect_timer()
        self._fun_watch.set_task_size(self.list_size)
        self.process_batch(self.work_list, self._scrape_page)

    def _scrape_page(self, item: Any, instance_id: int) -> None:
        """Per-thread worker: acquire proxy, fetch page, parse quotes, release proxy."""
        proxy = self.proxy_manager.acquire(self.logger)
        try:
            request = self.create_worker_request()
            request.set_proxy(proxy.url)

            response = request.get(item)
            if response is None:
                error_category = request.get_error_category()
                self.increment_failed(error_category=error_category)
                if error_category == ErrorCategory.PROXY:
                    self.proxy_manager.report_failure(proxy.ip_address)
                self.logger.warning(
                    "Failed to fetch page",
                    extra={"url": item, "worker_id": instance_id, "ip_address": proxy.ip_address},
                )
                return

            quotes = self.parser.parse_quotes(response.content)
            with self._data_lock:
                self.pending_quotes.extend(quotes)

            self.increment_solved()
            self.logger.info(
                "Processed page",
                extra={
                    "url": item, "worker_id": instance_id,
                    "quotes_found": len(quotes), "ip_address": proxy.ip_address,
                },
            )
        finally:
            self.proxy_manager.release(proxy.ip_address)

    def create_worker_request(self) -> Request:
        """Create per-thread Request with custom User-Agent."""
        request = Request(timeout=30, retries=2, metrics=self.metrics)
        request.set_headers({"User-Agent": "DataCollector/1.0 (example scraper)"})
        return request

    @fun_watch(log_lifecycle=False)
    def store(self, records: list[Any]) -> None:
        """Hash and merge quote records into database."""
        bulk_hash(records)
        with self.database.create_session() as session:
            self.database.merge(records, session, logger=self.logger)
        self._fun_watch.mark_solved(len(records))
        self.logger.debug("Records stored", extra={"record_count": len(records)})

    def cleanup(self) -> None:
        """Clean up expired blacklist entries and stop cleanup thread."""
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
            ),
            session,
            filter_cols=["group_name", "parent_name", "app_name"],
        )


def _update_runtime(
    database: Database, runtime: str, start_time: datetime, scraper: ProxyQuotes | None,
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
    """End-to-end multi-threaded proxy scraper example."""
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
        target_domain="quotes.toscrape.com",
        app_id=app_id,
    )

    scraper: ProxyQuotes | None = None
    try:
        metrics = RequestMetrics()
        scraper = ProxyQuotes(
            database, proxy_manager,
            logger=logger, runtime=runtime, app_id=app_id, metrics=metrics, max_workers=3,
            category_thresholds=DEFAULT_CATEGORY_THRESHOLDS,
        )
        scraper.prepare_list()
        scraper.collect()

        if scraper.pending_quotes:
            scraper.store(scraper.pending_quotes)

        scraper.fatal_check()

        if scraper.should_abort:
            scraper.next_run = scraper.get_retry_next_run()
        else:
            scraper.set_next_run()

        print(f"\nProxy Quotes scraper completed: solved={scraper.solved}, failed={scraper.failed}")
        print(f"  quotes={len(scraper.pending_quotes)}, max_workers={scraper.max_workers}")
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
