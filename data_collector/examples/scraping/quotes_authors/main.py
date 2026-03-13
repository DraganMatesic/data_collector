"""Two-pass threaded scraper: quotes + author details from quotes.toscrape.com.

Demonstrates:
    - ThreadedScraper with two process_batch() passes
    - Pass 1 (collect): fetch quote pages, extract quotes and author URLs
    - Pass 2 (enrich_authors): fetch author detail pages, merge biographies
    - Thread-safe data accumulation with threading.Lock
    - Per-thread Request via create_worker_request()
    - Real HTTP requests to quotes.toscrape.com (practice site)
    - ORM table deployment, data storage via bulk_hash + merge

Requires:
    DC_DB_MAIN_USERNAME, DC_DB_MAIN_PASSWORD, DC_DB_MAIN_DATABASENAME,
    DC_DB_MAIN_IP, DC_DB_MAIN_PORT environment variables.

Run:
    python -m data_collector.examples run scraping/quotes_authors/main
"""

from __future__ import annotations

import logging
import os
import threading
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import select

from data_collector.enums import FatalFlag, RunStatus
from data_collector.examples.scraping import SCHEMA
from data_collector.examples.scraping.quotes_authors.parser import Parser
from data_collector.examples.scraping.quotes_authors.tables import ExampleQuoteAuthor
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


class QuotesAuthors(ThreadedScraper):
    """Two-pass scraper: collect quotes, then enrich with author biographies.

    Pass 1 -- collect() via process_batch():
        Fetches quote listing pages in parallel. Each worker parses quotes
        and extracts author detail URLs. Results accumulate in pending_quotes
        and author_urls (guarded by a lock).

    Pass 2 -- enrich_authors() via process_batch():
        Fetches author detail pages in parallel. Matches author biographies
        back to pending quotes by author name, then stores the enriched records.
    """

    base_url = "https://quotes.toscrape.com"

    def __init__(self, database: Database, **kwargs: Any) -> None:
        super().__init__(database, **kwargs)
        self.parser = Parser()
        self.pending_quotes: list[ExampleQuoteAuthor] = []
        self.author_urls: list[str] = []
        self.author_details: dict[str, dict[str, str]] = {}
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
        """Pass 1: Fetch quote pages and discover author URLs."""
        self._start_collect_timer()
        self._fun_watch.set_task_size(self.list_size)
        self.process_batch(self.work_list, self._scrape_page)

    def _scrape_page(self, item: Any, instance_id: int) -> None:
        """Fetch a single quote page, parse quotes and extract author URLs."""
        request = self.create_worker_request()
        response = request.get(item)
        if response is None:
            self.logger.warning("Failed to fetch page", extra={"url": item, "worker_id": instance_id})
            self.increment_failed(error_category=request.get_error_category())
            return

        quotes = self.parser.parse_quotes(response.content)
        author_page_urls = self.parser.parse_author_urls(response.content, self.base_url)

        with self._data_lock:
            self.pending_quotes.extend(quotes)
            for url in author_page_urls:
                if url not in self.author_urls:
                    self.author_urls.append(url)

        self.increment_solved()
        self.logger.info(
            "Processed page",
            extra={
                "url": item, "worker_id": instance_id,
                "quotes_found": len(quotes), "authors_found": len(author_page_urls),
            },
        )

    def create_worker_request(self) -> Request:
        """Create per-thread Request with custom User-Agent."""
        request = Request(timeout=30, retries=2, metrics=self.metrics)
        request.set_headers({"User-Agent": "DataCollector/1.0 (example scraper)"})
        return request

    @fun_watch
    def enrich_authors(self) -> None:
        """Pass 2: Fetch author detail pages and merge biographies into quotes.

        Uses process_batch() with a custom worker to fetch author pages
        in parallel. Each worker creates its own Request instance and
        stores the parsed author details in self.author_details.
        """
        self.list_size += len(self.author_urls)
        self.logger.info("Starting author enrichment", extra={"author_count": len(self.author_urls)})
        self._fun_watch.set_task_size(len(self.author_urls))

        self.process_batch(self.author_urls, self._author_worker)

        # Match author details back to pending quotes
        enriched_count = 0
        for quote in self.pending_quotes:
            author_name: str = quote.author  # type: ignore[assignment]
            author_info = self.author_details.get(author_name)
            if author_info:
                quote.author_born_date = author_info.get("born_date")  # type: ignore[assignment]
                quote.author_born_location = author_info.get("born_location")  # type: ignore[assignment]
                quote.author_description = author_info.get("description")  # type: ignore[assignment]
                enriched_count += 1

        self.logger.info(
            "Author enrichment complete",
            extra={"enriched": enriched_count, "total_quotes": len(self.pending_quotes)},
        )

    def _author_worker(self, url: Any, index: int) -> None:
        """Per-thread worker: fetch and parse a single author detail page."""
        request = self.create_worker_request()
        response = request.get(url)
        if response is None:
            self.logger.warning("Failed to fetch author page", extra={"url": url, "worker_id": index})
            self.increment_failed(error_category=request.get_error_category())
            return

        details = self.parser.parse_author_detail(response.content)
        author_name = self.parser.extract_author_name_from_url(url)

        with self._data_lock:
            self.author_details[author_name] = details

        self.increment_solved()
        self.logger.info("Processed author page", extra={"url": url, "worker_id": index, "author_name": author_name})

    @fun_watch(log_lifecycle=False)
    def store(self, records: list[Any]) -> None:
        """Hash and merge quote records into database."""
        bulk_hash(records)
        with self.database.create_session() as session:
            self.database.merge(records, session, logger=self.logger)
        self._fun_watch.mark_solved(len(records))
        self.logger.debug("Records stored", extra={"record_count": len(records)})

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
    database: Database, runtime: str, start_time: datetime, scraper: QuotesAuthors | None,
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
    """End-to-end two-pass threaded scraper example."""
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

    scraper: QuotesAuthors | None = None
    try:
        metrics = RequestMetrics()
        scraper = QuotesAuthors(
            database, logger=logger, runtime=runtime, app_id=app_id, metrics=metrics, max_workers=3,
            category_thresholds=DEFAULT_CATEGORY_THRESHOLDS,
        )

        # Pass 1: Collect quotes and discover author URLs
        scraper.prepare_list()
        scraper.collect()

        # Pass 2: Enrich quotes with author biographies (second process_batch call)
        scraper.enrich_authors()

        # Store enriched records
        if scraper.pending_quotes:
            scraper.store(scraper.pending_quotes)

        scraper.fatal_check()

        if scraper.should_abort:
            scraper.next_run = scraper.get_retry_next_run()
        else:
            scraper.set_next_run()

        print(f"\nQuotes+Authors scraper completed: solved={scraper.solved}, failed={scraper.failed}")
        print(f"  quotes={len(scraper.pending_quotes)}, authors={len(scraper.author_details)}")
        print(f"  list_size={scraper.list_size}, max_workers={scraper.max_workers}")
        print(f"  next_run={scraper.next_run}")

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
