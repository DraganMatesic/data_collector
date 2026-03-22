"""Async scraper: jsonplaceholder.typicode.com REST API.

Demonstrates:
    - AsyncScraper with collect() + process_batch_async() pattern
    - Semaphore-controlled concurrency (max_concurrency)
    - self.request.async_get() for non-blocking HTTP
    - store_async() for serialized database writes from coroutines
    - Real HTTP requests to jsonplaceholder.typicode.com (practice API)
    - ORM table deployment, data storage via bulk_hash + merge
    - Structured logging with runtime context

Requires:
    DC_DB_MAIN_USERNAME, DC_DB_MAIN_PASSWORD, DC_DB_MAIN_DATABASENAME,
    DC_DB_MAIN_IP, DC_DB_MAIN_PORT environment variables.

Run:
    python -m data_collector.examples run scraping/posts/main
"""

from __future__ import annotations

import logging
import os
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import select

from data_collector.enums import FatalFlag, RunStatus
from data_collector.examples.scraping.posts.parser import Parser
from data_collector.scraping import DEFAULT_CATEGORY_THRESHOLDS, AsyncScraper
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
from data_collector.utilities.request import RequestMetrics

_REQUIRED_ENV = (
    "DC_DB_MAIN_USERNAME", "DC_DB_MAIN_PASSWORD", "DC_DB_MAIN_DATABASENAME",
    "DC_DB_MAIN_IP", "DC_DB_MAIN_PORT",
)


class Posts(AsyncScraper):
    """Async scraper for jsonplaceholder.typicode.com posts API."""

    base_url = "https://jsonplaceholder.typicode.com"

    def __init__(self, database: Database, **kwargs: Any) -> None:
        super().__init__(database, **kwargs)
        self.parser = Parser()

    @fun_watch
    def prepare_list(self) -> None:
        """Fetch post listing and extract IDs to process."""
        response = self.request.get(f"{self.base_url}/posts")
        if response is None:
            self.logger.warning("Failed to fetch post listing")
            return

        self.work_list = self.parser.parse_post_list(response.json())
        self.list_size = len(self.work_list)
        self.logger.info("Work list prepared", extra={"list_size": self.list_size})

    @fun_watch
    async def collect(self) -> None:
        """Distribute post IDs across async workers."""
        self._start_collect_timer()
        self._fun_watch.set_task_size(self.list_size)
        await self.process_batch_async(self.work_list, self._fetch_post)

    async def _fetch_post(self, item: Any, instance_id: int) -> None:
        """Fetch a single post by ID and store it."""
        response = await self.request.async_get(f"{self.base_url}/posts/{item}")
        if response is None:
            self.increment_failed(error_category=self.request.get_error_category())
            return

        post = self.parser.parse_post(response.json())
        await self.store_async([post])
        self.increment_solved()

    @fun_watch
    def store(self, records: list[Any]) -> None:
        """Hash and merge post records into database."""
        bulk_hash(records)
        with self.database.create_session() as session:
            self.database.merge(records, session, logger=self.logger)
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
            ),
            session,
            filter_cols=["group_name", "parent_name", "app_name"],
        )


def _update_runtime(
    database: Database, runtime: str, start_time: datetime, scraper: Posts | None,
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


async def main() -> None:
    """End-to-end async scraper example."""
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

    scraper: Posts | None = None
    try:
        metrics = RequestMetrics()
        scraper = Posts(
            database, logger=logger, runtime=runtime, app_id=app_id, metrics=metrics, max_concurrency=5,
            category_thresholds=DEFAULT_CATEGORY_THRESHOLDS,
        )
        scraper.prepare_list()
        await scraper.collect()
        scraper.fatal_check()

        if scraper.should_abort:
            scraper.next_run = scraper.get_retry_next_run()
        else:
            scraper.set_next_run()

        print(f"\nPosts scraper completed: solved={scraper.solved}, failed={scraper.failed}")
        print(f"  list_size={scraper.list_size}, max_concurrency={scraper.max_concurrency}")
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
