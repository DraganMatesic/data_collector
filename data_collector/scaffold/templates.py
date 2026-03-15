"""String templates for scaffolded application files.

Each template uses str.format() placeholders:
    {group}      -- App group name (e.g., "country")
    {parent}     -- Parent domain (e.g., "financials")
    {name}       -- App name (e.g., "company_data")
    {class_name} -- PascalCase class name (e.g., "CompanyData")
"""

INIT_TEMPLATE = '"""Application package for {group}.{parent}.{name}."""\n'

MAIN_SINGLE_TEMPLATE = '''"""Single-threaded scraper for {group}.{parent}.{name}."""

from __future__ import annotations

import argparse
import json
import uuid
from typing import Any

from data_collector.enums import RunStatus
from data_collector.scraping import DEFAULT_CATEGORY_THRESHOLDS, BaseScraper
from data_collector.scraping.base import update_app_status
from data_collector.settings.main import MainDatabaseSettings
from data_collector.utilities.database.main import Database
from data_collector.utilities.functions.runtime import get_app_info
from data_collector.utilities.log.main import LoggingService
from data_collector.utilities.request import RequestMetrics


class {class_name}(BaseScraper):
    """{class_name} scraper.

    # TODO: Add description of what this scraper collects.
    """

    base_url = ""  # TODO: Set target site root URL

    def __init__(self, database: Database, **kwargs: Any) -> None:
        super().__init__(database, **kwargs)
        # TODO: Initialize parser and any app-specific state
        # self.parser = Parser()

    def prepare_list(self) -> None:
        """Query database for items to process."""
        # TODO: Fetch work items from database
        # with self.database.create_session() as session:
        #     self.work_list = ...
        # self.list_size = len(self.work_list)
        pass

    def collect(self) -> None:
        """Fetch data from source for each item in work_list."""
        # TODO: Implement collection logic
        # self._start_collect_timer()
        # for item in self.work_list:
        #     if self.should_abort:
        #         break
        #     response = self.request.get(f"{{self.base_url}}/...")
        #     if response is None:
        #         self.increment_failed(error_category=self.request.get_error_category())
        #         self.update_progress()
        #         continue
        #     data = self.parser.parse(response.json())
        #     self.store([data])
        #     self.increment_solved()
        #     self.update_progress()
        pass

    def store(self, records: list[Any]) -> None:
        """Persist collected records to database."""
        # TODO: Hash and merge records
        # bulk_hash(records)
        # with self.database.create_session() as session:
        #     self.database.merge(records, session)
        pass

    def set_next_run(self) -> None:
        """Calculate next execution time."""
        # TODO: Set self.next_run based on collection schedule
        # self.next_run = datetime.now() + timedelta(days=1)
        pass


def init(runtime: str, args: dict[str, Any] | None = None) -> None:
    """Entry point called by the Manager process.

    Args:
        runtime: Unique execution identifier (UUID4 hex string).
        args: Optional dict of runtime arguments for targeted collection.
    """
    database = Database(MainDatabaseSettings())
    service = LoggingService("{group}.{parent}.{name}", db_engine=database.engine)
    logger = service.configure_logger()

    app_id = get_app_info(__file__, only_id=True)
    logger = logger.bind(app_id=app_id, runtime=runtime)

    update_app_status(database, app_id, run_status=RunStatus.RUNNING, runtime_id=runtime)

    scraper = None
    try:
        metrics = RequestMetrics()
        scraper = {class_name}(
            database, logger=logger, runtime=runtime, app_id=app_id, args=args, metrics=metrics,
            category_thresholds=DEFAULT_CATEGORY_THRESHOLDS,
        )
        scraper.prepare_list()
        scraper.collect()
        scraper.fatal_check()

        if scraper.should_abort:
            scraper.next_run = scraper.get_retry_next_run()
        else:
            scraper.set_next_run()

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

        scraper.metrics.log_stats(logger)
    finally:
        if scraper is not None:
            scraper.cleanup()
        service.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="{class_name} scraper")
    parser.add_argument("--args", type=json.loads, default=None, help="JSON string of runtime arguments")
    parsed = parser.parse_args()
    init(uuid.uuid4().hex, args=parsed.args)
'''

MAIN_THREADED_TEMPLATE = '''"""Multi-threaded scraper for {group}.{parent}.{name}."""

from __future__ import annotations

import argparse
import json
import uuid
from typing import Any

from data_collector.enums import RunStatus
from data_collector.scraping import DEFAULT_CATEGORY_THRESHOLDS, ThreadedScraper
from data_collector.scraping.base import update_app_status
from data_collector.settings.main import MainDatabaseSettings
from data_collector.utilities.database.main import Database
from data_collector.utilities.functions.runtime import get_app_info
from data_collector.utilities.log.main import LoggingService
from data_collector.utilities.fun_watch import fun_watch
from data_collector.utilities.request import Request, RequestMetrics


class {class_name}(ThreadedScraper):
    """{class_name} multi-threaded scraper.

    # TODO: Add description of what this scraper collects.
    """

    base_url = ""  # TODO: Set target site root URL

    def __init__(self, database: Database, **kwargs: Any) -> None:
        super().__init__(database, **kwargs)
        # TODO: Initialize parser and any app-specific state
        # self.parser = Parser()

    def prepare_list(self) -> None:
        """Query database for items to process."""
        # TODO: Fetch work items from database
        # with self.database.create_session() as session:
        #     self.work_list = ...
        # self.list_size = len(self.work_list)
        pass

    @fun_watch
    def collect(self) -> None:
        """Distribute work items across worker threads."""
        self._start_collect_timer()
        self._fun_watch.set_task_size(self.list_size)
        self.process_batch(self.work_list, self._worker)

    def _worker(self, item: Any, instance_id: int) -> None:
        """Per-thread: fetch, parse, and store one record."""
        request = self.create_worker_request()
        # TODO: Implement per-item collection
        # response = request.get(f"{{self.base_url}}/...")
        # data = self.parser.parse(response.content)
        # self.store([data])
        # self.increment_solved()
        _ = request, item, instance_id

    def create_worker_request(self) -> Request:
        """Create per-thread Request with custom configuration."""
        req = Request(timeout=120, retries=3, metrics=self.metrics)
        # TODO: Set headers, proxy, etc.
        # req.set_headers({{"User-Agent": "Mozilla/5.0"}})
        return req

    def store(self, records: list[Any]) -> None:
        """Persist collected records to database."""
        # TODO: Hash and merge records
        # bulk_hash(records)
        # with self.database.create_session() as session:
        #     self.database.merge(records, session)
        pass

    def set_next_run(self) -> None:
        """Calculate next execution time."""
        # TODO: Set self.next_run based on collection schedule
        # self.next_run = datetime.now() + timedelta(days=1)
        pass


def init(runtime: str, args: dict[str, Any] | None = None) -> None:
    """Entry point called by the Manager process.

    Args:
        runtime: Unique execution identifier (UUID4 hex string).
        args: Optional dict of runtime arguments for targeted collection.
    """
    database = Database(MainDatabaseSettings())
    service = LoggingService("{group}.{parent}.{name}", db_engine=database.engine)
    logger = service.configure_logger()

    app_id = get_app_info(__file__, only_id=True)
    logger = logger.bind(app_id=app_id, runtime=runtime)

    update_app_status(database, app_id, run_status=RunStatus.RUNNING, runtime_id=runtime)

    scraper = None
    try:
        metrics = RequestMetrics()
        scraper = {class_name}(
            database, logger=logger, runtime=runtime, app_id=app_id, args=args, metrics=metrics,
            category_thresholds=DEFAULT_CATEGORY_THRESHOLDS,
        )
        scraper.prepare_list()
        scraper.collect()
        scraper.fatal_check()

        if scraper.should_abort:
            scraper.next_run = scraper.get_retry_next_run()
        else:
            scraper.set_next_run()

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

        scraper.metrics.log_stats(logger)
    finally:
        if scraper is not None:
            scraper.cleanup()
        service.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="{class_name} scraper")
    parser.add_argument("--args", type=json.loads, default=None, help="JSON string of runtime arguments")
    parsed = parser.parse_args()
    init(uuid.uuid4().hex, args=parsed.args)
'''

MAIN_ASYNC_TEMPLATE = '''"""Async scraper for {group}.{parent}.{name}."""

from __future__ import annotations

import argparse
import asyncio
import json
import uuid
from typing import Any

from data_collector.enums import RunStatus
from data_collector.scraping import DEFAULT_CATEGORY_THRESHOLDS, AsyncScraper
from data_collector.scraping.base import update_app_status
from data_collector.settings.main import MainDatabaseSettings
from data_collector.utilities.database.main import Database
from data_collector.utilities.functions.runtime import get_app_info
from data_collector.utilities.log.main import LoggingService
from data_collector.utilities.fun_watch import fun_watch
from data_collector.utilities.request import RequestMetrics


class {class_name}(AsyncScraper):
    """{class_name} async scraper.

    # TODO: Add description of what this scraper collects.
    """

    base_url = ""  # TODO: Set target site root URL

    def __init__(self, database: Database, **kwargs: Any) -> None:
        super().__init__(database, **kwargs)
        # TODO: Initialize parser and any app-specific state
        # self.parser = Parser()

    def prepare_list(self) -> None:
        """Query database for items to process."""
        # TODO: Fetch work items from database
        # with self.database.create_session() as session:
        #     self.work_list = ...
        # self.list_size = len(self.work_list)
        pass

    @fun_watch
    async def collect(self) -> None:
        """Distribute work items across async workers."""
        self._start_collect_timer()
        self._fun_watch.set_task_size(self.list_size)
        await self.process_batch_async(self.work_list, self._worker)

    async def _worker(self, item: Any, instance_id: int) -> None:
        """Per-item async collection logic."""
        # TODO: Implement per-item collection
        # response = await self.request.async_get(f"{{self.base_url}}/...")
        # data = self.parser.parse(response.json())
        # await self.store_async([data])
        # self.increment_solved()
        _ = item, instance_id

    def store(self, records: list[Any]) -> None:
        """Persist collected records to database."""
        # TODO: Hash and merge records
        # bulk_hash(records)
        # with self.database.create_session() as session:
        #     self.database.merge(records, session)
        pass

    def set_next_run(self) -> None:
        """Calculate next execution time."""
        # TODO: Set self.next_run based on collection schedule
        # self.next_run = datetime.now() + timedelta(days=1)
        pass


def init(runtime: str, args: dict[str, Any] | None = None) -> None:
    """Entry point called by the Manager process.

    Args:
        runtime: Unique execution identifier (UUID4 hex string).
        args: Optional dict of runtime arguments for targeted collection.
    """
    database = Database(MainDatabaseSettings())
    service = LoggingService("{group}.{parent}.{name}", db_engine=database.engine)
    logger = service.configure_logger()

    app_id = get_app_info(__file__, only_id=True)
    logger = logger.bind(app_id=app_id, runtime=runtime)

    update_app_status(database, app_id, run_status=RunStatus.RUNNING, runtime_id=runtime)

    scraper = None
    try:
        metrics = RequestMetrics()
        scraper = {class_name}(
            database, logger=logger, runtime=runtime, app_id=app_id, args=args, metrics=metrics,
            category_thresholds=DEFAULT_CATEGORY_THRESHOLDS,
        )
        scraper.prepare_list()
        asyncio.run(scraper.collect())
        scraper.fatal_check()

        if scraper.should_abort:
            scraper.next_run = scraper.get_retry_next_run()
        else:
            scraper.set_next_run()

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

        scraper.metrics.log_stats(logger)
    finally:
        if scraper is not None:
            scraper.cleanup()
        service.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="{class_name} scraper")
    parser.add_argument("--args", type=json.loads, default=None, help="JSON string of runtime arguments")
    parsed = parser.parse_args()
    init(uuid.uuid4().hex, args=parsed.args)
'''

PARSER_TEMPLATE = '''"""Parser for {group}.{parent}.{name}."""

from __future__ import annotations


class Parser:
    """Parse responses from the target source.

    # TODO: Implement parsing methods for your data source.
    # Takes raw content (HTML bytes, JSON dict, XML string),
    # returns structured data (dicts or ORM objects).
    """

    def parse_list(self, content: bytes) -> list[dict[str, object]]:
        """Parse list page response.

        Args:
            content: Raw response content.

        Returns:
            List of parsed items.
        """
        # TODO: Implement list parsing
        _ = content
        return []

    def parse_detail(self, content: bytes) -> dict[str, object]:
        """Parse detail page response.

        Args:
            content: Raw response content.

        Returns:
            Parsed item dict.
        """
        # TODO: Implement detail parsing
        _ = content
        return {{}}
'''

TABLES_TEMPLATE = '''"""ORM models for {group}.{parent}.{name}."""

from sqlalchemy import Column, DateTime, String, func

from data_collector.tables.shared import Base
from data_collector.utilities.database.columns import auto_increment_column


class {class_name}Record(Base):
    """{class_name} data table.

    # TODO: Rename this class and customize columns for your data source.
    """

    __tablename__ = "{name}"

    id = auto_increment_column()
    # TODO: Add domain-specific columns here
    # example = Column(String(256))
    sha = Column(String(64), nullable=False, index=True, comment="Row hash for merge-based sync")
    archive = Column(DateTime, comment="Soft delete timestamp")
    date_created = Column(DateTime, server_default=func.now())
    date_modified = Column(DateTime, onupdate=func.now())
'''

TOPICS_TEMPLATE = '''"""Dramatiq topic and queue definitions for {group}.{parent}.{name}.

The framework auto-discovers this file at startup via ``get_topic_modules()``.
No manual registration required.
"""

from data_collector.dramatiq.topic.base import OCR_TOPIC_EXCHANGE, TopicExchangeQueue

MAIN_EXCHANGE_QUEUE = TopicExchangeQueue(
    name="dc_{name}",
    actor_name="{name}_worker",
    exchange_name=OCR_TOPIC_EXCHANGE.name,
    routing_key="ocr.new.{name}",
    actor_path="data_collector.{group}.{parent}.{name}.main",
)
"""Primary queue for {name} processing. TaskDispatcher reads this constant."""
'''

MAIN_DRAMATIQ_TEMPLATE = '''"""{class_name} Dramatiq worker for {group}.{parent}.{name}."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

import dramatiq
import structlog

from data_collector.enums import AppType
from data_collector.enums.runtime import FatalFlag, RunStatus
from data_collector.settings.main import MainDatabaseSettings
from data_collector.tables.apps import AppGroups, AppParents, Apps
from data_collector.tables.runtime import Runtime
from data_collector.utilities.database.main import Database
from data_collector.utilities.fun_watch import FunWatchMixin, fun_watch
from data_collector.utilities.functions.runtime import AppInfo, get_app_info

from .topics import MAIN_EXCHANGE_QUEUE

_APP_INFO: AppInfo = get_app_info(__file__, depth=-4)  # type: ignore[assignment]
_APP_ID: str = _APP_INFO["app_id"]
_DATABASE = Database(MainDatabaseSettings())


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
                app_type=AppType.DRAMATIQ,
            ),
            session,
            filter_cols=["group_name", "parent_name", "app_name"],
        )


_register_app(_DATABASE, _APP_INFO)


def _create_runtime() -> str:
    """Create a Runtime row for this actor invocation and return the runtime_id."""
    runtime_id = uuid.uuid4().hex
    with _DATABASE.create_session() as session:
        session.add(Runtime(
            runtime=runtime_id,
            app_id=_APP_ID,
            start_time=datetime.now(UTC),
        ))
        session.commit()
    return runtime_id


class {class_name}Processor(FunWatchMixin):
    """Processes tasks received from the {name} queue."""

    def __init__(self, app_id: str, runtime: str) -> None:
        self.app_id = app_id
        self.runtime = runtime
        self.logger = structlog.get_logger(__name__).bind(app_id=app_id, runtime=runtime)

    @fun_watch
    def run(self, event_id: int) -> None:
        """Process a single event.

        Args:
            event_id: ID of the event row from the Events table.
        """
        self.logger.info("Processing event %d", event_id)
        # TODO: Implement processing logic here
        self._fun_watch.mark_solved()


@dramatiq.actor(
    queue_name=MAIN_EXCHANGE_QUEUE.name,
    max_retries=3,
    on_retry_exhausted="log_dead_letter",
)
def {name}_worker(event_id: int) -> None:
    """Thin entry point -- delegates to the processor class."""
    runtime_id = _create_runtime()
    processor = {class_name}Processor(app_id=_APP_ID, runtime=runtime_id)
    processor.run(event_id)
'''
