"""Dramatiq worker entry point with app registration, logging, and actor discovery.

This module is the single entry point for all Dramatiq worker processes.
Module-level code runs once per worker process before any actor executes,
ensuring that the worker is registered as a DRAMATIQ app, ``LoggingService``
is configured with ``app_id`` and ``runtime`` context, and all actor modules
are imported for Dramatiq discovery.

Initialization is split into two phases:

1. **Bootstrap** (before ``@fun_watch`` can work): Database, app registration,
   Runtime row, LoggingService.  These log entries carry ``app_id``, ``runtime``,
   ``module_name``, ``function_name`` via ``_ContextFilter``.
2. **Initialization** (with ``@fun_watch``): Broker creation, topology
   declaration, binding sync, actor discovery.  These log entries carry full
   context including ``function_id``, ``call_chain``, and ``lineno``.

Usage (development)::

    dramatiq data_collector.dramatiq.actors -p 2 -t 4

Usage (production)::

    python -m data_collector.dramatiq.service install   # Windows
    python -m data_collector.dramatiq.service start
"""

from __future__ import annotations

import copy
import importlib
import logging
import os
import threading
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import structlog  # type: ignore[import-untyped]
from sqlalchemy import select

# Workaround: Dramatiq's StreamablePipe wraps stderr for multiprocessing
# workers using send_bytes(). Python 3.12+ raises ValueError on concurrent
# send_bytes() calls. The cli_wrapper module patches this in the main
# process. This guard handles the case where actors.py is imported
# directly (tests, in-process worker) without going through cli_wrapper.
from data_collector.dramatiq.broker import DramatiqBroker
from data_collector.dramatiq.topic.base import (
    RegularQueue,
    TopicExchangeQueue,
    get_topic_modules,
)
from data_collector.enums import AppType
from data_collector.settings.dramatiq import DramatiqSettings
from data_collector.settings.main import LogSettings, MainDatabaseSettings
from data_collector.settings.rabbitmq import RabbitMQSettings
from data_collector.tables.apps import AppGroups, AppParents, Apps
from data_collector.tables.runtime import Runtime
from data_collector.utilities.database.main import Database
from data_collector.utilities.fun_watch import FunWatchMixin, fun_watch
from data_collector.utilities.functions.runtime import AppInfo, get_app_info
from data_collector.utilities.log.main import LoggingService

logger = logging.getLogger(__name__)

_APP_INFO: AppInfo = get_app_info(__file__, depth=-3)  # type: ignore[assignment]
_APP_ID: str = _APP_INFO["app_id"]


class _ContextFilter(logging.Filter):
    """Injects structured context fields into every LogRecord.

    Attached to the ``QueueHandler`` on the ``"data_collector"`` logger
    so that ``DatabaseHandler.emit()`` (which reads from
    ``record.__dict__``) receives all ``DB_FIXED_KEYS`` fields on
    every log record -- both stdlib and structlog.
    """

    def __init__(self, app_id: str, runtime_id: str) -> None:
        super().__init__()
        self._app_id = app_id
        self._runtime_id = runtime_id

    def filter(self, record: logging.LogRecord) -> bool:
        """Add structured context fields to the record if not already present."""
        if not getattr(record, "app_id", None):
            record.app_id = self._app_id  # type: ignore[attr-defined]
        if not getattr(record, "runtime", None):
            record.runtime = self._runtime_id  # type: ignore[attr-defined]
        if not getattr(record, "module_name", None):
            record.module_name = Path(record.pathname).name  # type: ignore[attr-defined]
        if not getattr(record, "module_path", None):
            record.module_path = record.pathname  # type: ignore[attr-defined]
        if not getattr(record, "function_name", None):
            record.function_name = record.funcName  # type: ignore[attr-defined]
        if not getattr(record, "thread_id", None):
            record.thread_id = record.thread  # type: ignore[attr-defined]
        return True


class _DramatiqActorErrorFilter(logging.Filter):
    """Filters out Dramatiq internal errors that duplicate @fun_watch errors.

    When an actor raises an exception, both @fun_watch and Dramatiq's
    internal logger emit an error. The @fun_watch error has richer context
    (app_id, function_id, call_chain, full traceback). This filter drops
    the Dramatiq duplicate ("Failed to process message") while keeping
    infrastructure errors (broker disconnects, worker crashes) that only
    Dramatiq can report.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        """Return False to drop actor-level error duplicates."""
        message = record.getMessage()
        return "Failed to process message" not in message


class DramatiqProcessInitializer(FunWatchMixin):
    """Initializes the Dramatiq worker process with full ``@fun_watch`` tracking.

    After bootstrap (app registration + LoggingService), broker creation,
    topology declaration, binding sync, and actor discovery all run inside
    ``@fun_watch`` scope so every log entry carries ``function_id``,
    ``call_chain``, and ``lineno``.
    """

    def __init__(self, app_id: str, runtime: str, structured_logger: Any) -> None:
        self.app_id = app_id
        self.runtime = runtime
        self.logger = structured_logger

    @fun_watch
    def initialize(self, rabbitmq_settings: RabbitMQSettings) -> None:
        """Top-level initialization -- call_chain root.

        Args:
            rabbitmq_settings: RabbitMQ connection parameters.
        """
        self.initialize_broker(rabbitmq_settings)
        self.discover_actors()

    @fun_watch
    def initialize_broker(self, rabbitmq_settings: RabbitMQSettings) -> None:
        """Create Dramatiq broker, declare exchanges, sync bindings, clean orphan queues.

        Args:
            rabbitmq_settings: RabbitMQ connection parameters.
        """
        dramatiq_settings = DramatiqSettings()
        DramatiqBroker(
            rabbitmq_settings,
            dramatiq_settings=dramatiq_settings,
            load=True,
        )
        self.logger.info("Dramatiq broker initialized")

    @fun_watch
    def discover_actors(self) -> None:
        """Import all actor modules so Dramatiq can discover @dramatiq.actor functions.

        Scans ``get_topic_modules()`` for ``TopicExchangeQueue`` and ``RegularQueue``
        instances and imports their ``actor_path`` modules.
        """
        imported_modules: set[str] = set()

        for topic_module_path in get_topic_modules():
            try:
                topic_module = importlib.import_module(topic_module_path)
            except ImportError:
                self.logger.warning("Could not import topic module", module_path=topic_module_path)
                continue

            for attribute_name in dir(topic_module):
                attribute = getattr(topic_module, attribute_name)
                if (
                    isinstance(attribute, (TopicExchangeQueue, RegularQueue))
                    and attribute.actor_path
                    and attribute.actor_path not in imported_modules
                ):
                    try:
                        importlib.import_module(attribute.actor_path)
                        imported_modules.add(attribute.actor_path)
                        self.logger.debug("Discovered actor module", actor_path=attribute.actor_path)
                    except ImportError:
                        self.logger.warning(
                            "Could not import actor module",
                            actor_path=attribute.actor_path,
                            queue_name=attribute.name,
                        )

        self.logger.info("Actor discovery complete", module_count=len(imported_modules))


# ---------------------------------------------------------------------------
# Bootstrap functions (before @fun_watch is available)
# ---------------------------------------------------------------------------

def _register_app(database: Database, runtime_id: str) -> None:
    """Register the Dramatiq worker service as a DRAMATIQ app.

    Seeds ``AppGroups``, ``AppParents``, and ``Apps`` rows if they don't
    exist, then creates a ``Runtime`` row for this process startup.
    """
    group = _APP_INFO["app_group"]
    parent = _APP_INFO["app_parent"]
    app_name = _APP_INFO["app_name"]

    with database.create_session() as session:
        if not session.execute(select(AppGroups).where(AppGroups.name == group)).scalar():
            session.add(AppGroups(name=group))
            session.flush()
        if not session.execute(
            select(AppParents).where(AppParents.name == parent, AppParents.group_name == group)
        ).scalar():
            session.add(AppParents(name=parent, group_name=group))
            session.flush()
        session.merge(Apps(
            app=_APP_ID,
            group_name=group,
            parent_name=parent,
            app_name=app_name,
            app_type=AppType.DRAMATIQ,
        ))
        session.merge(Runtime(
            runtime=runtime_id,
            app_id=_APP_ID,
            start_time=datetime.now(UTC),
        ))
        session.commit()


def _bootstrap_logging(database: Database, runtime_id: str) -> None:
    """Configure LoggingService with app_id and runtime context.

    This is a bootstrap step that runs before ``@fun_watch`` is available.
    After this, all ``logging.getLogger()`` calls carry ``app_id`` and
    ``runtime`` via the ``_ContextFilter``.
    """
    log_settings = LogSettings()

    # Workers use a separate log file to avoid colliding with the Manager's error.log.
    # The validator already resolved directory -> error.log, so replace the filename.
    error_file_path = Path(log_settings.log_error_file)
    log_settings.log_error_file = str(error_file_path.parent / "pipeline_error.log")

    service = LoggingService(
        "data_collector",
        settings=log_settings,
        db_engine=database.engine,
    )
    service.configure_logger()

    structlog.contextvars.bind_contextvars(app_id=_APP_ID, runtime=runtime_id)

    context_filter = _ContextFilter(_APP_ID, runtime_id)
    data_collector_logger = logging.getLogger("data_collector")
    for handler in data_collector_logger.handlers:
        handler.addFilter(context_filter)

    # Attach the same handlers to the "dramatiq" logger so that Dramatiq
    # internal errors (worker crashes, broker disconnects, unexpected exits)
    # flow to DB and Splunk for alerting. Actor-level errors are filtered
    # out because @fun_watch already captures those with richer context.
    dramatiq_logger = logging.getLogger("dramatiq")
    dramatiq_logger.setLevel(logging.ERROR)
    dramatiq_logger.propagate = False
    actor_error_filter = _DramatiqActorErrorFilter()
    for handler in data_collector_logger.handlers:
        filtered_handler = copy.copy(handler)
        filtered_handler.addFilter(actor_error_filter)
        dramatiq_logger.addHandler(filtered_handler)


# ---------------------------------------------------------------------------
# Module-level initialization -- runs once per Dramatiq worker process
# ---------------------------------------------------------------------------

class _InitializationGuard:
    """Thread-safe once-only initialization guard.

    Replaces a ``global`` flag with a class-level ``threading.Event``.
    """

    _done = threading.Event()

    @classmethod
    def initialize(cls) -> None:
        """Run full worker process initialization.

        Phase 1 (bootstrap): app registration, Runtime row, LoggingService.
        Phase 2 (@fun_watch): broker init, topology sync, actor discovery.
        Guarded to run only once via ``threading.Event``.
        """
        if cls._done.is_set():
            return
        cls._done.set()

        runtime_id = uuid.uuid4().hex
        database = Database(MainDatabaseSettings())

        # Phase 1: Bootstrap (no @fun_watch yet)
        _register_app(database, runtime_id)
        _bootstrap_logging(database, runtime_id)

        # Phase 2: Initialization (with @fun_watch)
        structured_logger = structlog.get_logger(__name__).bind(app_id=_APP_ID, runtime=runtime_id)
        initializer = DramatiqProcessInitializer(
            app_id=_APP_ID,
            runtime=runtime_id,
            structured_logger=structured_logger,
        )
        initializer.initialize(RabbitMQSettings())

    @classmethod
    def reset(cls) -> None:
        """Reset the guard for testing purposes."""
        cls._done.clear()


# Auto-initialize when imported by the Dramatiq CLI worker process.
# Skipped when the _DC_SKIP_ACTOR_INIT environment variable is set (testing).
if not os.environ.get("_DC_SKIP_ACTOR_INIT"):
    _InitializationGuard.initialize()
