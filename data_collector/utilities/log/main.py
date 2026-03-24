"""Queue-based structlog service with optional DB and Splunk sinks."""

from __future__ import annotations

import logging
import threading
from logging.handlers import QueueHandler, QueueListener
from queue import Queue
from typing import Any, cast

import structlog  # type: ignore[import-untyped]
from structlog.stdlib import BoundLogger, ProcessorFormatter  # type: ignore[import-untyped]

from data_collector.settings.main import LogSettings
from data_collector.utilities.log.handlers import DatabaseHandler, SplunkHECHandler
from data_collector.utilities.log.processors import extract_caller_info, limit_context_size
from data_collector.utilities.log.router import RouterHandler


def _build_pre_chain(settings: LogSettings) -> list[Any]:
    """Build the shared structlog processor chain.

    Used by both ``_StructlogConfigurator.configure`` (structlog global config)
    and ``LoggingService._build_console_formatter`` (foreign record pre-chain).
    """
    return [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        extract_caller_info(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.stdlib.add_logger_name,
        limit_context_size(settings.log_context_max_keys),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]


class _StructlogConfigurator:
    """Process-wide structlog configuration singleton."""

    _instance: _StructlogConfigurator | None = None
    _instance_lock = threading.Lock()

    def __init__(self) -> None:
        self._configured = False
        self._lock = threading.Lock()

    @classmethod
    def instance(cls) -> _StructlogConfigurator:
        """Return singleton configurator instance."""
        if cls._instance is None:
            with cls._instance_lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    def configure(self, settings: LogSettings) -> None:
        """Configure structlog once; first service settings win process-wide."""
        if self._configured:
            return

        with self._lock:
            if self._configured:
                return

            pre_chain = _build_pre_chain(settings)

            structlog.configure(
                processors=cast(Any, [*pre_chain, ProcessorFormatter.wrap_for_formatter]),
                context_class=dict,
                logger_factory=structlog.stdlib.LoggerFactory(),
                wrapper_class=structlog.stdlib.BoundLogger,
                cache_logger_on_first_use=True,
            )
            self._configured = True


class _RawQueueHandler(QueueHandler):
    """QueueHandler that preserves structured records."""

    def prepare(self, record: logging.LogRecord) -> logging.LogRecord:
        """Return the unmodified record so listeners receive structured payloads."""
        return record


class _StructlogContextFilter(logging.Filter):
    """Bridge structlog contextvars to stdlib LogRecords.

    Reads the current structlog contextvar store and injects any bound
    values onto the LogRecord's ``__dict__``.  This allows library modules
    using ``logging.getLogger(__name__)`` to automatically carry the same
    context as structlog-native loggers when called from within
    ``@fun_watch``-decorated code.

    Only sets attributes that are not already present on the record,
    preserving any values set by the emitting code.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        """Inject structlog context variables onto the log record."""
        context = structlog.contextvars.get_contextvars()
        for key, value in context.items():
            if getattr(record, key, None) is None:
                setattr(record, key, value)
        return True


class _RequiredContextFilter(logging.Filter):
    """Block log records missing required tracing context from persistence sinks.

    Only fully-traceable records reach ``DatabaseHandler`` and ``SplunkHECHandler``.
    The required fields (``app_id``, ``runtime``, ``function_id``, ``call_chain``)
    are exclusively provided by ``@fun_watch`` -- no manual binding path is
    supported.  Records without these fields still reach console output for
    development use.
    """

    REQUIRED_FIELDS: frozenset[str] = frozenset({
        "app_id", "runtime", "function_id", "call_chain",
    })

    def filter(self, record: logging.LogRecord) -> bool:
        """Return True only when all required context fields are present, non-None, and non-empty.

        Checks three sources (in priority order):

        1. LogRecord attributes -- set by ``_StructlogContextFilter`` for stdlib
           loggers or by explicit ``setattr`` calls.
        2. ``record.msg`` dict -- structlog-native loggers store their event dict
           (including contextvars merged by ``merge_contextvars``) as the record
           message before it reaches the QueueHandler.
        3. Structlog contextvar store -- fallback for same-thread callers where
           contextvars haven't been merged onto the record yet.
        """
        # For structlog records, record.msg is a dict with merged contextvars
        message = record.msg
        structlog_event: dict[str, object] = (
            cast(dict[str, object], message) if isinstance(message, dict) else {}
        )
        context = structlog.contextvars.get_contextvars()
        return all(
            getattr(record, field, None) or structlog_event.get(field) or context.get(field)
            for field in self.REQUIRED_FIELDS
        )


class LoggingService:
    """Configure logger + queue listener + sink routing in one place."""

    def __init__(
        self,
        logger_name: str,
        settings: LogSettings | None = None,
        db_engine: Any | None = None,
        log_level: int | None = None,
    ) -> None:
        self.db_engine = db_engine
        self.settings: LogSettings = settings if settings else LogSettings()
        self.propagate: bool = False
        self.sinks: list[logging.Handler] = []

        self.logger_name: str = logger_name
        self.logger = logging.getLogger(self.logger_name)
        self.logger_level: int = self.settings.log_level if log_level is None else log_level

        self.log_queue: Queue[logging.LogRecord] | None = None
        self.log_listener: QueueListener | None = None
        self.debug: bool = False

    def set_propagate(self, propagate: bool) -> None:
        """Set logger propagation behavior."""
        self.propagate = propagate

    def append_sink(self, sink: logging.Handler) -> None:
        """Append a sink handler routed by `RouterHandler`."""
        self.sinks.append(sink)

    def _build_console_formatter(self) -> ProcessorFormatter:
        """Create ProcessorFormatter with renderer selected by `log_format`."""
        renderer: Any
        if self.settings.log_format == "json":
            renderer = structlog.processors.JSONRenderer()
        else:
            renderer = structlog.dev.ConsoleRenderer()

        foreign_pre_chain = _build_pre_chain(self.settings)
        return ProcessorFormatter(
            foreign_pre_chain=cast(Any, foreign_pre_chain),
            processors=[
                ProcessorFormatter.remove_processors_meta,
                renderer,
            ],
        )

    def configure_logger(self) -> BoundLogger:
        """Configure queue-driven logger and return a structlog BoundLogger.

        Database and Splunk persistence requires full tracing context provided
        exclusively by ``@fun_watch``.  The required fields (``app_id``,
        ``runtime``, ``function_id``, ``call_chain``) are bound to structlog
        contextvars by the framework's execution infrastructure:

            - ``app_id`` and ``runtime``: bound at process startup by Manager
              or Dramatiq worker bootstrap.
            - ``function_id`` and ``call_chain``: bound per-invocation by
              ``@fun_watch``.

        Library modules using ``logging.getLogger(__name__)`` automatically
        inherit this context when called from within ``@fun_watch``-decorated
        code.  Their log records are persisted with full traceability.

        Log messages emitted outside ``@fun_watch`` context appear on console
        only and are not persisted to the Logs table or forwarded to Splunk.
        This is by design -- untraceable records are not permitted in
        persistence sinks.
        """
        _StructlogConfigurator.instance().configure(self.settings)

        self.logger.setLevel(self.logger_level)
        self.logger.propagate = self.propagate

        if self.logger.handlers:
            return cast(BoundLogger, structlog.get_logger(self.logger_name).bind())

        if not self.debug:
            if self.settings.log_to_db and self.db_engine:
                self.append_sink(DatabaseHandler(self.db_engine))
            if self.settings.log_to_splunk and self.settings.splunk_hec_url and self.settings.splunk_token:
                self.append_sink(
                    SplunkHECHandler(
                        self.settings.splunk_hec_url,
                        self.settings.splunk_token,
                        verify_tls=self.settings.splunk_verify_tls,
                        ca_bundle=self.settings.splunk_ca_bundle,
                        index=self.settings.splunk_index,
                        sourcetype=self.settings.splunk_sourcetype,
                    )
                )

        # Attach data quality gate to persistence sinks -- only records with
        # full @fun_watch context (app_id, runtime, function_id, call_chain)
        # are persisted.  Console output is unrestricted.
        # Attach data quality gate to persistence sinks -- only records with
        # full @fun_watch context (app_id, runtime, function_id, call_chain)
        # are persisted.  Console output is unrestricted.
        # The TypeError guard handles test mocking where SplunkHECHandler is
        # replaced by MagicMock (not a valid type for isinstance).
        required_context_filter = _RequiredContextFilter()
        for sink in self.sinks:
            try:
                is_persistence_sink = isinstance(sink, (DatabaseHandler, SplunkHECHandler))
            except TypeError:
                is_persistence_sink = False
            if is_persistence_sink:
                sink.addFilter(required_context_filter)

        console = logging.StreamHandler()
        console.setLevel(self.logger_level)
        console.setFormatter(self._build_console_formatter())
        self.sinks.append(console)

        router = RouterHandler(
            self.sinks,
            error_file=self.settings.log_error_file,
            error_max_bytes=self.settings.log_error_max_bytes,
            error_backup_count=self.settings.log_error_backup_count,
        )
        self.log_queue = self.log_queue or Queue(maxsize=self.settings.log_max_queue)

        if not self.log_listener:
            self.log_listener = QueueListener(self.log_queue, router, respect_handler_level=True)

        if not any(
            isinstance(handler, _RawQueueHandler) and getattr(handler, "queue", None) is self.log_queue
            for handler in self.logger.handlers
        ):
            self.logger.addHandler(_RawQueueHandler(self.log_queue))

        # Configure the "data_collector" parent logger so that all framework
        # library modules using logging.getLogger(__name__) inherit handlers
        # through Python's logger hierarchy propagation.  Without this, only
        # the app-specific logger receives messages; library loggers like
        # "data_collector.processing.pdf" have no handlers and their
        # INFO/DEBUG messages are silently dropped.
        framework_logger = logging.getLogger("data_collector")
        if not any(
            isinstance(handler, _RawQueueHandler) and getattr(handler, "queue", None) is self.log_queue
            for handler in framework_logger.handlers
        ):
            framework_handler = _RawQueueHandler(self.log_queue)
            framework_handler.addFilter(_StructlogContextFilter())
            framework_logger.addHandler(framework_handler)
            framework_logger.setLevel(min(framework_logger.level or logging.WARNING, self.logger_level))

        self.start()
        return cast(BoundLogger, structlog.get_logger(self.logger_name).bind())

    def start(self) -> None:
        """Start queue listener when not already alive."""
        if self.log_listener is None:
            return

        thread = getattr(self.log_listener, "_thread", None)
        if thread is None or not thread.is_alive():
            self.log_listener.start()

    def stop(self) -> None:
        """Stop queue listener if started."""
        if self.log_listener:
            thread = getattr(self.log_listener, "_thread", None)
            if thread is not None and thread.is_alive():
                self.log_listener.stop()
