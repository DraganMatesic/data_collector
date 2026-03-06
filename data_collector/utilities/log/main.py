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
        """Configure queue-driven logger and return a structlog BoundLogger."""
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
            self.log_listener.stop()
