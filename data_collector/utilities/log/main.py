"""Queue-based logging service with optional DB and Splunk sinks."""


from __future__ import annotations

import logging
from logging.handlers import QueueHandler, QueueListener
from queue import Queue
from typing import Any

from data_collector.settings.main import LogSettings
from data_collector.utilities.log.handlers import DatabaseHandler, SplunkHECHandler
from data_collector.utilities.log.router import RouterHandler


class LoggingService:
    """Configure logger + queue listener + sink routing in one place."""

    def __init__(
        self,
        logger_name: str,
        settings: LogSettings | None = None,
        db_engine: Any | None = None,
        log_level: int = logging.DEBUG,
    ) -> None:
        self.db_engine = db_engine
        self.settings: LogSettings = settings if settings else LogSettings()
        self.propagate: bool = False
        self.sinks: list[logging.Handler] = []

        self.logger_name: str = logger_name
        self.logger = logging.getLogger(self.logger_name)
        self.logger_level: int = log_level

        self.log_queue: Queue[logging.LogRecord] | None = None
        self.log_listener: QueueListener | None = None
        self.debug: bool = False

    def set_propagate(self, propagate: bool) -> None:
        """Set logger propagation behavior."""
        self.propagate = propagate

    def append_sink(self, sink: logging.Handler) -> None:
        """Append a sink handler routed by `RouterHandler`."""
        self.sinks.append(sink)

    def configure_logger(self) -> logging.Logger:
        """Configure queue-driven logger and return it."""
        self.logger.setLevel(self.logger_level)
        self.logger.propagate = self.propagate

        if self.logger.handlers:
            return self.logger

        if not self.debug:
            if self.settings.log_to_db and self.db_engine:
                self.append_sink(DatabaseHandler(self.db_engine))
            if self.settings.log_to_splunk and self.settings.splunk_hec_url and self.settings.splunk_token:
                self.append_sink(SplunkHECHandler(self.settings.splunk_hec_url, self.settings.splunk_token))

        console = logging.StreamHandler()
        console.setLevel(self.logger_level)
        console.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
        self.sinks.append(console)

        router = RouterHandler(self.sinks)
        self.log_queue = self.log_queue or Queue(maxsize=self.settings.log_max_queue)

        if not self.log_listener:
            self.log_listener = QueueListener(self.log_queue, router, respect_handler_level=True)

        if not any(
            isinstance(handler, QueueHandler) and getattr(handler, "queue", None) is self.log_queue
            for handler in self.logger.handlers
        ):
            self.logger.addHandler(QueueHandler(self.log_queue))

        self.start()
        return self.logger

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
