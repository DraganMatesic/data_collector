import logging
from queue import Queue
from typing import Optional, List
from logging.handlers import QueueHandler, QueueListener

from data_collector.settings.main import LogSettings
from data_collector.utilities.log.router import RouterHandler
from data_collector.utilities.log.handlers import DatabaseHandler, SplunkHECHandler


class LoggingService:
    def __init__(self, logger_name: str, settings: Optional[LogSettings]=None, db_engine=None, log_level=logging.DEBUG):

        self.db_engine = db_engine
        self.settings: LogSettings = settings if settings else LogSettings()
        self.propagate = False
        self.sinks: List = []

        self.logger_name: str = logger_name
        self.logger = logging.getLogger(self.logger_name)
        self.logger_level = log_level

        self.log_queue: Optional[Queue] = None
        self.log_listener: Optional[QueueListener] = None
        self.debug: bool = False

    def set_propagate(self, propagate: bool):
        self.propagate = propagate

    def append_sink(self, sink:logging.Handler):
        self.sinks.append(sink)

    def configure_logger(self) -> logging.Logger:
        # Set logging level
        self.logger.setLevel(self.logger_level)

        # Record is handled only by the handlers you attached to this logger
        self.logger.propagate = self.propagate

        # Idempotence guard — if handlers are already attached, return to avoid duplicates
        if self.logger.handlers:
            return self.logger

        # Append sinks that will be forwarded to router
        if not self.debug:
            if self.settings.log_to_db and self.db_engine:
                self.append_sink(DatabaseHandler(self.db_engine))
            if self.settings.log_to_splunk and self.settings.splunk_hec_url and self.settings.splunk_token:
                self.append_sink(SplunkHECHandler(self.settings.splunk_hec_url, self.settings.splunk_token))

        # Always keep console in dev
        console = logging.StreamHandler()
        console.setLevel(self.logger_level)
        console.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(name)s %(message)s'))
        self.sinks.append(console)

        # routing sinks to their handler
        router = RouterHandler(self.sinks)

        # Decouple app from IO: QueueHandler -> QueueListener(router)
        self.log_queue = self.log_queue or Queue(maxsize=self.settings.log_max_queue)
        if not self.log_listener:
            self.log_listener = QueueListener(self.log_queue, router, respect_handler_level=True)

        # Only add a QueueHandler if it’s not already wired to the same queue
        if not any(isinstance(h, QueueHandler) and getattr(h, "queue", None) is self.log_queue
                   for h in self.logger.handlers):
            self.logger.addHandler(QueueHandler(self.log_queue))

        self.start()

        return self.logger

    def start(self):
        if not self.log_listener._thread or not self.log_listener._thread.is_alive():
            self.log_listener.start()

    def stop(self):
        if self.log_listener:
            self.log_listener.stop()
