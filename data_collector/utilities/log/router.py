"""Router handler that forwards log records to multiple sink handlers."""

from __future__ import annotations

import logging
import sys
import traceback
from logging.handlers import RotatingFileHandler
from types import TracebackType

from data_collector.utilities.log.processors import normalize_log_record


class RouterHandler(logging.Handler):
    """Forward each record to all configured handlers."""

    def __init__(
        self,
        handlers: list[logging.Handler],
        swallow_errors: bool = True,
        error_file: str = "error.log",
        error_max_bytes: int = 5_242_880,
        error_backup_count: int = 3,
    ) -> None:
        super().__init__()
        self.handlers = handlers
        self.swallow_errors = swallow_errors
        self._fallback = RotatingFileHandler(
            error_file,
            maxBytes=error_max_bytes,
            backupCount=error_backup_count,
        )
        self._fallback.setFormatter(logging.Formatter("%(asctime)s SINK_FAILURE %(message)s"))

    def emit(self, record: logging.LogRecord) -> None:
        """Dispatch record to child handlers while isolating sink failures."""
        for handler in self.handlers:
            try:
                handler.handle(record)
            except Exception:
                if not self.swallow_errors:
                    raise
                self._log_sink_failure(handler, record, sys.exc_info())

    def _log_sink_failure(
        self,
        handler: logging.Handler,
        record: logging.LogRecord,
        exc_info: tuple[type[BaseException] | None, BaseException | None, TracebackType | None],
    ) -> None:
        """Write sink failure details to fallback file."""
        payload = normalize_log_record(record)
        traceback_text = "".join(traceback.format_exception(*exc_info))
        failure_record = logging.LogRecord(
            name="sink_fallback",
            level=logging.ERROR,
            pathname="",
            lineno=0,
            msg="",
            args=(),
            exc_info=None,
        )
        handler_name = type(handler).__name__
        error = exc_info[1]
        failure_record.msg = (
            f"handler={handler_name} | "
            f"error={error!r} | "
            f"original_level={record.levelname} | "
            f"original_msg={record.getMessage()} | "
            f"original_payload={payload}\n"
            f"Traceback:\n{traceback_text}"
        )
        self._fallback.emit(failure_record)
