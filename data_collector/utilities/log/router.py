"""Router handler that forwards log records to multiple sink handlers."""

from __future__ import annotations

import logging


class RouterHandler(logging.Handler):
    """Forward each record to all configured handlers."""

    def __init__(self, handlers: list[logging.Handler], swallow_errors: bool = True) -> None:
        super().__init__()
        self.handlers = handlers
        self.swallow_errors = swallow_errors

    def emit(self, record: logging.LogRecord) -> None:
        """Dispatch record to child handlers while isolating sink failures."""
        for handler in self.handlers:
            try:
                handler.handle(record)
            except Exception:
                if not self.swallow_errors:
                    raise
                self.handleError(record)
