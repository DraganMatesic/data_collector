import logging

class RouterHandler(logging.Handler):
    def __init__(self, handlers: list[logging.Handler], swallow_errors: bool = True):
        """
        :param handlers:
        :param swallow_errors:
                safety switch if a downstream handler (DB/Splunk) throws inside emit(),
                the router catches it, calls handleError(record)
                (so you get a traceback in dev if logging.raiseExceptions is True),
                and continues to the next handler. Your app/workers don’t crash just because logging failed.
                Set False if for testing and debugging
        """
        super().__init__()
        self.handlers = handlers
        self.swallow_errors = swallow_errors

    def emit(self, record: logging.LogRecord):
        for h in self.handlers:
            try:
                # Run h.filter + h.emit
                h.handle(record)
            except Exception:
                # Fail fast
                if not self.swallow_errors:
                    raise
                # Log/traceback depending on env, then continue
                self.handleError(record)
