import json
import time
import logging
import requests
from data_collector.tables.log import Logs

class DatabaseHandler(logging.Handler):
    def __init__(self, engine):
        super().__init__()
        self.engine = engine

    def emit(self, record: logging.LogRecord):
        try:
            payload = {
                "ts": time.time(),
                "level": record.levelname,
                "logger": record.name,
                "message": record.getMessage(),
                **getattr(record, "extra", {})  # optional structured extras
            }
            # SQLAlchemy pseudo-code; make this non-blocking or buffered if needed
            with self.engine.begin() as conn:
                conn.execute(
                    "INSERT INTO app_logs(ts, level, logger, message, extra_json) VALUES (:ts,:level,:logger,:message,:extra)",
                    dict(ts=payload["ts"], level=payload["level"], logger=payload["logger"],
                         message=payload["message"], extra=json.dumps(payload))
                )
        except Exception:
            self.handleError(record)

class SplunkHECHandler(logging.Handler):
    def __init__(self, hec_url: str, token: str):
        super().__init__()
        self.url = hec_url.rstrip("/") + "/event"
        self.headers = {"Authorization": f"Splunk {token}"}

    def emit(self, record: logging.LogRecord):
        try:
            event = {
                "time": time.time(),
                "event": {
                    "level": record.levelname,
                    "logger": record.name,
                    "message": record.getMessage(),
                    **getattr(record, "extra", {})
                }
            }
            # Make resilient: timeouts, no raise
            requests.post(self.url, headers=self.headers, json=event, timeout=2.5)
        except Exception:
            self.handleError(record)
