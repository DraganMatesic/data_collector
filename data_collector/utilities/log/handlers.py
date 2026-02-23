"""Custom logging handlers for DB persistence and Splunk forwarding."""

from __future__ import annotations

import json
import logging
import time
from typing import Any

import requests  # type: ignore[import-untyped]


class DatabaseHandler(logging.Handler):
    """Persist log records to a database table."""

    def __init__(self, engine: Any) -> None:
        super().__init__()
        self.engine = engine

    def emit(self, record: logging.LogRecord) -> None:
        """Insert log payload into DB, swallowing sink errors."""
        try:
            payload = {
                "ts": time.time(),
                "level": record.levelname,
                "logger": record.name,
                "message": record.getMessage(),
                **getattr(record, "extra", {}),
            }
            with self.engine.begin() as conn:
                sql = (
                    "INSERT INTO app_logs(ts, level, logger, message, extra_json)"
                    " VALUES (:ts,:level,:logger,:message,:extra)"
                )
                conn.execute(
                    sql,
                    {
                        "ts": payload["ts"],
                        "level": payload["level"],
                        "logger": payload["logger"],
                        "message": payload["message"],
                        "extra": json.dumps(payload),
                    },
                )
        except Exception:
            self.handleError(record)


class SplunkHECHandler(logging.Handler):
    """Forward log records to Splunk HEC endpoint."""

    def __init__(self, hec_url: str, token: str) -> None:
        super().__init__()
        self.url = hec_url.rstrip("/") + "/event"
        self.headers = {"Authorization": f"Splunk {token}"}

    def emit(self, record: logging.LogRecord) -> None:
        """Post event payload to Splunk HEC, swallowing sink errors."""
        try:
            event = {
                "time": time.time(),
                "event": {
                    "level": record.levelname,
                    "logger": record.name,
                    "message": record.getMessage(),
                    **getattr(record, "extra", {}),
                },
            }
            requests.post(self.url, headers=self.headers, json=event, timeout=2.5)
        except Exception:
            self.handleError(record)
