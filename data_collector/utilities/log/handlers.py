"""Custom logging handlers for DB persistence and Splunk forwarding."""

from __future__ import annotations

import json
import logging
import socket
import time
from typing import Any

import requests  # type: ignore[import-untyped]
from sqlalchemy.orm import Session

from data_collector.enums import LogLevel
from data_collector.tables.log import Logs
from data_collector.utilities.log.processors import normalize_log_record, separate_fixed_context


class DatabaseHandler(logging.Handler):
    """Persist log records to a database table."""

    def __init__(self, engine: Any) -> None:
        super().__init__()
        self.engine = engine

    def emit(self, record: logging.LogRecord) -> None:
        """Insert structured payload into the `logs` table."""
        event_dict = normalize_log_record(record)
        fixed_context, context_json = separate_fixed_context(event_dict)
        event_message = str(event_dict.get("event", record.getMessage()))
        level_value = _resolve_log_level(event_dict.get("level"), record.levelno)
        overflow_context = json.dumps(context_json, default=str) if context_json else None

        entry = Logs(
            app_id=fixed_context.get("app_id"),
            module_name=fixed_context.get("module_name"),
            module_path=fixed_context.get("module_path"),
            function_name=fixed_context.get("function_name"),
            function_id=fixed_context.get("function_id"),
            call_chain=fixed_context.get("call_chain"),
            thread_id=_coerce_int(fixed_context.get("thread_id")),
            lineno=_coerce_int(fixed_context.get("lineno")),
            log_level=level_value,
            msg=event_message,
            context_json=overflow_context,
            runtime=fixed_context.get("runtime"),
        )

        with Session(self.engine) as session:
            session.add(entry)
            session.commit()


class SplunkHECHandler(logging.Handler):
    """Forward log records to Splunk HEC endpoint."""

    def __init__(
        self,
        hec_url: str,
        token: str,
        verify_tls: bool = True,
        ca_bundle: str | None = None,
        index: str = "default",
        sourcetype: str = "data_collector:structured",
    ) -> None:
        super().__init__()
        self.url = hec_url.rstrip("/") + "/event"
        self.headers = {"Authorization": f"Splunk {token}"}
        self.verify: bool | str = ca_bundle if ca_bundle else verify_tls
        self.index = index
        self.sourcetype = sourcetype
        self.host = socket.gethostname()

    def emit(self, record: logging.LogRecord) -> None:
        """Post structured event payload to Splunk HEC."""
        event_dict = normalize_log_record(record)
        payload = {
            "time": time.time(),
            "host": self.host,
            "source": event_dict.get("app_id", record.name),
            "sourcetype": self.sourcetype,
            "index": self.index,
            "event": event_dict,
        }
        response = requests.post(
            self.url,
            headers=self.headers,
            json=payload,
            timeout=2.5,
            verify=self.verify,
        )
        response.raise_for_status()


def _resolve_log_level(level_name: Any, default_level: int) -> int:
    """Resolve log level names to framework enum values."""
    if isinstance(level_name, str):
        normalized = level_name.strip().upper()
        if normalized in LogLevel.__members__:
            return int(LogLevel[normalized])
    return int(default_level)


def _coerce_int(value: Any) -> int | None:
    """Safely coerce optional values to int."""
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
