"""Structlog processors and record normalization helpers."""

from __future__ import annotations

import inspect
import logging
import threading
from collections.abc import Callable
from pathlib import Path
from types import FrameType
from typing import Any, cast

STRUCTLOG_INTERNAL_MODULE_PREFIXES: tuple[str, ...] = (
    "logging",
    "structlog",
    "queue",
    "threading",
    "concurrent.futures",
    "data_collector.utilities.log",
    "data_collector.utilities.fun_watch",
    "runpy",
    "importlib",
    "_frozen_importlib",
)

DB_FIXED_KEYS: frozenset[str] = frozenset(
    {
        "app_id",
        "runtime",
        "function_id",
        "module_name",
        "module_path",
        "function_name",
        "lineno",
        "call_chain",
        "thread_id",
    }
)

FIXED_CONTEXT_KEYS: frozenset[str] = frozenset(
    {
        *DB_FIXED_KEYS,
        "event",
        "level",
        "logger",
        "timestamp",
        "exception",
        "exc_info",
        "stack",
    }
)

_STANDARD_LOG_RECORD_KEYS: frozenset[str] = frozenset(
    logging.LogRecord(
        name="",
        level=logging.NOTSET,
        pathname="",
        lineno=0,
        msg="",
        args=(),
        exc_info=None,
    ).__dict__.keys()
)

Processor = Callable[[Any, str, dict[str, Any]], dict[str, Any]]


def _is_internal_frame(frame: FrameType) -> bool:
    """Return True when frame belongs to logging internals."""
    module_name = str(frame.f_globals.get("__name__", ""))
    return module_name.startswith(STRUCTLOG_INTERNAL_MODULE_PREFIXES)


def extract_caller_info() -> Processor:
    """Add caller metadata (module, function, line) to event dict.

    Finds the first non-internal frame and extracts its metadata.
    Does NOT build ``call_chain`` — that is exclusively set by ``@fun_watch``
    via ``structlog.contextvars.bind_contextvars()``.
    """

    def processor(_logger: Any, _method_name: str, event_dict: dict[str, Any]) -> dict[str, Any]:
        frame = inspect.currentframe()
        if frame is None:
            return event_dict

        frame = frame.f_back
        caller_frame: FrameType | None = None

        try:
            while frame is not None:
                if not _is_internal_frame(frame):
                    caller_frame = frame
                    break
                frame = frame.f_back
        finally:
            del frame

        if caller_frame is None:
            return event_dict

        module_path = caller_frame.f_code.co_filename
        event_dict.setdefault("module_name", Path(module_path).name)
        event_dict.setdefault("module_path", module_path)
        event_dict.setdefault("function_name", caller_frame.f_code.co_name)
        event_dict.setdefault("lineno", caller_frame.f_lineno)
        event_dict.setdefault("thread_id", threading.get_ident())

        return event_dict

    return processor


def limit_context_size(max_keys: int) -> Processor:
    """Limit arbitrary context keys while preserving fixed keys."""

    def processor(_logger: Any, _method_name: str, event_dict: dict[str, Any]) -> dict[str, Any]:
        if max_keys < 0:
            return event_dict

        arbitrary_keys = sorted(key for key in event_dict if key not in FIXED_CONTEXT_KEYS)
        if len(arbitrary_keys) <= max_keys:
            return event_dict

        for key in arbitrary_keys[max_keys:]:
            event_dict.pop(key, None)

        return event_dict

    return processor


def separate_fixed_context(event_dict: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    """Split event payload into fixed DB columns and overflow context."""
    fixed_context = {key: event_dict[key] for key in DB_FIXED_KEYS if key in event_dict}
    context_json = {
        key: value
        for key, value in event_dict.items()
        if key not in DB_FIXED_KEYS and key not in {"event", "level"}
    }
    return fixed_context, context_json


def normalize_log_record(record: logging.LogRecord) -> dict[str, Any]:
    """Normalize stdlib/structlog records to a structured event dict."""
    message = record.msg
    if isinstance(message, dict):
        event_dict = {str(key): value for key, value in cast(dict[Any, Any], message).items()}
    else:
        event_dict = {"event": record.getMessage()}

    extra_values = {
        key: value
        for key, value in record.__dict__.items()
        if key not in _STANDARD_LOG_RECORD_KEYS and key not in {"_logger", "_name"}
    }

    for key, value in extra_values.items():
        event_dict.setdefault(key, value)

    event_dict.pop("_record", None)
    event_dict.pop("_from_structlog", None)

    event_dict.setdefault("event", record.getMessage())
    level_name = str(event_dict.get("level", record.levelname)).lower()
    event_dict["level"] = level_name
    event_dict.setdefault("logger", record.name)
    return event_dict
