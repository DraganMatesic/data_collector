from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

from data_collector.utilities.log.processors import (
    STRUCTLOG_INTERNAL_MODULE_PREFIXES,
    extract_caller_info,
    limit_context_size,
    normalize_log_record,
    separate_fixed_context,
)

Processor = Callable[[Any, str, dict[str, Any]], dict[str, Any]]


def _log_from_target(processor: Processor) -> dict[str, Any]:
    return processor(None, "info", {"event": "hello"})


def test_extract_caller_info_populates_caller_fields() -> None:
    event = _log_from_target(extract_caller_info())
    assert event["module_name"] == "test_processors.py"
    assert str(event["module_path"]).endswith("test_processors.py")
    assert event["function_name"] == "_log_from_target"
    assert isinstance(event["lineno"], int)
    assert "call_chain" not in event


def test_limit_context_size_preserves_fixed_keys() -> None:
    payload = {
        "event": "processed",
        "level": "info",
        "app_id": "app_hash",
        "runtime": "runtime_hash",
        "function_id": "fn_hash",
        "extra_c": 3,
        "extra_a": 1,
        "extra_b": 2,
    }
    processor = limit_context_size(2)
    result = processor(None, "info", payload)

    assert result["app_id"] == "app_hash"
    assert result["runtime"] == "runtime_hash"
    assert result["function_id"] == "fn_hash"
    assert "extra_a" in result
    assert "extra_b" in result
    assert "extra_c" not in result


def test_separate_fixed_context_splits_payload_for_db_mapping() -> None:
    payload = {
        "app_id": "app_hash",
        "runtime": "runtime_hash",
        "function_id": "function_hash",
        "module_name": "module.py",
        "module_path": "/app/module.py",
        "function_name": "run",
        "lineno": 42,
        "call_chain": "main -> run",
        "thread_id": 140234567890,
        "event": "Completed",
        "level": "info",
        "batch_size": 500,
    }
    fixed, context = separate_fixed_context(payload)

    assert fixed["app_id"] == "app_hash"
    assert fixed["runtime"] == "runtime_hash"
    assert fixed["function_id"] == "function_hash"
    assert fixed["lineno"] == 42
    assert fixed["thread_id"] == 140234567890
    assert "batch_size" not in fixed
    assert "thread_id" not in context
    assert context == {"batch_size": 500}


def test_normalize_log_record_with_structured_message() -> None:
    record = logging.LogRecord(
        name="tests.logger",
        level=logging.INFO,
        pathname=__file__,
        lineno=10,
        msg={"event": "Structured event", "app_id": "app_hash", "runtime": "runtime_hash"},
        args=(),
        exc_info=None,
    )
    event_dict = normalize_log_record(record)

    assert event_dict["event"] == "Structured event"
    assert event_dict["app_id"] == "app_hash"
    assert event_dict["runtime"] == "runtime_hash"
    assert event_dict["level"] == "info"
    assert event_dict["logger"] == "tests.logger"


def test_normalize_log_record_with_string_message_and_args() -> None:
    record = logging.LogRecord(
        name="tests.logger",
        level=logging.WARNING,
        pathname=__file__,
        lineno=20,
        msg="Batch %s finished",
        args=("A",),
        exc_info=None,
    )
    record.batch_size = 120

    event_dict = normalize_log_record(record)

    assert event_dict["event"] == "Batch A finished"
    assert event_dict["batch_size"] == 120
    assert event_dict["level"] == "warning"
    assert event_dict["logger"] == "tests.logger"


def test_internal_module_prefixes_include_fun_watch() -> None:
    assert "data_collector.utilities.fun_watch" in STRUCTLOG_INTERNAL_MODULE_PREFIXES


def test_internal_module_prefixes_include_bootstrap_modules() -> None:
    assert "runpy" in STRUCTLOG_INTERNAL_MODULE_PREFIXES
    assert "importlib" in STRUCTLOG_INTERNAL_MODULE_PREFIXES
    assert "_frozen_importlib" in STRUCTLOG_INTERNAL_MODULE_PREFIXES


def test_extract_caller_info_does_not_set_call_chain() -> None:
    event = _log_from_target(extract_caller_info())
    assert "call_chain" not in event
