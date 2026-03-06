from __future__ import annotations

import json
import logging
from typing import Any, cast
from unittest.mock import MagicMock, patch

import pytest
import requests

from data_collector.enums import LogLevel
from data_collector.tables.log import Logs
from data_collector.utilities.log.handlers import DatabaseHandler, SplunkHECHandler


def _make_structured_record() -> logging.LogRecord:
    return logging.LogRecord(
        name="tests.handler",
        level=logging.WARNING,
        pathname=__file__,
        lineno=25,
        msg={
            "event": "Record failed",
            "level": "warning",
            "app_id": "app_hash",
            "runtime": "runtime_hash",
            "function_id": "function_hash",
            "module_name": "module.py",
            "module_path": "/app/module.py",
            "function_name": "collect",
            "lineno": 55,
            "call_chain": "main -> collect",
            "thread_id": 140234567890,
            "record_id": 42,
            "source": "api",
        },
        args=(),
        exc_info=None,
    )


@patch("data_collector.utilities.log.handlers.Session")
def test_database_handler_maps_structured_payload_to_logs_model(mock_session: MagicMock) -> None:
    session = MagicMock()
    context_manager = MagicMock()
    context_manager.__enter__.return_value = session
    context_manager.__exit__.return_value = False
    mock_session.return_value = context_manager

    handler = DatabaseHandler(engine=object())
    handler.emit(_make_structured_record())

    assert session.add.called
    assert session.commit.called
    inserted = session.add.call_args.args[0]
    assert isinstance(inserted, Logs)
    inserted_data = cast(dict[str, Any], inserted.__dict__)
    assert inserted_data["app_id"] == "app_hash"
    assert inserted_data["runtime"] == "runtime_hash"
    assert inserted_data["function_id"] == "function_hash"
    assert inserted_data["msg"] == "Record failed"
    assert inserted_data["thread_id"] == 140234567890
    assert inserted_data["log_level"] == int(LogLevel.WARNING)
    assert inserted_data["context_json"] is not None

    context_json = json.loads(cast(str, inserted_data["context_json"]))
    assert context_json["record_id"] == 42
    assert context_json["source"] == "api"
    assert context_json["logger"] == "tests.handler"


@patch("data_collector.utilities.log.handlers.Session")
def test_database_handler_propagates_sink_errors(mock_session: MagicMock) -> None:
    session = MagicMock()
    session.add.side_effect = RuntimeError("db unavailable")
    context_manager = MagicMock()
    context_manager.__enter__.return_value = session
    context_manager.__exit__.return_value = False
    mock_session.return_value = context_manager

    handler = DatabaseHandler(engine=object())
    with pytest.raises(RuntimeError, match="db unavailable"):
        handler.emit(_make_structured_record())


@patch("data_collector.utilities.log.handlers.requests.post")
def test_splunk_handler_posts_structured_payload(mock_post: MagicMock) -> None:
    response = MagicMock()
    mock_post.return_value = response

    handler = SplunkHECHandler("https://splunk.local/services/collector", "token")
    handler.emit(_make_structured_record())

    assert mock_post.called
    assert mock_post.call_args.args[0].endswith("/event")
    assert mock_post.call_args.kwargs["headers"]["Authorization"] == "Splunk token"
    assert mock_post.call_args.kwargs["timeout"] == 2.5
    assert mock_post.call_args.kwargs["verify"] is True
    payload = mock_post.call_args.kwargs["json"]
    assert payload["event"]["event"] == "Record failed"
    assert payload["event"]["record_id"] == 42
    assert payload["source"] == "app_hash"
    assert payload["sourcetype"] == "data_collector:structured"
    assert payload["index"] == "default"
    assert isinstance(payload["host"], str)
    assert len(payload["host"]) > 0
    response.raise_for_status.assert_called_once()


@patch("data_collector.utilities.log.handlers.requests.post")
def test_splunk_handler_uses_custom_index_and_sourcetype(mock_post: MagicMock) -> None:
    response = MagicMock()
    mock_post.return_value = response

    handler = SplunkHECHandler(
        "https://splunk.local/services/collector", "token",
        index="my_index", sourcetype="my_sourcetype",
    )
    handler.emit(_make_structured_record())

    payload = mock_post.call_args.kwargs["json"]
    assert payload["index"] == "my_index"
    assert payload["sourcetype"] == "my_sourcetype"


@patch("data_collector.utilities.log.handlers.requests.post")
def test_splunk_handler_source_falls_back_to_logger_name(mock_post: MagicMock) -> None:
    response = MagicMock()
    mock_post.return_value = response

    record = logging.LogRecord(
        name="fallback.logger",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg={"event": "simple event", "level": "info"},
        args=(),
        exc_info=None,
    )
    handler = SplunkHECHandler("https://splunk.local/services/collector", "token")
    handler.emit(record)

    payload = mock_post.call_args.kwargs["json"]
    assert payload["source"] == "fallback.logger"


@patch("data_collector.utilities.log.handlers.requests.post")
def test_splunk_handler_respects_verify_tls_setting(mock_post: MagicMock) -> None:
    response = MagicMock()
    mock_post.return_value = response

    handler = SplunkHECHandler("https://splunk.local/services/collector", "token", verify_tls=False)
    handler.emit(_make_structured_record())

    assert mock_post.call_args.kwargs["verify"] is False
    response.raise_for_status.assert_called_once()


@patch("data_collector.utilities.log.handlers.requests.post")
def test_splunk_handler_prefers_ca_bundle_over_verify_flag(mock_post: MagicMock) -> None:
    response = MagicMock()
    mock_post.return_value = response

    ca_bundle = "C:/certs/splunk-ca.pem"
    handler = SplunkHECHandler(
        "https://splunk.local/services/collector",
        "token",
        verify_tls=False,
        ca_bundle=ca_bundle,
    )
    handler.emit(_make_structured_record())

    assert mock_post.call_args.kwargs["verify"] == ca_bundle
    response.raise_for_status.assert_called_once()


@patch("data_collector.utilities.log.handlers.requests.post")
def test_splunk_handler_propagates_http_errors(mock_post: MagicMock) -> None:
    response = MagicMock()
    response.raise_for_status.side_effect = requests.HTTPError("bad response")
    mock_post.return_value = response

    handler = SplunkHECHandler("https://splunk.local/services/collector", "token")
    with pytest.raises(requests.HTTPError, match="bad response"):
        handler.emit(_make_structured_record())
