from __future__ import annotations

import logging
import uuid
from logging.handlers import QueueHandler
from pathlib import Path
from typing import Any, cast
from unittest.mock import MagicMock, patch

import pytest
from structlog.stdlib import BoundLogger, ProcessorFormatter  # type: ignore[import-untyped]

from data_collector.settings.main import LogSettings
from data_collector.utilities.log.handlers import DatabaseHandler, SplunkHECHandler
from data_collector.utilities.log.main import LoggingService, _build_pre_chain  # pyright: ignore[reportPrivateUsage]


@pytest.fixture(autouse=True)
def error_log_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:  # noqa: PT004
    """Point DC_LOG_ERROR_FILE to a temp file so RouterHandler can create the fallback."""
    monkeypatch.setenv("DC_LOG_ERROR_FILE", str(tmp_path / "error.log"))


def _build_service(settings: LogSettings, db_engine: object | None = None) -> LoggingService:
    logger_name = f"tests.log.{uuid.uuid4().hex}"
    logger = logging.getLogger(logger_name)
    logger.handlers.clear()
    return LoggingService(logger_name=logger_name, settings=settings, db_engine=db_engine)


def _get_stream_handler(service: LoggingService) -> logging.StreamHandler[Any]:
    for sink in service.sinks:
        if isinstance(sink, logging.StreamHandler):
            return cast(logging.StreamHandler[Any], sink)
    raise AssertionError("Missing stream handler")


def test_configure_logger_returns_bound_logger() -> None:
    service = _build_service(LogSettings(log_to_db=False, log_to_splunk=False))
    try:
        logger = service.configure_logger()
        assert isinstance(logger, BoundLogger)
    finally:
        service.stop()


def test_configure_logger_is_idempotent_for_queue_handler_and_listener() -> None:
    service = _build_service(LogSettings(log_to_db=False, log_to_splunk=False))
    try:
        first_logger = service.configure_logger()
        first_listener = service.log_listener
        second_logger = service.configure_logger()
        second_listener = service.log_listener

        queue_handlers = [handler for handler in service.logger.handlers if isinstance(handler, QueueHandler)]
        assert len(queue_handlers) == 1
        assert first_listener is second_listener
        assert isinstance(first_logger, BoundLogger)
        assert isinstance(second_logger, BoundLogger)
    finally:
        service.stop()


def test_debug_mode_disables_auto_db_and_splunk_sinks() -> None:
    settings = LogSettings(
        log_to_db=True,
        log_to_splunk=True,
        splunk_hec_url="https://splunk.local/services/collector",
        splunk_token="token",
    )
    service = _build_service(settings=settings, db_engine=object())
    service.debug = True

    try:
        service.configure_logger()
        assert not any(isinstance(sink, DatabaseHandler) for sink in service.sinks)
        assert not any(isinstance(sink, SplunkHECHandler) for sink in service.sinks)
    finally:
        service.stop()


@patch("data_collector.utilities.log.main.SplunkHECHandler")
def test_configure_logger_passes_splunk_tls_settings(mock_splunk_handler: MagicMock) -> None:
    mock_splunk_handler.return_value = cast(Any, MagicMock())
    settings = LogSettings(
        log_to_db=False,
        log_to_splunk=True,
        splunk_hec_url="https://splunk.local/services/collector",
        splunk_token="token",
        splunk_verify_tls=False,
        splunk_ca_bundle="C:/certs/splunk-ca.pem",
        splunk_index="default",
    )
    service = _build_service(settings=settings)

    try:
        service.configure_logger()
        mock_splunk_handler.assert_called_once_with(
            "https://splunk.local/services/collector",
            "token",
            verify_tls=False,
            ca_bundle="C:/certs/splunk-ca.pem",
            index="default",
            sourcetype="data_collector:structured",
        )
    finally:
        service.stop()


def test_console_formatter_uses_json_renderer_when_configured() -> None:
    service = _build_service(LogSettings(log_to_db=False, log_to_splunk=False, log_format="json"))
    try:
        service.configure_logger()
        stream_handler = _get_stream_handler(service)
        assert isinstance(stream_handler.formatter, ProcessorFormatter)
        assert stream_handler.formatter.processors[-1].__class__.__name__ == "JSONRenderer"
    finally:
        service.stop()


def test_console_formatter_uses_console_renderer_by_default() -> None:
    service = _build_service(LogSettings(log_to_db=False, log_to_splunk=False, log_format="console"))
    try:
        service.configure_logger()
        stream_handler = _get_stream_handler(service)
        assert isinstance(stream_handler.formatter, ProcessorFormatter)
        assert stream_handler.formatter.processors[-1].__class__.__name__ == "ConsoleRenderer"
    finally:
        service.stop()


def test_queue_listener_start_and_stop_lifecycle() -> None:
    service = _build_service(LogSettings(log_to_db=False, log_to_splunk=False))
    try:
        service.configure_logger()
        assert service.log_listener is not None
        thread = getattr(service.log_listener, "_thread", None)
        assert thread is not None and thread.is_alive()
        service.stop()
        thread_after = getattr(service.log_listener, "_thread", None)
        assert thread_after is None or not thread_after.is_alive()
    finally:
        service.stop()


def test_build_pre_chain_returns_eight_processors() -> None:
    settings = LogSettings(log_to_db=False, log_to_splunk=False)
    chain = _build_pre_chain(settings)
    assert isinstance(chain, list)
    assert len(chain) == 8
    for processor in chain:
        assert callable(processor)
