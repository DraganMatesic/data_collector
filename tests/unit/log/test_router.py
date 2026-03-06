from __future__ import annotations

import logging
from pathlib import Path

import pytest

from data_collector.utilities.log.router import RouterHandler


class _TrackingHandler(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


class _FailingHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        raise RuntimeError("sink failure")


def _make_record() -> logging.LogRecord:
    return logging.LogRecord(
        name="tests.router",
        level=logging.ERROR,
        pathname=__file__,
        lineno=42,
        msg={"event": "Record failed", "record_id": 42, "level": "error"},
        args=(),
        exc_info=None,
    )


def test_router_logs_sink_failures_to_fallback_file(tmp_path: Path) -> None:
    error_file = tmp_path / "error.log"
    tracking_handler = _TrackingHandler()
    router = RouterHandler(
        [tracking_handler, _FailingHandler()],
        swallow_errors=True,
        error_file=str(error_file),
    )

    router.emit(_make_record())

    assert len(tracking_handler.records) == 1
    assert error_file.exists()
    content = error_file.read_text(encoding="utf-8")
    assert "handler=_FailingHandler" in content
    assert "Traceback:" in content
    assert "record_id" in content


def test_router_reraises_when_swallow_errors_is_false(tmp_path: Path) -> None:
    error_file = tmp_path / "error.log"
    router = RouterHandler(
        [_FailingHandler()],
        swallow_errors=False,
        error_file=str(error_file),
    )

    with pytest.raises(RuntimeError, match="sink failure"):
        router.emit(_make_record())
