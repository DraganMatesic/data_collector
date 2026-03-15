"""Tests for the framework retry predicate."""

from __future__ import annotations

import httpx
import pytest
from httpx import Request, Response

from data_collector.dramatiq.retry import should_retry_message


class TestShouldRetryMessage:
    """Tests for should_retry_message predicate."""

    @pytest.mark.parametrize("exception_class", [
        ValueError,
        KeyError,
        TypeError,
        FileNotFoundError,
        PermissionError,
        NotImplementedError,
    ])
    def test_permanent_exceptions_not_retried(self, exception_class: type[BaseException]) -> None:
        assert should_retry_message(0, exception_class("test")) is False

    @pytest.mark.parametrize("exception_class", [
        ConnectionError,
        TimeoutError,
        OSError,
        RuntimeError,
    ])
    def test_transient_exceptions_retried(self, exception_class: type[BaseException]) -> None:
        assert should_retry_message(0, exception_class("test")) is True

    @pytest.mark.parametrize("status_code", [400, 401, 403, 404, 405, 422])
    def test_http_permanent_status_not_retried(self, status_code: int) -> None:
        request = Request("GET", "https://example.com")
        response = Response(status_code, request=request)
        exception = httpx.HTTPStatusError("error", request=request, response=response)
        assert should_retry_message(0, exception) is False

    @pytest.mark.parametrize("status_code", [408, 429, 500, 502, 503, 504])
    def test_http_transient_status_retried(self, status_code: int) -> None:
        request = Request("GET", "https://example.com")
        response = Response(status_code, request=request)
        exception = httpx.HTTPStatusError("error", request=request, response=response)
        assert should_retry_message(0, exception) is True

    def test_retry_count_does_not_affect_classification(self) -> None:
        assert should_retry_message(5, ConnectionError("test")) is True
        assert should_retry_message(5, ValueError("test")) is False
