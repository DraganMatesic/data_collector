"""Unit tests for Request.get_error_category() mapping."""

from __future__ import annotations

from data_collector.utilities.request import Request, RequestErrorType


class TestGetErrorCategory:
    """Test Request.get_error_category() maps RequestErrorType to ErrorCategory strings."""

    def test_no_error_returns_none(self) -> None:
        request = Request()
        assert request.get_error_category() is None

    def test_timeout_maps_to_http(self) -> None:
        request = Request()
        request.exception_descriptor.add_error(RequestErrorType.TIMEOUT, "Connection timed out")
        assert request.get_error_category() == "http"

    def test_proxy_maps_to_proxy(self) -> None:
        request = Request()
        request.exception_descriptor.add_error(RequestErrorType.PROXY, "Proxy connection failed")
        assert request.get_error_category() == "proxy"

    def test_bad_status_maps_to_http(self) -> None:
        request = Request()
        request.exception_descriptor.add_error(RequestErrorType.BAD_STATUS, "HTTP 500")
        assert request.get_error_category() == "http"

    def test_redirect_maps_to_http(self) -> None:
        request = Request()
        request.exception_descriptor.add_error(RequestErrorType.REDIRECT, "Too many redirects")
        assert request.get_error_category() == "http"

    def test_request_maps_to_http(self) -> None:
        request = Request()
        request.exception_descriptor.add_error(RequestErrorType.REQUEST, "HTTP error")
        assert request.get_error_category() == "http"

    def test_other_maps_to_unknown(self) -> None:
        request = Request()
        request.exception_descriptor.add_error(RequestErrorType.OTHER, "Unexpected error")
        assert request.get_error_category() == "unknown"

    def test_unknown_type_maps_to_unknown(self) -> None:
        request = Request()
        request.exception_descriptor.add_error("nonexistent_type", "Some error")
        assert request.get_error_category() == "unknown"

    def test_last_error_wins(self) -> None:
        request = Request()
        request.exception_descriptor.add_error(RequestErrorType.TIMEOUT, "First error")
        request.exception_descriptor.add_error(RequestErrorType.PROXY, "Second error")
        assert request.get_error_category() == "proxy"

    def test_cleared_errors_returns_none(self) -> None:
        request = Request()
        request.exception_descriptor.add_error(RequestErrorType.TIMEOUT, "Error")
        request.exception_descriptor.clear()
        assert request.get_error_category() is None
