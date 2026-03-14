import logging
from pathlib import Path
from unittest.mock import patch

import httpx
import respx

from data_collector.utilities.request import Request, RequestErrorType, RequestMetrics

# ---------------------------------------------------------------------------
# Basic GET / POST
# ---------------------------------------------------------------------------

@respx.mock
def test_get_success() -> None:
    respx.get("https://example.com/page").mock(return_value=httpx.Response(200, text="OK"))
    req = Request(timeout=5, retries=0)
    resp = req.get("https://example.com/page")
    assert resp is not None
    assert resp.status_code == 200


@respx.mock
def test_post_success() -> None:
    respx.post("https://example.com/api").mock(return_value=httpx.Response(200, json={"ok": True}))
    req = Request(timeout=5, retries=0)
    resp = req.post("https://example.com/api", json={"q": "test"})
    assert resp is not None
    assert resp.status_code == 200


# ---------------------------------------------------------------------------
# Headers / Cookies / Auth
# ---------------------------------------------------------------------------

@respx.mock
def test_set_headers_applied() -> None:
    route = respx.get("https://example.com/page").mock(return_value=httpx.Response(200))
    req = Request(timeout=5, retries=0)
    req.set_headers({"X-Custom": "value"})
    req.get("https://example.com/page")
    assert route.calls.last.request.headers["x-custom"] == "value"


@respx.mock
def test_reset_headers() -> None:
    route = respx.get("https://example.com/page").mock(return_value=httpx.Response(200))
    req = Request(timeout=5, retries=0)
    req.set_headers({"X-Custom": "value"})
    req.reset_headers()
    req.get("https://example.com/page")
    assert "x-custom" not in route.calls.last.request.headers


@respx.mock
def test_set_cookies_applied() -> None:
    route = respx.get("https://example.com/page").mock(return_value=httpx.Response(200))
    req = Request(timeout=5, retries=0)
    req.set_cookies({"session": "abc123"})
    req.get("https://example.com/page")
    assert "session=abc123" in str(route.calls.last.request.headers.get("cookie", ""))


@respx.mock
def test_set_auth_applied() -> None:
    route = respx.get("https://example.com/page").mock(return_value=httpx.Response(200))
    req = Request(timeout=5, retries=0)
    req.set_auth("user", "pass")
    req.get("https://example.com/page")
    assert "authorization" in route.calls.last.request.headers


# ---------------------------------------------------------------------------
# Retry logic
# ---------------------------------------------------------------------------

@respx.mock
def test_retry_on_503_then_success() -> None:
    route = respx.get("https://example.com/page")
    route.side_effect = [
        httpx.Response(503),
        httpx.Response(200, text="OK"),
    ]
    with patch("time.sleep"):
        req = Request(timeout=5, retries=2)
        resp = req.get("https://example.com/page")
    assert resp is not None
    assert resp.status_code == 200
    assert route.call_count == 2


@respx.mock
def test_has_errors_false_after_successful_retry() -> None:
    route = respx.get("https://example.com/page")
    route.side_effect = [
        httpx.Response(503),
        httpx.Response(200, text="OK"),
    ]
    with patch("time.sleep"):
        req = Request(timeout=5, retries=2)
        req.get("https://example.com/page")
    assert req.has_errors() is False


@respx.mock
def test_should_abort_false_after_successful_retry() -> None:
    route = respx.get("https://example.com/page")
    route.side_effect = [
        httpx.Response(503),
        httpx.Response(200, text="OK"),
    ]
    logger = logging.getLogger("test")
    with patch("time.sleep"):
        req = Request(timeout=5, retries=2)
        req.get("https://example.com/page")
    assert req.should_abort(logger) is False


@respx.mock
def test_has_errors_false_after_exception_then_success() -> None:
    route = respx.get("https://example.com/page")
    route.side_effect = [
        httpx.ReadTimeout("timeout"),
        httpx.Response(200, text="OK"),
    ]
    with patch("time.sleep"):
        req = Request(timeout=5, retries=2)
        req.get("https://example.com/page")
    assert req.has_errors() is False


@respx.mock
def test_no_retry_on_401() -> None:
    route = respx.get("https://example.com/page").mock(return_value=httpx.Response(401))
    req = Request(timeout=5, retries=3)
    resp = req.get("https://example.com/page")
    assert resp is not None
    assert resp.status_code == 401
    assert route.call_count == 1


@respx.mock
def test_no_retry_on_403() -> None:
    route = respx.get("https://example.com/page").mock(return_value=httpx.Response(403))
    req = Request(timeout=5, retries=3)
    resp = req.get("https://example.com/page")
    assert resp is not None
    assert resp.status_code == 403
    assert route.call_count == 1


@respx.mock
def test_no_retry_on_404() -> None:
    route = respx.get("https://example.com/page").mock(return_value=httpx.Response(404))
    req = Request(timeout=5, retries=3)
    resp = req.get("https://example.com/page")
    assert resp is not None
    assert resp.status_code == 404
    assert route.call_count == 1


@respx.mock
def test_retry_exponential_backoff_with_jitter() -> None:
    respx.get("https://example.com/page").mock(return_value=httpx.Response(503))
    sleep_calls: list[float] = []
    with patch("time.sleep", side_effect=sleep_calls.append):
        req = Request(timeout=5, retries=3, backoff_factor=2)
        req.get("https://example.com/page")
    # 3 retries after initial attempt; jitter in [0, 2^attempt]
    assert len(sleep_calls) == 3
    assert 0 <= sleep_calls[0] <= 1   # 2^0 = 1
    assert 0 <= sleep_calls[1] <= 2   # 2^1 = 2
    assert 0 <= sleep_calls[2] <= 4   # 2^2 = 4


# ---------------------------------------------------------------------------
# Error counters
# ---------------------------------------------------------------------------

@respx.mock
def test_timeout_error_increments_counter() -> None:
    respx.get("https://example.com/page").mock(side_effect=httpx.ReadTimeout("timeout"))
    with patch("time.sleep"):
        req = Request(timeout=5, retries=0)
        req.get("https://example.com/page")
    assert req.timeout_err == 1


@respx.mock
def test_connection_error_increments_proxy_err() -> None:
    respx.get("https://example.com/page").mock(side_effect=httpx.ConnectError("refused"))
    with patch("time.sleep"):
        req = Request(timeout=5, retries=0)
        req.get("https://example.com/page")
    assert req.proxy_err == 1


@respx.mock
def test_bad_status_increments_counter() -> None:
    respx.get("https://example.com/page").mock(return_value=httpx.Response(500))
    with patch("time.sleep"):
        req = Request(timeout=5, retries=0)
        req.get("https://example.com/page")
    assert req.bad_status_code_err == 1


@respx.mock
def test_request_count_incremented_on_success() -> None:
    respx.get("https://example.com/page").mock(return_value=httpx.Response(200))
    req = Request(timeout=5, retries=0)
    req.get("https://example.com/page")
    assert req.request_count == 1


@respx.mock
def test_request_count_includes_timeout() -> None:
    respx.get("https://example.com/page").mock(side_effect=httpx.ReadTimeout("timeout"))
    with patch("time.sleep"):
        req = Request(timeout=5, retries=0)
        req.get("https://example.com/page")
    assert req.request_count == 1


@respx.mock
def test_request_count_includes_all_retry_attempts() -> None:
    route = respx.get("https://example.com/page")
    route.side_effect = [
        httpx.ReadTimeout("timeout"),
        httpx.Response(200, text="OK"),
    ]
    with patch("time.sleep"):
        req = Request(timeout=5, retries=2)
        req.get("https://example.com/page")
    # 1 failed attempt (exception) + 1 successful attempt = 2
    assert req.request_count == 2


# ---------------------------------------------------------------------------
# Error introspection
# ---------------------------------------------------------------------------

@respx.mock
def test_has_errors_true_after_failure() -> None:
    respx.get("https://example.com/page").mock(side_effect=httpx.ReadTimeout("timeout"))
    with patch("time.sleep"):
        req = Request(timeout=5, retries=0)
        req.get("https://example.com/page")
    assert req.has_errors() is True


@respx.mock
def test_has_errors_false_on_success() -> None:
    respx.get("https://example.com/page").mock(return_value=httpx.Response(200))
    req = Request(timeout=5, retries=0)
    req.get("https://example.com/page")
    assert req.has_errors() is False


@respx.mock
def test_is_blocked_on_403() -> None:
    respx.get("https://example.com/page").mock(return_value=httpx.Response(403))
    req = Request(timeout=5, retries=0)
    req.get("https://example.com/page")
    assert req.is_blocked() is True


@respx.mock
def test_is_timeout_on_timeout() -> None:
    respx.get("https://example.com/page").mock(side_effect=httpx.ReadTimeout("timeout"))
    with patch("time.sleep"):
        req = Request(timeout=5, retries=0)
        req.get("https://example.com/page")
    assert req.is_timeout() is True


@respx.mock
def test_is_server_down_on_500() -> None:
    respx.get("https://example.com/page").mock(return_value=httpx.Response(500))
    with patch("time.sleep"):
        req = Request(timeout=5, retries=0)
        req.get("https://example.com/page")
    assert req.is_server_down() is True


@respx.mock
def test_is_proxy_error_on_connect_error() -> None:
    respx.get("https://example.com/page").mock(side_effect=httpx.ConnectError("refused"))
    with patch("time.sleep"):
        req = Request(timeout=5, retries=0)
        req.get("https://example.com/page")
    assert req.is_proxy_error() is True


# ---------------------------------------------------------------------------
# should_abort
# ---------------------------------------------------------------------------

@respx.mock
def test_should_abort_on_timeout() -> None:
    respx.get("https://example.com/page").mock(side_effect=httpx.ReadTimeout("timeout"))
    logger = logging.getLogger("test")
    with patch("time.sleep"):
        req = Request(timeout=5, retries=0)
        req.get("https://example.com/page")
    assert req.should_abort(logger) is True


@respx.mock
def test_should_abort_proxy_on_blocked() -> None:
    respx.get("https://example.com/page").mock(return_value=httpx.Response(403))
    logger = logging.getLogger("test")
    req = Request(timeout=5, retries=0)
    req.get("https://example.com/page")
    assert req.should_abort(logger, proxy_on=True) is True


@respx.mock
def test_should_abort_false_on_success() -> None:
    respx.get("https://example.com/page").mock(return_value=httpx.Response(200))
    logger = logging.getLogger("test")
    req = Request(timeout=5, retries=0)
    req.get("https://example.com/page")
    assert req.should_abort(logger) is False


# ---------------------------------------------------------------------------
# Response helpers
# ---------------------------------------------------------------------------

@respx.mock
def test_get_json() -> None:
    respx.get("https://example.com/api").mock(return_value=httpx.Response(200, json={"key": "val"}))
    req = Request(timeout=5, retries=0)
    req.get("https://example.com/api")
    assert req.get_json() == {"key": "val"}


@respx.mock
def test_get_content() -> None:
    respx.get("https://example.com/page").mock(return_value=httpx.Response(200, text="hello"))
    req = Request(timeout=5, retries=0)
    req.get("https://example.com/page")
    assert req.get_content() == b"hello"


@respx.mock
def test_get_content_length() -> None:
    respx.get("https://example.com/page").mock(
        return_value=httpx.Response(200, text="hello", headers={"content-length": "5"})
    )
    req = Request(timeout=5, retries=0)
    req.get("https://example.com/page")
    assert req.get_content_length() == 5


@respx.mock
def test_get_json_none_when_no_response() -> None:
    req = Request(timeout=5, retries=0)
    assert req.get_json() is None


@respx.mock
def test_get_content_none_when_no_response() -> None:
    req = Request(timeout=5, retries=0)
    assert req.get_content() is None


@respx.mock
def test_save_html(tmp_path: Path) -> None:
    respx.get("https://example.com/page").mock(return_value=httpx.Response(200, text="<html>test</html>"))
    req = Request(timeout=5, retries=0)
    req.get("https://example.com/page")
    save_path = str(tmp_path / "output.html")
    req.save_html(save_path)
    assert Path(save_path).read_text(encoding="utf-8") == "<html>test</html>"


@respx.mock
def test_save_responses_auto(tmp_path: Path) -> None:
    respx.get("https://example.com/page").mock(return_value=httpx.Response(200, text="<html>saved</html>"))
    req = Request(timeout=5, retries=0, save_responses=True, save_dir=str(tmp_path))
    req.get("https://example.com/page")
    saved_files = list(tmp_path.iterdir())
    assert len(saved_files) == 1
    assert saved_files[0].read_text(encoding="utf-8") == "<html>saved</html>"


# ---------------------------------------------------------------------------
# log_stats
# ---------------------------------------------------------------------------

@respx.mock
def test_log_stats_basic() -> None:
    respx.get("https://example.com/page").mock(return_value=httpx.Response(200))
    logger = logging.getLogger("test")
    req = Request(timeout=5, retries=0)
    req.get("https://example.com/page")
    stats = req.log_stats(logger)
    assert stats["total_requests"] == 1
    assert stats["total_errors"] == 0


@respx.mock
def test_log_stats_delegates_to_metrics() -> None:
    respx.get("https://example.com/page").mock(return_value=httpx.Response(200))
    metrics = RequestMetrics()
    logger = logging.getLogger("test")
    req = Request(timeout=5, retries=0, metrics=metrics)
    req.get("https://example.com/page")
    stats = req.log_stats(logger)
    # Should come from metrics, which has timing info
    assert "timing" in stats
    assert "by_domain" in stats


# ---------------------------------------------------------------------------
# Circuit breaker delegation
# ---------------------------------------------------------------------------

def test_is_target_unhealthy_no_metrics() -> None:
    req = Request(timeout=5, retries=0)
    assert req.is_target_unhealthy("https://example.com/page") is False


def test_is_target_unhealthy_with_metrics() -> None:
    metrics = RequestMetrics(max_target_failures=2, min_distinct_proxies=1)
    metrics.record_error("example.com", "proxy1", "timeout")
    metrics.record_error("example.com", "proxy1", "timeout")
    req = Request(timeout=5, retries=0, metrics=metrics)
    assert req.is_target_unhealthy("https://example.com/page") is True


# ---------------------------------------------------------------------------
# Metrics integration
# ---------------------------------------------------------------------------

@respx.mock
def test_metrics_record_request_on_success() -> None:
    respx.get("https://example.com/page").mock(return_value=httpx.Response(200))
    metrics = RequestMetrics()
    req = Request(timeout=5, retries=0, metrics=metrics)
    req.get("https://example.com/page")
    assert metrics.request_count == 1


@respx.mock
def test_metrics_record_error_on_timeout() -> None:
    respx.get("https://example.com/page").mock(side_effect=httpx.ReadTimeout("timeout"))
    metrics = RequestMetrics()
    with patch("time.sleep"):
        req = Request(timeout=5, retries=0, metrics=metrics)
        req.get("https://example.com/page")
    assert metrics.timeout_err == 1


# ---------------------------------------------------------------------------
# set_proxy in _build_client_kwargs
# ---------------------------------------------------------------------------

def test_set_proxy_applied() -> None:
    req = Request(timeout=5, retries=0)
    req.set_proxy("http://user:pass@proxy.example.com:8080")
    kwargs = req._build_client_kwargs()  # pyright: ignore[reportPrivateUsage]
    assert kwargs["proxy"] == "http://user:pass@proxy.example.com:8080"


def test_set_proxy_cleared() -> None:
    req = Request(timeout=5, retries=0)
    req.set_proxy("http://proxy:8080")
    req.set_proxy(None)
    kwargs = req._build_client_kwargs()  # pyright: ignore[reportPrivateUsage]
    assert "proxy" not in kwargs


# ---------------------------------------------------------------------------
# _auto_save_response JSON extension
# ---------------------------------------------------------------------------

@respx.mock
def test_auto_save_json_extension(tmp_path: Path) -> None:
    respx.get("https://example.com/api/data").mock(
        return_value=httpx.Response(200, json={"k": "v"}, headers={"content-type": "application/json"})
    )
    req = Request(timeout=5, retries=0, save_responses=True, save_dir=str(tmp_path))
    req.get("https://example.com/api/data")
    saved_files = list(tmp_path.iterdir())
    assert len(saved_files) == 1
    assert saved_files[0].suffix == ".json"



# ---------------------------------------------------------------------------
# _get_proxy_key credential stripping (verified through metrics)
# ---------------------------------------------------------------------------

@respx.mock
def test_proxy_key_strips_credentials() -> None:
    respx.get("https://example.com/page").mock(return_value=httpx.Response(200))
    metrics = RequestMetrics()
    logger = logging.getLogger("test")
    req = Request(timeout=5, retries=0, metrics=metrics)
    req.set_proxy("http://user:s3cret@proxy.example.com:8080")
    req.get("https://example.com/page")
    stats = metrics.log_stats(logger)
    # Credentials must be stripped — only host:port in metrics
    assert "proxy.example.com:8080" in stats["by_proxy"]
    assert "user" not in str(stats["by_proxy"])
    assert "s3cret" not in str(stats["by_proxy"])


@respx.mock
def test_proxy_key_direct_without_proxy() -> None:
    respx.get("https://example.com/page").mock(return_value=httpx.Response(200))
    metrics = RequestMetrics()
    logger = logging.getLogger("test")
    req = Request(timeout=5, retries=0, metrics=metrics)
    req.get("https://example.com/page")
    stats = metrics.log_stats(logger)
    assert "direct" in stats["by_proxy"]


# ---------------------------------------------------------------------------
# is_blocked — false-positive resistance
# ---------------------------------------------------------------------------

@respx.mock
def test_is_blocked_no_false_positive_on_url_containing_403() -> None:
    """URL path containing '403' must not trigger is_blocked()."""
    respx.get("https://example.com/page/403/details").mock(
        side_effect=httpx.ConnectError("Connection failed")
    )
    req = Request(timeout=5, retries=0)
    req.get("https://example.com/page/403/details")
    assert req.is_blocked() is False


@respx.mock
def test_is_blocked_no_false_positive_on_port_4013() -> None:
    """Port number resembling '401' must not trigger is_blocked()."""
    req = Request(timeout=5, retries=0)
    req.exception_descriptor.add_error("proxy", "Connection refused on port 4013", "https://example.com")
    req._last_request_time = req.exception_descriptor.errors.copy().popitem()[0]  # pyright: ignore[reportPrivateUsage]
    assert req.is_blocked() is False


@respx.mock
def test_is_blocked_true_on_forcibly_closed() -> None:
    """'forcibly closed' message triggers is_blocked()."""
    req = Request(timeout=5, retries=0)
    req.exception_descriptor.add_error("other", "Connection forcibly closed by remote host", "https://example.com")
    req._last_request_time = req.exception_descriptor.errors.copy().popitem()[0]  # pyright: ignore[reportPrivateUsage]
    assert req.is_blocked() is True


@respx.mock
def test_is_blocked_not_stale_after_soap_error() -> None:
    """Stale 403 response must not trigger is_blocked() when last error is SOAP fault."""
    respx.get("https://example.com/page").mock(return_value=httpx.Response(403))
    req = Request(timeout=5, retries=0)
    req.get("https://example.com/page")
    assert req.is_blocked() is True
    # Simulate a subsequent SOAP error (records error but does not clear self.response)
    req.exception_descriptor.clear()
    req._last_request_time = None  # pyright: ignore[reportPrivateUsage]
    req.exception_descriptor.add_error(RequestErrorType.REQUEST, "SOAP Fault: invalid input", "soap")
    req._last_request_time = req.exception_descriptor.errors.copy().popitem()[0]  # pyright: ignore[reportPrivateUsage]
    # self.response still holds 403, but last error is SOAP — is_blocked must be False
    assert req.is_blocked() is False


# ---------------------------------------------------------------------------
# is_server_down — false-positive resistance
# ---------------------------------------------------------------------------

@respx.mock
def test_is_server_down_no_false_positive_on_timeout_500ms() -> None:
    """Timeout message containing '500' must not trigger is_server_down()."""
    req = Request(timeout=5, retries=0)
    req.exception_descriptor.add_error("timeout", "Read timed out after 500ms", "https://example.com")
    req._last_request_time = req.exception_descriptor.errors.copy().popitem()[0]  # pyright: ignore[reportPrivateUsage]
    assert req.is_server_down() is False


@respx.mock
def test_is_server_down_not_stale_after_soap_error() -> None:
    """Stale 500 response must not trigger is_server_down() when last error is SOAP fault."""
    respx.get("https://example.com/page").mock(return_value=httpx.Response(500))
    req = Request(timeout=5, retries=0)
    req.get("https://example.com/page")
    assert req.is_server_down() is True
    # Simulate a subsequent SOAP error
    req.exception_descriptor.clear()
    req._last_request_time = None  # pyright: ignore[reportPrivateUsage]
    req.exception_descriptor.add_error(RequestErrorType.REQUEST, "SOAP Fault: timeout", "soap")
    req._last_request_time = req.exception_descriptor.errors.copy().popitem()[0]  # pyright: ignore[reportPrivateUsage]
    # self.response still holds 500, but last error is SOAP — is_server_down must be False
    assert req.is_server_down() is False


# ---------------------------------------------------------------------------
# Exception error recording — only on final attempt
# ---------------------------------------------------------------------------

@respx.mock
def test_exception_error_recorded_only_on_final_attempt() -> None:
    """Instance-level error counter must increment once, not per retry attempt."""
    respx.get("https://example.com/page").mock(side_effect=httpx.ReadTimeout("timeout"))
    with patch("time.sleep"):
        req = Request(timeout=5, retries=3)
        req.get("https://example.com/page")
    # 4 attempts (1 initial + 3 retries), but only 1 instance-level error
    assert req.request_count == 4
    assert req.timeout_err == 1


@respx.mock
def test_exception_error_not_recorded_during_successful_retry() -> None:
    """No instance-level error when exception retry succeeds."""
    route = respx.get("https://example.com/page")
    route.side_effect = [
        httpx.ReadTimeout("timeout"),
        httpx.ReadTimeout("timeout"),
        httpx.Response(200, text="OK"),
    ]
    with patch("time.sleep"):
        req = Request(timeout=5, retries=3)
        req.get("https://example.com/page")
    assert req.timeout_err == 0
    assert len(req.exception_descriptor.errors) == 0


# ---------------------------------------------------------------------------
# _classify_exception — full httpx hierarchy coverage
# ---------------------------------------------------------------------------

def test_classify_exception_timeout_types() -> None:
    """All TimeoutException subclasses classify as TIMEOUT, retryable."""
    req = Request(timeout=5, retries=0)
    for exc_class in (httpx.ConnectTimeout, httpx.ReadTimeout, httpx.WriteTimeout, httpx.PoolTimeout):
        error_type, retryable = req._classify_exception(exc_class("test"))  # pyright: ignore[reportPrivateUsage]
        assert error_type == RequestErrorType.TIMEOUT
        assert retryable is True


def test_classify_exception_proxy_error() -> None:
    """ProxyError classifies as PROXY, retryable."""
    req = Request(timeout=5, retries=0)
    error_type, retryable = req._classify_exception(httpx.ProxyError("test"))  # pyright: ignore[reportPrivateUsage]
    assert error_type == RequestErrorType.PROXY
    assert retryable is True


def test_classify_exception_connect_error() -> None:
    """ConnectError classifies as PROXY, retryable."""
    req = Request(timeout=5, retries=0)
    error_type, retryable = req._classify_exception(httpx.ConnectError("test"))  # pyright: ignore[reportPrivateUsage]
    assert error_type == RequestErrorType.PROXY
    assert retryable is True


def test_classify_exception_read_write_error_retryable() -> None:
    """ReadError and WriteError are transient network I/O — REQUEST, retryable."""
    req = Request(timeout=5, retries=0)
    for exc_class in (httpx.ReadError, httpx.WriteError):
        error_type, retryable = req._classify_exception(exc_class("test"))  # pyright: ignore[reportPrivateUsage]
        assert error_type == RequestErrorType.REQUEST
        assert retryable is True


def test_classify_exception_remote_protocol_error_retryable() -> None:
    """RemoteProtocolError (server glitch) is REQUEST, retryable."""
    req = Request(timeout=5, retries=0)
    error_type, retryable = req._classify_exception(httpx.RemoteProtocolError("test"))  # pyright: ignore[reportPrivateUsage]
    assert error_type == RequestErrorType.REQUEST
    assert retryable is True


def test_classify_exception_too_many_redirects() -> None:
    """TooManyRedirects classifies as REDIRECT, not retryable."""
    req = Request(timeout=5, retries=0)
    error_type, retryable = req._classify_exception(httpx.TooManyRedirects("test"))  # pyright: ignore[reportPrivateUsage]
    assert error_type == RequestErrorType.REDIRECT
    assert retryable is False


def test_classify_exception_non_retryable_http_errors() -> None:
    """CloseError, LocalProtocolError, UnsupportedProtocol, DecodingError — REQUEST, not retryable."""
    req = Request(timeout=5, retries=0)
    for exc_class in (httpx.CloseError, httpx.LocalProtocolError, httpx.UnsupportedProtocol, httpx.DecodingError):
        error_type, retryable = req._classify_exception(exc_class("test"))  # pyright: ignore[reportPrivateUsage]
        assert error_type == RequestErrorType.REQUEST
        assert retryable is False


def test_classify_exception_non_httpx() -> None:
    """Non-httpx exceptions classify as OTHER, not retryable."""
    req = Request(timeout=5, retries=0)
    error_type, retryable = req._classify_exception(ValueError("test"))  # pyright: ignore[reportPrivateUsage]
    assert error_type == RequestErrorType.OTHER
    assert retryable is False


@respx.mock
def test_read_error_retried() -> None:
    """ReadError is retried (transient network failure)."""
    route = respx.get("https://example.com/page")
    route.side_effect = [
        httpx.ReadError("connection reset"),
        httpx.Response(200, text="OK"),
    ]
    with patch("time.sleep"):
        req = Request(timeout=5, retries=2)
        resp = req.get("https://example.com/page")
    assert resp is not None
    assert resp.status_code == 200
    assert route.call_count == 2


@respx.mock
def test_remote_protocol_error_retried() -> None:
    """RemoteProtocolError is retried (server-side glitch)."""
    route = respx.get("https://example.com/page")
    route.side_effect = [
        httpx.RemoteProtocolError("malformed response"),
        httpx.Response(200, text="OK"),
    ]
    with patch("time.sleep"):
        req = Request(timeout=5, retries=2)
        resp = req.get("https://example.com/page")
    assert resp is not None
    assert resp.status_code == 200
    assert route.call_count == 2


# ---------------------------------------------------------------------------
# get_error_category
# ---------------------------------------------------------------------------

@respx.mock
def test_get_error_category_timeout() -> None:
    respx.get("https://example.com/page").mock(side_effect=httpx.ReadTimeout("timeout"))
    req = Request(timeout=5, retries=0)
    req.get("https://example.com/page")
    assert req.get_error_category() == "http"


@respx.mock
def test_get_error_category_proxy() -> None:
    respx.get("https://example.com/page").mock(side_effect=httpx.ConnectError("refused"))
    req = Request(timeout=5, retries=0)
    req.get("https://example.com/page")
    assert req.get_error_category() == "proxy"


@respx.mock
def test_get_error_category_bad_status() -> None:
    respx.get("https://example.com/page").mock(return_value=httpx.Response(500))
    req = Request(timeout=5, retries=0)
    req.get("https://example.com/page")
    assert req.get_error_category() == "http"


@respx.mock
def test_get_error_category_none_on_success() -> None:
    respx.get("https://example.com/page").mock(return_value=httpx.Response(200))
    req = Request(timeout=5, retries=0)
    req.get("https://example.com/page")
    assert req.get_error_category() is None


def test_get_error_category_none_when_no_request() -> None:
    req = Request(timeout=5, retries=0)
    assert req.get_error_category() is None


def test_get_error_category_all_types_mapped() -> None:
    """Every RequestErrorType value must have a mapping in _REQUEST_ERROR_TO_CATEGORY."""
    mapping = Request._REQUEST_ERROR_TO_CATEGORY  # pyright: ignore[reportPrivateUsage]
    for error_type in RequestErrorType:
        assert error_type in mapping, f"{error_type} missing from _REQUEST_ERROR_TO_CATEGORY"
