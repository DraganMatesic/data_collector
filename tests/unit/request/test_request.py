import logging
from pathlib import Path
from unittest.mock import patch

import httpx
import respx

from data_collector.utilities.request import Request, RequestMetrics


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
    assert route.calls[0].request.headers["x-custom"] == "value"


@respx.mock
def test_reset_headers() -> None:
    route = respx.get("https://example.com/page").mock(return_value=httpx.Response(200))
    req = Request(timeout=5, retries=0)
    req.set_headers({"X-Custom": "value"})
    req.reset_headers()
    req.get("https://example.com/page")
    assert "x-custom" not in route.calls[0].request.headers


@respx.mock
def test_set_cookies_applied() -> None:
    route = respx.get("https://example.com/page").mock(return_value=httpx.Response(200))
    req = Request(timeout=5, retries=0)
    req.set_cookies({"session": "abc123"})
    req.get("https://example.com/page")
    assert "session=abc123" in str(route.calls[0].request.headers.get("cookie", ""))


@respx.mock
def test_set_auth_applied() -> None:
    route = respx.get("https://example.com/page").mock(return_value=httpx.Response(200))
    req = Request(timeout=5, retries=0)
    req.set_auth("user", "pass")
    req.get("https://example.com/page")
    assert "authorization" in route.calls[0].request.headers


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
    assert resp.status_code == 403
    assert route.call_count == 1


@respx.mock
def test_no_retry_on_404() -> None:
    route = respx.get("https://example.com/page").mock(return_value=httpx.Response(404))
    req = Request(timeout=5, retries=3)
    resp = req.get("https://example.com/page")
    assert resp.status_code == 404
    assert route.call_count == 1


@respx.mock
def test_retry_exponential_backoff() -> None:
    respx.get("https://example.com/page").mock(return_value=httpx.Response(503))
    sleep_calls = []
    with patch("time.sleep", side_effect=lambda s: sleep_calls.append(s)):
        req = Request(timeout=5, retries=3, backoff_factor=2)
        req.get("https://example.com/page")
    # Backoff: 2^0=1, 2^1=2, 2^2=4 (3 retries after first attempt)
    assert sleep_calls == [1, 2, 4]


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
    assert Path(save_path).read_text() == "<html>test</html>"


@respx.mock
def test_save_responses_auto(tmp_path: Path) -> None:
    respx.get("https://example.com/page").mock(return_value=httpx.Response(200, text="<html>saved</html>"))
    req = Request(timeout=5, retries=0, save_responses=True, save_dir=str(tmp_path))
    req.get("https://example.com/page")
    saved_files = list(tmp_path.iterdir())
    assert len(saved_files) == 1
    assert saved_files[0].read_text() == "<html>saved</html>"


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
