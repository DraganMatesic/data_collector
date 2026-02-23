import logging
from concurrent.futures import ThreadPoolExecutor

from data_collector.utilities.request import RequestMetrics


# ---------------------------------------------------------------------------
# record_request
# ---------------------------------------------------------------------------

def test_record_request_increments_count() -> None:
    m = RequestMetrics()
    m.record_request("example.com", None, 200, 100.0)
    m.record_request("example.com", None, 200, 150.0)
    assert m.request_count == 2


def test_record_request_tracks_domain_timing() -> None:
    m = RequestMetrics()
    m.record_request("example.com", None, 200, 100.0)
    m.record_request("example.com", None, 200, 200.0)
    assert len(m._domain_timings["example.com"]) == 2


def test_record_request_tracks_status_codes() -> None:
    m = RequestMetrics()
    m.record_request("example.com", None, 200, 100.0)
    m.record_request("example.com", None, 200, 100.0)
    m.record_request("example.com", None, 503, 100.0)
    codes = m._domain_status_codes["example.com"]
    assert codes[200] == 2
    assert codes[503] == 1


def test_record_request_tracks_proxy_stats() -> None:
    m = RequestMetrics()
    m.record_request("example.com", "DE:8080", 200, 100.0)
    m.record_request("example.com", "DE:8080", 200, 150.0)
    m.record_request("example.com", "US:9090", 200, 200.0)
    assert m._proxy_stats["DE:8080"]["count"] == 2
    assert m._proxy_stats["DE:8080"]["success"] == 2
    assert m._proxy_stats["US:9090"]["count"] == 1


def test_record_request_no_proxy_uses_direct() -> None:
    m = RequestMetrics()
    m.record_request("example.com", None, 200, 100.0)
    assert "direct" in m._proxy_stats


# ---------------------------------------------------------------------------
# record_error
# ---------------------------------------------------------------------------

def test_record_error_increments_counters() -> None:
    m = RequestMetrics()
    m.record_error("example.com", None, "timeout")
    m.record_error("example.com", None, "proxy")
    m.record_error("example.com", None, "timeout")
    assert m.timeout_err == 2
    assert m.proxy_err == 1


def test_record_error_bad_status_code() -> None:
    m = RequestMetrics()
    m.record_error("example.com", None, "bad_status_code")
    assert m.bad_status_code_err == 1


def test_record_error_increments_request_count() -> None:
    m = RequestMetrics()
    m.record_error("example.com", None, "timeout")
    m.record_error("example.com", None, "proxy")
    assert m.request_count == 2


def test_error_rate_percent_with_mixed_results() -> None:
    m = RequestMetrics()
    m.record_request("example.com", None, 200, 100.0)
    m.record_error("example.com", None, "timeout")
    logger = logging.getLogger("test")
    stats = m.log_stats(logger)
    assert stats["total_requests"] == 2
    assert stats["total_errors"] == 1
    assert stats["error_rate_percent"] == 50.0


def test_record_error_updates_circuit_breaker() -> None:
    m = RequestMetrics()
    m.record_error("example.com", "proxy1", "timeout")
    assert "example.com" in m._target_failures
    assert m._target_failures["example.com"]["failures"] == 1


# ---------------------------------------------------------------------------
# is_target_unhealthy (circuit breaker)
# ---------------------------------------------------------------------------

def test_is_target_unhealthy_false_below_threshold() -> None:
    m = RequestMetrics(max_target_failures=3, min_distinct_proxies=2)
    m.record_error("example.com", "proxy1", "timeout")
    m.record_error("example.com", "proxy2", "timeout")
    assert m.is_target_unhealthy("https://example.com/page") is False


def test_is_target_unhealthy_true_above_threshold() -> None:
    m = RequestMetrics(max_target_failures=3, min_distinct_proxies=2)
    m.record_error("example.com", "proxy1", "timeout")
    m.record_error("example.com", "proxy2", "timeout")
    m.record_error("example.com", "proxy1", "timeout")
    assert m.is_target_unhealthy("https://example.com/page") is True


def test_is_target_unhealthy_requires_min_proxies() -> None:
    m = RequestMetrics(max_target_failures=3, min_distinct_proxies=2)
    m.record_error("example.com", "proxy1", "timeout")
    m.record_error("example.com", "proxy1", "timeout")
    m.record_error("example.com", "proxy1", "timeout")
    # 3 failures but only 1 proxy — should be healthy
    assert m.is_target_unhealthy("https://example.com/page") is False


def test_is_target_unhealthy_resets_on_success() -> None:
    m = RequestMetrics(max_target_failures=3, min_distinct_proxies=2)
    m.record_error("example.com", "proxy1", "timeout")
    m.record_error("example.com", "proxy2", "timeout")
    m.record_error("example.com", "proxy1", "timeout")
    assert m.is_target_unhealthy("https://example.com/page") is True
    # Success resets circuit breaker
    m.record_request("example.com", "proxy1", 200, 100.0)
    assert m.is_target_unhealthy("https://example.com/page") is False


# ---------------------------------------------------------------------------
# log_stats
# ---------------------------------------------------------------------------

def test_log_stats_returns_expected_structure() -> None:
    m = RequestMetrics()
    m.record_request("example.com", None, 200, 100.0)
    m.record_request("example.com", None, 200, 200.0)
    logger = logging.getLogger("test")
    stats = m.log_stats(logger)
    assert stats["total_requests"] == 2
    assert stats["total_errors"] == 0
    assert stats["error_rate_percent"] == 0.0
    assert "timing" in stats
    assert "by_domain" in stats
    assert "by_proxy" in stats


def test_log_stats_timing_percentiles() -> None:
    m = RequestMetrics()
    # Add 100 requests with known timing: 1, 2, ..., 100
    for i in range(1, 101):
        m.record_request("example.com", None, 200, float(i))
    logger = logging.getLogger("test")
    stats = m.log_stats(logger)
    timing = stats["timing"]
    # p_n = sorted_data[int(100 * pct)]: index 50->51, 95->96, 99->100
    assert timing["p50_ms"] == 51
    assert timing["p95_ms"] == 96
    assert timing["p99_ms"] == 100


def test_log_stats_empty() -> None:
    m = RequestMetrics()
    logger = logging.getLogger("test")
    stats = m.log_stats(logger)
    assert stats["total_requests"] == 0
    assert stats["timing"]["avg_ms"] == 0


def test_log_stats_with_errors() -> None:
    m = RequestMetrics()
    m.record_request("example.com", None, 200, 100.0)
    m.record_error("example.com", None, "timeout")
    m.record_error("example.com", None, "proxy")
    logger = logging.getLogger("test")
    stats = m.log_stats(logger)
    assert stats["total_errors"] == 2
    assert stats["error_breakdown"]["timeout"] == 1
    assert stats["error_breakdown"]["proxy"] == 1


# ---------------------------------------------------------------------------
# reservoir sampling bounds
# ---------------------------------------------------------------------------

def test_reservoir_sampling_bounded() -> None:
    m = RequestMetrics()
    for i in range(5000):
        m.record_request("example.com", None, 200, float(i))
    assert len(m._domain_timings["example.com"]) == RequestMetrics.RESERVOIR_SIZE


# ---------------------------------------------------------------------------
# thread safety
# ---------------------------------------------------------------------------

def test_thread_safety() -> None:
    m = RequestMetrics()
    n_threads = 10
    requests_per_thread = 100

    def worker(thread_id: int) -> None:
        for i in range(requests_per_thread):
            m.record_request("example.com", f"proxy{thread_id}", 200, float(i))

    with ThreadPoolExecutor(max_workers=n_threads) as pool:
        futures = [pool.submit(worker, t) for t in range(n_threads)]
        for f in futures:
            f.result()

    assert m.request_count == n_threads * requests_per_thread
