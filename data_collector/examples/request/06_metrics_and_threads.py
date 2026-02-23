"""RequestMetrics with multi-threaded collection and circuit breaker.

Demonstrates:
    - RequestMetrics as a shared thread-safe collector
    - ThreadPoolExecutor with per-thread Request instances
    - log_stats() — aggregated statistics with timing percentiles
    - is_target_unhealthy() — circuit breaker pattern
    - Per-domain and per-proxy breakdown

Run:
    python -m data_collector.examples.request.06_metrics_and_threads
"""


import json
import logging
from concurrent.futures import ThreadPoolExecutor

from data_collector.utilities.request import Request, RequestMetrics

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def main() -> None:
    """Run multi-threaded metrics collection and circuit-breaker examples."""
    # --- Shared metrics collector ---
    metrics = RequestMetrics(max_target_failures=3, min_distinct_proxies=2)

    # --- Multi-threaded collection ---
    print("=== Multi-threaded collection (5 threads, 3 URLs each) ===")
    urls = [
        "https://httpbin.org/get",
        "https://httpbin.org/json",
        "https://httpbin.org/headers",
    ]

    def worker(thread_id: int) -> None:
        req = Request(timeout=10, retries=1, metrics=metrics)
        req.set_headers({"User-Agent": f"DataCollector-Worker-{thread_id}"})
        for url in urls:
            req.get(url)
            if req.should_abort(logger):
                return

    with ThreadPoolExecutor(max_workers=5) as pool:
        futures = [pool.submit(worker, i) for i in range(5)]
        for f in futures:
            f.result()

    # --- Aggregated stats ---
    print("\n=== Aggregated statistics ===")
    stats = metrics.log_stats(logger)
    print(json.dumps(stats, indent=2))

    # --- Circuit breaker demo ---
    print("\n=== Circuit breaker demo ===")
    cb_metrics = RequestMetrics(max_target_failures=2, min_distinct_proxies=1)

    # Simulate failures on a target
    print("Simulating 2 failures on example.com from proxy1...")
    cb_metrics.record_error("example.com", "proxy1", "timeout")
    cb_metrics.record_error("example.com", "proxy1", "timeout")

    req = Request(timeout=10, retries=0, metrics=cb_metrics)
    healthy = req.is_target_unhealthy("https://example.com/data")
    print(f"is_target_unhealthy: {healthy}")

    # Simulate recovery
    print("\nSimulating successful request (circuit breaker resets)...")
    cb_metrics.record_request("example.com", "proxy1", 200, 150.0)
    healthy = req.is_target_unhealthy("https://example.com/data")
    print(f"is_target_unhealthy after success: {healthy}")


if __name__ == "__main__":
    main()
