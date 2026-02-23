"""Error handling, introspection, and abort decisions.

Demonstrates:
    - Error introspection: is_blocked(), is_timeout(), is_server_down()
    - should_abort() — centralized abort logic
    - ExceptionDescriptor — direct error inspection
    - Error counters (timeout_err, bad_status_code_err, etc.)

Run:
    python -m data_collector.examples.request.03_error_handling
"""


import logging

from data_collector.utilities.request import Request

logging.basicConfig(level=logging.DEBUG, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def main() -> None:
    """Run error and abort-decision examples for Request behavior."""
    req = Request(timeout=10, retries=0)

    # --- 404 Not Found (no retry) ---
    print("=== 404 response ===")
    resp = req.get("https://httpbin.org/status/404")
    print(f"Status: {resp.status_code if resp else 'None'}")
    print(f"has_errors: {req.has_errors()}")
    print(f"is_blocked: {req.is_blocked()}")

    # --- 403 Forbidden (blocked) ---
    print("\n=== 403 response (blocked) ===")
    resp = req.get("https://httpbin.org/status/403")
    print(f"Status: {resp.status_code if resp else 'None'}")
    print(f"is_blocked: {req.is_blocked()}")
    print(f"should_abort(proxy_on=True): {req.should_abort(logger, proxy_on=True)}")

    # --- 500 Server Error ---
    print("\n=== 500 response (server down) ===")
    resp = req.get("https://httpbin.org/status/500")
    print(f"is_server_down: {req.is_server_down()}")
    print(f"should_abort: {req.should_abort(logger)}")

    # --- Timeout (very short timeout) ---
    print("\n=== Timeout (1s timeout on 3s delay) ===")
    slow_req = Request(timeout=1, retries=0)
    resp = slow_req.get("https://httpbin.org/delay/3")
    print(f"Response: {resp}")
    print(f"is_timeout: {slow_req.is_timeout()}")
    print(f"should_abort: {slow_req.should_abort(logger)}")

    # --- ExceptionDescriptor inspection ---
    print("\n=== ExceptionDescriptor ===")
    last_err = slow_req.exception_descriptor.get_last_error()
    print(f"Last error: {last_err}")
    all_timeouts = slow_req.exception_descriptor.get_errors_by_type("timeout")
    print(f"Timeout errors: {len(all_timeouts)}")

    # --- Error counters ---
    print("\n=== Error counters ===")
    print(f"req.bad_status_code_err: {req.bad_status_code_err}")
    print(f"slow_req.timeout_err: {slow_req.timeout_err}")

    # --- should_abort in a collection loop ---
    print("\n=== Collection loop with should_abort ===")
    loop_req = Request(timeout=10, retries=0)
    urls = [
        "https://httpbin.org/get",
        "https://httpbin.org/status/500",
        "https://httpbin.org/get",  # This will be skipped
    ]
    for url in urls:
        resp = loop_req.get(url)
        if loop_req.should_abort(logger):
            print(f"  Aborted at: {url}")
            break
        status = resp.status_code if resp else "None"
        print(f"  OK: {url} -> {status}")


if __name__ == "__main__":
    main()
