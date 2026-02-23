"""Asynchronous HTTP requests with async_get and async_post.

Demonstrates:
    - async_get() — asynchronous GET via httpx.AsyncClient
    - async_post() — asynchronous POST
    - Concurrent async requests with asyncio.gather
    - Error handling in async context

Run:
    python -m data_collector.examples.request.05_async_requests
"""


import asyncio
import time

from data_collector.utilities.request import Request


async def main() -> None:
    """Run async GET/POST and concurrent-request examples."""
    req = Request(timeout=10, retries=1)

    # --- Basic async GET ---
    print("=== Async GET ===")
    resp = await req.async_get("https://httpbin.org/get")
    if resp:
        data = req.get_json()
        print(f"Status: {resp.status_code}")
        payload = data if isinstance(data, dict) else {}
        print(f"URL: {payload.get('url', '<missing>')}")

    # --- Basic async POST ---
    print("\n=== Async POST ===")
    resp = await req.async_post("https://httpbin.org/post", json={"async": True})
    if resp:
        data = req.get_json()
        payload = data if isinstance(data, dict) else {}
        print(f"Echoed JSON: {payload.get('json', '<missing>')}")

    # --- Concurrent requests (each with its own Request instance) ---
    print("\n=== Concurrent async requests ===")
    urls = [
        "https://httpbin.org/delay/1",
        "https://httpbin.org/delay/1",
        "https://httpbin.org/delay/1",
    ]

    async def fetch(url: str, idx: int) -> str:
        r = Request(timeout=10, retries=0)
        resp = await r.async_get(url)
        status = resp.status_code if resp else "error"
        return f"Request {idx}: {status}"

    start = time.monotonic()
    results = await asyncio.gather(*[fetch(url, i) for i, url in enumerate(urls)])
    elapsed = time.monotonic() - start

    for result in results:
        print(f"  {result}")
    print(f"Elapsed: {elapsed:.1f}s (3 x 1s delays run concurrently)")

    # --- Async error handling ---
    print("\n=== Async timeout ===")
    slow_req = Request(timeout=1, retries=0)
    resp = await slow_req.async_get("https://httpbin.org/delay/3")
    print(f"Response: {resp}")
    print(f"is_timeout: {slow_req.is_timeout()}")
    print(f"timeout_err: {slow_req.timeout_err}")


if __name__ == "__main__":
    asyncio.run(main())
