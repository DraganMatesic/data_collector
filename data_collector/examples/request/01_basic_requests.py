"""Basic synchronous GET and POST requests with response helpers.

Demonstrates:
    - Request constructor with transport config
    - Synchronous GET and POST
    - Response helpers: get_json(), get_content(), get_content_length()
    - Request count tracking

Run:
    python -m data_collector.examples.request.01_basic_requests
"""


from data_collector.utilities.request import Request


def main() -> None:
    """Run basic sync GET/POST examples and response helper usage."""
    req = Request(timeout=10, retries=2, backoff_factor=2)

    # --- GET request returning JSON ---
    print("=== GET https://httpbin.org/get ===")
    resp = req.get("https://httpbin.org/get")
    if resp:
        print(f"Status: {resp.status_code}")
        data = req.get_json()
        payload = data if isinstance(data, dict) else {}
        print(f"Origin IP: {payload.get('origin', '<missing>')}")
        print(f"URL: {payload.get('url', '<missing>')}")

    # --- POST request with JSON body ---
    print("\n=== POST https://httpbin.org/post ===")
    resp = req.post("https://httpbin.org/post", json={"name": "data_collector", "version": "0.2.0"})
    if resp:
        print(f"Status: {resp.status_code}")
        data = req.get_json()
        payload = data if isinstance(data, dict) else {}
        print(f"Echoed JSON: {payload.get('json', '<missing>')}")

    # --- Response helpers ---
    print("\n=== Response helpers ===")
    resp = req.get("https://httpbin.org/bytes/256")
    if resp:
        content = req.get_content()
        length = req.get_content_length()
        content_size = len(content) if content is not None else 0
        print(f"Content length header: {length}")
        print(f"Actual content bytes: {content_size}")

    # --- Request count ---
    print(f"\nTotal requests made: {req.request_count}")


if __name__ == "__main__":
    main()
