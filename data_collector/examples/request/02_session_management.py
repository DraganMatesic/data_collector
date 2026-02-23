"""Session management: headers, cookies, auth, and reset.

Demonstrates:
    - set_headers() — custom headers persist across requests
    - set_cookies() — cookie injection
    - set_auth() — HTTP basic authentication
    - reset_headers() / reset_cookies() — clearing session state
    - Session persistence across multiple requests

Run:
    python -m data_collector.examples.request.02_session_management
"""


from data_collector.utilities.request import Request


def main() -> None:
    """Run session state examples for headers, cookies, auth, and resets."""
    req = Request(timeout=10, retries=1)

    # --- Custom headers ---
    print("=== Custom headers ===")
    req.set_headers({
        "User-Agent": "DataCollector/0.2.0",
        "Accept": "application/json",
        "X-Custom-Header": "example-value",
    })
    resp = req.get("https://httpbin.org/headers")
    if resp:
        headers_sent = resp.json()["headers"]
        print(f"User-Agent: {headers_sent.get('User-Agent')}")
        print(f"X-Custom-Header: {headers_sent.get('X-Custom-Header')}")

    # --- Headers persist across requests ---
    print("\n=== Headers persist ===")
    resp = req.get("https://httpbin.org/headers")
    if resp:
        headers_sent = resp.json()["headers"]
        print(f"User-Agent still set: {headers_sent.get('User-Agent')}")

    # --- Cookies ---
    print("\n=== Cookies ===")
    req.set_cookies({"session_id": "abc123", "lang": "en"})
    resp = req.get("https://httpbin.org/cookies")
    if resp:
        cookies = resp.json()["cookies"]
        print(f"Cookies received by server: {cookies}")

    # --- HTTP Basic Auth ---
    print("\n=== Basic Auth ===")
    req.set_auth("testuser", "testpass")
    resp = req.get("https://httpbin.org/basic-auth/testuser/testpass")
    if resp:
        print(f"Auth status: {resp.status_code}")
        print(f"Authenticated: {resp.json().get('authenticated')}")

    # --- Reset session state ---
    print("\n=== After reset ===")
    req.reset_headers()
    req.reset_cookies()
    resp = req.get("https://httpbin.org/headers")
    if resp:
        headers_sent = resp.json()["headers"]
        has_custom = "X-Custom-Header" in headers_sent
        print(f"Custom header still present: {has_custom}")

    print(f"\nTotal requests: {req.request_count}")


if __name__ == "__main__":
    main()
