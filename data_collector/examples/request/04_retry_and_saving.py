"""Retry with exponential backoff and response saving.

Demonstrates:
    - Automatic retry on 5xx status codes
    - Exponential backoff timing
    - save_responses=True for automatic response saving
    - save_html() for manual response saving
    - retry_on_status configuration

Run:
    python -m data_collector.examples.request.04_retry_and_saving
"""


import tempfile
from pathlib import Path

from data_collector.utilities.request import Request


def main() -> None:
    """Run retry and response-saving examples."""
    # --- Retry on 500 (will exhaust retries) ---
    print("=== Retry on 500 (retries=2, backoff_factor=1) ===")
    req = Request(timeout=10, retries=2, backoff_factor=1, retry_on_status=[500, 502, 503])
    resp = req.get("https://httpbin.org/status/500")
    print(f"Final response: {resp.status_code if resp else 'None'}")
    print(f"bad_status_code_err: {req.bad_status_code_err}")
    print("(Retried 2 times with 1s, 1s backoff before giving up)")

    # --- Auto-save responses ---
    print("\n=== Auto-save responses ===")
    with tempfile.TemporaryDirectory() as save_dir:
        req = Request(timeout=10, retries=0, save_responses=True, save_dir=save_dir)
        req.get("https://httpbin.org/html")
        req.get("https://httpbin.org/json")

        saved = list(Path(save_dir).iterdir())
        print(f"Saved {len(saved)} response(s):")
        for f in sorted(saved):
            print(f"  {f.name} ({f.stat().st_size} bytes)")

    # --- Manual save_html ---
    print("\n=== Manual save_html ===")
    with tempfile.TemporaryDirectory() as save_dir:
        req = Request(timeout=10, retries=0)
        req.get("https://httpbin.org/html")
        save_path = str(Path(save_dir) / "page.html")
        req.save_html(save_path)
        size = Path(save_path).stat().st_size
        print(f"Saved to: {save_path}")
        print(f"File size: {size} bytes")

    # --- Custom retry_on_status ---
    print("\n=== Custom retry_on_status (only 429) ===")
    req = Request(timeout=10, retries=1, backoff_factor=1, retry_on_status=[429])
    resp = req.get("https://httpbin.org/status/500")
    print(f"500 with retry_on_status=[429]: status={resp.status_code if resp else 'None'}")
    print(f"bad_status_code_err: {req.bad_status_code_err}")
    print("(No retry because 500 is not in retry_on_status)")


if __name__ == "__main__":
    main()
