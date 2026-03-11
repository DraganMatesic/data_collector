"""Proxy judge verification for IP address discovery."""

import logging
import re

from data_collector.utilities.request import Request

logger = logging.getLogger(__name__)

PROXY_JUDGES: list[str] = [
    "https://httpbin.org/ip",
    "https://api.ipify.org?format=json",
    "https://ifconfig.me/ip",
]

_IPV4_PATTERN = re.compile(r"\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}")

_IPV6_PATTERN = re.compile(
    r"(?:[0-9a-fA-F]{1,4}:){2,7}[0-9a-fA-F]{1,4}"
    r"|(?:[0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}"
    r"|::(?:[0-9a-fA-F]{1,4}:){0,5}[0-9a-fA-F]{1,4}"
)

_MAX_JUDGE_RESPONSE_LENGTH = 365


def extract_ip(content: bytes) -> str | None:
    """Extract an IP address from proxy judge response content.

    Handles JSON responses (e.g., ``{"origin": "1.2.3.4"}``) and plain
    text responses (e.g., ``1.2.3.4\\n``). Tries IPv4 first, then IPv6.

    Args:
        content: Raw response body from a proxy judge endpoint.

    Returns:
        IP address string, or None if no valid IP found.
    """
    text = content.decode("utf-8", errors="replace")
    ipv4_match = _IPV4_PATTERN.search(text)
    if ipv4_match:
        return ipv4_match.group(0)
    ipv6_match = _IPV6_PATTERN.search(text)
    if ipv6_match:
        return ipv6_match.group(0)
    return None


def verify_ip(proxy_url: str, judges: list[str]) -> str | None:
    """Verify the IP address assigned by a proxy provider via judge endpoints.

    Iterates through proxy judge URLs with failover. Each judge is queried
    through the proxy, and the response is parsed for an IP address. A sanity
    check on response length prevents parsing HTML error pages.

    Args:
        proxy_url: Proxy URL string to route requests through.
        judges: Ordered list of judge endpoint URLs to try.

    Returns:
        Verified IP address string, or None if all judges failed.
    """
    request = Request(timeout=10, retries=1)
    request.set_proxy(proxy_url)

    for judge_url in judges:
        try:
            response = request.get(judge_url)
            if response is None:
                continue
            if len(response.content) >= _MAX_JUDGE_RESPONSE_LENGTH:
                logger.debug("Judge response too large, skipping", extra={"judge_url": judge_url})
                continue
            ip_address = extract_ip(response.content)
            if ip_address is not None:
                return ip_address
        except Exception:
            logger.debug("Judge verification failed", extra={"judge_url": judge_url}, exc_info=True)
            continue
    return None
