"""Proxy data models, exceptions, and domain utilities."""

from dataclasses import dataclass

import tldextract


@dataclass(frozen=True)
class ProxyData:
    """Centralized proxy configuration passed to providers and scrapers.

    Args:
        host: Gateway hostname (e.g., "zproxy.lum-superproxy.io").
        port: Gateway port (e.g., 22225).
        username: Provider account username.
        password: Provider account password.
        country: ISO country code for geo-targeting (e.g., "hr", "us", "de").
        protocol: Proxy protocol (http, https, socks5).
    """

    host: str
    port: int
    username: str
    password: str
    country: str | None = None
    protocol: str = "http"


@dataclass(frozen=True)
class Proxy:
    """Acquired proxy with verified IP and reservation metadata.

    Args:
        url: Proxy URL string ready for Request.set_proxy().
        ip_address: Verified IP address from proxy judge.
        session_id: Provider session ID for debugging and logging.
        target_domain: Full target domain (including subdomain) this proxy is used for.
    """

    url: str
    ip_address: str
    session_id: str
    target_domain: str


class ProxyAcquisitionTimeout(Exception):
    """Raised when no unique proxy could be acquired within the timeout period."""


def extract_root_domain(domain: str) -> str:
    """Extract the registered root domain from a full domain string.

    Uses the Mozilla Public Suffix List via tldextract to handle TLD
    edge cases like .gov.de, .co.uk, .com.au correctly.

    Args:
        domain: Full domain string (e.g., "sub.gov.de", "api.example.co.uk").

    Returns:
        Registered root domain (e.g., "gov.de", "example.co.uk").

    Raises:
        ValueError: If domain cannot be parsed into a valid root domain.
    """
    result = tldextract.extract(domain)
    root_domain = result.top_domain_under_public_suffix
    if not root_domain:
        raise ValueError(f"Cannot extract root domain from '{domain}'")
    return root_domain
