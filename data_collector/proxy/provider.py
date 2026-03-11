"""Proxy provider interface and implementations."""

import logging
import uuid
from abc import ABC, abstractmethod

from data_collector.proxy.judges import PROXY_JUDGES, verify_ip
from data_collector.proxy.models import ProxyData

logger = logging.getLogger(__name__)


class ProxyProvider(ABC):
    """Abstract interface for proxy service providers.

    All residential proxy services (BrightData, Oxylabs, Smartproxy, NetNut)
    use the same super-proxy gateway model. The only difference is the proxy
    URL format. This interface abstracts that.

    Args:
        proxy_data: Centralized proxy configuration.
    """

    def __init__(self, proxy_data: ProxyData) -> None:
        self.proxy_data = proxy_data

    @abstractmethod
    def build_proxy_url(self, session_id: str) -> str:
        """Build a proxy URL string for a given session ID.

        Args:
            session_id: Unique session identifier for IP pinning.

        Returns:
            Proxy URL string (e.g., "http://user:pass@host:port") ready
            for Request.set_proxy().
        """
        ...

    def is_healthy(self) -> bool:
        """Check if the proxy provider infrastructure is available.

        Default implementation generates a test session, builds a proxy URL,
        and attempts IP verification via the first proxy judge. Subclasses
        can override with provider-specific health APIs.

        Returns:
            True if a request through the proxy succeeds, False otherwise.
        """
        test_session_id = uuid.uuid4().hex
        proxy_url = self.build_proxy_url(test_session_id)
        ip_address = verify_ip(proxy_url, PROXY_JUDGES[:1])
        return ip_address is not None


class BrightDataProvider(ProxyProvider):
    """BrightData residential proxy provider.

    Constructs proxy URLs in BrightData's super-proxy format with optional
    country-level geo-targeting and session pinning.
    """

    def build_proxy_url(self, session_id: str) -> str:
        """Build BrightData proxy URL with session pinning.

        Args:
            session_id: Session identifier for IP pinning on the provider side.

        Returns:
            Proxy URL string with BrightData-specific format.
        """
        username = self.proxy_data.username
        if self.proxy_data.country:
            username = f"{username}-country-{self.proxy_data.country}"
        return (
            f"{self.proxy_data.protocol}://{username}-session-{session_id}"
            f":{self.proxy_data.password}@{self.proxy_data.host}:{self.proxy_data.port}"
        )
