"""Base captcha provider interface.

BaseCaptchaProvider defines the abstract solve interface that all captcha
provider implementations must satisfy. Shared retry logic lives in the
base class; provider-specific API communication is delegated to subclasses.

Follows the same pattern as ProxyProvider/BrightDataProvider in the proxy
package.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from collections.abc import Callable

from data_collector.captcha.metrics import CaptchaMetrics
from data_collector.captcha.models import CaptchaResult, CaptchaTaskType, CaptchaTimeout
from data_collector.utilities.request import Request

logger = logging.getLogger(__name__)


class BaseCaptchaProvider(ABC):
    """Abstract interface for captcha solving service providers.

    All major captcha services (AntiCaptcha, 2Captcha, CapSolver, CapMonster)
    use the same create-task/poll-result REST pattern. This interface abstracts
    the solve methods; each provider implements the API-specific communication.

    Args:
        request: Request instance for HTTP communication with the provider API.
        timeout: Maximum seconds to wait for a captcha solution.
        max_retries: Number of retries on CaptchaTimeout before giving up.
        poll_interval: Seconds between polling attempts for task result.
        metrics: Optional shared CaptchaMetrics instance for cost tracking.
    """

    def __init__(
        self,
        request: Request,
        *,
        timeout: int = 120,
        max_retries: int = 2,
        poll_interval: int = 5,
        metrics: CaptchaMetrics | None = None,
    ) -> None:
        self.request = request
        self.timeout = timeout
        self.max_retries = max_retries
        self.poll_interval = poll_interval
        self.metrics = metrics

    @abstractmethod
    def solve_recaptcha_v2(self, site_key: str, page_url: str) -> CaptchaResult:
        """Solve a reCAPTCHA v2 challenge without proxy.

        Args:
            site_key: The reCAPTCHA site key from the target page.
            page_url: URL of the page where the captcha appears.

        Returns:
            CaptchaResult with the solution token.

        Raises:
            CaptchaError: If the provider returns an API error.
            CaptchaTimeout: If polling exceeds the timeout after all retries.
        """
        ...

    @abstractmethod
    def solve_recaptcha_v2_proxy(
        self,
        site_key: str,
        page_url: str,
        proxy_type: str,
        proxy_address: str,
        proxy_port: int,
        proxy_login: str,
        proxy_password: str,
    ) -> CaptchaResult:
        """Solve a reCAPTCHA v2 challenge using a proxy.

        The proxy is used by the captcha service worker, not by this client.
        Use this when the target site serves different captcha challenges
        based on the visitor's IP address.

        Args:
            site_key: The reCAPTCHA site key from the target page.
            page_url: URL of the page where the captcha appears.
            proxy_type: Proxy protocol ("http", "socks4", or "socks5").
            proxy_address: Proxy IP address (IPv4 or IPv6).
            proxy_port: Proxy port number.
            proxy_login: Proxy authentication username.
            proxy_password: Proxy authentication password.

        Returns:
            CaptchaResult with the solution token.

        Raises:
            CaptchaError: If the provider returns an API error.
            CaptchaTimeout: If polling exceeds the timeout after all retries.
        """
        ...

    @abstractmethod
    def solve_recaptcha_v3(
        self,
        site_key: str,
        page_url: str,
        action: str,
        min_score: float = 0.3,
    ) -> CaptchaResult:
        """Solve a reCAPTCHA v3 challenge.

        Args:
            site_key: The reCAPTCHA site key from the target page.
            page_url: URL of the page where the captcha appears.
            action: The action value from ``grecaptcha.execute()``.
            min_score: Minimum acceptable score (0.3, 0.7, or 0.9).

        Returns:
            CaptchaResult with the solution token.

        Raises:
            CaptchaError: If the provider returns an API error.
            CaptchaTimeout: If polling exceeds the timeout after all retries.
        """
        ...

    @abstractmethod
    def solve_turnstile(self, site_key: str, page_url: str) -> CaptchaResult:
        """Solve a Cloudflare Turnstile challenge without proxy.

        Args:
            site_key: The Turnstile site key from the target page.
            page_url: URL of the page where the captcha appears.

        Returns:
            CaptchaResult with the solution token.

        Raises:
            CaptchaError: If the provider returns an API error.
            CaptchaTimeout: If polling exceeds the timeout after all retries.
        """
        ...

    @abstractmethod
    def solve_turnstile_proxy(
        self,
        site_key: str,
        page_url: str,
        proxy_type: str,
        proxy_address: str,
        proxy_port: int,
        proxy_login: str,
        proxy_password: str,
    ) -> CaptchaResult:
        """Solve a Cloudflare Turnstile challenge using a proxy.

        Args:
            site_key: The Turnstile site key from the target page.
            page_url: URL of the page where the captcha appears.
            proxy_type: Proxy protocol ("http", "socks4", or "socks5").
            proxy_address: Proxy IP address (IPv4 or IPv6).
            proxy_port: Proxy port number.
            proxy_login: Proxy authentication username.
            proxy_password: Proxy authentication password.

        Returns:
            CaptchaResult with the solution token.

        Raises:
            CaptchaError: If the provider returns an API error.
            CaptchaTimeout: If polling exceeds the timeout after all retries.
        """
        ...

    @abstractmethod
    def solve_image(self, image_data: bytes) -> CaptchaResult:
        """Solve an image captcha (distorted text recognition).

        Args:
            image_data: Raw image bytes (PNG, JPEG, GIF, or BMP).

        Returns:
            CaptchaResult with the recognized text.

        Raises:
            CaptchaError: If the provider returns an API error.
            CaptchaTimeout: If polling exceeds the timeout after all retries.
        """
        ...

    @abstractmethod
    def get_balance(self) -> float:
        """Query the provider account balance.

        Returns:
            Account balance in USD.

        Raises:
            CaptchaError: If the provider returns an API error.
        """
        ...

    def _create_and_poll(
        self,
        create_function: Callable[[], tuple[str, float]],
        task_type: CaptchaTaskType,
        poll_function: Callable[[str, CaptchaTaskType, float], CaptchaResult],
    ) -> CaptchaResult:
        """Retry loop for captcha task creation and polling.

        Orchestrates the create-task/poll-result cycle with retry on
        CaptchaTimeout. Records metrics at each stage when a CaptchaMetrics
        instance is available.

        Args:
            create_function: Callable that creates the task and returns
                ``(task_id, start_time)`` tuple. Provider subclasses pass
                their own ``_create_task`` wrapper here.
            task_type: The captcha type being solved (for result metadata).
            poll_function: Callable that polls for the result, receiving
                ``(task_id, task_type, start_time)`` and returning a
                CaptchaResult. Raises CaptchaTimeout if deadline exceeded.

        Returns:
            CaptchaResult from a successful solve.

        Raises:
            CaptchaError: If the provider returns an API error.
            CaptchaTimeout: If all retry attempts are exhausted.
        """
        last_timeout: CaptchaTimeout | None = None

        for attempt in range(1 + self.max_retries):
            if attempt > 0:
                logger.info("Captcha retry %d/%d for task type %s", attempt, self.max_retries, task_type)

            if self.metrics is not None:
                self.metrics.record_submitted()

            task_id, start_time = create_function()

            try:
                result = poll_function(task_id, task_type, start_time)
            except CaptchaTimeout as timeout_error:
                last_timeout = timeout_error
                if self.metrics is not None:
                    self.metrics.record_timed_out()
                logger.warning("Captcha task %s timed out (attempt %d/%d)", task_id, attempt + 1, 1 + self.max_retries)
                continue

            if self.metrics is not None:
                self.metrics.record_solved(result.cost)
            return result

        raise last_timeout  # type: ignore[misc]
