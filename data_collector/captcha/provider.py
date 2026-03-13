"""Base captcha provider interface.

BaseCaptchaProvider defines the abstract solve interface that all captcha
provider implementations must satisfy. Shared retry logic lives in the
base class; provider-specific API communication is delegated to subclasses.

Follows the same pattern as ProxyProvider/BrightDataProvider in the proxy
package.
"""

from __future__ import annotations

import logging
import time
import urllib.parse
from abc import ABC, abstractmethod
from collections.abc import Callable

from sqlalchemy import update

from data_collector.captcha.metrics import CaptchaMetrics
from data_collector.captcha.models import CaptchaError, CaptchaResult, CaptchaTaskType, CaptchaTimeout
from data_collector.enums.captcha import CaptchaErrorCategory, CaptchaSolveStatus
from data_collector.tables.captcha import CaptchaLog, CaptchaLogError
from data_collector.utilities.database.main import Database
from data_collector.utilities.request import Request

logger = logging.getLogger(__name__)


def _sanitize_url(url: str) -> str:
    """Strip query string and fragment from a URL, keeping scheme + host + path.

    Args:
        url: Full URL possibly containing query parameters and fragment.

    Returns:
        URL with only scheme, netloc, and path components.
    """
    parsed = urllib.parse.urlparse(url)
    return urllib.parse.urlunparse((parsed.scheme, parsed.netloc, parsed.path, "", "", ""))


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
        database: Optional Database instance for persisting CaptchaLog records.
        app_id: Application identifier for CaptchaLog records.
        runtime: Runtime identifier for CaptchaLog records.
    """

    def __init__(
        self,
        request: Request,
        *,
        timeout: int = 120,
        max_retries: int = 2,
        poll_interval: int = 5,
        metrics: CaptchaMetrics | None = None,
        database: Database | None = None,
        app_id: str | None = None,
        runtime: str | None = None,
    ) -> None:
        self.request = request
        self.timeout = timeout
        self.max_retries = max_retries
        self.poll_interval = poll_interval
        self.metrics = metrics
        self._database = database
        self._app_id = app_id
        self._runtime = runtime

    @property
    @abstractmethod
    def provider_name(self) -> str:
        """Machine-readable provider identifier for logging (e.g., ``"anti_captcha"``)."""
        ...

    @property
    def _log_enabled(self) -> bool:
        """Return True when all database logging prerequisites are available."""
        return self._database is not None and self._app_id is not None and self._runtime is not None

    # --- Abstract solve methods ---

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
    def solve_image(self, image_data: bytes, page_url: str) -> CaptchaResult:
        """Solve an image captcha (distorted text recognition).

        Args:
            image_data: Raw image bytes (PNG, JPEG, GIF, or BMP).
            page_url: URL of the page where the captcha was encountered.

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

    # --- Reporting methods ---

    def report_correct(self, task_id: str, task_type: CaptchaTaskType) -> bool:
        """Report that a captcha solution was verified as correct.

        Providers use this feedback to improve solve quality and may offer
        account credits. Must be called within the provider's reporting
        window (typically 60 seconds after solve completion).

        The default implementation returns False. Subclasses override for
        supported captcha types.

        Args:
            task_id: The provider task identifier to report.
            task_type: The captcha type that was solved.

        Returns:
            True if the report was accepted by the provider, False otherwise.
        """
        return False

    def report_incorrect(self, task_id: str, task_type: CaptchaTaskType) -> bool:
        """Report that a captcha solution was verified as incorrect.

        Providers use this feedback to improve solve quality and may refund
        the cost. Must be called within the provider's reporting window
        (typically 60 seconds after solve completion).

        The default implementation returns False. Subclasses override for
        supported captcha types.

        Args:
            task_id: The provider task identifier to report.
            task_type: The captcha type that was solved.

        Returns:
            True if the report was accepted by the provider, False otherwise.
        """
        return False

    # --- Shared retry logic ---

    def _create_and_poll(
        self,
        create_function: Callable[[], tuple[str, float]],
        task_type: CaptchaTaskType,
        poll_function: Callable[[str, CaptchaTaskType, float], CaptchaResult],
        page_url: str,
    ) -> CaptchaResult:
        """Retry loop for captcha task creation and polling.

        Orchestrates the create-task/poll-result cycle with retry on
        CaptchaTimeout. Records metrics at each stage when a CaptchaMetrics
        instance is available. Persists CaptchaLog records when database
        logging is enabled.

        Args:
            create_function: Callable that creates the task and returns
                ``(task_id, start_time)`` tuple. Provider subclasses pass
                their own ``_create_task`` wrapper here.
            task_type: The captcha type being solved (for result metadata).
            poll_function: Callable that polls for the result, receiving
                ``(task_id, task_type, start_time)`` and returning a
                CaptchaResult. Raises CaptchaTimeout if deadline exceeded.
            page_url: URL of the page where the captcha was encountered
                (sanitized before persistence).

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
                elapsed = time.monotonic() - start_time
                if self.metrics is not None:
                    self.metrics.record_timed_out()
                self._persist_log(
                    task_id=task_id,
                    task_type=task_type,
                    page_url=page_url,
                    status=CaptchaSolveStatus.TIMED_OUT,
                    elapsed_seconds=round(elapsed, 2),
                )
                logger.warning("Captcha task %s timed out (attempt %d/%d)", task_id, attempt + 1, 1 + self.max_retries)
                continue
            except CaptchaError as captcha_error:
                elapsed = time.monotonic() - start_time
                if self.metrics is not None:
                    self.metrics.record_failed()
                self._persist_log(
                    task_id=task_id,
                    task_type=task_type,
                    page_url=page_url,
                    status=CaptchaSolveStatus.FAILED,
                    elapsed_seconds=round(elapsed, 2),
                    error_category=captcha_error.category,
                    error_code=captcha_error.error_code,
                    error_description=captcha_error.error_description,
                )
                raise

            if self.metrics is not None:
                self.metrics.record_solved(result.cost)
            self._persist_log(
                task_id=task_id,
                task_type=task_type,
                page_url=page_url,
                status=CaptchaSolveStatus.SOLVED,
                cost=result.cost,
                elapsed_seconds=result.elapsed_seconds,
            )
            return result

        raise last_timeout  # type: ignore[misc]

    # --- Database logging ---

    def _persist_log(
        self,
        *,
        task_id: str,
        task_type: CaptchaTaskType,
        page_url: str,
        status: CaptchaSolveStatus,
        cost: float = 0.0,
        elapsed_seconds: float = 0.0,
        error_category: CaptchaErrorCategory | None = None,
        error_code: str | None = None,
        error_description: str | None = None,
    ) -> None:
        """Persist a CaptchaLog record (and CaptchaLogError if applicable).

        CaptchaLog stores solve metrics. If any error fields are present,
        a CaptchaLogError row is created in the same transaction with the
        provider error details.

        Never raises -- logging failures are logged as warnings but do not
        interrupt the captcha solve flow.

        Args:
            task_id: Provider's task identifier.
            task_type: Captcha type being solved.
            page_url: Page URL (sanitized before persistence).
            status: Solve outcome status.
            cost: Cost in USD (0.0 for failed/timed-out tasks).
            elapsed_seconds: Seconds elapsed before outcome.
            error_category: Provider-agnostic error classification (None when solved).
            error_code: Provider error code (None when solved).
            error_description: Human-readable error text (None when solved).
        """
        if not self._log_enabled:
            return

        try:
            assert self._database is not None  # noqa: S101 -- guarded by _log_enabled
            record = CaptchaLog(
                app_id=self._app_id,
                runtime=self._runtime,
                provider_name=self.provider_name,
                task_id=task_id,
                task_type=str(task_type),
                page_url=_sanitize_url(page_url),
                cost=cost,
                elapsed_seconds=elapsed_seconds,
                status=status.value,
            )
            with self._database.create_session() as session:
                session.add(record)
                has_error = error_code is not None or error_description is not None or error_category is not None
                if has_error:
                    session.flush()
                    error_record = CaptchaLogError(
                        captcha_log_id=record.id,
                        error_code=error_code,
                        error_description=error_description,
                        error_category=error_category.value if error_category is not None else None,
                    )
                    session.add(error_record)
                session.commit()
        except Exception:
            logger.warning("Failed to persist CaptchaLog for task %s", task_id, exc_info=True)

    def _update_log_correctness(self, task_id: str, is_correct: bool) -> None:
        """Update the is_correct flag on a CaptchaLog record.

        Called after a successful report_correct/report_incorrect API call.
        Never raises -- update failures are logged as warnings.

        Args:
            task_id: Provider's task identifier to update.
            is_correct: Whether the solution was verified as correct.
        """
        if not self._log_enabled:
            return

        try:
            assert self._database is not None  # noqa: S101 -- guarded by _log_enabled

            statement = (
                update(CaptchaLog)
                .where(CaptchaLog.task_id == task_id)
                .values(is_correct=is_correct)
            )
            with self._database.create_session() as session:
                session.execute(statement)
                session.commit()
        except Exception:
            logger.warning("Failed to update CaptchaLog correctness for task %s", task_id, exc_info=True)
