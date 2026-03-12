"""AntiCaptcha provider implementation.

Implements the BaseCaptchaProvider interface for the AntiCaptcha service
(anti-captcha.com). Communicates via REST endpoints: createTask,
getTaskResult, getBalance, and reporting methods.
"""

from __future__ import annotations

import base64
import json
import logging
import time
from typing import Any

from data_collector.captcha.metrics import CaptchaMetrics
from data_collector.captcha.models import CaptchaError, CaptchaResult, CaptchaTaskType, CaptchaTimeout
from data_collector.captcha.provider import BaseCaptchaProvider
from data_collector.enums.captcha import CaptchaErrorCategory
from data_collector.utilities.database.main import Database
from data_collector.utilities.request import Request

logger = logging.getLogger(__name__)

_BASE_URL = "https://api.anti-captcha.com"


class AntiCaptchaProvider(BaseCaptchaProvider):
    """AntiCaptcha captcha solving service provider.

    Implements all solve methods by building AntiCaptcha-specific task
    payloads and polling for results via the REST API. All HTTP
    communication uses the framework's Request class.

    Args:
        api_key: AntiCaptcha API key (clientKey).
        request: Request instance for HTTP communication.
        timeout: Maximum seconds to wait for a captcha solution.
        max_retries: Number of retries on CaptchaTimeout before giving up.
        poll_interval: Seconds between polling attempts for task result.
        metrics: Optional shared CaptchaMetrics instance for cost tracking.
        database: Optional Database instance for persisting CaptchaLog records.
        app_id: Application identifier for CaptchaLog records.
        runtime: Runtime identifier for CaptchaLog records.
    """

    _TASK_TYPE_MAP: dict[CaptchaTaskType, str] = {
        CaptchaTaskType.RECAPTCHA_V2: "RecaptchaV2TaskProxyless",
        CaptchaTaskType.RECAPTCHA_V2_PROXY: "RecaptchaV2Task",
        CaptchaTaskType.RECAPTCHA_V3: "RecaptchaV3TaskProxyless",
        CaptchaTaskType.TURNSTILE: "TurnstileTaskProxyless",
        CaptchaTaskType.TURNSTILE_PROXY: "TurnstileTask",
        CaptchaTaskType.IMAGE: "ImageToTextTask",
    }

    _SOLUTION_FIELD_MAP: dict[CaptchaTaskType, str] = {
        CaptchaTaskType.RECAPTCHA_V2: "gRecaptchaResponse",
        CaptchaTaskType.RECAPTCHA_V2_PROXY: "gRecaptchaResponse",
        CaptchaTaskType.RECAPTCHA_V3: "gRecaptchaResponse",
        CaptchaTaskType.TURNSTILE: "token",
        CaptchaTaskType.TURNSTILE_PROXY: "token",
        CaptchaTaskType.IMAGE: "text",
    }

    _ERROR_CATEGORY_MAP: dict[str, CaptchaErrorCategory] = {
        # AUTH -- bad key, suspended account, IP restrictions
        "ERROR_KEY_DOES_NOT_EXIST": CaptchaErrorCategory.AUTH,
        "ERROR_IP_NOT_ALLOWED": CaptchaErrorCategory.AUTH,
        "ERROR_IP_BLOCKED": CaptchaErrorCategory.AUTH,
        "ERROR_ACCOUNT_SUSPENDED": CaptchaErrorCategory.AUTH,
        # BALANCE -- zero or negative balance
        "ERROR_ZERO_BALANCE": CaptchaErrorCategory.BALANCE,
        # PROXY -- proxy connection, authentication, and compatibility errors
        "ERROR_PROXY_CONNECT_REFUSED": CaptchaErrorCategory.PROXY,
        "ERROR_PROXY_CONNECT_TIMEOUT": CaptchaErrorCategory.PROXY,
        "ERROR_PROXY_READ_TIMEOUT": CaptchaErrorCategory.PROXY,
        "ERROR_PROXY_BANNED": CaptchaErrorCategory.PROXY,
        "ERROR_PROXY_TRANSPARENT": CaptchaErrorCategory.PROXY,
        "ERROR_PROXY_HAS_NO_IMAGE_SUPPORT": CaptchaErrorCategory.PROXY,
        "ERROR_PROXY_INCOMPATIBLE_HTTP_VERSION": CaptchaErrorCategory.PROXY,
        "ERROR_PROXY_NOT_AUTHORISED": CaptchaErrorCategory.PROXY,
        # TASK -- unsupported task type, bad parameters, missing fields
        "ERROR_TASK_ABSENT": CaptchaErrorCategory.TASK,
        "ERROR_TASK_NOT_SUPPORTED": CaptchaErrorCategory.TASK,
        "ERROR_INCORRECT_SESSION_DATA": CaptchaErrorCategory.TASK,
        "ERROR_IMAGE_TYPE_NOT_SUPPORTED": CaptchaErrorCategory.TASK,
        "ERROR_ZERO_CAPTCHA_FILESIZE": CaptchaErrorCategory.TASK,
        "ERROR_TOO_BIG_CAPTCHA_FILESIZE": CaptchaErrorCategory.TASK,
        "ERROR_NO_SUCH_METHOD": CaptchaErrorCategory.TASK,
        # SOLVE -- unsolvable captcha, worker failures, invalid site configuration
        "ERROR_CAPTCHA_UNSOLVABLE": CaptchaErrorCategory.SOLVE,
        "ERROR_BAD_DUPLICATES": CaptchaErrorCategory.SOLVE,
        "ERROR_RECAPTCHA_TIMEOUT": CaptchaErrorCategory.SOLVE,
        "ERROR_RECAPTCHA_INVALID_SITEKEY": CaptchaErrorCategory.SOLVE,
        "ERROR_RECAPTCHA_INVALID_DOMAIN": CaptchaErrorCategory.SOLVE,
        "ERROR_RECAPTCHA_OLD_BROWSER": CaptchaErrorCategory.SOLVE,
        "ERROR_TOKEN_EXPIRED": CaptchaErrorCategory.SOLVE,
        "ERROR_VISIBLE_RECAPTCHA": CaptchaErrorCategory.SOLVE,
        "ERROR_ALL_WORKERS_FILTERED": CaptchaErrorCategory.SOLVE,
        "ERROR_FAILED_LOADING_WIDGET": CaptchaErrorCategory.SOLVE,
        "ERROR_TASK_CANCELED": CaptchaErrorCategory.SOLVE,
        "ERROR_NO_SUCH_CAPCHA_ID": CaptchaErrorCategory.SOLVE,
        "ERROR_TEMPLATE_NOT_FOUND": CaptchaErrorCategory.SOLVE,
        "ERROR_INVALID_KEY_TYPE": CaptchaErrorCategory.SOLVE,
        # RATE_LIMIT -- no available workers or slots
        "ERROR_NO_SLOT_AVAILABLE": CaptchaErrorCategory.RATE_LIMIT,
    }

    _RECAPTCHA_TYPES: frozenset[CaptchaTaskType] = frozenset({
        CaptchaTaskType.RECAPTCHA_V2,
        CaptchaTaskType.RECAPTCHA_V2_PROXY,
        CaptchaTaskType.RECAPTCHA_V3,
    })

    _REPORT_INCORRECT_MAP: dict[CaptchaTaskType, str] = {
        CaptchaTaskType.RECAPTCHA_V2: "/reportIncorrectRecaptcha",
        CaptchaTaskType.RECAPTCHA_V2_PROXY: "/reportIncorrectRecaptcha",
        CaptchaTaskType.RECAPTCHA_V3: "/reportIncorrectRecaptcha",
        CaptchaTaskType.IMAGE: "/reportIncorrectImageCaptcha",
    }

    def __init__(
        self,
        api_key: str,
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
        super().__init__(
            request,
            timeout=timeout,
            max_retries=max_retries,
            poll_interval=poll_interval,
            metrics=metrics,
            database=database,
            app_id=app_id,
            runtime=runtime,
        )
        self.api_key = api_key

    @property
    def provider_name(self) -> str:
        """Return the machine-readable provider identifier."""
        return "anti_captcha"

    # --- Public solve methods ---

    def solve_recaptcha_v2(self, site_key: str, page_url: str) -> CaptchaResult:
        """Solve a reCAPTCHA v2 challenge without proxy."""
        task_type = CaptchaTaskType.RECAPTCHA_V2
        task = {
            "type": self._TASK_TYPE_MAP[task_type],
            "websiteURL": page_url,
            "websiteKey": site_key,
        }
        return self._create_and_poll(
            create_function=lambda: self._create_task(task),
            task_type=task_type,
            poll_function=self._poll_result,
            page_url=page_url,
        )

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
        """Solve a reCAPTCHA v2 challenge using a proxy."""
        task_type = CaptchaTaskType.RECAPTCHA_V2_PROXY
        task = {
            "type": self._TASK_TYPE_MAP[task_type],
            "websiteURL": page_url,
            "websiteKey": site_key,
            **self._build_proxy_fields(proxy_type, proxy_address, proxy_port, proxy_login, proxy_password),
        }
        return self._create_and_poll(
            create_function=lambda: self._create_task(task),
            task_type=task_type,
            poll_function=self._poll_result,
            page_url=page_url,
        )

    def solve_recaptcha_v3(
        self,
        site_key: str,
        page_url: str,
        action: str,
        min_score: float = 0.3,
    ) -> CaptchaResult:
        """Solve a reCAPTCHA v3 challenge."""
        task_type = CaptchaTaskType.RECAPTCHA_V3
        task = {
            "type": self._TASK_TYPE_MAP[task_type],
            "websiteURL": page_url,
            "websiteKey": site_key,
            "minScore": min_score,
            "pageAction": action,
        }
        return self._create_and_poll(
            create_function=lambda: self._create_task(task),
            task_type=task_type,
            poll_function=self._poll_result,
            page_url=page_url,
        )

    def solve_turnstile(self, site_key: str, page_url: str) -> CaptchaResult:
        """Solve a Cloudflare Turnstile challenge without proxy."""
        task_type = CaptchaTaskType.TURNSTILE
        task = {
            "type": self._TASK_TYPE_MAP[task_type],
            "websiteURL": page_url,
            "websiteKey": site_key,
        }
        return self._create_and_poll(
            create_function=lambda: self._create_task(task),
            task_type=task_type,
            poll_function=self._poll_result,
            page_url=page_url,
        )

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
        """Solve a Cloudflare Turnstile challenge using a proxy."""
        task_type = CaptchaTaskType.TURNSTILE_PROXY
        task = {
            "type": self._TASK_TYPE_MAP[task_type],
            "websiteURL": page_url,
            "websiteKey": site_key,
            **self._build_proxy_fields(proxy_type, proxy_address, proxy_port, proxy_login, proxy_password),
        }
        return self._create_and_poll(
            create_function=lambda: self._create_task(task),
            task_type=task_type,
            poll_function=self._poll_result,
            page_url=page_url,
        )

    def solve_image(self, image_data: bytes, page_url: str) -> CaptchaResult:
        """Solve an image captcha (distorted text recognition)."""
        task_type = CaptchaTaskType.IMAGE
        encoded_body = base64.b64encode(image_data).decode("ascii")
        task = {
            "type": self._TASK_TYPE_MAP[task_type],
            "body": encoded_body,
        }
        return self._create_and_poll(
            create_function=lambda: self._create_task(task),
            task_type=task_type,
            poll_function=self._poll_result,
            page_url=page_url,
        )

    def get_balance(self) -> float:
        """Query the AntiCaptcha account balance.

        Returns:
            Account balance in USD.

        Raises:
            CaptchaError: If the API returns an error response.
        """
        payload = {"clientKey": self.api_key}
        response_data = self._post_api("/getBalance", payload)
        return float(response_data["balance"])

    # --- Reporting methods ---

    def report_correct(self, task_id: str, task_type: CaptchaTaskType) -> bool:
        """Report that a reCAPTCHA solution was verified as correct.

        Only supported for reCAPTCHA v2/v3 types. Must be called within
        60 seconds of task completion.

        Args:
            task_id: The provider task identifier to report.
            task_type: The captcha type that was solved.

        Returns:
            True if the report was accepted, False if unsupported or failed.
        """
        if task_type not in self._RECAPTCHA_TYPES:
            return False

        try:
            self._post_api("/reportCorrectRecaptcha", {
                "clientKey": self.api_key,
                "taskId": int(task_id),
            })
            self._update_log_correctness(task_id, is_correct=True)
            return True
        except CaptchaError:
            logger.warning("Failed to report correct for task %s", task_id, exc_info=True)
            return False

    def report_incorrect(self, task_id: str, task_type: CaptchaTaskType) -> bool:
        """Report that a captcha solution was verified as incorrect.

        Supported for reCAPTCHA v2/v3 and image captcha types. Must be
        called within 60 seconds of task completion.

        Args:
            task_id: The provider task identifier to report.
            task_type: The captcha type that was solved.

        Returns:
            True if the report was accepted, False if unsupported or failed.
        """
        endpoint = self._REPORT_INCORRECT_MAP.get(task_type)
        if endpoint is None:
            return False

        try:
            self._post_api(endpoint, {
                "clientKey": self.api_key,
                "taskId": int(task_id),
            })
            self._update_log_correctness(task_id, is_correct=False)
            return True
        except CaptchaError:
            logger.warning("Failed to report incorrect for task %s", task_id, exc_info=True)
            return False

    # --- Private API methods ---

    def _post_api(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        """POST to an AntiCaptcha API endpoint and return the parsed response.

        Args:
            path: API path (e.g., "/createTask").
            payload: JSON request body.

        Returns:
            Parsed JSON response as a dictionary.

        Raises:
            CaptchaError: If the response contains a non-zero errorId,
                or if the HTTP request itself fails.
        """
        url = f"{_BASE_URL}{path}"
        response = self.request.post(url, json=payload)

        if response is None:
            raise CaptchaError(
                error_id=-1,
                error_code="REQUEST_FAILED",
                error_description=f"HTTP request to {url} returned None",
            )

        try:
            response_data: dict[str, Any] = response.json()
        except (json.JSONDecodeError, ValueError) as decode_error:
            raise CaptchaError(
                error_id=-1,
                error_code="INVALID_RESPONSE",
                error_description=f"Non-JSON response from {url} (HTTP {response.status_code})",
            ) from decode_error

        error_id = response_data.get("errorId", 0)

        if error_id != 0:
            error_code = response_data.get("errorCode", "UNKNOWN")
            category = self._ERROR_CATEGORY_MAP.get(error_code, CaptchaErrorCategory.UNKNOWN)
            raise CaptchaError(
                error_id=error_id,
                error_code=error_code,
                error_description=response_data.get("errorDescription", "Unknown error"),
                category=category,
            )

        return response_data

    def _create_task(self, task: dict[str, Any]) -> tuple[str, float]:
        """Submit a captcha task to the AntiCaptcha API.

        Args:
            task: Task object with type-specific fields.

        Returns:
            Tuple of (task_id as string, start_time from time.monotonic()).

        Raises:
            CaptchaError: If the API returns an error response.
        """
        payload = {
            "clientKey": self.api_key,
            "task": task,
        }
        start_time = time.monotonic()
        response_data = self._post_api("/createTask", payload)
        task_id = str(response_data["taskId"])
        logger.debug("Created captcha task %s (type: %s)", task_id, task.get("type"))
        return task_id, start_time

    def _poll_result(
        self,
        task_id: str,
        task_type: CaptchaTaskType,
        start_time: float,
    ) -> CaptchaResult:
        """Poll for a captcha task result until ready or timeout.

        Args:
            task_id: The task identifier returned by _create_task.
            task_type: The captcha type being solved.
            start_time: Monotonic timestamp from task creation.

        Returns:
            CaptchaResult with the solution.

        Raises:
            CaptchaTimeout: If polling exceeds self.timeout seconds.
            CaptchaError: If the API returns an error during polling.
        """
        payload = {
            "clientKey": self.api_key,
            "taskId": int(task_id),
        }

        while True:
            elapsed = time.monotonic() - start_time
            if elapsed >= self.timeout:
                raise CaptchaTimeout(task_id=task_id, timeout_seconds=self.timeout)

            time.sleep(self.poll_interval)

            response_data = self._post_api("/getTaskResult", payload)
            status = response_data.get("status", "")

            if status == "processing":
                continue

            if status == "ready":
                solution_data: dict[str, Any] = response_data.get("solution", {})
                solution_text = self._extract_solution(solution_data, task_type)
                cost = float(response_data.get("cost", "0"))
                total_elapsed = time.monotonic() - start_time

                return CaptchaResult(
                    task_id=task_id,
                    task_type=task_type,
                    solution=solution_text,
                    cost=cost,
                    elapsed_seconds=round(total_elapsed, 2),
                )

    def _extract_solution(self, solution_data: dict[str, Any], task_type: CaptchaTaskType) -> str:
        """Extract the solution string from an AntiCaptcha response.

        Args:
            solution_data: The "solution" object from getTaskResult response.
            task_type: The captcha type, used to select the correct field.

        Returns:
            The solution string (token or recognized text).
        """
        field_name = self._SOLUTION_FIELD_MAP[task_type]
        return str(solution_data.get(field_name, ""))

    @staticmethod
    def _build_proxy_fields(
        proxy_type: str,
        proxy_address: str,
        proxy_port: int,
        proxy_login: str,
        proxy_password: str,
    ) -> dict[str, str | int]:
        """Build the proxy fields dict for proxy-enabled task types.

        Args:
            proxy_type: Proxy protocol ("http", "socks4", or "socks5").
            proxy_address: Proxy IP address (IPv4 or IPv6).
            proxy_port: Proxy port number.
            proxy_login: Proxy authentication username.
            proxy_password: Proxy authentication password.

        Returns:
            Dictionary of proxy fields for the AntiCaptcha task payload.
        """
        return {
            "proxyType": proxy_type,
            "proxyAddress": proxy_address,
            "proxyPort": proxy_port,
            "proxyLogin": proxy_login,
            "proxyPassword": proxy_password,
        }
