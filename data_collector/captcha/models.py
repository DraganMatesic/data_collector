"""Captcha data models, enums, and exception types.

Provides provider-agnostic types shared across all captcha provider
implementations. CaptchaTaskType identifiers are logical names that
each provider maps to its own API-specific task type strings internally.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class CaptchaTaskType(StrEnum):
    """Provider-agnostic captcha type identifiers.

    Each captcha provider maps these to its own API-specific task type
    strings internally (e.g., AntiCaptcha maps RECAPTCHA_V2 to
    ``"RecaptchaV2TaskProxyless"``).
    """

    RECAPTCHA_V2 = "recaptcha_v2"
    RECAPTCHA_V2_PROXY = "recaptcha_v2_proxy"
    RECAPTCHA_V3 = "recaptcha_v3"
    TURNSTILE = "turnstile"
    TURNSTILE_PROXY = "turnstile_proxy"
    IMAGE = "image"


@dataclass(frozen=True)
class CaptchaResult:
    """Immutable result from a captcha solve operation.

    Attributes:
        task_id: Provider-assigned task identifier (str to accommodate
            providers that use non-integer identifiers).
        task_type: Which captcha type was solved.
        solution: The token (reCAPTCHA/Turnstile) or text (image).
        cost: Cost in USD for this solve.
        elapsed_seconds: Wall-clock time from task creation to solution.
    """

    task_id: str
    task_type: CaptchaTaskType
    solution: str
    cost: float
    elapsed_seconds: float


class CaptchaError(Exception):
    """Raised when the captcha provider returns an API error response.

    Attributes:
        error_id: Numeric error identifier from the provider.
        error_code: Machine-readable error code string.
        error_description: Human-readable error description.
    """

    def __init__(self, error_id: int, error_code: str, error_description: str) -> None:
        self.error_id = error_id
        self.error_code = error_code
        self.error_description = error_description
        super().__init__(f"Captcha API error {error_id}: {error_code} - {error_description}")


class CaptchaTimeout(Exception):
    """Raised when polling for a captcha solution exceeds the deadline.

    Attributes:
        task_id: The provider task identifier that timed out.
        timeout_seconds: The deadline that was exceeded.
    """

    def __init__(self, task_id: str, timeout_seconds: int) -> None:
        self.task_id = task_id
        self.timeout_seconds = timeout_seconds
        super().__init__(f"Captcha task {task_id} timed out after {timeout_seconds}s")
