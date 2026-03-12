"""Captcha-related enums for solve status and error classification."""

from enum import IntEnum


class CaptchaSolveStatus(IntEnum):
    """Outcome of a captcha solve attempt.

    Used by CaptchaLog to record the final status of each task submitted
    to a captcha provider. Maps directly to the three possible outcomes
    of the _create_and_poll retry loop.
    """

    SOLVED = 1
    TIMED_OUT = 2
    FAILED = 3


class CaptchaErrorCategory(IntEnum):
    """Provider-agnostic classification of captcha API errors.

    Each captcha provider maps its own error codes to these categories
    internally. Scrapers use the category to decide on retry strategy
    without hardcoding provider-specific error strings.

    Categories and recommended scraper actions:

        AUTH        -- Abort (fatal). Bad API key, suspended account.
        BALANCE     -- Abort (fatal). Zero or negative balance.
        PROXY       -- Rotate proxy and retry. Proxy connection or auth errors.
        TASK        -- Abort (code bug). Unsupported task type, bad parameters.
        SOLVE       -- Retry or skip. Unsolvable captcha, worker failures.
        RATE_LIMIT  -- Backoff and retry. No available workers or slots.
        UNKNOWN     -- Log and decide. Unmapped or unexpected error codes.
    """

    AUTH = 1
    BALANCE = 2
    PROXY = 3
    TASK = 4
    SOLVE = 5
    RATE_LIMIT = 6
    UNKNOWN = 7
