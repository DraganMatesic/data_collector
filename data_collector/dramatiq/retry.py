"""Framework retry predicate for Dramatiq actors.

Classifies exceptions as transient (retryable) or permanent (non-retryable)
to prevent wasting retry attempts on errors that will never succeed.

Used by the ``Retries`` middleware via the ``retry_when`` parameter.
"""

from __future__ import annotations

import httpx

# Exceptions that represent permanent failures -- retrying will not help.
_PERMANENT_EXCEPTIONS: tuple[type[BaseException], ...] = (
    ValueError,
    KeyError,
    TypeError,
    FileNotFoundError,
    PermissionError,
    NotImplementedError,
)

# HTTP status codes that represent permanent failures.
# 408 (Request Timeout) and 429 (Too Many Requests) are excluded
# because they are transient and should be retried.
_NON_RETRYABLE_STATUS_CODES: frozenset[int] = frozenset({400, 401, 403, 404, 405, 422})


def should_retry_message(retries: int, exception: BaseException) -> bool:
    """Determine whether a failed message should be retried.

    Transient errors (network, timeout, 5xx HTTP, 408, 429) are retried.
    Permanent errors (validation, not found, type error) are not.

    Args:
        retries: Number of retries already attempted.
        exception: The exception that caused the failure.

    Returns:
        True if the message should be retried.
    """
    if isinstance(exception, _PERMANENT_EXCEPTIONS):
        return False

    if isinstance(exception, httpx.HTTPStatusError):
        return exception.response.status_code not in _NON_RETRYABLE_STATUS_CODES

    # Default: retry (covers ConnectionError, TimeoutError, OSError, etc.)
    return True
