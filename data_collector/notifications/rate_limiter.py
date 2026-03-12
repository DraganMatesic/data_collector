"""Per-channel rate limiter for notification throttling."""

from __future__ import annotations

import threading
import time
from collections import deque

from data_collector.enums.notifications import AlertSeverity


class RateLimiter:
    """Per-channel rate limiter using sliding window with burst protection.

    Thread-safe. CRITICAL severity alerts bypass rate limiting entirely.

    Args:
        min_interval_seconds: Minimum seconds between consecutive sends.
        burst_limit: Maximum sends within the rolling window.
        window_seconds: Rolling window size for burst counting. Defaults to
            ``min_interval_seconds * burst_limit``.
    """

    def __init__(
        self,
        min_interval_seconds: int = 30,
        burst_limit: int = 10,
        window_seconds: int | None = None,
    ) -> None:
        self.min_interval_seconds = min_interval_seconds
        self.burst_limit = burst_limit
        self.window_seconds = window_seconds if window_seconds is not None else min_interval_seconds * burst_limit
        self._last_send_time: float = 0.0
        self._send_timestamps: deque[float] = deque()
        self._lock = threading.Lock()

    def is_allowed(self, severity: AlertSeverity) -> bool:
        """Check if a send is allowed under current rate limits.

        CRITICAL severity always returns True, bypassing all rate limits.

        Args:
            severity: The alert severity to check.

        Returns:
            True if the send is allowed, False if rate-limited.
        """
        if severity == AlertSeverity.CRITICAL:
            return True

        with self._lock:
            now = time.monotonic()
            self._purge_expired(now)

            if self._last_send_time > 0 and (now - self._last_send_time) < self.min_interval_seconds:
                return False

            return len(self._send_timestamps) < self.burst_limit

    def record_send(self) -> None:
        """Record that a send occurred. Updates internal timestamps."""
        with self._lock:
            now = time.monotonic()
            self._last_send_time = now
            self._send_timestamps.append(now)
            self._purge_expired(now)

    def reset(self) -> None:
        """Clear all rate limiting state."""
        with self._lock:
            self._last_send_time = 0.0
            self._send_timestamps.clear()

    def _purge_expired(self, now: float) -> None:
        """Remove timestamps older than the rolling window.

        Args:
            now: Current monotonic time.
        """
        cutoff = now - self.window_seconds
        while self._send_timestamps and self._send_timestamps[0] < cutoff:
            self._send_timestamps.popleft()
