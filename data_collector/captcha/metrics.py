"""Thread-safe captcha cost and performance metrics.

CaptchaMetrics tracks submitted, solved, timed-out, and failed captcha
tasks along with total cost. Shared across worker threads, protected
by a threading.Lock following the same pattern as RequestMetrics.
"""

from __future__ import annotations

import logging
import threading
from typing import Any


class CaptchaMetrics:
    """Thread-safe captcha cost and performance tracker.

    Created once per runtime and shared across all worker threads.
    Each CaptchaProvider instance records metrics through the same
    CaptchaMetrics object.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.submitted: int = 0
        self.solved: int = 0
        self.timed_out: int = 0
        self.failed: int = 0
        self.total_cost: float = 0.0

    def record_submitted(self) -> None:
        """Record a captcha task submission."""
        with self._lock:
            self.submitted += 1

    def record_solved(self, cost: float) -> None:
        """Record a successfully solved captcha with its cost.

        Args:
            cost: Cost in USD for this solve.
        """
        with self._lock:
            self.solved += 1
            self.total_cost += cost

    def record_timed_out(self) -> None:
        """Record a captcha task that exceeded the polling deadline."""
        with self._lock:
            self.timed_out += 1

    def record_failed(self) -> None:
        """Record a captcha task that returned an API error."""
        with self._lock:
            self.failed += 1

    def log_stats(self, logger: logging.Logger) -> dict[str, Any]:
        """Log and return aggregated captcha statistics.

        Args:
            logger: Logger instance for output.

        Returns:
            Dictionary with submitted, solved, timed_out, failed,
            total_cost, and solve_rate_percent.
        """
        with self._lock:
            solve_rate = (self.solved / self.submitted * 100) if self.submitted > 0 else 0.0
            stats: dict[str, Any] = {
                "submitted": self.submitted,
                "solved": self.solved,
                "timed_out": self.timed_out,
                "failed": self.failed,
                "total_cost": round(self.total_cost, 6),
                "solve_rate_percent": round(solve_rate, 2),
            }

        logger.info("Captcha statistics: %s", stats)
        return stats
