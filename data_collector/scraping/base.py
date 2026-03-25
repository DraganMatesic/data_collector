"""BaseScraper class for convention-based data collection.

BaseScraper provides convention-based lifecycle hooks for data collection
applications. Every scraper inherits from this class and overrides the
hooks it needs: prepare_list(), collect(), store(), cleanup(), set_next_run().

The update_app_status() function is re-exported here for backward
compatibility. Its canonical location is data_collector.utilities.app_status.
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from data_collector.enums import ErrorCategory, FatalFlag
from data_collector.enums.notifications import AlertSeverity
from data_collector.notifications.models import Notification
from data_collector.proxy.models import ProxyData
from data_collector.utilities.app_status import update_app_status as update_app_status
from data_collector.utilities.database.main import Database
from data_collector.utilities.fun_watch import FunWatchMixin, FunWatchRegistry, fun_watch
from data_collector.utilities.request import Request, RequestMetrics


@dataclass(frozen=True)
class CategoryThreshold:
    """Per-category failure threshold configuration.

    Each instance defines when a specific ErrorCategory should trigger
    a fatal stop. All checks are independent -- the first one that
    fires sets fatal_flag.

    Args:
        category: The error category this threshold applies to.
        max_count: Absolute failure count threshold (0 = disabled).
        max_rate: Failure rate as fraction of processed items (0.0 = disabled).
        min_sample: Minimum items processed before rate check applies.
        max_consecutive: Consecutive failures in this category (0 = disabled).
        is_blocker: When True, fatal stops the app permanently (no auto-retry).
            When False, the app retries after a configurable interval.
    """

    category: ErrorCategory
    max_count: int
    max_rate: float
    min_sample: int
    max_consecutive: int
    is_blocker: bool = False


DEFAULT_CATEGORY_THRESHOLDS: tuple[CategoryThreshold, ...] = (
    CategoryThreshold(
        ErrorCategory.DATABASE, max_count=1, max_rate=0.0, min_sample=0, max_consecutive=1, is_blocker=True,
    ),
    CategoryThreshold(
        ErrorCategory.IO_WRITE, max_count=1, max_rate=0.0, min_sample=0, max_consecutive=1, is_blocker=True,
    ),
    CategoryThreshold(ErrorCategory.CAPTCHA, max_count=5, max_rate=0.10, min_sample=10, max_consecutive=3),
    CategoryThreshold(ErrorCategory.PROXY, max_count=0, max_rate=0.30, min_sample=10, max_consecutive=10),
    CategoryThreshold(ErrorCategory.HTTP, max_count=0, max_rate=0.20, min_sample=10, max_consecutive=5),
    CategoryThreshold(ErrorCategory.PARSE, max_count=0, max_rate=0.15, min_sample=10, max_consecutive=5),
    CategoryThreshold(ErrorCategory.UNKNOWN, max_count=0, max_rate=0.20, min_sample=10, max_consecutive=5),
)


class BaseScraper(FunWatchMixin):
    """Convention-based scraper with lifecycle hooks and progress tracking.

    Subclasses override lifecycle hooks: prepare_list(), collect(),
    store(), cleanup(), set_next_run(). BaseScraper provides HTTP client,
    metrics, error tracking, and progress counters.

    Attributes:
        base_url: Target site root URL. Set as class attribute in subclasses.
    """

    base_url: str = ""

    def __init__(
        self,
        database: Database,
        *,
        logger: logging.Logger,
        runtime: str,
        app_id: str,
        args: dict[str, Any] | None = None,
        metrics: RequestMetrics | None = None,
        max_consecutive_failures: int = 5,
        max_error_rate: float = 0.20,
        min_error_sample: int = 10,
        category_thresholds: tuple[CategoryThreshold, ...] | None = None,
    ) -> None:
        self.database = database
        self.logger = logger
        self.runtime = runtime
        self.app_id = app_id
        self.args = args

        self.metrics: RequestMetrics = metrics if metrics is not None else RequestMetrics()
        self.request = Request(metrics=self.metrics)

        self.work_list: list[Any] = []
        self.solved: int = 0
        self.failed: int = 0
        self.list_size: int = 0
        self.progress: int = 0

        self.fatal_flag: int = FatalFlag.NONE
        self.fatal_msg: str = ""
        self.fatal_time: datetime | None = None

        self.next_run: datetime | None = None
        self.alert_threshold: float = 0.20
        self.progress_interval: float = 5.0
        self._collect_start_time: float | None = None
        self._last_progress_update: float = 0.0
        self._progress_lock = threading.Lock()

        self.max_consecutive_failures: int = max_consecutive_failures
        self.max_error_rate: float = max_error_rate
        self.min_error_sample: int = min_error_sample
        self._consecutive_failures: int = 0

        self._category_thresholds: tuple[CategoryThreshold, ...] | None = category_thresholds
        self._category_thresholds_map: dict[str, CategoryThreshold] = (
            {threshold.category.value: threshold for threshold in category_thresholds}
            if category_thresholds is not None
            else {}
        )
        self._category_failures: dict[str, int] = {}
        self._category_consecutive: dict[str, int] = {}

        self._abort_event = threading.Event()
        self._fatal_is_blocker: bool = False
        self._fatal_category: str = ""

        self.proxy_data: ProxyData | None = None
        self.notification_dispatcher: Any | None = None

    def prepare_list(self) -> None:
        """Query database for items to process. Override in subclass."""

    def collect(self) -> None:
        """Main collection logic (HTTP requests, parsing, storing). Override in subclass."""

    def store(self, records: list[Any]) -> None:
        """Persist collected records via merge() or bulk_insert(). Override in subclass."""

    def cleanup(self) -> None:
        """Release resources (logout, close sessions, release proxies). Override in subclass."""

    def set_next_run(self) -> None:
        """Calculate next execution time based on runtime conditions. Override in subclass."""

    @property
    def should_abort(self) -> bool:
        """Check if collection should stop immediately.

        Returns True when a failure threshold has been breached and
        fatal_flag is set. Use in single-threaded collection loops::

            for url in self.work_list:
                if self.should_abort:
                    break
                ...

        ThreadedScraper and AsyncScraper check this automatically in
        their batch processing loops.
        """
        return self.fatal_flag != FatalFlag.NONE

    def get_retry_next_run(self, retry_interval: timedelta = timedelta(minutes=30)) -> datetime | None:
        """Return a retry-scheduled next_run for non-blocker fatals.

        When a fatal threshold is breached:
        - **Blocker** (DATABASE, IO_WRITE): returns None. The app should not
          auto-retry -- it needs human intervention.
        - **Non-blocker** (HTTP, PROXY, CAPTCHA, PARSE, UNKNOWN): returns
          ``now + retry_interval``. The Manager will schedule a retry.

        Returns None when no fatal has occurred (use normal ``set_next_run()``).

        Args:
            retry_interval: Time before next retry attempt for non-blockers.
        """
        if self.fatal_flag == FatalFlag.NONE:
            return None
        if self._fatal_is_blocker:
            return None
        return datetime.now(UTC) + retry_interval

    def increment_solved(self, count: int = 1) -> None:
        """Thread-safe increment of solved counter with FunWatchContext forwarding.

        Resets all consecutive failure counters (flat and per-category).

        Args:
            count: Number to add (default 1).
        """
        with self._progress_lock:
            self.solved += count
            self._consecutive_failures = 0
            for category_key in self._category_consecutive:
                self._category_consecutive[category_key] = 0
        active_context = FunWatchRegistry.instance().try_get_active_context()
        if active_context is not None:
            active_context.mark_solved(count)

    def increment_failed(
        self,
        count: int = 1,
        *,
        error_category: ErrorCategory | str | None = None,
    ) -> None:
        """Thread-safe increment of failed counter with FunWatchContext forwarding.

        Args:
            count: Number to add (default 1).
            error_category: ErrorCategory for per-category threshold evaluation.
        """
        with self._progress_lock:
            self.failed += count
            self._consecutive_failures += count
            if error_category is not None:
                category_key = error_category.value if isinstance(error_category, ErrorCategory) else error_category
                self._category_failures[category_key] = self._category_failures.get(category_key, 0) + count
                self._category_consecutive[category_key] = (
                    self._category_consecutive.get(category_key, 0) + count
                )
        active_context = FunWatchRegistry.instance().try_get_active_context()
        if active_context is not None:
            active_context.mark_failed(count)
        self._check_failure_threshold()

    def _check_failure_threshold(self) -> None:
        """Evaluate item-level failure thresholds and set fatal_flag if exceeded.

        When category thresholds are configured, evaluates per-category
        counts, consecutive failures, and rates independently. Otherwise
        falls back to flat consecutive/rate checks.

        Thread-safe: reads counters under ``_progress_lock``.
        """
        if self.fatal_flag != FatalFlag.NONE:
            return

        with self._progress_lock:
            consecutive = self._consecutive_failures
            solved = self.solved
            failed = self.failed
            category_failures_snapshot = dict(self._category_failures)
            category_consecutive_snapshot = dict(self._category_consecutive)

        processed = solved + failed

        if self._category_thresholds_map:
            self._check_category_thresholds(processed, category_failures_snapshot, category_consecutive_snapshot)
            return

        self._check_flat_thresholds(consecutive, solved, failed, processed)

    def _check_category_thresholds(
        self,
        processed: int,
        category_failures_snapshot: dict[str, int],
        category_consecutive_snapshot: dict[str, int],
    ) -> None:
        """Evaluate per-category failure thresholds.

        Args:
            processed: Total items processed (solved + failed).
            category_failures_snapshot: Copy of per-category failure counts.
            category_consecutive_snapshot: Copy of per-category consecutive counts.
        """
        for category_key, threshold in self._category_thresholds_map.items():
            category_count = category_failures_snapshot.get(category_key, 0)
            category_consecutive = category_consecutive_snapshot.get(category_key, 0)

            if threshold.max_count > 0 and category_count >= threshold.max_count:
                self._set_category_fatal(
                    category_key, "count", category_count, threshold.max_count,
                )
                return

            if threshold.max_consecutive > 0 and category_consecutive >= threshold.max_consecutive:
                self._set_category_fatal(
                    category_key, "consecutive", category_consecutive, threshold.max_consecutive,
                )
                return

            if (
                threshold.max_rate > 0.0
                and processed >= threshold.min_sample
                and category_count / processed > threshold.max_rate
            ):
                self._set_category_fatal(
                    category_key, "rate", category_count / processed, threshold.max_rate,
                )
                return

    def _set_category_fatal(
        self, category_key: str, reason_type: str, actual: int | float, threshold_value: int | float,
    ) -> None:
        """Set fatal state with category-enriched message and abort signal.

        Args:
            category_key: ErrorCategory value string.
            reason_type: One of "count", "consecutive", or "rate".
            actual: The actual value that exceeded the threshold.
            threshold_value: The configured threshold value.
        """
        threshold = self._category_thresholds_map.get(category_key)
        self.fatal_flag = FatalFlag.UNEXPECTED_BEHAVIOUR
        self._fatal_category = category_key
        self._fatal_is_blocker = threshold.is_blocker if threshold is not None else False
        if reason_type == "rate":
            self.fatal_msg = (
                f"Category '{category_key}' error rate {actual:.1%} exceeds "
                f"threshold {threshold_value:.0%}."
            )
        else:
            self.fatal_msg = (
                f"Category '{category_key}' {reason_type} ({actual}) reached "
                f"threshold ({threshold_value})."
            )
        self.fatal_time = datetime.now(UTC)
        self._abort_event.set()
        self.logger.warning(
            "Early exit: category failure threshold exceeded",
            extra={
                "error_category": category_key,
                "reason_type": reason_type,
                "actual": actual,
                "threshold": threshold_value,
                "is_blocker": self._fatal_is_blocker,
            },
        )
        self._dispatch_fatal_notification()

    def _dispatch_fatal_notification(self) -> None:
        """Send a CRITICAL notification when fatal_flag is set.

        Requires ``notification_dispatcher`` to be set (a
        ``NotificationDispatcher`` instance). If not set, this is a no-op.
        Delivery failures are logged but never propagated -- notification
        errors must not interrupt scraper lifecycle.
        """
        if self.notification_dispatcher is None:
            return

        try:
            notification = Notification(
                severity=AlertSeverity.CRITICAL,
                title=self.app_id,
                message=self.fatal_msg,
                app_id=self.app_id,
                metadata={"runtime": self.runtime},
            )
            self.notification_dispatcher.send(notification)
        except Exception:
            self.logger.warning("Failed to dispatch fatal notification", exc_info=True)

    def _check_flat_thresholds(
        self, consecutive: int, solved: int, failed: int, processed: int,
    ) -> None:
        """Evaluate flat (non-categorized) failure thresholds.

        Args:
            consecutive: Current consecutive failure count.
            solved: Total solved items.
            failed: Total failed items.
            processed: Total items processed (solved + failed).
        """
        if self.max_consecutive_failures > 0 and consecutive >= self.max_consecutive_failures:
            self.fatal_flag = FatalFlag.UNEXPECTED_BEHAVIOUR
            self.fatal_msg = (
                f"Consecutive failures ({consecutive}) reached threshold "
                f"({self.max_consecutive_failures})."
            )
            self.fatal_time = datetime.now(UTC)
            self._abort_event.set()
            self.logger.warning(
                "Early exit: consecutive failure threshold exceeded",
                extra={
                    "consecutive_failures": consecutive,
                    "max_consecutive_failures": self.max_consecutive_failures,
                    "solved": solved,
                    "failed": failed,
                },
            )
            return

        if (
            self.max_error_rate > 0.0
            and processed >= self.min_error_sample
            and failed / processed > self.max_error_rate
        ):
            error_rate = failed / processed
            self.fatal_flag = FatalFlag.UNEXPECTED_BEHAVIOUR
            self.fatal_msg = (
                f"Error rate {error_rate:.1%} exceeds threshold {self.max_error_rate:.0%} "
                f"(sample={processed}, min_sample={self.min_error_sample})."
            )
            self.fatal_time = datetime.now(UTC)
            self._abort_event.set()
            self.logger.warning(
                "Early exit: error rate threshold exceeded",
                extra={
                    "error_rate": error_rate,
                    "max_error_rate": self.max_error_rate,
                    "processed": processed,
                    "min_error_sample": self.min_error_sample,
                    "solved": solved,
                    "failed": failed,
                },
            )

    def update_progress(self, *, force: bool = False) -> None:
        """Write current progress, counters, and ETA to the Apps table.

        Throttled to at most one DB write per ``progress_interval`` seconds
        unless *force* is True.

        Args:
            force: Bypass the time-based throttle.
        """
        now = time.monotonic()
        if not force and (now - self._last_progress_update) < self.progress_interval:
            return
        self._last_progress_update = now

        with self._progress_lock:
            solved, failed = self.solved, self.failed
        total = self.list_size
        processed = solved + failed
        progress_pct = int((processed / total) * 100) if total > 0 else 0

        eta_dt: datetime | None = None
        if processed > 0 and self._collect_start_time is not None and total > processed:
            elapsed = time.monotonic() - self._collect_start_time
            eta_seconds = (elapsed / processed) * (total - processed)
            eta_dt = datetime.now(UTC) + timedelta(seconds=eta_seconds)

        update_app_status(
            self.database, self.app_id,
            solved=solved, failed=failed, task_size=total, progress=progress_pct, eta=eta_dt,
        )

    def _start_collect_timer(self) -> None:
        """Record the monotonic timestamp when collection begins."""
        self._collect_start_time = time.monotonic()

    @fun_watch(task_size=False)
    def fatal_check(self) -> None:
        """Evaluate RequestMetrics error ratios against alert_threshold.

        When category thresholds are configured, maps RequestMetrics
        per-type counters to ErrorCategory.PROXY and ErrorCategory.HTTP
        for per-category rate evaluation. Otherwise falls back to flat
        aggregate error rate check.

        Sets fatal_flag, fatal_msg, fatal_time when the error rate exceeds
        the configured threshold. Notification dispatch is deferred to WP-07.
        """
        if self.metrics.request_count == 0:
            return

        stats = self.metrics.log_stats(self.logger)
        error_rate = stats.get("error_rate_percent", 0.0) / 100.0
        error_breakdown: dict[str, int] = stats.get("error_breakdown", {})

        if self._category_thresholds_map:
            self._fatal_check_categorized(error_breakdown)
            return

        if error_rate > self.alert_threshold:
            self.fatal_flag = FatalFlag.UNEXPECTED_BEHAVIOUR
            self.fatal_msg = (
                f"Error rate {error_rate:.1%} exceeds threshold {self.alert_threshold:.0%}. "
                f"Breakdown: {error_breakdown}"
            )
            self.fatal_time = datetime.now(UTC)
            self.logger.warning(
                "Fatal check triggered",
                extra={
                    "fatal_flag": self.fatal_flag,
                    "fatal_msg": self.fatal_msg,
                    "error_rate": error_rate,
                    "threshold": self.alert_threshold,
                },
            )
            self._dispatch_fatal_notification()

    def _fatal_check_categorized(self, error_breakdown: dict[str, int]) -> None:
        """Evaluate RequestMetrics counters against per-category thresholds.

        Maps RequestMetrics error breakdown keys to ErrorCategory values:
        - "proxy" -> ErrorCategory.PROXY
        - "timeout", "bad_status_code", "redirect", "request" -> ErrorCategory.HTTP
        - "other" -> ErrorCategory.UNKNOWN

        Args:
            error_breakdown: Error counts by type from RequestMetrics.log_stats().
        """
        if self.fatal_flag != FatalFlag.NONE:
            return

        request_count = self.metrics.request_count
        if request_count == 0:
            return

        category_counts: dict[str, int] = {}
        metrics_to_category: dict[str, str] = {
            "proxy": ErrorCategory.PROXY.value,
            "timeout": ErrorCategory.HTTP.value,
            "bad_status_code": ErrorCategory.HTTP.value,
            "redirect": ErrorCategory.HTTP.value,
            "request": ErrorCategory.HTTP.value,
            "other": ErrorCategory.UNKNOWN.value,
        }

        for metrics_key, count in error_breakdown.items():
            category_key = metrics_to_category.get(metrics_key, ErrorCategory.UNKNOWN.value)
            category_counts[category_key] = category_counts.get(category_key, 0) + count

        for category_key, count in category_counts.items():
            threshold = self._category_thresholds_map.get(category_key)
            if threshold is None:
                continue

            if threshold.max_count > 0 and count >= threshold.max_count:
                self._set_category_fatal(category_key, "count", count, threshold.max_count)
                return

            if (
                threshold.max_rate > 0.0
                and request_count >= threshold.min_sample
                and count / request_count > threshold.max_rate
            ):
                self._set_category_fatal(category_key, "rate", count / request_count, threshold.max_rate)
                return
