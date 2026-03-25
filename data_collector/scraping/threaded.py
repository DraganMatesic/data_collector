"""ThreadedScraper -- BaseScraper variant with ThreadPoolExecutor support.

Provides process_batch() for parallel item processing. Subclasses
implement collect() directly, calling process_batch() with their own
worker methods. create_worker_request() provides per-thread Request
instances (httpx clients are not thread-safe).
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from typing import Any

from data_collector.scraping.base import BaseScraper, CategoryThreshold
from data_collector.utilities.database.main import Database
from data_collector.utilities.fun_watch import FunWatchContext, FunWatchRegistry
from data_collector.utilities.request import Request, RequestMetrics


class ThreadedScraper(BaseScraper):
    """Multi-threaded scraper with process_batch() for parallel work.

    Subclasses implement collect() by calling process_batch() with a
    worker callable. Each worker should create its own Request via
    create_worker_request() (httpx clients are not thread-safe).

    Attributes:
        max_workers: Number of concurrent threads (default 5).
    """

    def __init__(
        self,
        database: Database,
        *,
        logger: logging.Logger,
        runtime: str,
        app_id: str,
        args: dict[str, Any] | None = None,
        metrics: RequestMetrics | None = None,
        max_workers: int = 5,
        max_consecutive_failures: int = 5,
        max_error_rate: float = 0.20,
        min_error_sample: int = 10,
        category_thresholds: tuple[CategoryThreshold, ...] | None = None,
    ) -> None:
        super().__init__(
            database,
            logger=logger,
            runtime=runtime,
            app_id=app_id,
            args=args,
            metrics=metrics,
            max_consecutive_failures=max_consecutive_failures,
            max_error_rate=max_error_rate,
            min_error_sample=min_error_sample,
            category_thresholds=category_thresholds,
        )
        self.max_workers = max_workers

    def process_batch(
        self,
        items: list[Any],
        worker: Callable[[Any, int], None],
        *,
        max_workers: int | None = None,
        track_progress: bool = True,
    ) -> None:
        """Execute worker callable for each item using a ThreadPoolExecutor.

        Registers the worker callback in AppFunctions and creates an aggregate
        FunctionLog row with ``log_role='thread'``.  Worker threads bind the
        thread context (not the parent) so that ``increment_solved()`` /
        ``increment_failed()`` update the thread's FunctionLog row.  Errors
        logged inside workers carry the thread's ``function_id`` in structlog.

        After all workers complete, solved/failed counters are propagated back
        to the caller's (parent) aggregate context.

        Args:
            items: Work items to process in parallel.
            worker: Callable receiving (item, index), executed per thread.
            max_workers: Thread pool size. Defaults to self.max_workers.
            track_progress: Whether to call update_progress() after each item.
        """
        effective_workers = max_workers if max_workers is not None else self.max_workers
        registry = FunWatchRegistry.instance()
        parent_context = registry.try_get_active_context()

        thread_context: FunWatchContext | None = None
        thread_function_hash: str | None = None
        if parent_context is not None:
            thread_context, thread_function_hash = registry.register_thread(
                worker, self.app_id, self.runtime,
                main_app=getattr(self, "main_app", None),
                caller_log_id=parent_context.log_id,
            )
            thread_context.task_size = len(items)

        with ThreadPoolExecutor(max_workers=effective_workers) as executor:
            wrapped_worker = (
                registry.wrap_with_thread_context(
                    worker, thread_context, thread_function_hash, application_logger=self.logger,
                )
                if thread_context is not None and thread_function_hash is not None
                else worker
            )
            futures = {
                executor.submit(wrapped_worker, item, idx): item
                for idx, item in enumerate(items)
            }
            for future in as_completed(futures):
                if self._abort_event.is_set():
                    executor.shutdown(wait=False, cancel_futures=True)
                    break
                try:
                    future.result()
                except Exception:
                    with self._progress_lock:
                        self.failed += 1
                        self._consecutive_failures += 1
                    self._check_failure_threshold()
                if track_progress:
                    self.update_progress()

        if thread_context is not None:
            solved, failed = thread_context.snapshot()
            if parent_context is not None:
                parent_context.mark_solved(solved)
                if failed > 0:
                    parent_context.mark_failed(failed)
            timing = thread_context.timing_snapshot()
            registry.complete_function_log(
                log_id=thread_context.log_id,
                solved=solved,
                failed=failed,
                call_count=len(items),
                end_time=datetime.now(UTC),
                total_elapsed_ms=timing[0],
                average_elapsed_ms=timing[1],
                median_elapsed_ms=timing[2],
                min_elapsed_ms=timing[3],
                max_elapsed_ms=timing[4],
                task_size=thread_context.task_size,
            )

    def create_worker_request(self) -> Request:
        """Create a Request instance for a worker thread.

        Override to customize timeout, retries, headers, or proxy per worker.
        The default creates a basic Request sharing self.metrics.

        Returns:
            A new Request instance with shared metrics.
        """
        return Request(metrics=self.metrics)
