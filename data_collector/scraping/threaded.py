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
from typing import Any

from data_collector.scraping.base import BaseScraper, CategoryThreshold
from data_collector.utilities.database.main import Database
from data_collector.utilities.fun_watch import FunWatchRegistry
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

        Propagates FunWatchContext to worker threads when an active context
        exists. Checks fatal_flag after each completed future and breaks
        early if set.

        Args:
            items: Work items to process in parallel.
            worker: Callable receiving (item, index), executed per thread.
            max_workers: Thread pool size. Defaults to self.max_workers.
            track_progress: Whether to call update_progress() after each item.
        """
        effective_workers = max_workers if max_workers is not None else self.max_workers
        registry = FunWatchRegistry.instance()
        has_context = registry.try_get_active_context() is not None

        with ThreadPoolExecutor(max_workers=effective_workers) as executor:
            futures = {
                executor.submit(
                    registry.wrap_with_active_context(worker) if has_context else worker,
                    item, idx,
                ): item
                for idx, item in enumerate(items)
            }
            for future in as_completed(futures):
                if self.fatal_flag:
                    break
                future.result()
                if track_progress:
                    self.update_progress()

    def create_worker_request(self) -> Request:
        """Create a Request instance for a worker thread.

        Override to customize timeout, retries, headers, or proxy per worker.
        The default creates a basic Request sharing self.metrics.

        Returns:
            A new Request instance with shared metrics.
        """
        return Request(metrics=self.metrics)
