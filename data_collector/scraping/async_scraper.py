"""AsyncScraper -- BaseScraper variant with asyncio concurrency support.

Provides process_batch_async() for parallel item processing with
semaphore-controlled concurrency. Subclasses implement collect()
directly, calling process_batch_async() with their own async worker
methods. store_async() serializes database writes from coroutines.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Any

from data_collector.scraping.base import BaseScraper, CategoryThreshold
from data_collector.utilities.database.main import Database
from data_collector.utilities.fun_watch import FunWatchContext, FunWatchRegistry
from data_collector.utilities.request import RequestMetrics


class AsyncScraper(BaseScraper):
    """Async scraper with process_batch_async() for concurrent work.

    Subclasses implement collect() by calling process_batch_async() with
    an async worker callable. Uses a single shared Request instance
    (httpx.AsyncClient is coroutine-safe).

    Database operations remain synchronous. Use store_async() to serialize
    DB writes from concurrent coroutines.

    Attributes:
        max_concurrency: Maximum concurrent coroutines (default 10).
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
        max_concurrency: int = 10,
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
        self.max_concurrency = max_concurrency
        self._store_lock = asyncio.Lock()

    async def process_batch_async(
        self,
        items: list[Any],
        worker: Callable[[Any, int], Awaitable[None]],
        *,
        max_concurrency: int | None = None,
        track_progress: bool = True,
    ) -> None:
        """Execute async worker for each item with semaphore control.

        Registers the worker callback in AppFunctions and creates an aggregate
        FunctionLog row with ``log_role='thread'``.  Each coroutine binds the
        thread context so that ``increment_solved()`` / ``increment_failed()``
        update the thread's FunctionLog row.

        Args:
            items: Work items to process concurrently.
            worker: Async callable receiving (item, index).
            max_concurrency: Semaphore limit. Defaults to self.max_concurrency.
            track_progress: Whether to call update_progress() after each item.
        """
        effective_concurrency = max_concurrency if max_concurrency is not None else self.max_concurrency
        semaphore = asyncio.Semaphore(effective_concurrency)

        registry = FunWatchRegistry.instance()
        parent_context = registry.try_get_active_context()

        thread_context: FunWatchContext | None = None
        if parent_context is not None:
            thread_context, _thread_function_hash = registry.register_thread(
                worker, self.app_id, self.runtime,
                main_app=getattr(self, "main_app", None),
                caller_log_id=parent_context.log_id,
            )
            thread_context.task_size = len(items)

        async def _wrapper(item: Any, instance_id: int) -> None:
            if self.fatal_flag:
                return
            async with semaphore:
                if self.fatal_flag:
                    return
                if thread_context is not None:
                    token = registry.bind_context(thread_context)
                    invocation_start = datetime.now(UTC)
                    try:
                        await worker(item, instance_id)
                    finally:
                        duration_ms = (datetime.now(UTC) - invocation_start).total_seconds() * 1000.0
                        thread_context.record_invocation_duration(duration_ms)
                        registry.unbind_context(token)
                else:
                    await worker(item, instance_id)
            if track_progress:
                self.update_progress()

        tasks = [_wrapper(item, idx) for idx, item in enumerate(items)]
        await asyncio.gather(*tasks)

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

    async def store_async(self, records: list[Any]) -> None:
        """Serialized async wrapper for store().

        Acquires an asyncio.Lock before calling the sync store() method
        to prevent concurrent database writes from multiple coroutines.

        Args:
            records: List of ORM objects or dicts to persist.
        """
        async with self._store_lock:
            self.store(records)
