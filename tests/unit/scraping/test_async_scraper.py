"""Unit tests for AsyncScraper class."""

from __future__ import annotations

import asyncio
import inspect
from typing import Any
from unittest.mock import MagicMock

import pytest

from data_collector.enums import FatalFlag
from data_collector.scraping.async_scraper import AsyncScraper
from data_collector.scraping.base import BaseScraper
from data_collector.utilities.request import RequestMetrics


def _make_scraper(**overrides: object) -> AsyncScraper:
    """Create an AsyncScraper with mocked dependencies."""
    defaults: dict[str, object] = {
        "database": MagicMock(),
        "logger": MagicMock(),
        "runtime": "test_runtime",
        "app_id": "test_app_id",
    }
    defaults.update(overrides)
    return AsyncScraper(**defaults)  # type: ignore[arg-type]


class TestInheritance:
    """Test AsyncScraper class hierarchy."""

    def test_inherits_base_scraper(self) -> None:
        assert issubclass(AsyncScraper, BaseScraper)

    def test_instance_is_base_scraper(self) -> None:
        scraper = _make_scraper()
        assert isinstance(scraper, BaseScraper)


class TestConstructor:
    """Test AsyncScraper constructor."""

    def test_default_max_concurrency(self) -> None:
        scraper = _make_scraper()
        assert scraper.max_concurrency == 10

    def test_custom_max_concurrency(self) -> None:
        scraper = _make_scraper(max_concurrency=20)
        assert scraper.max_concurrency == 20

    def test_inherits_base_attributes(self) -> None:
        scraper = _make_scraper()
        assert scraper.solved == 0
        assert scraper.failed == 0
        assert scraper.work_list == []
        assert isinstance(scraper.metrics, RequestMetrics)


class TestAsyncMethods:
    """Test that async methods are coroutines."""

    def test_store_async_is_coroutine(self) -> None:
        scraper = _make_scraper()
        assert inspect.iscoroutinefunction(scraper.store_async)

    def test_process_batch_async_is_coroutine(self) -> None:
        scraper = _make_scraper()
        assert inspect.iscoroutinefunction(scraper.process_batch_async)


class TestStoreAsync:
    """Test store_async() serialization."""

    @pytest.mark.asyncio
    async def test_store_async_calls_store(self) -> None:
        scraper = _make_scraper()

        store_calls: list[list[Any]] = []

        def fake_store(records: list[Any]) -> None:
            store_calls.append(records)

        scraper.store = fake_store  # type: ignore[assignment]

        await scraper.store_async([{"key": "value"}])
        assert len(store_calls) == 1
        assert store_calls[0] == [{"key": "value"}]

    @pytest.mark.asyncio
    async def test_store_async_serializes_writes(self) -> None:
        scraper = _make_scraper(max_concurrency=5)

        store_order: list[int] = []

        def fake_store(records: list[Any]) -> None:
            store_order.append(records[0])

        scraper.store = fake_store  # type: ignore[assignment]

        async def worker(item: Any, instance_id: int) -> None:
            await scraper.store_async([item])

        await scraper.process_batch_async(list(range(10)), worker)

        assert sorted(store_order) == list(range(10))


class TestProcessBatchAsync:
    """Test process_batch_async() reusable async method."""

    @pytest.mark.asyncio
    async def test_calls_worker_per_item(self) -> None:
        scraper = _make_scraper(max_concurrency=2)
        calls: list[tuple[Any, int]] = []

        async def worker(item: Any, index: int) -> None:
            calls.append((item, index))

        await scraper.process_batch_async(["x", "y", "z"], worker)

        assert len(calls) == 3
        items = {c[0] for c in calls}
        assert items == {"x", "y", "z"}

    @pytest.mark.asyncio
    async def test_custom_max_concurrency(self) -> None:
        scraper = _make_scraper(max_concurrency=1)

        max_concurrent = 0
        current_concurrent = 0
        lock = asyncio.Lock()

        async def worker(item: Any, index: int) -> None:
            nonlocal max_concurrent, current_concurrent
            async with lock:
                current_concurrent += 1
                if current_concurrent > max_concurrent:
                    max_concurrent = current_concurrent
            await asyncio.sleep(0.01)
            async with lock:
                current_concurrent -= 1

        await scraper.process_batch_async(list(range(5)), worker, max_concurrency=2)

        assert max_concurrent <= 2

    @pytest.mark.asyncio
    async def test_fatal_flag_stops_processing(self) -> None:
        scraper = _make_scraper(max_concurrency=1)
        calls: list[int] = []

        async def worker(item: Any, index: int) -> None:
            calls.append(item)
            if len(calls) == 3:
                scraper.fatal_flag = FatalFlag.UNEXPECTED_BEHAVIOUR

        await scraper.process_batch_async(list(range(10)), worker)

        assert len(calls) == 3

    @pytest.mark.asyncio
    async def test_track_progress_true_calls_update(self) -> None:
        scraper = _make_scraper(max_concurrency=1)
        scraper.update_progress = MagicMock()  # type: ignore[method-assign]

        async def worker(item: Any, index: int) -> None:
            pass

        await scraper.process_batch_async(["a", "b"], worker, track_progress=True)

        assert scraper.update_progress.call_count == 2  # type: ignore[union-attr]

    @pytest.mark.asyncio
    async def test_track_progress_false_skips_update(self) -> None:
        scraper = _make_scraper(max_concurrency=1)
        scraper.update_progress = MagicMock()  # type: ignore[method-assign]

        async def worker(item: Any, index: int) -> None:
            pass

        await scraper.process_batch_async(["a", "b"], worker, track_progress=False)

        scraper.update_progress.assert_not_called()  # type: ignore[union-attr]

    @pytest.mark.asyncio
    async def test_empty_items_list(self) -> None:
        scraper = _make_scraper()
        calls: list[Any] = []

        async def worker(item: Any, index: int) -> None:
            calls.append(item)

        await scraper.process_batch_async([], worker)

        assert len(calls) == 0
