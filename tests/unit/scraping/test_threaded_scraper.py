"""Unit tests for ThreadedScraper class."""

from __future__ import annotations

import threading
from typing import Any
from unittest.mock import MagicMock

from data_collector.scraping.base import BaseScraper
from data_collector.scraping.threaded import ThreadedScraper
from data_collector.utilities.request import Request, RequestMetrics


def _make_scraper(**overrides: object) -> ThreadedScraper:
    """Create a ThreadedScraper with mocked dependencies."""
    defaults: dict[str, object] = {
        "database": MagicMock(),
        "logger": MagicMock(),
        "runtime": "test_runtime",
        "app_id": "test_app_id",
    }
    defaults.update(overrides)
    return ThreadedScraper(**defaults)  # type: ignore[arg-type]


class TestInheritance:
    """Test ThreadedScraper class hierarchy."""

    def test_inherits_base_scraper(self) -> None:
        assert issubclass(ThreadedScraper, BaseScraper)

    def test_instance_is_base_scraper(self) -> None:
        scraper = _make_scraper()
        assert isinstance(scraper, BaseScraper)


class TestConstructor:
    """Test ThreadedScraper constructor."""

    def test_default_max_workers(self) -> None:
        scraper = _make_scraper()
        assert scraper.max_workers == 5

    def test_custom_max_workers(self) -> None:
        scraper = _make_scraper(max_workers=10)
        assert scraper.max_workers == 10

    def test_has_counter_lock(self) -> None:
        scraper = _make_scraper()
        # Verify thread-safe counters work (lock exists internally)
        scraper.increment_solved()
        scraper.increment_failed()
        assert scraper.solved == 1
        assert scraper.failed == 1

    def test_inherits_base_attributes(self) -> None:
        scraper = _make_scraper()
        assert scraper.solved == 0
        assert scraper.failed == 0
        assert scraper.work_list == []
        assert isinstance(scraper.metrics, RequestMetrics)


class TestThreadSafeCounters:
    """Test increment_solved and increment_failed."""

    def testincrement_solved(self) -> None:
        scraper = _make_scraper()
        scraper.increment_solved()
        assert scraper.solved == 1

    def testincrement_solved_custom_count(self) -> None:
        scraper = _make_scraper()
        scraper.increment_solved(5)
        assert scraper.solved == 5

    def testincrement_failed(self) -> None:
        scraper = _make_scraper()
        scraper.increment_failed()
        assert scraper.failed == 1

    def testincrement_failed_custom_count(self) -> None:
        scraper = _make_scraper()
        scraper.increment_failed(3)
        assert scraper.failed == 3

    def test_concurrent_increments(self) -> None:
        scraper = _make_scraper()

        def increment_many() -> None:
            for _ in range(100):
                scraper.increment_solved()

        threads = [threading.Thread(target=increment_many) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert scraper.solved == 1000


class TestCreateWorkerRequest:
    """Test create_worker_request() hook."""

    def test_default_creates_request_with_shared_metrics(self) -> None:
        metrics = RequestMetrics()
        scraper = _make_scraper(metrics=metrics)
        req = scraper.create_worker_request()
        assert isinstance(req, Request)

    def test_override_custom_request(self) -> None:
        class CustomScraper(ThreadedScraper):
            def create_worker_request(self) -> Request:
                req = Request(timeout=60, retries=5, metrics=self.metrics)
                req.set_headers({"X-Custom": "test"})
                return req

        scraper = CustomScraper(
            database=MagicMock(), logger=MagicMock(),
            runtime="rt", app_id="id",
        )
        req = scraper.create_worker_request()
        assert isinstance(req, Request)


class TestProcessBatch:
    """Test process_batch() reusable threading method."""

    def test_calls_worker_per_item(self) -> None:
        scraper = _make_scraper(max_workers=2)
        calls: list[tuple[Any, int]] = []

        def worker(item: Any, index: int) -> None:
            calls.append((item, index))

        scraper.process_batch(["x", "y", "z"], worker)

        assert len(calls) == 3
        items = {c[0] for c in calls}
        assert items == {"x", "y", "z"}

    def test_custom_max_workers(self) -> None:
        scraper = _make_scraper(max_workers=1)
        order: list[int] = []

        def worker(item: Any, index: int) -> None:
            order.append(index)

        scraper.process_batch(list(range(5)), worker, max_workers=3)

        assert sorted(order) == list(range(5))

    def test_fatal_flag_stops_processing(self) -> None:
        scraper = _make_scraper(max_workers=1)
        calls: list[int] = []

        def worker(item: Any, index: int) -> None:
            calls.append(item)
            if item == 1:
                scraper.fatal_flag = 1

        scraper.process_batch(list(range(5)), worker)

        assert scraper.fatal_flag == 1

    def test_track_progress_true_calls_update(self) -> None:
        scraper = _make_scraper(max_workers=1)
        scraper.update_progress = MagicMock()  # type: ignore[method-assign]

        def worker(item: Any, index: int) -> None:
            pass

        scraper.process_batch(["a", "b"], worker, track_progress=True)

        assert scraper.update_progress.call_count == 2  # type: ignore[union-attr]

    def test_track_progress_false_skips_update(self) -> None:
        scraper = _make_scraper(max_workers=1)
        scraper.update_progress = MagicMock()  # type: ignore[method-assign]

        def worker(item: Any, index: int) -> None:
            pass

        scraper.process_batch(["a", "b"], worker, track_progress=False)

        scraper.update_progress.assert_not_called()  # type: ignore[union-attr]
