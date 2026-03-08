"""Unit tests for BaseScraper progress tracking, ETA, and FunWatchContext forwarding."""

from __future__ import annotations

import time
from unittest.mock import MagicMock, patch

from data_collector.enums import FatalFlag
from data_collector.scraping.base import BaseScraper
from data_collector.utilities.fun_watch import FunWatchContext, FunWatchRegistry


def _make_scraper(**overrides: object) -> BaseScraper:
    """Create a BaseScraper with mocked dependencies."""
    defaults: dict[str, object] = {
        "database": MagicMock(),
        "logger": MagicMock(),
        "runtime": "test_runtime",
        "app_id": "test_app_id",
    }
    defaults.update(overrides)
    return BaseScraper(**defaults)  # type: ignore[arg-type]


class TestIncrementSolved:
    """Test increment_solved counter and FunWatchContext forwarding."""

    def test_updates_counter(self) -> None:
        scraper = _make_scraper()
        scraper.increment_solved()
        assert scraper.solved == 1

    def test_updates_counter_custom_count(self) -> None:
        scraper = _make_scraper()
        scraper.increment_solved(5)
        assert scraper.solved == 5

    def test_no_context_no_error(self) -> None:
        scraper = _make_scraper()
        scraper.increment_solved()
        assert scraper.solved == 1

    def test_forwards_to_fun_watch_context(self) -> None:
        registry = FunWatchRegistry.instance()
        context = FunWatchContext()
        token = registry.bind_context(context)
        try:
            scraper = _make_scraper()
            scraper.increment_solved(3)
            assert scraper.solved == 3
            assert context.solved == 3
        finally:
            registry.unbind_context(token)


class TestIncrementFailed:
    """Test increment_failed counter and FunWatchContext forwarding."""

    def test_updates_counter(self) -> None:
        scraper = _make_scraper()
        scraper.increment_failed()
        assert scraper.failed == 1

    def test_updates_counter_custom_count(self) -> None:
        scraper = _make_scraper()
        scraper.increment_failed(3)
        assert scraper.failed == 3

    def test_forwards_to_fun_watch_context(self) -> None:
        registry = FunWatchRegistry.instance()
        context = FunWatchContext()
        token = registry.bind_context(context)
        try:
            scraper = _make_scraper()
            scraper.increment_failed(2, error_type="timeout", error_message="Connection timed out")
            assert scraper.failed == 2
            assert context.failed == 2
            count, types_json, samples_json = context.error_snapshot()
            assert count == 2
            assert types_json is not None
            assert "timeout" in types_json
            assert samples_json is not None
            assert "Connection timed out" in samples_json
        finally:
            registry.unbind_context(token)


class TestUpdateProgress:
    """Test update_progress throttle, percentage, and ETA."""

    def test_computes_percentage(self) -> None:
        scraper = _make_scraper()
        scraper.list_size = 10
        scraper.solved = 3
        scraper.failed = 2
        scraper._start_collect_timer()  # pyright: ignore[reportPrivateUsage]

        with patch("data_collector.scraping.base.update_app_status") as mock_update:
            scraper.update_progress(force=True)

        mock_update.assert_called_once()
        call_kwargs = mock_update.call_args[1]
        assert call_kwargs["progress"] == 50
        assert call_kwargs["solved"] == 3
        assert call_kwargs["failed"] == 2
        assert call_kwargs["task_size"] == 10

    def test_throttled_by_interval(self) -> None:
        scraper = _make_scraper()
        scraper.list_size = 10
        scraper.solved = 1

        with patch("data_collector.scraping.base.update_app_status") as mock_update:
            scraper.update_progress(force=True)
            assert mock_update.call_count == 1

            scraper.update_progress()
            assert mock_update.call_count == 1

    def test_force_bypasses_throttle(self) -> None:
        scraper = _make_scraper()
        scraper.list_size = 10
        scraper.solved = 1

        with patch("data_collector.scraping.base.update_app_status") as mock_update:
            scraper.update_progress(force=True)
            scraper.update_progress(force=True)
            assert mock_update.call_count == 2

    def test_eta_present_when_items_remaining(self) -> None:
        scraper = _make_scraper()
        scraper.list_size = 10
        scraper.solved = 5
        scraper.failed = 0
        scraper._start_collect_timer()  # pyright: ignore[reportPrivateUsage]
        time.sleep(0.01)

        with patch("data_collector.scraping.base.update_app_status") as mock_update:
            scraper.update_progress(force=True)

        call_kwargs = mock_update.call_args[1]
        assert call_kwargs["eta"] is not None

    def test_no_eta_when_all_processed(self) -> None:
        scraper = _make_scraper()
        scraper.list_size = 5
        scraper.solved = 5
        scraper.failed = 0
        scraper._start_collect_timer()  # pyright: ignore[reportPrivateUsage]

        with patch("data_collector.scraping.base.update_app_status") as mock_update:
            scraper.update_progress(force=True)

        call_kwargs = mock_update.call_args[1]
        assert call_kwargs["eta"] is None
        assert call_kwargs["progress"] == 100


class TestStartCollectTimer:
    """Test _start_collect_timer sets the monotonic timestamp."""

    def test_sets_timestamp(self) -> None:
        scraper = _make_scraper()
        before = time.monotonic()
        scraper._start_collect_timer()  # pyright: ignore[reportPrivateUsage]
        after = time.monotonic()
        internal = scraper._collect_start_time  # pyright: ignore[reportPrivateUsage]
        assert internal is not None
        assert before <= internal <= after


class TestConsecutiveFailureThreshold:
    """Test max_consecutive_failures early exit."""

    def test_fatal_flag_after_max_consecutive(self) -> None:
        scraper = _make_scraper(max_consecutive_failures=5)
        for _ in range(5):
            scraper.increment_failed()
        assert scraper.fatal_flag == FatalFlag.UNEXPECTED_BEHAVIOUR
        assert "Consecutive failures (5)" in scraper.fatal_msg
        assert scraper.fatal_time is not None

    def test_consecutive_resets_on_solved(self) -> None:
        scraper = _make_scraper(max_consecutive_failures=5)
        for _ in range(4):
            scraper.increment_failed()
        scraper.increment_solved()
        scraper.increment_failed()
        assert scraper.fatal_flag == FatalFlag.NONE

    def test_disabled_when_zero(self) -> None:
        scraper = _make_scraper(max_consecutive_failures=0, max_error_rate=0.0)
        for _ in range(100):
            scraper.increment_failed()
        assert scraper.fatal_flag == FatalFlag.NONE

    def test_no_double_trigger(self) -> None:
        scraper = _make_scraper(max_consecutive_failures=3)
        for _ in range(3):
            scraper.increment_failed()
        first_msg = scraper.fatal_msg
        first_time = scraper.fatal_time
        for _ in range(3):
            scraper.increment_failed()
        assert scraper.fatal_msg == first_msg
        assert scraper.fatal_time == first_time


class TestErrorRateThreshold:
    """Test max_error_rate early exit."""

    def test_fatal_flag_when_rate_exceeds(self) -> None:
        scraper = _make_scraper(max_error_rate=0.20, min_error_sample=10, max_consecutive_failures=0)
        for _ in range(7):
            scraper.increment_solved()
        for _ in range(3):
            scraper.increment_failed()
        assert scraper.fatal_flag == FatalFlag.UNEXPECTED_BEHAVIOUR
        assert "Error rate" in scraper.fatal_msg

    def test_no_trigger_below_min_sample(self) -> None:
        scraper = _make_scraper(max_error_rate=0.20, min_error_sample=10, max_consecutive_failures=0)
        scraper.increment_solved(2)
        scraper.increment_failed(2)
        assert scraper.fatal_flag == FatalFlag.NONE

    def test_no_trigger_below_rate(self) -> None:
        scraper = _make_scraper(max_error_rate=0.20, min_error_sample=10, max_consecutive_failures=0)
        scraper.increment_solved(9)
        scraper.increment_failed(1)
        assert scraper.fatal_flag == FatalFlag.NONE

    def test_disabled_when_zero(self) -> None:
        scraper = _make_scraper(max_error_rate=0.0, min_error_sample=10, max_consecutive_failures=0)
        scraper.increment_solved(5)
        scraper.increment_failed(50)
        assert scraper.fatal_flag == FatalFlag.NONE


class TestShouldAbort:
    """Test should_abort property."""

    def test_false_when_no_fatal(self) -> None:
        scraper = _make_scraper()
        assert scraper.should_abort is False

    def test_true_after_consecutive_threshold(self) -> None:
        scraper = _make_scraper(max_consecutive_failures=3)
        for _ in range(3):
            scraper.increment_failed()
        assert scraper.should_abort is True

    def test_true_after_rate_threshold(self) -> None:
        scraper = _make_scraper(max_error_rate=0.20, min_error_sample=10, max_consecutive_failures=0)
        scraper.increment_solved(7)
        for _ in range(3):
            scraper.increment_failed()
        assert scraper.should_abort is True

    def test_abort_event_set_on_consecutive_threshold(self) -> None:
        scraper = _make_scraper(max_consecutive_failures=3)
        assert not scraper._abort_event.is_set()  # pyright: ignore[reportPrivateUsage]
        for _ in range(3):
            scraper.increment_failed()
        assert scraper._abort_event.is_set()  # pyright: ignore[reportPrivateUsage]

    def test_abort_event_set_on_rate_threshold(self) -> None:
        scraper = _make_scraper(max_error_rate=0.20, min_error_sample=10, max_consecutive_failures=0)
        scraper.increment_solved(7)
        for _ in range(3):
            scraper.increment_failed()
        assert scraper._abort_event.is_set()  # pyright: ignore[reportPrivateUsage]

    def test_abort_event_not_set_below_threshold(self) -> None:
        scraper = _make_scraper(max_consecutive_failures=5)
        for _ in range(4):
            scraper.increment_failed()
        assert not scraper._abort_event.is_set()  # pyright: ignore[reportPrivateUsage]
        assert scraper.should_abort is False
