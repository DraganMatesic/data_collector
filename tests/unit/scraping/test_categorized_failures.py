"""Unit tests for per-category failure thresholds in BaseScraper."""

from __future__ import annotations

from unittest.mock import MagicMock

from data_collector.enums import ErrorCategory, FatalFlag
from data_collector.scraping.base import (
    DEFAULT_CATEGORY_THRESHOLDS,
    BaseScraper,
    CategoryThreshold,
)


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


class TestCategoryThresholdDataclass:
    """Test CategoryThreshold frozen dataclass."""

    def test_construction(self) -> None:
        threshold = CategoryThreshold(
            category=ErrorCategory.DATABASE, max_count=1, max_rate=0.0,
            min_sample=0, max_consecutive=1,
        )
        assert threshold.category == ErrorCategory.DATABASE
        assert threshold.max_count == 1
        assert threshold.max_rate == 0.0
        assert threshold.min_sample == 0
        assert threshold.max_consecutive == 1

    def test_frozen(self) -> None:
        threshold = CategoryThreshold(
            category=ErrorCategory.HTTP, max_count=0, max_rate=0.20,
            min_sample=10, max_consecutive=5,
        )
        try:
            threshold.max_count = 10  # type: ignore[misc]
            raised = False
        except AttributeError:
            raised = True
        assert raised


class TestDefaultCategoryThresholds:
    """Test DEFAULT_CATEGORY_THRESHOLDS tuple."""

    def test_all_categories_covered(self) -> None:
        categories = {threshold.category for threshold in DEFAULT_CATEGORY_THRESHOLDS}
        expected = {
            ErrorCategory.DATABASE, ErrorCategory.IO_WRITE, ErrorCategory.CAPTCHA,
            ErrorCategory.PROXY, ErrorCategory.HTTP, ErrorCategory.PARSE, ErrorCategory.UNKNOWN,
        }
        assert categories == expected

    def test_database_immediate_stop(self) -> None:
        database_threshold = next(
            threshold for threshold in DEFAULT_CATEGORY_THRESHOLDS
            if threshold.category == ErrorCategory.DATABASE
        )
        assert database_threshold.max_count == 1
        assert database_threshold.max_consecutive == 1

    def test_io_write_immediate_stop(self) -> None:
        io_threshold = next(
            threshold for threshold in DEFAULT_CATEGORY_THRESHOLDS
            if threshold.category == ErrorCategory.IO_WRITE
        )
        assert io_threshold.max_count == 1
        assert io_threshold.max_consecutive == 1


class TestDatabaseCategoryImmediateStop:
    """Test that DATABASE category triggers fatal on first error."""

    def test_single_database_error_triggers_fatal(self) -> None:
        scraper = _make_scraper(category_thresholds=DEFAULT_CATEGORY_THRESHOLDS)
        scraper.increment_failed(error_category=ErrorCategory.DATABASE)
        assert scraper.fatal_flag == FatalFlag.UNEXPECTED_BEHAVIOUR
        assert "database" in scraper.fatal_msg
        assert scraper.fatal_time is not None

    def test_single_io_write_error_triggers_fatal(self) -> None:
        scraper = _make_scraper(category_thresholds=DEFAULT_CATEGORY_THRESHOLDS)
        scraper.increment_failed(error_category=ErrorCategory.IO_WRITE)
        assert scraper.fatal_flag == FatalFlag.UNEXPECTED_BEHAVIOUR
        assert "io_write" in scraper.fatal_msg


class TestHTTPCategoryThreshold:
    """Test HTTP category with consecutive and rate thresholds."""

    def test_consecutive_http_errors_trigger_fatal(self) -> None:
        thresholds = (
            CategoryThreshold(ErrorCategory.HTTP, max_count=0, max_rate=0.0, min_sample=0, max_consecutive=3),
        )
        scraper = _make_scraper(category_thresholds=thresholds)
        for _ in range(3):
            scraper.increment_failed(error_category=ErrorCategory.HTTP)
        assert scraper.fatal_flag == FatalFlag.UNEXPECTED_BEHAVIOUR
        assert "http" in scraper.fatal_msg
        assert "consecutive" in scraper.fatal_msg

    def test_consecutive_resets_on_solved(self) -> None:
        thresholds = (
            CategoryThreshold(ErrorCategory.HTTP, max_count=0, max_rate=0.0, min_sample=0, max_consecutive=3),
        )
        scraper = _make_scraper(category_thresholds=thresholds)
        scraper.increment_failed(error_category=ErrorCategory.HTTP)
        scraper.increment_failed(error_category=ErrorCategory.HTTP)
        scraper.increment_solved()
        scraper.increment_failed(error_category=ErrorCategory.HTTP)
        assert scraper.fatal_flag == FatalFlag.NONE

    def test_rate_threshold(self) -> None:
        thresholds = (
            CategoryThreshold(ErrorCategory.HTTP, max_count=0, max_rate=0.20, min_sample=10, max_consecutive=0),
        )
        scraper = _make_scraper(category_thresholds=thresholds)
        scraper.increment_solved(7)
        for _ in range(3):
            scraper.increment_failed(error_category=ErrorCategory.HTTP)
        assert scraper.fatal_flag == FatalFlag.UNEXPECTED_BEHAVIOUR
        assert "http" in scraper.fatal_msg
        assert "rate" in scraper.fatal_msg.lower()

    def test_rate_no_trigger_below_min_sample(self) -> None:
        thresholds = (
            CategoryThreshold(ErrorCategory.HTTP, max_count=0, max_rate=0.20, min_sample=10, max_consecutive=0),
        )
        scraper = _make_scraper(category_thresholds=thresholds)
        scraper.increment_solved(2)
        scraper.increment_failed(2, error_category=ErrorCategory.HTTP)
        assert scraper.fatal_flag == FatalFlag.NONE


class TestProxyCategoryThreshold:
    """Test PROXY category with higher tolerance."""

    def test_proxy_errors_below_threshold_no_fatal(self) -> None:
        thresholds = (
            CategoryThreshold(ErrorCategory.PROXY, max_count=0, max_rate=0.30, min_sample=10, max_consecutive=10),
        )
        scraper = _make_scraper(category_thresholds=thresholds)
        scraper.increment_solved(8)
        scraper.increment_failed(2, error_category=ErrorCategory.PROXY)
        assert scraper.fatal_flag == FatalFlag.NONE

    def test_proxy_consecutive_threshold(self) -> None:
        thresholds = (
            CategoryThreshold(ErrorCategory.PROXY, max_count=0, max_rate=0.0, min_sample=0, max_consecutive=3),
        )
        scraper = _make_scraper(category_thresholds=thresholds)
        for _ in range(3):
            scraper.increment_failed(error_category=ErrorCategory.PROXY)
        assert scraper.fatal_flag == FatalFlag.UNEXPECTED_BEHAVIOUR
        assert "proxy" in scraper.fatal_msg


class TestMixedCategories:
    """Test that different categories are tracked independently."""

    def test_database_triggers_before_proxy_threshold(self) -> None:
        scraper = _make_scraper(category_thresholds=DEFAULT_CATEGORY_THRESHOLDS)
        scraper.increment_failed(error_category=ErrorCategory.PROXY)
        scraper.increment_failed(error_category=ErrorCategory.PROXY)
        assert scraper.fatal_flag == FatalFlag.NONE
        scraper.increment_failed(error_category=ErrorCategory.DATABASE)
        assert scraper.fatal_flag == FatalFlag.UNEXPECTED_BEHAVIOUR
        assert "database" in scraper.fatal_msg

    def test_categories_tracked_independently(self) -> None:
        thresholds = (
            CategoryThreshold(ErrorCategory.HTTP, max_count=0, max_rate=0.0, min_sample=0, max_consecutive=3),
            CategoryThreshold(ErrorCategory.PROXY, max_count=0, max_rate=0.0, min_sample=0, max_consecutive=3),
        )
        scraper = _make_scraper(category_thresholds=thresholds)
        scraper.increment_failed(error_category=ErrorCategory.HTTP)
        scraper.increment_failed(error_category=ErrorCategory.PROXY)
        scraper.increment_failed(error_category=ErrorCategory.HTTP)
        scraper.increment_failed(error_category=ErrorCategory.PROXY)
        assert scraper.fatal_flag == FatalFlag.NONE

    def test_uncategorized_failures_tracked_in_flat_counter(self) -> None:
        thresholds = (
            CategoryThreshold(ErrorCategory.HTTP, max_count=0, max_rate=0.0, min_sample=0, max_consecutive=100),
        )
        scraper = _make_scraper(category_thresholds=thresholds)
        scraper.increment_failed(error_category=ErrorCategory.HTTP)
        assert scraper.failed == 1
        assert scraper._consecutive_failures == 1  # pyright: ignore[reportPrivateUsage]


class TestBackwardCompatibility:
    """Test that category_thresholds=None preserves flat threshold behavior."""

    def test_flat_consecutive_still_works(self) -> None:
        scraper = _make_scraper(max_consecutive_failures=3)
        for _ in range(3):
            scraper.increment_failed()
        assert scraper.fatal_flag == FatalFlag.UNEXPECTED_BEHAVIOUR
        assert "Consecutive failures" in scraper.fatal_msg

    def test_flat_rate_still_works(self) -> None:
        scraper = _make_scraper(max_error_rate=0.20, min_error_sample=10, max_consecutive_failures=0)
        scraper.increment_solved(7)
        for _ in range(3):
            scraper.increment_failed()
        assert scraper.fatal_flag == FatalFlag.UNEXPECTED_BEHAVIOUR
        assert "Error rate" in scraper.fatal_msg

    def test_no_category_thresholds_by_default(self) -> None:
        scraper = _make_scraper()
        assert scraper._category_thresholds is None  # pyright: ignore[reportPrivateUsage]
        assert scraper._category_thresholds_map == {}  # pyright: ignore[reportPrivateUsage]

    def test_error_category_ignored_in_flat_mode(self) -> None:
        scraper = _make_scraper(max_consecutive_failures=3)
        for _ in range(3):
            scraper.increment_failed(error_category=ErrorCategory.HTTP)
        assert scraper.fatal_flag == FatalFlag.UNEXPECTED_BEHAVIOUR
        assert "Consecutive failures" in scraper.fatal_msg


class TestCategoryCountThreshold:
    """Test absolute count threshold."""

    def test_count_threshold_triggers_at_exact_count(self) -> None:
        thresholds = (
            CategoryThreshold(ErrorCategory.CAPTCHA, max_count=3, max_rate=0.0, min_sample=0, max_consecutive=0),
        )
        scraper = _make_scraper(category_thresholds=thresholds)
        scraper.increment_failed(error_category=ErrorCategory.CAPTCHA)
        scraper.increment_failed(error_category=ErrorCategory.CAPTCHA)
        assert scraper.fatal_flag == FatalFlag.NONE
        scraper.increment_failed(error_category=ErrorCategory.CAPTCHA)
        assert scraper.fatal_flag == FatalFlag.UNEXPECTED_BEHAVIOUR
        assert "captcha" in scraper.fatal_msg
        assert "count" in scraper.fatal_msg

    def test_count_disabled_when_zero(self) -> None:
        thresholds = (
            CategoryThreshold(ErrorCategory.HTTP, max_count=0, max_rate=0.0, min_sample=0, max_consecutive=0),
        )
        scraper = _make_scraper(category_thresholds=thresholds)
        for _ in range(100):
            scraper.increment_failed(error_category=ErrorCategory.HTTP)
        assert scraper.fatal_flag == FatalFlag.NONE


class TestNoDoubleTrigger:
    """Test that fatal flag is set only once."""

    def test_no_double_trigger_with_categories(self) -> None:
        scraper = _make_scraper(category_thresholds=DEFAULT_CATEGORY_THRESHOLDS)
        scraper.increment_failed(error_category=ErrorCategory.DATABASE)
        first_msg = scraper.fatal_msg
        first_time = scraper.fatal_time
        scraper.increment_failed(error_category=ErrorCategory.DATABASE)
        assert scraper.fatal_msg == first_msg
        assert scraper.fatal_time == first_time


class TestErrorCategoryAsString:
    """Test that error_category accepts both ErrorCategory and plain strings."""

    def test_string_category_tracked(self) -> None:
        thresholds = (
            CategoryThreshold(ErrorCategory.HTTP, max_count=2, max_rate=0.0, min_sample=0, max_consecutive=0),
        )
        scraper = _make_scraper(category_thresholds=thresholds)
        scraper.increment_failed(error_category="http")
        scraper.increment_failed(error_category="http")
        assert scraper.fatal_flag == FatalFlag.UNEXPECTED_BEHAVIOUR

    def test_enum_and_string_equivalent(self) -> None:
        thresholds = (
            CategoryThreshold(ErrorCategory.PROXY, max_count=2, max_rate=0.0, min_sample=0, max_consecutive=0),
        )
        scraper = _make_scraper(category_thresholds=thresholds)
        scraper.increment_failed(error_category=ErrorCategory.PROXY)
        scraper.increment_failed(error_category="proxy")
        assert scraper.fatal_flag == FatalFlag.UNEXPECTED_BEHAVIOUR


class TestIncrementSolvedResetsAllCategories:
    """Test that increment_solved resets all per-category consecutive counters."""

    def test_resets_all_category_consecutive(self) -> None:
        thresholds = (
            CategoryThreshold(ErrorCategory.HTTP, max_count=0, max_rate=0.0, min_sample=0, max_consecutive=5),
            CategoryThreshold(ErrorCategory.PROXY, max_count=0, max_rate=0.0, min_sample=0, max_consecutive=5),
        )
        scraper = _make_scraper(category_thresholds=thresholds)
        scraper.increment_failed(error_category=ErrorCategory.HTTP)
        scraper.increment_failed(error_category=ErrorCategory.PROXY)
        scraper.increment_failed(error_category=ErrorCategory.PROXY)

        scraper.increment_solved()

        consecutive = scraper._category_consecutive  # pyright: ignore[reportPrivateUsage]
        assert consecutive.get("http", 0) == 0
        assert consecutive.get("proxy", 0) == 0
