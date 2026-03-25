"""Unit tests for BaseScraper class."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

from data_collector.enums import FatalFlag
from data_collector.scraping.base import BaseScraper
from data_collector.utilities.fun_watch import FunWatchMixin, FunWatchRegistry
from data_collector.utilities.request import RequestMetrics

_REGISTRY = "data_collector.utilities.fun_watch.FunWatchRegistry"


def _make_scraper(**overrides: object) -> BaseScraper:
    """Create a BaseScraper with mocked dependencies."""
    defaults: dict[str, object] = {
        "database": MagicMock(),
        "logger": MagicMock(),
        "runtime": "test_runtime_hash",
        "app_id": "test_app_hash",
    }
    defaults.update(overrides)
    return BaseScraper(**defaults)  # type: ignore[arg-type]


class TestConstructor:
    """Test BaseScraper constructor attribute initialization."""

    def test_sets_core_attributes(self) -> None:
        db = MagicMock()
        log = MagicMock()
        scraper = BaseScraper(database=db, logger=log, runtime="rt1", app_id="id1")
        assert scraper.database is db
        assert scraper.logger is log
        assert scraper.runtime == "rt1"
        assert scraper.app_id == "id1"
        assert scraper.args is None

    def test_sets_args(self) -> None:
        args = {"company_id": "123"}
        scraper = _make_scraper(args=args)
        assert scraper.args == args

    def test_creates_default_metrics(self) -> None:
        scraper = _make_scraper()
        assert isinstance(scraper.metrics, RequestMetrics)

    def test_uses_provided_metrics(self) -> None:
        metrics = RequestMetrics()
        scraper = _make_scraper(metrics=metrics)
        assert scraper.metrics is metrics

    def test_creates_request_with_metrics(self) -> None:
        metrics = RequestMetrics()
        scraper = _make_scraper(metrics=metrics)
        # Verify the request instance exists and metrics is accessible
        assert scraper.request is not None
        assert scraper.metrics is metrics

    def test_progress_counters_initialized(self) -> None:
        scraper = _make_scraper()
        assert scraper.solved == 0
        assert scraper.failed == 0
        assert scraper.list_size == 0
        assert scraper.progress == 0

    def test_fatal_tracking_initialized(self) -> None:
        scraper = _make_scraper()
        assert scraper.fatal_flag == FatalFlag.NONE
        assert scraper.fatal_msg == ""
        assert scraper.fatal_time is None

    def test_scheduling_initialized(self) -> None:
        scraper = _make_scraper()
        assert scraper.next_run is None
        assert scraper.alert_threshold == 0.20

    def test_base_url_default(self) -> None:
        scraper = _make_scraper()
        assert scraper.base_url == ""

    def test_work_list_empty(self) -> None:
        scraper = _make_scraper()
        assert scraper.work_list == []


class TestLifecycleHooks:
    """Test that lifecycle hooks are callable no-ops by default."""

    def test_prepare_list_noop(self) -> None:
        scraper = _make_scraper()
        scraper.prepare_list()  # should not raise

    def test_collect_noop(self) -> None:
        scraper = _make_scraper()
        scraper.collect()

    def test_store_noop(self) -> None:
        scraper = _make_scraper()
        scraper.store([{"key": "value"}])

    def test_cleanup_noop(self) -> None:
        scraper = _make_scraper()
        scraper.cleanup()

    def test_set_next_run_noop(self) -> None:
        scraper = _make_scraper()
        scraper.set_next_run()
        assert scraper.next_run is None


class TestSubclassing:
    """Test that subclasses can override lifecycle hooks."""

    def test_subclass_overrides(self) -> None:
        class MyScraper(BaseScraper):
            base_url = "https://example.com"

            def prepare_list(self) -> None:
                self.work_list = [1, 2, 3]
                self.list_size = len(self.work_list)

        scraper = MyScraper(
            database=MagicMock(), logger=MagicMock(), runtime="rt", app_id="id",
        )
        scraper.prepare_list()
        assert scraper.work_list == [1, 2, 3]
        assert scraper.list_size == 3
        assert scraper.base_url == "https://example.com"


@patch(f"{_REGISTRY}.complete_function_log")
@patch(f"{_REGISTRY}.start_function_log", return_value=1)
@patch(f"{_REGISTRY}.update_last_seen")
@patch(f"{_REGISTRY}.register_function")
class TestFatalCheck:
    """Test fatal_check() error evaluation."""

    def setup_method(self) -> None:
        FunWatchRegistry.reset()

    def test_no_requests_no_fatal(self, *_mocks: MagicMock) -> None:
        scraper = _make_scraper()
        scraper.metrics.request_count = 0
        scraper.fatal_check()
        assert scraper.fatal_flag == FatalFlag.NONE

    def test_below_threshold_no_fatal(self, *_mocks: MagicMock) -> None:
        scraper = _make_scraper()
        scraper.metrics.request_count = 100
        scraper.metrics.log_stats = MagicMock(return_value={  # type: ignore[method-assign]
            "error_rate_percent": 10.0,
            "error_breakdown": {"timeout": 10},
        })
        scraper.fatal_check()
        assert scraper.fatal_flag == FatalFlag.NONE
        assert scraper.fatal_msg == ""

    def test_above_threshold_triggers_fatal(self, *_mocks: MagicMock) -> None:
        scraper = _make_scraper()
        scraper.metrics.request_count = 100
        scraper.metrics.log_stats = MagicMock(return_value={  # type: ignore[method-assign]
            "error_rate_percent": 30.0,
            "error_breakdown": {"timeout": 20, "proxy": 10},
        })
        before = datetime.now(UTC)
        scraper.fatal_check()
        assert scraper.fatal_flag == FatalFlag.UNEXPECTED_BEHAVIOUR
        assert "30.0%" in scraper.fatal_msg
        assert scraper.fatal_time is not None
        assert scraper.fatal_time >= before

    def test_custom_threshold(self, *_mocks: MagicMock) -> None:
        scraper = _make_scraper()
        scraper.alert_threshold = 0.50
        scraper.metrics.request_count = 100
        scraper.metrics.log_stats = MagicMock(return_value={  # type: ignore[method-assign]
            "error_rate_percent": 30.0,
            "error_breakdown": {},
        })
        scraper.fatal_check()
        assert scraper.fatal_flag == FatalFlag.NONE

    def test_exact_threshold_no_fatal(self, *_mocks: MagicMock) -> None:
        scraper = _make_scraper()
        scraper.metrics.request_count = 100
        scraper.metrics.log_stats = MagicMock(return_value={  # type: ignore[method-assign]
            "error_rate_percent": 20.0,
            "error_breakdown": {},
        })
        scraper.fatal_check()
        assert scraper.fatal_flag == FatalFlag.NONE


class TestInheritance:
    """Test BaseScraper class hierarchy."""

    def test_inherits_fun_watch_mixin(self) -> None:
        assert issubclass(BaseScraper, FunWatchMixin)
