"""Tests for BaseCaptchaProvider abstract interface."""

from unittest.mock import MagicMock, patch

import pytest

from data_collector.captcha.metrics import CaptchaMetrics
from data_collector.captcha.models import CaptchaError, CaptchaResult, CaptchaTaskType, CaptchaTimeout
from data_collector.captcha.provider import BaseCaptchaProvider, _sanitize_url  # pyright: ignore[reportPrivateUsage]
from data_collector.enums.captcha import CaptchaErrorCategory
from data_collector.utilities.request import Request


def _make_stub_class() -> type[BaseCaptchaProvider]:
    """Return a minimal concrete subclass of BaseCaptchaProvider."""

    class _StubProvider(BaseCaptchaProvider):
        @property
        def provider_name(self) -> str:
            return "stub"

        def solve_recaptcha_v2(self, site_key: str, page_url: str) -> CaptchaResult:
            raise NotImplementedError

        def solve_recaptcha_v2_proxy(
            self, site_key: str, page_url: str,
            proxy_type: str, proxy_address: str, proxy_port: int,
            proxy_login: str, proxy_password: str,
        ) -> CaptchaResult:
            raise NotImplementedError

        def solve_recaptcha_v3(
            self, site_key: str, page_url: str, action: str, min_score: float = 0.3,
        ) -> CaptchaResult:
            raise NotImplementedError

        def solve_turnstile(self, site_key: str, page_url: str) -> CaptchaResult:
            raise NotImplementedError

        def solve_turnstile_proxy(
            self, site_key: str, page_url: str,
            proxy_type: str, proxy_address: str, proxy_port: int,
            proxy_login: str, proxy_password: str,
        ) -> CaptchaResult:
            raise NotImplementedError

        def solve_image(self, image_data: bytes, page_url: str) -> CaptchaResult:
            raise NotImplementedError

        def get_balance(self) -> float:
            raise NotImplementedError

    return _StubProvider


class TestBaseCaptchaProviderAbstract:
    """Tests for BaseCaptchaProvider as an abstract class."""

    def test_cannot_instantiate_directly(self) -> None:
        with pytest.raises(TypeError, match="abstract"):
            BaseCaptchaProvider(Request())  # type: ignore[abstract]

    def test_stores_constructor_args(self) -> None:
        stub_class = _make_stub_class()
        request = Request()
        metrics = CaptchaMetrics()
        provider = stub_class(request, timeout=60, max_retries=1, poll_interval=3, metrics=metrics)

        assert provider.request is request
        assert provider.timeout == 60
        assert provider.max_retries == 1
        assert provider.poll_interval == 3
        assert provider.metrics is metrics

    def test_stores_database_params(self) -> None:
        stub_class = _make_stub_class()
        database = MagicMock()
        provider = stub_class(Request(), database=database, app_id="abc123", runtime="rt-1")

        assert provider._database is database  # pyright: ignore[reportPrivateUsage]
        assert provider._app_id == "abc123"  # pyright: ignore[reportPrivateUsage]
        assert provider._runtime == "rt-1"  # pyright: ignore[reportPrivateUsage]

    def test_database_params_default_to_none(self) -> None:
        stub_class = _make_stub_class()
        provider = stub_class(Request())

        assert provider._database is None  # pyright: ignore[reportPrivateUsage]
        assert provider._app_id is None  # pyright: ignore[reportPrivateUsage]
        assert provider._runtime is None  # pyright: ignore[reportPrivateUsage]


class TestLogEnabled:
    """Tests for _log_enabled property."""

    def test_enabled_when_all_set(self) -> None:
        stub_class = _make_stub_class()
        provider = stub_class(Request(), database=MagicMock(), app_id="a", runtime="r")
        assert provider._log_enabled is True  # pyright: ignore[reportPrivateUsage]

    def test_disabled_when_database_missing(self) -> None:
        stub_class = _make_stub_class()
        provider = stub_class(Request(), app_id="a", runtime="r")
        assert provider._log_enabled is False  # pyright: ignore[reportPrivateUsage]

    def test_disabled_when_app_id_missing(self) -> None:
        stub_class = _make_stub_class()
        provider = stub_class(Request(), database=MagicMock(), runtime="r")
        assert provider._log_enabled is False  # pyright: ignore[reportPrivateUsage]

    def test_disabled_when_runtime_missing(self) -> None:
        stub_class = _make_stub_class()
        provider = stub_class(Request(), database=MagicMock(), app_id="a")
        assert provider._log_enabled is False  # pyright: ignore[reportPrivateUsage]


class TestReportDefaults:
    """Tests for default report_correct/report_incorrect."""

    def test_report_correct_returns_false(self) -> None:
        stub_class = _make_stub_class()
        provider = stub_class(Request())
        assert provider.report_correct("123", CaptchaTaskType.RECAPTCHA_V2) is False

    def test_report_incorrect_returns_false(self) -> None:
        stub_class = _make_stub_class()
        provider = stub_class(Request())
        assert provider.report_incorrect("123", CaptchaTaskType.IMAGE) is False


class TestSanitizeUrl:
    """Tests for _sanitize_url helper function."""

    def test_strips_query_string(self) -> None:
        assert _sanitize_url("https://example.com/page?token=abc&id=1") == "https://example.com/page"

    def test_strips_fragment(self) -> None:
        assert _sanitize_url("https://example.com/page#section") == "https://example.com/page"

    def test_preserves_path(self) -> None:
        assert _sanitize_url("https://example.com/a/b/c") == "https://example.com/a/b/c"

    def test_preserves_scheme_and_host(self) -> None:
        assert _sanitize_url("http://sub.example.com:8080/path") == "http://sub.example.com:8080/path"

    def test_empty_path(self) -> None:
        assert _sanitize_url("https://example.com") == "https://example.com"


class TestPersistLog:
    """Tests for _persist_log database logging."""

    def test_no_op_when_log_disabled(self) -> None:
        stub_class = _make_stub_class()
        provider = stub_class(Request())
        # Should not raise even with no database
        provider._persist_log(  # pyright: ignore[reportPrivateUsage]
            task_id="1",
            task_type=CaptchaTaskType.RECAPTCHA_V2,
            page_url="https://example.com",
            status=MagicMock(),
            cost=0.002,
            elapsed_seconds=10.0,
        )

    def test_persists_when_log_enabled(self) -> None:
        stub_class = _make_stub_class()
        mock_database = MagicMock()
        mock_session = MagicMock()
        mock_database.create_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_database.create_session.return_value.__exit__ = MagicMock(return_value=False)

        provider = stub_class(Request(), database=mock_database, app_id="app1", runtime="rt1")
        from data_collector.enums.captcha import CaptchaSolveStatus

        provider._persist_log(  # pyright: ignore[reportPrivateUsage]
            task_id="123",
            task_type=CaptchaTaskType.RECAPTCHA_V2,
            page_url="https://example.com/page?q=1",
            status=CaptchaSolveStatus.SOLVED,
            cost=0.002,
            elapsed_seconds=15.0,
        )

        mock_session.add.assert_called_once()
        mock_session.commit.assert_called_once()
        mock_session.flush.assert_not_called()
        record = mock_session.add.call_args[0][0]
        assert record.page_url == "https://example.com/page"  # query stripped

    def test_persists_error_record_when_error_fields_present(self) -> None:
        stub_class = _make_stub_class()
        mock_database = MagicMock()
        mock_session = MagicMock()
        mock_database.create_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_database.create_session.return_value.__exit__ = MagicMock(return_value=False)

        provider = stub_class(Request(), database=mock_database, app_id="app1", runtime="rt1")
        from data_collector.enums.captcha import CaptchaErrorCategory, CaptchaSolveStatus

        provider._persist_log(  # pyright: ignore[reportPrivateUsage]
            task_id="456",
            task_type=CaptchaTaskType.RECAPTCHA_V2,
            page_url="https://example.com",
            status=CaptchaSolveStatus.FAILED,
            error_code="ERROR_ZERO_BALANCE",
            error_description="No funds",
            error_category=CaptchaErrorCategory.BALANCE,
        )

        assert mock_session.add.call_count == 2
        mock_session.flush.assert_called_once()
        mock_session.commit.assert_called_once()

        from data_collector.tables.captcha import CaptchaLog, CaptchaLogError

        log_record = mock_session.add.call_args_list[0][0][0]
        assert isinstance(log_record, CaptchaLog)

        error_record = mock_session.add.call_args_list[1][0][0]
        assert isinstance(error_record, CaptchaLogError)
        assert error_record.error_code == "ERROR_ZERO_BALANCE"  # pyright: ignore[reportGeneralTypeIssues]
        assert error_record.error_description == "No funds"  # pyright: ignore[reportGeneralTypeIssues]
        assert error_record.error_category == CaptchaErrorCategory.BALANCE.value  # pyright: ignore[reportGeneralTypeIssues]

    def test_exception_does_not_propagate(self) -> None:
        stub_class = _make_stub_class()
        mock_database = MagicMock()
        mock_database.create_session.side_effect = RuntimeError("DB down")

        provider = stub_class(Request(), database=mock_database, app_id="app1", runtime="rt1")
        from data_collector.enums.captcha import CaptchaSolveStatus

        # Should not raise
        provider._persist_log(  # pyright: ignore[reportPrivateUsage]
            task_id="1",
            task_type=CaptchaTaskType.RECAPTCHA_V2,
            page_url="https://example.com",
            status=CaptchaSolveStatus.FAILED,
        )


class TestCreateAndPollRetry:
    """Tests for BaseCaptchaProvider._create_and_poll retry logic."""

    def _make_provider(self, max_retries: int = 2, metrics: CaptchaMetrics | None = None) -> BaseCaptchaProvider:
        stub_class = _make_stub_class()
        return stub_class(Request(), max_retries=max_retries, metrics=metrics)

    def test_success_on_first_attempt(self) -> None:
        provider = self._make_provider()
        expected = CaptchaResult(
            task_id="1", task_type=CaptchaTaskType.RECAPTCHA_V2,
            solution="token", cost=0.002, elapsed_seconds=10.0,
        )
        create_function = MagicMock(return_value=("1", 0.0))
        poll_function = MagicMock(return_value=expected)

        result = provider._create_and_poll(  # pyright: ignore[reportPrivateUsage]
            create_function, CaptchaTaskType.RECAPTCHA_V2, poll_function, page_url="https://example.com")

        assert result is expected
        assert create_function.call_count == 1
        assert poll_function.call_count == 1

    def test_retry_on_timeout_then_success(self) -> None:
        metrics = CaptchaMetrics()
        provider = self._make_provider(max_retries=2, metrics=metrics)
        expected = CaptchaResult(
            task_id="2", task_type=CaptchaTaskType.IMAGE,
            solution="abc", cost=0.001, elapsed_seconds=5.0,
        )
        create_function = MagicMock(side_effect=[("1", 100.0), ("2", 100.0)])
        poll_function = MagicMock(side_effect=[
            CaptchaTimeout(task_id="1", timeout_seconds=120),
            expected,
        ])

        with patch("data_collector.captcha.provider.time.monotonic", return_value=105.0):
            result = provider._create_and_poll(  # pyright: ignore[reportPrivateUsage]
                create_function, CaptchaTaskType.IMAGE, poll_function, page_url="https://example.com")

        assert result is expected
        assert create_function.call_count == 2
        assert poll_function.call_count == 2
        assert metrics.submitted == 2
        assert metrics.timed_out == 1
        assert metrics.solved == 1

    def test_all_retries_exhausted_raises_timeout(self) -> None:
        metrics = CaptchaMetrics()
        provider = self._make_provider(max_retries=1, metrics=metrics)
        create_function = MagicMock(side_effect=[("1", 100.0), ("2", 100.0)])
        poll_function = MagicMock(side_effect=[
            CaptchaTimeout(task_id="1", timeout_seconds=120),
            CaptchaTimeout(task_id="2", timeout_seconds=120),
        ])

        with patch("data_collector.captcha.provider.time.monotonic", return_value=105.0), \
                pytest.raises(CaptchaTimeout):
            provider._create_and_poll(  # pyright: ignore[reportPrivateUsage]
                create_function, CaptchaTaskType.TURNSTILE, poll_function, page_url="https://example.com")

        assert metrics.submitted == 2
        assert metrics.timed_out == 2
        assert metrics.solved == 0

    def test_captcha_error_is_logged_and_reraised(self) -> None:
        metrics = CaptchaMetrics()
        provider = self._make_provider(max_retries=0, metrics=metrics)
        create_function = MagicMock(return_value=("1", 100.0))
        poll_function = MagicMock(side_effect=CaptchaError(
            error_id=10, error_code="ERROR_ZERO_BALANCE", error_description="No funds",
            category=CaptchaErrorCategory.BALANCE,
        ))

        with patch("data_collector.captcha.provider.time.monotonic", return_value=102.0), \
                pytest.raises(CaptchaError, match="ERROR_ZERO_BALANCE"):
            provider._create_and_poll(  # pyright: ignore[reportPrivateUsage]
                create_function, CaptchaTaskType.RECAPTCHA_V2, poll_function, page_url="https://example.com")

        assert metrics.failed == 1

    def test_no_metrics_does_not_fail(self) -> None:
        provider = self._make_provider(metrics=None)
        expected = CaptchaResult(
            task_id="1", task_type=CaptchaTaskType.RECAPTCHA_V3,
            solution="token", cost=0.003, elapsed_seconds=8.0,
        )
        create_function = MagicMock(return_value=("1", 0.0))
        poll_function = MagicMock(return_value=expected)

        result = provider._create_and_poll(  # pyright: ignore[reportPrivateUsage]
            create_function, CaptchaTaskType.RECAPTCHA_V3, poll_function, page_url="https://example.com")
        assert result is expected
