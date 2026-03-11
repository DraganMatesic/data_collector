"""Tests for BaseCaptchaProvider abstract interface."""

from unittest.mock import MagicMock

import pytest

from data_collector.captcha.metrics import CaptchaMetrics
from data_collector.captcha.models import CaptchaResult, CaptchaTaskType, CaptchaTimeout
from data_collector.captcha.provider import BaseCaptchaProvider
from data_collector.utilities.request import Request


class TestBaseCaptchaProviderAbstract:
    """Tests for BaseCaptchaProvider as an abstract class."""

    def test_cannot_instantiate_directly(self) -> None:
        with pytest.raises(TypeError, match="abstract"):
            BaseCaptchaProvider(Request())  # type: ignore[abstract]

    def test_stores_constructor_args(self) -> None:
        """Verify constructor args are stored via a concrete subclass."""

        class _StubProvider(BaseCaptchaProvider):
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

            def solve_image(self, image_data: bytes) -> CaptchaResult:
                raise NotImplementedError

            def get_balance(self) -> float:
                raise NotImplementedError

        request = Request()
        metrics = CaptchaMetrics()
        provider = _StubProvider(request, timeout=60, max_retries=1, poll_interval=3, metrics=metrics)

        assert provider.request is request
        assert provider.timeout == 60
        assert provider.max_retries == 1
        assert provider.poll_interval == 3
        assert provider.metrics is metrics


class TestCreateAndPollRetry:
    """Tests for BaseCaptchaProvider._create_and_poll retry logic."""

    def _make_stub_provider(self, max_retries: int = 2, metrics: CaptchaMetrics | None = None) -> BaseCaptchaProvider:
        """Create a minimal concrete provider for testing retry logic."""

        class _StubProvider(BaseCaptchaProvider):
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

            def solve_image(self, image_data: bytes) -> CaptchaResult:
                raise NotImplementedError

            def get_balance(self) -> float:
                raise NotImplementedError

        return _StubProvider(Request(), max_retries=max_retries, metrics=metrics)

    def test_success_on_first_attempt(self) -> None:
        provider = self._make_stub_provider()
        expected = CaptchaResult(
            task_id="1", task_type=CaptchaTaskType.RECAPTCHA_V2,
            solution="token", cost=0.002, elapsed_seconds=10.0,
        )
        create_function = MagicMock(return_value=("1", 0.0))
        poll_function = MagicMock(return_value=expected)

        result = provider._create_and_poll(  # pyright: ignore[reportPrivateUsage]
            create_function, CaptchaTaskType.RECAPTCHA_V2, poll_function)

        assert result is expected
        assert create_function.call_count == 1
        assert poll_function.call_count == 1

    def test_retry_on_timeout_then_success(self) -> None:
        metrics = CaptchaMetrics()
        provider = self._make_stub_provider(max_retries=2, metrics=metrics)
        expected = CaptchaResult(
            task_id="2", task_type=CaptchaTaskType.IMAGE,
            solution="abc", cost=0.001, elapsed_seconds=5.0,
        )
        create_function = MagicMock(side_effect=[("1", 0.0), ("2", 0.0)])
        poll_function = MagicMock(side_effect=[
            CaptchaTimeout(task_id="1", timeout_seconds=120),
            expected,
        ])

        result = provider._create_and_poll(  # pyright: ignore[reportPrivateUsage]
            create_function, CaptchaTaskType.IMAGE, poll_function)

        assert result is expected
        assert create_function.call_count == 2
        assert poll_function.call_count == 2
        assert metrics.submitted == 2
        assert metrics.timed_out == 1
        assert metrics.solved == 1

    def test_all_retries_exhausted_raises_timeout(self) -> None:
        metrics = CaptchaMetrics()
        provider = self._make_stub_provider(max_retries=1, metrics=metrics)
        create_function = MagicMock(side_effect=[("1", 0.0), ("2", 0.0)])
        poll_function = MagicMock(side_effect=[
            CaptchaTimeout(task_id="1", timeout_seconds=120),
            CaptchaTimeout(task_id="2", timeout_seconds=120),
        ])

        with pytest.raises(CaptchaTimeout):
            provider._create_and_poll(  # pyright: ignore[reportPrivateUsage]
            create_function, CaptchaTaskType.TURNSTILE, poll_function)

        assert metrics.submitted == 2
        assert metrics.timed_out == 2
        assert metrics.solved == 0

    def test_no_metrics_does_not_fail(self) -> None:
        provider = self._make_stub_provider(metrics=None)
        expected = CaptchaResult(
            task_id="1", task_type=CaptchaTaskType.RECAPTCHA_V3,
            solution="token", cost=0.003, elapsed_seconds=8.0,
        )
        create_function = MagicMock(return_value=("1", 0.0))
        poll_function = MagicMock(return_value=expected)

        result = provider._create_and_poll(  # pyright: ignore[reportPrivateUsage]
            create_function, CaptchaTaskType.RECAPTCHA_V3, poll_function)
        assert result is expected
