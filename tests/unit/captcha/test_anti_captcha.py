"""Tests for AntiCaptchaProvider implementation."""

from __future__ import annotations

import base64
from unittest.mock import MagicMock, patch

import pytest

from data_collector.captcha.anti_captcha import AntiCaptchaProvider
from data_collector.captcha.metrics import CaptchaMetrics
from data_collector.captcha.models import CaptchaError, CaptchaResult, CaptchaTaskType, CaptchaTimeout
from data_collector.utilities.request import Request

# Alias to avoid repeating the pyright ignore on every access
_TASK_MAP = AntiCaptchaProvider._TASK_TYPE_MAP  # pyright: ignore[reportPrivateUsage]
_SOLUTION_MAP = AntiCaptchaProvider._SOLUTION_FIELD_MAP  # pyright: ignore[reportPrivateUsage]


def _make_provider(
    api_key: str = "test-key",
    timeout: int = 10,
    max_retries: int = 0,
    poll_interval: int = 0,
    metrics: CaptchaMetrics | None = None,
) -> AntiCaptchaProvider:
    """Create an AntiCaptchaProvider with test defaults."""
    return AntiCaptchaProvider(
        api_key=api_key,
        request=Request(),
        timeout=timeout,
        max_retries=max_retries,
        poll_interval=poll_interval,
        metrics=metrics,
    )


def _mock_response(data: dict[str, object]) -> MagicMock:
    """Create a mock httpx.Response with a .json() method."""
    response = MagicMock()
    response.json.return_value = data
    return response


class TestTaskTypeMap:
    """Tests for _TASK_TYPE_MAP correctness."""

    def test_recaptcha_v2_maps_to_proxyless(self) -> None:
        assert _TASK_MAP[CaptchaTaskType.RECAPTCHA_V2] == "RecaptchaV2TaskProxyless"

    def test_recaptcha_v2_proxy_maps(self) -> None:
        assert _TASK_MAP[CaptchaTaskType.RECAPTCHA_V2_PROXY] == "RecaptchaV2Task"

    def test_recaptcha_v3_maps(self) -> None:
        assert _TASK_MAP[CaptchaTaskType.RECAPTCHA_V3] == "RecaptchaV3TaskProxyless"

    def test_turnstile_maps_to_proxyless(self) -> None:
        assert _TASK_MAP[CaptchaTaskType.TURNSTILE] == "TurnstileTaskProxyless"

    def test_turnstile_proxy_maps(self) -> None:
        assert _TASK_MAP[CaptchaTaskType.TURNSTILE_PROXY] == "TurnstileTask"

    def test_image_maps(self) -> None:
        assert _TASK_MAP[CaptchaTaskType.IMAGE] == "ImageToTextTask"

    def test_all_task_types_mapped(self) -> None:
        for task_type in CaptchaTaskType:
            assert task_type in _TASK_MAP


class TestSolutionFieldMap:
    """Tests for _SOLUTION_FIELD_MAP correctness."""

    def test_recaptcha_v2_uses_g_recaptcha_response(self) -> None:
        assert _SOLUTION_MAP[CaptchaTaskType.RECAPTCHA_V2] == "gRecaptchaResponse"

    def test_recaptcha_v3_uses_g_recaptcha_response(self) -> None:
        assert _SOLUTION_MAP[CaptchaTaskType.RECAPTCHA_V3] == "gRecaptchaResponse"

    def test_turnstile_uses_token(self) -> None:
        assert _SOLUTION_MAP[CaptchaTaskType.TURNSTILE] == "token"

    def test_image_uses_text(self) -> None:
        assert _SOLUTION_MAP[CaptchaTaskType.IMAGE] == "text"

    def test_all_task_types_mapped(self) -> None:
        for task_type in CaptchaTaskType:
            assert task_type in _SOLUTION_MAP


class TestCreateTask:
    """Tests for _create_task API call."""

    def test_success_returns_task_id(self) -> None:
        provider = _make_provider()
        mock_response = _mock_response({"errorId": 0, "taskId": 12345})

        with patch.object(provider.request, "post", return_value=mock_response):
            task_id, start_time = provider._create_task({"type": "RecaptchaV2TaskProxyless"})  # pyright: ignore[reportPrivateUsage]

        assert task_id == "12345"
        assert isinstance(start_time, float)

    def test_api_error_raises_captcha_error(self) -> None:
        provider = _make_provider()
        mock_response = _mock_response({
            "errorId": 1,
            "errorCode": "ERROR_KEY_DOES_NOT_EXIST",
            "errorDescription": "Account authorization key not found",
        })

        with patch.object(provider.request, "post", return_value=mock_response), \
                pytest.raises(CaptchaError, match="ERROR_KEY_DOES_NOT_EXIST"):
            provider._create_task({"type": "RecaptchaV2TaskProxyless"})  # pyright: ignore[reportPrivateUsage]

    def test_request_failure_raises_captcha_error(self) -> None:
        provider = _make_provider()

        with patch.object(provider.request, "post", return_value=None), \
                pytest.raises(CaptchaError, match="REQUEST_FAILED"):
            provider._create_task({"type": "RecaptchaV2TaskProxyless"})  # pyright: ignore[reportPrivateUsage]

    def test_sends_correct_payload(self) -> None:
        provider = _make_provider(api_key="my-key")
        mock_response = _mock_response({"errorId": 0, "taskId": 1})

        with patch.object(provider.request, "post", return_value=mock_response) as mock_post:
            provider._create_task({"type": "RecaptchaV2TaskProxyless", "websiteURL": "https://example.com"})  # pyright: ignore[reportPrivateUsage]

        call_args = mock_post.call_args
        assert "api.anti-captcha.com/createTask" in call_args[0][0]
        payload = call_args[1]["json"]
        assert payload["clientKey"] == "my-key"
        assert payload["task"]["type"] == "RecaptchaV2TaskProxyless"


class TestPollResult:
    """Tests for _poll_result polling logic."""

    def test_ready_immediately(self) -> None:
        provider = _make_provider(poll_interval=0)
        mock_response = _mock_response({
            "errorId": 0,
            "status": "ready",
            "solution": {"gRecaptchaResponse": "token-abc"},
            "cost": "0.002",
        })

        with patch.object(provider.request, "post", return_value=mock_response), \
                patch("data_collector.captcha.anti_captcha.time.monotonic", return_value=100.0):
            result = provider._poll_result("123", CaptchaTaskType.RECAPTCHA_V2, 100.0)  # pyright: ignore[reportPrivateUsage]

        assert isinstance(result, CaptchaResult)
        assert result.task_id == "123"
        assert result.solution == "token-abc"
        assert result.cost == 0.002

    def test_processing_then_ready(self) -> None:
        provider = _make_provider(timeout=30, poll_interval=0)
        processing_response = _mock_response({"errorId": 0, "status": "processing"})
        ready_response = _mock_response({
            "errorId": 0,
            "status": "ready",
            "solution": {"token": "turnstile-token"},
            "cost": "0.001",
        })

        with patch.object(provider.request, "post", side_effect=[processing_response, ready_response]), \
                patch("data_collector.captcha.anti_captcha.time.sleep"), \
                patch("data_collector.captcha.anti_captcha.time.monotonic", return_value=100.0):
            result = provider._poll_result("456", CaptchaTaskType.TURNSTILE, 100.0)  # pyright: ignore[reportPrivateUsage]

        assert result.solution == "turnstile-token"
        assert result.task_type == CaptchaTaskType.TURNSTILE

    def test_timeout_raises(self) -> None:
        provider = _make_provider(timeout=0, poll_interval=0)

        with patch.object(provider.request, "post") as mock_post, \
                patch("data_collector.captcha.anti_captcha.time.sleep"), \
                pytest.raises(CaptchaTimeout):
            mock_post.return_value = _mock_response({"errorId": 0, "status": "processing"})
            provider._poll_result("789", CaptchaTaskType.IMAGE, 0.0)  # pyright: ignore[reportPrivateUsage]

    def test_image_solution_extracts_text(self) -> None:
        provider = _make_provider(poll_interval=0)
        mock_response = _mock_response({
            "errorId": 0,
            "status": "ready",
            "solution": {"text": "deditur", "url": "http://example.com/img.jpg"},
            "cost": "0.0007",
        })

        with patch.object(provider.request, "post", return_value=mock_response), \
                patch("data_collector.captcha.anti_captcha.time.monotonic", return_value=100.0):
            result = provider._poll_result("100", CaptchaTaskType.IMAGE, 100.0)  # pyright: ignore[reportPrivateUsage]

        assert result.solution == "deditur"


class TestSolveRecaptchaV2:
    """Tests for solve_recaptcha_v2 end-to-end."""

    def test_success(self) -> None:
        metrics = CaptchaMetrics()
        provider = _make_provider(metrics=metrics, poll_interval=0)

        create_response = _mock_response({"errorId": 0, "taskId": 999})
        result_response = _mock_response({
            "errorId": 0,
            "status": "ready",
            "solution": {"gRecaptchaResponse": "03ADUVZw-token"},
            "cost": "0.00095",
        })

        with patch.object(provider.request, "post", side_effect=[create_response, result_response]):
            result = provider.solve_recaptcha_v2(
                site_key="6Le-wvkSAAAAAPBM",
                page_url="https://example.com/form",
            )

        assert result.solution == "03ADUVZw-token"
        assert result.task_type == CaptchaTaskType.RECAPTCHA_V2
        assert result.cost == 0.00095
        assert metrics.submitted == 1
        assert metrics.solved == 1


class TestSolveRecaptchaV3:
    """Tests for solve_recaptcha_v3."""

    def test_sends_action_and_min_score(self) -> None:
        provider = _make_provider(poll_interval=0)

        create_response = _mock_response({"errorId": 0, "taskId": 1})
        result_response = _mock_response({
            "errorId": 0,
            "status": "ready",
            "solution": {"gRecaptchaResponse": "v3-token"},
            "cost": "0.002",
        })

        with patch.object(provider.request, "post", side_effect=[create_response, result_response]) as mock_post:
            result = provider.solve_recaptcha_v3(
                site_key="6Le-key",
                page_url="https://example.com",
                action="login",
                min_score=0.9,
            )

        assert result.solution == "v3-token"
        create_payload = mock_post.call_args_list[0][1]["json"]
        assert create_payload["task"]["minScore"] == 0.9
        assert create_payload["task"]["pageAction"] == "login"


class TestSolveTurnstile:
    """Tests for solve_turnstile."""

    def test_success(self) -> None:
        provider = _make_provider(poll_interval=0)

        create_response = _mock_response({"errorId": 0, "taskId": 50})
        result_response = _mock_response({
            "errorId": 0,
            "status": "ready",
            "solution": {"token": "0.vtJqmZnvobaU"},
            "cost": "0.001",
        })

        with patch.object(provider.request, "post", side_effect=[create_response, result_response]):
            result = provider.solve_turnstile(
                site_key="turnstile-key",
                page_url="https://cf-protected.com",
            )

        assert result.solution == "0.vtJqmZnvobaU"
        assert result.task_type == CaptchaTaskType.TURNSTILE


class TestSolveImage:
    """Tests for solve_image."""

    def test_encodes_image_as_base64(self) -> None:
        provider = _make_provider(poll_interval=0)
        image_bytes = b"\x89PNG\r\n\x1a\nfake-image-data"

        create_response = _mock_response({"errorId": 0, "taskId": 77})
        result_response = _mock_response({
            "errorId": 0,
            "status": "ready",
            "solution": {"text": "captcha123"},
            "cost": "0.0007",
        })

        with patch.object(provider.request, "post", side_effect=[create_response, result_response]) as mock_post:
            result = provider.solve_image(image_bytes)

        assert result.solution == "captcha123"
        create_payload = mock_post.call_args_list[0][1]["json"]
        expected_base64 = base64.b64encode(image_bytes).decode("ascii")
        assert create_payload["task"]["body"] == expected_base64
        assert create_payload["task"]["type"] == "ImageToTextTask"


class TestSolveWithProxy:
    """Tests for proxy-enabled solve methods."""

    def test_recaptcha_v2_proxy_includes_proxy_fields(self) -> None:
        provider = _make_provider(poll_interval=0)

        create_response = _mock_response({"errorId": 0, "taskId": 1})
        result_response = _mock_response({
            "errorId": 0,
            "status": "ready",
            "solution": {"gRecaptchaResponse": "token"},
            "cost": "0.001",
        })

        with patch.object(provider.request, "post", side_effect=[create_response, result_response]) as mock_post:
            provider.solve_recaptcha_v2_proxy(
                site_key="key",
                page_url="https://example.com",
                proxy_type="http",
                proxy_address="1.2.3.4",
                proxy_port=8080,
                proxy_login="user",
                proxy_password="pass",
            )

        create_payload = mock_post.call_args_list[0][1]["json"]
        task = create_payload["task"]
        assert task["type"] == "RecaptchaV2Task"
        assert task["proxyType"] == "http"
        assert task["proxyAddress"] == "1.2.3.4"
        assert task["proxyPort"] == 8080
        assert task["proxyLogin"] == "user"
        assert task["proxyPassword"] == "pass"

    def test_turnstile_proxy_includes_proxy_fields(self) -> None:
        provider = _make_provider(poll_interval=0)

        create_response = _mock_response({"errorId": 0, "taskId": 2})
        result_response = _mock_response({
            "errorId": 0,
            "status": "ready",
            "solution": {"token": "cf-token"},
            "cost": "0.001",
        })

        with patch.object(provider.request, "post", side_effect=[create_response, result_response]) as mock_post:
            provider.solve_turnstile_proxy(
                site_key="key",
                page_url="https://example.com",
                proxy_type="socks5",
                proxy_address="5.6.7.8",
                proxy_port=1080,
                proxy_login="u",
                proxy_password="p",
            )

        create_payload = mock_post.call_args_list[0][1]["json"]
        assert create_payload["task"]["type"] == "TurnstileTask"
        assert create_payload["task"]["proxyType"] == "socks5"


class TestGetBalance:
    """Tests for get_balance."""

    def test_returns_float_balance(self) -> None:
        provider = _make_provider()
        mock_response = _mock_response({"errorId": 0, "balance": 12.3456})

        with patch.object(provider.request, "post", return_value=mock_response):
            balance = provider.get_balance()

        assert balance == 12.3456
        assert isinstance(balance, float)

    def test_api_error_raises(self) -> None:
        provider = _make_provider()
        mock_response = _mock_response({
            "errorId": 1,
            "errorCode": "ERROR_KEY_DOES_NOT_EXIST",
            "errorDescription": "bad key",
        })

        with patch.object(provider.request, "post", return_value=mock_response), \
                pytest.raises(CaptchaError):
            provider.get_balance()


class TestRetryIntegration:
    """Tests for retry behavior with AntiCaptchaProvider."""

    def test_retry_on_timeout_then_success(self) -> None:
        metrics = CaptchaMetrics()
        provider = _make_provider(timeout=0, max_retries=1, poll_interval=0, metrics=metrics)

        create_response_1 = _mock_response({"errorId": 0, "taskId": 1})
        create_response_2 = _mock_response({"errorId": 0, "taskId": 2})

        with patch.object(provider.request, "post", side_effect=[create_response_1, create_response_2]), \
                patch.object(
                    provider, "_poll_result",
                    side_effect=[
                        CaptchaTimeout(task_id="1", timeout_seconds=10),
                        CaptchaResult(
                            task_id="2", task_type=CaptchaTaskType.RECAPTCHA_V2,
                            solution="retry-token", cost=0.002, elapsed_seconds=8.0,
                        ),
                    ],
                ):
            result = provider.solve_recaptcha_v2(site_key="key", page_url="https://example.com")

        assert result.solution == "retry-token"
        assert metrics.submitted == 2
        assert metrics.timed_out == 1
        assert metrics.solved == 1
