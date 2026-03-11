"""Tests for captcha data models, enums, and exception types."""

import pytest

from data_collector.captcha.models import CaptchaError, CaptchaResult, CaptchaTaskType, CaptchaTimeout


class TestCaptchaTaskType:
    """Tests for CaptchaTaskType enum values."""

    def test_recaptcha_v2_value(self) -> None:
        assert CaptchaTaskType.RECAPTCHA_V2 == "recaptcha_v2"

    def test_recaptcha_v2_proxy_value(self) -> None:
        assert CaptchaTaskType.RECAPTCHA_V2_PROXY == "recaptcha_v2_proxy"

    def test_recaptcha_v3_value(self) -> None:
        assert CaptchaTaskType.RECAPTCHA_V3 == "recaptcha_v3"

    def test_turnstile_value(self) -> None:
        assert CaptchaTaskType.TURNSTILE == "turnstile"

    def test_turnstile_proxy_value(self) -> None:
        assert CaptchaTaskType.TURNSTILE_PROXY == "turnstile_proxy"

    def test_image_value(self) -> None:
        assert CaptchaTaskType.IMAGE == "image"

    def test_member_count(self) -> None:
        assert len(CaptchaTaskType) == 6

    def test_is_str_enum(self) -> None:
        assert isinstance(CaptchaTaskType.RECAPTCHA_V2, str)


class TestCaptchaResult:
    """Tests for CaptchaResult frozen dataclass."""

    def test_basic_construction(self) -> None:
        result = CaptchaResult(
            task_id="12345",
            task_type=CaptchaTaskType.RECAPTCHA_V2,
            solution="03ADUVZw-token",
            cost=0.002,
            elapsed_seconds=15.5,
        )
        assert result.task_id == "12345"
        assert result.task_type == CaptchaTaskType.RECAPTCHA_V2
        assert result.solution == "03ADUVZw-token"
        assert result.cost == 0.002
        assert result.elapsed_seconds == 15.5

    def test_frozen(self) -> None:
        result = CaptchaResult(
            task_id="1",
            task_type=CaptchaTaskType.IMAGE,
            solution="abc",
            cost=0.001,
            elapsed_seconds=5.0,
        )
        with pytest.raises(AttributeError):
            result.solution = "other"  # type: ignore[misc]

    def test_task_id_is_string(self) -> None:
        result = CaptchaResult(
            task_id="non-numeric-id",
            task_type=CaptchaTaskType.TURNSTILE,
            solution="token",
            cost=0.0,
            elapsed_seconds=1.0,
        )
        assert isinstance(result.task_id, str)


class TestCaptchaError:
    """Tests for CaptchaError exception."""

    def test_attributes(self) -> None:
        error = CaptchaError(
            error_id=1,
            error_code="ERROR_KEY_DOES_NOT_EXIST",
            error_description="Account authorization key not found",
        )
        assert error.error_id == 1
        assert error.error_code == "ERROR_KEY_DOES_NOT_EXIST"
        assert error.error_description == "Account authorization key not found"

    def test_is_exception(self) -> None:
        error = CaptchaError(error_id=1, error_code="ERR", error_description="desc")
        assert isinstance(error, Exception)

    def test_message_format(self) -> None:
        error = CaptchaError(error_id=2, error_code="NO_FUNDS", error_description="No money")
        assert "2" in str(error)
        assert "NO_FUNDS" in str(error)
        assert "No money" in str(error)

    def test_can_be_raised_and_caught(self) -> None:
        with pytest.raises(CaptchaError, match="ERROR_KEY"):
            raise CaptchaError(error_id=1, error_code="ERROR_KEY", error_description="bad key")


class TestCaptchaTimeout:
    """Tests for CaptchaTimeout exception."""

    def test_attributes(self) -> None:
        error = CaptchaTimeout(task_id="99999", timeout_seconds=120)
        assert error.task_id == "99999"
        assert error.timeout_seconds == 120

    def test_is_exception(self) -> None:
        error = CaptchaTimeout(task_id="1", timeout_seconds=60)
        assert isinstance(error, Exception)

    def test_message_format(self) -> None:
        error = CaptchaTimeout(task_id="42", timeout_seconds=30)
        assert "42" in str(error)
        assert "30" in str(error)

    def test_can_be_raised_and_caught(self) -> None:
        with pytest.raises(CaptchaTimeout, match="timed out"):
            raise CaptchaTimeout(task_id="1", timeout_seconds=120)
