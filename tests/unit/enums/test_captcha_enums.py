"""Tests for captcha-related enums: CaptchaSolveStatus and CaptchaErrorCategory."""

from enum import IntEnum

from data_collector.enums.captcha import CaptchaErrorCategory, CaptchaSolveStatus

# ---------------------------------------------------------------------------
# CaptchaSolveStatus
# ---------------------------------------------------------------------------


def test_solve_status_is_int_enum() -> None:
    assert issubclass(CaptchaSolveStatus, IntEnum)


def test_solve_status_values() -> None:
    assert CaptchaSolveStatus.SOLVED == 1
    assert CaptchaSolveStatus.TIMED_OUT == 2
    assert CaptchaSolveStatus.FAILED == 3


def test_solve_status_member_count() -> None:
    assert len(CaptchaSolveStatus) == 3


def test_solve_status_members_are_ints() -> None:
    for member in CaptchaSolveStatus:
        assert isinstance(member, int)


# ---------------------------------------------------------------------------
# CaptchaErrorCategory
# ---------------------------------------------------------------------------


def test_error_category_is_int_enum() -> None:
    assert issubclass(CaptchaErrorCategory, IntEnum)


def test_error_category_values() -> None:
    assert CaptchaErrorCategory.AUTH == 1
    assert CaptchaErrorCategory.BALANCE == 2
    assert CaptchaErrorCategory.PROXY == 3
    assert CaptchaErrorCategory.TASK == 4
    assert CaptchaErrorCategory.SOLVE == 5
    assert CaptchaErrorCategory.RATE_LIMIT == 6
    assert CaptchaErrorCategory.UNKNOWN == 7


def test_error_category_member_count() -> None:
    assert len(CaptchaErrorCategory) == 7


def test_error_category_members_are_ints() -> None:
    for member in CaptchaErrorCategory:
        assert isinstance(member, int)
