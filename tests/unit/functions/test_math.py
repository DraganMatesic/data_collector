from datetime import datetime

from data_collector.utilities.functions.math import get_totalh, get_totalm, get_totals


def test_get_totals_returns_seconds_difference() -> None:
    start = datetime(2026, 1, 1, 12, 0, 0)
    end = datetime(2026, 1, 1, 12, 1, 5)

    assert get_totals(start, end) == 65


def test_get_totalm_returns_minutes_difference() -> None:
    start = datetime(2026, 1, 1, 12, 0, 0)
    end = datetime(2026, 1, 1, 12, 3, 10)

    assert get_totalm(start, end) == 3


def test_get_totalh_returns_hours_difference() -> None:
    start = datetime(2026, 1, 1, 8, 0, 0)
    end = datetime(2026, 1, 1, 11, 30, 0)

    assert get_totalh(start, end) == 3
