"""Tests for CaptchaMetrics thread-safe cost tracking."""

import logging

from data_collector.captcha.metrics import CaptchaMetrics


class TestCaptchaMetricsCounters:
    """Tests for individual counter operations."""

    def test_initial_state(self) -> None:
        metrics = CaptchaMetrics()
        assert metrics.submitted == 0
        assert metrics.solved == 0
        assert metrics.timed_out == 0
        assert metrics.failed == 0
        assert metrics.total_cost == 0.0

    def test_record_submitted(self) -> None:
        metrics = CaptchaMetrics()
        metrics.record_submitted()
        metrics.record_submitted()
        assert metrics.submitted == 2

    def test_record_solved_increments_and_adds_cost(self) -> None:
        metrics = CaptchaMetrics()
        metrics.record_solved(0.002)
        metrics.record_solved(0.003)
        assert metrics.solved == 2
        assert abs(metrics.total_cost - 0.005) < 1e-9

    def test_record_timed_out(self) -> None:
        metrics = CaptchaMetrics()
        metrics.record_timed_out()
        assert metrics.timed_out == 1

    def test_record_failed(self) -> None:
        metrics = CaptchaMetrics()
        metrics.record_failed()
        metrics.record_failed()
        assert metrics.failed == 2


class TestCaptchaMetricsLogStats:
    """Tests for log_stats aggregation."""

    def test_log_stats_empty(self) -> None:
        metrics = CaptchaMetrics()
        stats = metrics.log_stats(logging.getLogger("test"))
        assert stats["submitted"] == 0
        assert stats["solved"] == 0
        assert stats["timed_out"] == 0
        assert stats["failed"] == 0
        assert stats["total_cost"] == 0.0
        assert stats["solve_rate_percent"] == 0.0

    def test_log_stats_with_data(self) -> None:
        metrics = CaptchaMetrics()
        metrics.record_submitted()
        metrics.record_submitted()
        metrics.record_submitted()
        metrics.record_solved(0.002)
        metrics.record_solved(0.003)
        metrics.record_timed_out()

        stats = metrics.log_stats(logging.getLogger("test"))
        assert stats["submitted"] == 3
        assert stats["solved"] == 2
        assert stats["timed_out"] == 1
        assert stats["failed"] == 0
        assert abs(stats["total_cost"] - 0.005) < 1e-9
        assert abs(stats["solve_rate_percent"] - 66.67) < 0.01

    def test_log_stats_returns_dict(self) -> None:
        metrics = CaptchaMetrics()
        stats = metrics.log_stats(logging.getLogger("test"))
        assert isinstance(stats, dict)
        expected_keys = {"submitted", "solved", "timed_out", "failed", "total_cost", "solve_rate_percent"}
        assert set(stats.keys()) == expected_keys
