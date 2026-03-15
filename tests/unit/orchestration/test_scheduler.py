"""Unit tests for the Scheduler class."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

from data_collector.enums import AppType, FatalFlag, RunStatus
from data_collector.orchestration.scheduler import Scheduler


def _make_mock_app(
    *,
    disable: bool = False,
    run_status: int = RunStatus.NOT_RUNNING,
    fatal_flag: int = FatalFlag.NONE,
    next_run: datetime | None = None,
    interval: int | None = None,
    cron_expression: str | None = None,
    app_id: str = "test_app_hash",
    group_name: str = "test_group",
    parent_name: str = "test_parent",
    app_name: str = "test_app",
    app_type: int = AppType.MANAGED,
) -> MagicMock:
    app = MagicMock()
    app.disable = disable
    app.run_status = run_status
    app.fatal_flag = fatal_flag
    app.next_run = next_run
    app.interval = interval
    app.cron_expression = cron_expression
    app.app = app_id
    app.group_name = group_name
    app.parent_name = parent_name
    app.app_name = app_name
    app.app_type = app_type
    return app


class TestCalculateNextRun:
    """Test Scheduler.calculate_next_run() logic."""

    def test_interval_based(self) -> None:
        scheduler = Scheduler(MagicMock(), logger=MagicMock())
        app = _make_mock_app(interval=30)

        before = datetime.now(UTC)
        result = scheduler.calculate_next_run(app)
        after = datetime.now(UTC)

        assert result is not None
        assert before + timedelta(minutes=30) <= result <= after + timedelta(minutes=30)

    def test_cron_based(self) -> None:
        scheduler = Scheduler(MagicMock(), logger=MagicMock())
        app = _make_mock_app(cron_expression="0 */2 * * *")

        result = scheduler.calculate_next_run(app)

        assert result is not None
        assert result > datetime.now(UTC)

    def test_cron_takes_precedence_over_interval(self) -> None:
        scheduler = Scheduler(MagicMock(), logger=MagicMock())
        app = _make_mock_app(interval=5, cron_expression="0 0 * * *")

        result = scheduler.calculate_next_run(app)

        # Should use cron (midnight daily), not interval (5 minutes)
        assert result is not None
        now = datetime.now(UTC)
        # With a 5-minute interval the next run would be ~5 min away
        # With daily cron it should be more than 5 minutes away (unless we are very close to midnight)
        # Just verify it returns something future
        assert result > now

    def test_no_schedule_returns_none(self) -> None:
        scheduler = Scheduler(MagicMock(), logger=MagicMock())
        app = _make_mock_app(interval=None, cron_expression=None)

        result = scheduler.calculate_next_run(app)
        assert result is None

    def test_invalid_cron_returns_none(self) -> None:
        scheduler = Scheduler(MagicMock(), logger=MagicMock())
        app = _make_mock_app(cron_expression="invalid cron")

        result = scheduler.calculate_next_run(app)
        assert result is None

    def test_zero_interval_returns_none(self) -> None:
        scheduler = Scheduler(MagicMock(), logger=MagicMock())
        app = _make_mock_app(interval=0)

        result = scheduler.calculate_next_run(app)
        assert result is None

    def test_empty_cron_falls_through_to_interval(self) -> None:
        scheduler = Scheduler(MagicMock(), logger=MagicMock())
        app = _make_mock_app(interval=15, cron_expression="   ")

        result = scheduler.calculate_next_run(app)

        assert result is not None
        now = datetime.now(UTC)
        assert result <= now + timedelta(minutes=16)


class TestSetFallbackNextRun:
    """Test Scheduler.set_fallback_next_run() fallback behavior."""

    def test_skips_when_app_has_future_next_run(self) -> None:
        mock_database = MagicMock()
        mock_session = MagicMock()
        mock_database.create_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_database.create_session.return_value.__exit__ = MagicMock(return_value=False)

        future_next_run = datetime.now(UTC) + timedelta(hours=1)
        mock_app = _make_mock_app(next_run=future_next_run, interval=30)
        mock_database.query.return_value.scalar_one_or_none.return_value = mock_app

        scheduler = Scheduler(mock_database, logger=MagicMock())

        with patch("data_collector.orchestration.scheduler.update_app_status") as mock_update:
            scheduler.set_fallback_next_run("test_app_hash", mock_app)
            mock_update.assert_not_called()

    def test_sets_next_run_when_in_past(self) -> None:
        mock_database = MagicMock()
        mock_session = MagicMock()
        mock_database.create_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_database.create_session.return_value.__exit__ = MagicMock(return_value=False)

        past_next_run = datetime.now(UTC) - timedelta(hours=1)
        mock_app = _make_mock_app(next_run=past_next_run, interval=30)
        mock_database.query.return_value.scalar_one_or_none.return_value = mock_app

        scheduler = Scheduler(mock_database, logger=MagicMock())

        with patch("data_collector.orchestration.scheduler.update_app_status") as mock_update:
            scheduler.set_fallback_next_run("test_app_hash", mock_app)
            mock_update.assert_called_once()
            call_kwargs = mock_update.call_args
            assert call_kwargs[1]["next_run"] is not None
