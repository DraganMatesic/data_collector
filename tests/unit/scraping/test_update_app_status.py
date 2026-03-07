"""Unit tests for update_app_status() function."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock

from data_collector.enums import RunStatus
from data_collector.scraping.base import update_app_status

_MODULE = "data_collector.scraping.base"


class TestUpdateAppStatus:
    """Test update_app_status() database write behavior."""

    def test_update_run_status(self) -> None:
        mock_db = MagicMock()
        mock_session = MagicMock()
        mock_db.create_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_db.create_session.return_value.__exit__ = MagicMock(return_value=False)

        mock_row = MagicMock()
        mock_session.execute.return_value.scalar_one_or_none.return_value = mock_row

        update_app_status(mock_db, "test_app_id", run_status=RunStatus.RUNNING)

        assert mock_row.run_status == RunStatus.RUNNING
        mock_session.commit.assert_called_once()

    def test_update_multiple_fields(self) -> None:
        mock_db = MagicMock()
        mock_session = MagicMock()
        mock_db.create_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_db.create_session.return_value.__exit__ = MagicMock(return_value=False)

        mock_row = MagicMock()
        mock_session.execute.return_value.scalar_one_or_none.return_value = mock_row

        now = datetime.now(UTC)
        update_app_status(
            mock_db, "test_app_id",
            solved=42, failed=3, task_size=100, last_run=now,
        )

        assert mock_row.solved == 42
        assert mock_row.failed == 3
        assert mock_row.task_size == 100
        assert mock_row.last_run == now
        mock_session.commit.assert_called_once()

    def test_noop_when_app_not_found(self) -> None:
        mock_db = MagicMock()
        mock_session = MagicMock()
        mock_db.create_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_db.create_session.return_value.__exit__ = MagicMock(return_value=False)

        mock_session.execute.return_value.scalar_one_or_none.return_value = None

        update_app_status(mock_db, "nonexistent_app", run_status=RunStatus.RUNNING)

        mock_session.commit.assert_not_called()

    def test_noop_when_all_none(self) -> None:
        mock_db = MagicMock()
        update_app_status(mock_db, "test_app_id")
        mock_db.create_session.assert_not_called()

    def test_only_nonnull_fields_written(self) -> None:
        mock_db = MagicMock()
        mock_session = MagicMock()
        mock_db.create_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_db.create_session.return_value.__exit__ = MagicMock(return_value=False)

        mock_row = MagicMock()
        mock_row.run_status = RunStatus.NOT_RUNNING
        mock_row.solved = 0
        mock_session.execute.return_value.scalar_one_or_none.return_value = mock_row

        update_app_status(mock_db, "test_app_id", run_status=RunStatus.RUNNING)

        assert mock_row.run_status == RunStatus.RUNNING
        # solved should not have been reassigned (no setattr call for it)
        mock_session.commit.assert_called_once()

    def test_running_resets_eta_and_progress(self) -> None:
        mock_db = MagicMock()
        mock_session = MagicMock()
        mock_db.create_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_db.create_session.return_value.__exit__ = MagicMock(return_value=False)

        mock_row = MagicMock()
        mock_row.eta = datetime.now(UTC)
        mock_row.progress = 75
        mock_session.execute.return_value.scalar_one_or_none.return_value = mock_row

        update_app_status(mock_db, "test_app_id", run_status=RunStatus.RUNNING)

        assert mock_row.eta is None
        assert mock_row.progress == 0
        mock_session.commit.assert_called_once()
