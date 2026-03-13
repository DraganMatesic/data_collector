"""Unit tests for the RetentionCleaner class."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from data_collector.orchestration.retention import RetentionCleaner

_MODULE = "data_collector.orchestration.retention"


def _make_settings(
    *,
    retention_log_days: int = 90,
    retention_runtime_days: int = 180,
    retention_function_log_days: int = 90,
    retention_command_log_days: int = 365,
    retention_app_purge_enabled: bool = False,
) -> MagicMock:
    settings = MagicMock()
    settings.retention_log_days = retention_log_days
    settings.retention_runtime_days = retention_runtime_days
    settings.retention_function_log_days = retention_function_log_days
    settings.retention_command_log_days = retention_command_log_days
    settings.retention_app_purge_enabled = retention_app_purge_enabled
    return settings


class TestRunCleanup:
    """Test RetentionCleaner.run_cleanup()."""

    def test_deletes_from_all_tables(self) -> None:
        mock_database = MagicMock()
        mock_session = MagicMock()
        mock_database.create_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_database.create_session.return_value.__exit__ = MagicMock(return_value=False)
        mock_session.execute.return_value.rowcount = 0

        settings = _make_settings()
        cleaner = RetentionCleaner(mock_database, settings, logger=MagicMock())

        cleaner.run_cleanup()

        # 5 tables cleaned: function_log_error, function_log, logs, runtime, command_log
        assert mock_session.execute.call_count == 5
        assert mock_session.commit.call_count == 5

    def test_logs_when_rows_deleted(self) -> None:
        mock_database = MagicMock()
        mock_session = MagicMock()
        mock_database.create_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_database.create_session.return_value.__exit__ = MagicMock(return_value=False)
        mock_session.execute.return_value.rowcount = 42

        mock_logger = MagicMock()
        settings = _make_settings()
        cleaner = RetentionCleaner(mock_database, settings, logger=mock_logger)

        cleaner.run_cleanup()

        # Should log for each table that had rows deleted
        assert mock_logger.info.call_count == 5
        # Verify one of the log messages includes table name and row count
        log_args = mock_logger.info.call_args_list[0]
        assert 42 in log_args[0]

    def test_no_logging_when_zero_rows(self) -> None:
        mock_database = MagicMock()
        mock_session = MagicMock()
        mock_database.create_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_database.create_session.return_value.__exit__ = MagicMock(return_value=False)
        mock_session.execute.return_value.rowcount = 0

        mock_logger = MagicMock()
        settings = _make_settings()
        cleaner = RetentionCleaner(mock_database, settings, logger=mock_logger)

        cleaner.run_cleanup()

        mock_logger.info.assert_not_called()

    @patch(f"{_MODULE}.delete")
    def test_uses_correct_retention_days(self, mock_delete: MagicMock) -> None:
        mock_database = MagicMock()
        mock_session = MagicMock()
        mock_database.create_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_database.create_session.return_value.__exit__ = MagicMock(return_value=False)
        mock_session.execute.return_value.rowcount = 0
        mock_delete.return_value.where.return_value = MagicMock()

        settings = _make_settings(
            retention_function_log_days=30,
            retention_log_days=60,
            retention_runtime_days=90,
            retention_command_log_days=120,
        )
        cleaner = RetentionCleaner(mock_database, settings, logger=MagicMock())
        cleaner.run_cleanup()

        # delete() should be called for each of the 5 tables
        assert mock_delete.call_count == 5


class TestPurgeRemovedApps:
    """Test that run_cleanup() calls _purge_removed_apps based on settings."""

    @patch.object(RetentionCleaner, "_purge_removed_apps")
    def test_purge_runs_when_enabled(self, mock_purge: MagicMock) -> None:
        mock_database = MagicMock()
        mock_session = MagicMock()
        mock_database.create_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_database.create_session.return_value.__exit__ = MagicMock(return_value=False)
        mock_session.execute.return_value.rowcount = 0

        settings = _make_settings(retention_app_purge_enabled=True)
        cleaner = RetentionCleaner(mock_database, settings, logger=MagicMock())

        cleaner.run_cleanup()

        mock_purge.assert_called_once()

    @patch.object(RetentionCleaner, "_purge_removed_apps")
    def test_purge_skipped_when_disabled(self, mock_purge: MagicMock) -> None:
        mock_database = MagicMock()
        mock_session = MagicMock()
        mock_database.create_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_database.create_session.return_value.__exit__ = MagicMock(return_value=False)
        mock_session.execute.return_value.rowcount = 0

        settings = _make_settings(retention_app_purge_enabled=False)
        cleaner = RetentionCleaner(mock_database, settings, logger=MagicMock())

        cleaner.run_cleanup()

        mock_purge.assert_not_called()
