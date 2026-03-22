"""Tests for StorageJanitor periodic maintenance."""

from __future__ import annotations

from collections import namedtuple
from unittest.mock import MagicMock, patch

from data_collector.settings.manager import ManagerSettings
from data_collector.settings.storage import StorageSettings
from data_collector.storage.janitor import StorageJanitor

_DiskUsage = namedtuple("_DiskUsage", ["total", "used", "free"])


def _make_janitor(
    *,
    min_free_disk_gb: float = 10.0,
    max_storage_alert_gb: float | None = None,
) -> tuple[StorageJanitor, MagicMock, MagicMock]:
    """Create a StorageJanitor with mock database and logger."""
    database = MagicMock()
    logger = MagicMock()
    manager_settings = ManagerSettings(
        storage_janitor_enabled=True,
        storage_janitor_check_interval=3600,
    )
    storage_settings = StorageSettings(
        min_free_disk_gb=min_free_disk_gb,
        max_storage_alert_gb=max_storage_alert_gb,
    )
    janitor = StorageJanitor(
        database, manager_settings, storage_settings, logger=logger,
    )
    return janitor, database, logger


def _make_backend_row(
    location_name: str,
    root_path: str,
    *,
    min_free_disk_gb: float | None = None,
    max_storage_alert_gb: float | None = None,
) -> MagicMock:
    """Create a mock StorageBackend row with optional per-backend thresholds."""
    row = MagicMock()
    row.location_name = location_name
    row.root_path = root_path
    row.is_active = True
    row.min_free_disk_gb = min_free_disk_gb
    row.max_storage_alert_gb = max_storage_alert_gb
    return row


def _setup_database_query(
    database: MagicMock,
    backend_rows: list[MagicMock],
    storage_bytes: int = 0,
) -> None:
    """Configure mock database: first query returns backends, rest return storage size."""
    call_count = 0

    def query_side_effect(statement: object, session: object, **kwargs: object) -> MagicMock:
        nonlocal call_count
        call_count += 1
        result = MagicMock()
        if call_count == 1:
            result.scalars.return_value.all.return_value = backend_rows
        else:
            result.scalar_one.return_value = storage_bytes
        return result

    database.query.side_effect = query_side_effect
    database.create_session.return_value.__enter__ = MagicMock(return_value=MagicMock())
    database.create_session.return_value.__exit__ = MagicMock(return_value=False)


class TestRunMaintenance:
    """Tests for StorageJanitor.run_maintenance()."""

    @patch("data_collector.storage.janitor.shutil.disk_usage", return_value=_DiskUsage(100e9, 50e9, 50e9))
    @patch("data_collector.storage.janitor.enforce_retention_by_database", return_value=0)
    def test_enforces_retention_per_backend(self, mock_enforce: MagicMock, mock_disk: MagicMock) -> None:
        janitor, database, logger = _make_janitor()
        backend_a = _make_backend_row("local", "/storage/local")
        backend_b = _make_backend_row("fs_market_hr", "/storage/hr")
        _setup_database_query(database, [backend_a, backend_b])

        janitor.run_maintenance()

        assert mock_enforce.call_count == 2

    @patch("data_collector.storage.janitor.shutil.disk_usage", return_value=_DiskUsage(100e9, 50e9, 50e9))
    @patch("data_collector.storage.janitor.enforce_retention_by_database", return_value=0)
    def test_budget_warning_when_exceeded(self, mock_enforce: MagicMock, mock_disk: MagicMock) -> None:
        janitor, database, logger = _make_janitor(max_storage_alert_gb=1.0)
        backend_row = _make_backend_row("local", "/storage")
        _setup_database_query(database, [backend_row], storage_bytes=2 * 1024 ** 3)

        janitor.run_maintenance()

        warning_calls = [
            call for call in logger.warning.call_args_list
            if "budget" in str(call)
        ]
        assert len(warning_calls) == 1

    @patch("data_collector.storage.janitor.shutil.disk_usage", return_value=_DiskUsage(100e9, 50e9, 50e9))
    @patch("data_collector.storage.janitor.enforce_retention_by_database", return_value=0)
    def test_no_budget_check_when_disabled(self, mock_enforce: MagicMock, mock_disk: MagicMock) -> None:
        janitor, database, logger = _make_janitor(max_storage_alert_gb=None)
        backend_row = _make_backend_row("local", "/storage")
        _setup_database_query(database, [backend_row])

        janitor.run_maintenance()

        budget_warnings = [
            call for call in logger.warning.call_args_list
            if "budget" in str(call)
        ]
        assert len(budget_warnings) == 0

    def test_empty_backends_returns_cleanly(self) -> None:
        janitor, database, logger = _make_janitor()
        _setup_database_query(database, [])

        janitor.run_maintenance()

        logger.debug.assert_called()

    @patch("data_collector.storage.janitor.shutil.disk_usage", return_value=_DiskUsage(100e9, 50e9, 50e9))
    @patch("data_collector.storage.janitor.enforce_retention_by_database", return_value=5)
    def test_logs_total_deleted_count(self, mock_enforce: MagicMock, mock_disk: MagicMock) -> None:
        janitor, database, logger = _make_janitor()
        backend_row = _make_backend_row("local", "/storage")
        _setup_database_query(database, [backend_row])

        janitor.run_maintenance()

        info_calls = [
            call for call in logger.info.call_args_list
            if "deleted" in str(call)
        ]
        assert len(info_calls) == 1


class TestDiskFreeSpace:
    """Tests for disk free space monitoring."""

    @patch("data_collector.storage.janitor.enforce_retention_by_database", return_value=0)
    def test_logs_warning_on_low_disk_space(self, mock_enforce: MagicMock) -> None:
        janitor, database, logger = _make_janitor(min_free_disk_gb=20.0)
        backend_row = _make_backend_row("local", "/storage")
        _setup_database_query(database, [backend_row])

        # 5 GB free < 20 GB threshold
        with patch("data_collector.storage.janitor.shutil.disk_usage", return_value=_DiskUsage(100e9, 95e9, 5e9)):
            janitor.run_maintenance()

        disk_warnings = [
            call for call in logger.warning.call_args_list
            if "disk free" in str(call)
        ]
        assert len(disk_warnings) == 1

    @patch("data_collector.storage.janitor.enforce_retention_by_database", return_value=0)
    def test_no_warning_on_sufficient_disk_space(self, mock_enforce: MagicMock) -> None:
        janitor, database, logger = _make_janitor(min_free_disk_gb=10.0)
        backend_row = _make_backend_row("local", "/storage")
        _setup_database_query(database, [backend_row])

        # 50 GB free > 10 GB threshold
        with patch("data_collector.storage.janitor.shutil.disk_usage", return_value=_DiskUsage(100e9, 50e9, 50e9)):
            janitor.run_maintenance()

        logger.warning.assert_not_called()

    @patch("data_collector.storage.janitor.enforce_retention_by_database", return_value=0)
    def test_disk_check_handles_oserror(self, mock_enforce: MagicMock) -> None:
        janitor, database, logger = _make_janitor()
        backend_row = _make_backend_row("fs_unreachable", "//offline_server/share")
        _setup_database_query(database, [backend_row])

        with patch("data_collector.storage.janitor.shutil.disk_usage", side_effect=OSError("Network path not found")):
            janitor.run_maintenance()

        disk_warnings = [
            call for call in logger.warning.call_args_list
            if "cannot check disk" in str(call)
        ]
        assert len(disk_warnings) == 1


class TestPerBackendThresholds:
    """Tests for per-backend threshold resolution."""

    @patch("data_collector.storage.janitor.enforce_retention_by_database", return_value=0)
    def test_per_backend_overrides_global(self, mock_enforce: MagicMock) -> None:
        janitor, database, logger = _make_janitor(min_free_disk_gb=10.0, max_storage_alert_gb=100.0)
        # Backend has its own thresholds: 50 GB free, 500 GB budget
        backend_row = _make_backend_row(
            "fs_market_hr", "/storage/hr",
            min_free_disk_gb=50.0,
            max_storage_alert_gb=500.0,
        )
        _setup_database_query(database, [backend_row], storage_bytes=200 * 1024 ** 3)

        # 30 GB free < 50 GB per-backend threshold (would pass 10 GB global)
        with patch("data_collector.storage.janitor.shutil.disk_usage", return_value=_DiskUsage(1000e9, 970e9, 30e9)):
            janitor.run_maintenance()

        disk_warnings = [
            call for call in logger.warning.call_args_list
            if "disk free" in str(call)
        ]
        assert len(disk_warnings) == 1

    @patch("data_collector.storage.janitor.enforce_retention_by_database", return_value=0)
    def test_falls_back_to_global_when_no_override(self, mock_enforce: MagicMock) -> None:
        janitor, database, logger = _make_janitor(min_free_disk_gb=10.0)
        # Backend has no per-backend thresholds (None)
        backend_row = _make_backend_row("local", "/storage")
        _setup_database_query(database, [backend_row])

        # 50 GB free > 10 GB global threshold
        with patch("data_collector.storage.janitor.shutil.disk_usage", return_value=_DiskUsage(100e9, 50e9, 50e9)):
            janitor.run_maintenance()

        logger.warning.assert_not_called()
