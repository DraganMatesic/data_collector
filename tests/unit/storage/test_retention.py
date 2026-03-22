"""Tests for file retention policy enforcement."""

from __future__ import annotations

import os
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

from data_collector.storage.backend import FilesystemBackend
from data_collector.storage.retention import RetentionPolicy, enforce_retention, enforce_retention_by_database
from data_collector.tables.storage import StoredFile


def _create_file(directory: Path, name: str, content: bytes = b"data") -> Path:
    """Create a file in the given directory and return its path."""
    file_path = directory / name
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_bytes(content)
    return file_path


def _age_file(file_path: Path, days: int) -> None:
    """Set a file's modification time to *days* ago."""
    old_time = time.time() - (days * 86400)
    os.utime(file_path, (old_time, old_time))


class TestRetentionPolicy:
    """Tests for RetentionPolicy dataclass."""

    def test_frozen(self) -> None:
        policy = RetentionPolicy(max_age_days=30)
        try:
            policy.max_age_days = 60  # type: ignore[misc]
            raise AssertionError("Should have raised FrozenInstanceError")
        except AttributeError:
            pass

    def test_permanent_policy(self) -> None:
        policy = RetentionPolicy(max_age_days=None)
        assert policy.max_age_days is None


class TestEnforceRetention:
    """Tests for filesystem-based retention enforcement."""

    def test_keep_all_deletes_nothing(self, tmp_path: Path) -> None:
        backend = FilesystemBackend(tmp_path)
        _create_file(tmp_path, "keep.txt")
        _age_file(tmp_path / "keep.txt", days=1000)
        policy = RetentionPolicy(max_age_days=None)

        deleted = enforce_retention(backend, policy)

        assert deleted == 0
        assert (tmp_path / "keep.txt").exists()

    def test_deletes_old_files(self, tmp_path: Path) -> None:
        backend = FilesystemBackend(tmp_path)
        old_file = _create_file(tmp_path, "2024-01-01/old.txt")
        _age_file(old_file, days=100)
        recent_file = _create_file(tmp_path, "2025-03-19/recent.txt")
        policy = RetentionPolicy(max_age_days=30)

        deleted = enforce_retention(backend, policy)

        assert deleted == 1
        assert not old_file.exists()
        assert recent_file.exists()

    def test_keeps_recent_files(self, tmp_path: Path) -> None:
        backend = FilesystemBackend(tmp_path)
        recent = _create_file(tmp_path, "recent.txt")
        policy = RetentionPolicy(max_age_days=30)

        deleted = enforce_retention(backend, policy)

        assert deleted == 0
        assert recent.exists()

    def test_removes_empty_directories(self, tmp_path: Path) -> None:
        backend = FilesystemBackend(tmp_path)
        old_file = _create_file(tmp_path, "2024-01-01/old.txt")
        _age_file(old_file, days=100)
        policy = RetentionPolicy(max_age_days=30)

        enforce_retention(backend, policy)

        assert not (tmp_path / "2024-01-01").exists()

    def test_nonexistent_directory_returns_zero(self, tmp_path: Path) -> None:
        nonexistent = tmp_path / "does_not_exist"
        backend = FilesystemBackend(nonexistent)
        policy = RetentionPolicy(max_age_days=30)

        deleted = enforce_retention(backend, policy)

        assert deleted == 0

    def test_empty_directory_returns_zero(self, tmp_path: Path) -> None:
        backend = FilesystemBackend(tmp_path)
        policy = RetentionPolicy(max_age_days=30)

        deleted = enforce_retention(backend, policy)

        assert deleted == 0

    def test_returns_deleted_count(self, tmp_path: Path) -> None:
        backend = FilesystemBackend(tmp_path)
        for index in range(5):
            old_file = _create_file(tmp_path, f"old_{index}.txt")
            _age_file(old_file, days=100)
        _create_file(tmp_path, "recent.txt")
        policy = RetentionPolicy(max_age_days=30)

        deleted = enforce_retention(backend, policy)

        assert deleted == 5

    def test_permission_error_logged_and_skipped(self, tmp_path: Path) -> None:
        backend = FilesystemBackend(tmp_path)
        locked_file = _create_file(tmp_path, "locked.txt")
        _age_file(locked_file, days=100)
        logger = MagicMock()

        with patch.object(Path, "unlink", side_effect=PermissionError("Access denied")):
            deleted = enforce_retention(backend, RetentionPolicy(max_age_days=30), logger=logger)

        assert deleted == 0
        logger.warning.assert_called()


class TestEnforceRetentionByDatabase:
    """Tests for DB-driven retention enforcement."""

    def test_deletes_expired_files_and_rows(self, tmp_path: Path) -> None:
        backend = FilesystemBackend(tmp_path, location="local")
        relative = Path("app/txt/2024-01-01/abc123.txt")
        backend.store(b"expired content", relative)

        expired_record = MagicMock(spec=StoredFile)
        expired_record.stored_path = str(relative)
        expired_record.id = 1

        database = MagicMock()
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = [expired_record]
        database.query.return_value = mock_result
        database.create_session.return_value.__enter__ = MagicMock(return_value=MagicMock())
        database.create_session.return_value.__exit__ = MagicMock(return_value=False)

        deleted = enforce_retention_by_database(database, backend)

        assert deleted == 1
        assert not (tmp_path / relative).exists()
        database.run.assert_called_once()

    def test_no_expired_files_returns_zero(self, tmp_path: Path) -> None:
        backend = FilesystemBackend(tmp_path, location="local")

        database = MagicMock()
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = []
        database.query.return_value = mock_result
        database.create_session.return_value.__enter__ = MagicMock(return_value=MagicMock())
        database.create_session.return_value.__exit__ = MagicMock(return_value=False)

        deleted = enforce_retention_by_database(database, backend)

        assert deleted == 0
        database.run.assert_not_called()

    def test_cleans_orphaned_rows_when_file_missing(self, tmp_path: Path) -> None:
        backend = FilesystemBackend(tmp_path, location="local")
        # File does NOT exist on disk -- orphaned DB row
        orphaned_record = MagicMock(spec=StoredFile)
        orphaned_record.stored_path = "app/txt/2024-01-01/missing.txt"
        orphaned_record.id = 42

        database = MagicMock()
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = [orphaned_record]
        database.query.return_value = mock_result
        database.create_session.return_value.__enter__ = MagicMock(return_value=MagicMock())
        database.create_session.return_value.__exit__ = MagicMock(return_value=False)

        deleted = enforce_retention_by_database(database, backend)

        # Orphaned row should be cleaned up even though file was already gone
        assert deleted == 1
        database.run.assert_called_once()

    def test_permission_error_skips_file(self, tmp_path: Path) -> None:
        backend = FilesystemBackend(tmp_path, location="local")
        relative = Path("app/txt/2024-01-01/locked.txt")
        backend.store(b"locked content", relative)

        locked_record = MagicMock(spec=StoredFile)
        locked_record.stored_path = str(relative)
        locked_record.id = 1

        database = MagicMock()
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = [locked_record]
        database.query.return_value = mock_result
        database.create_session.return_value.__enter__ = MagicMock(return_value=MagicMock())
        database.create_session.return_value.__exit__ = MagicMock(return_value=False)

        with patch.object(FilesystemBackend, "delete", side_effect=PermissionError("Access denied")):
            deleted = enforce_retention_by_database(database, backend)

        assert deleted == 0
        database.run.assert_not_called()
