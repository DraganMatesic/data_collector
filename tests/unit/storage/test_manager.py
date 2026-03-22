"""Tests for StorageManager file storage, deduplication, and transfer."""

from __future__ import annotations

import hashlib
from pathlib import Path
from unittest.mock import MagicMock

from data_collector.enums.storage import FileRetention
from data_collector.settings.storage import StorageSettings
from data_collector.storage.backend import FilesystemBackend
from data_collector.storage.manager import StorageManager
from data_collector.tables.storage import StoredFile


def _make_database(retention_days: int | None = 365) -> MagicMock:
    """Create a mock Database with standard method stubs.

    The mock returns different results depending on the query:
    - StoredFile queries (dedup/list): returns no results
    - CodebookFileRetention queries (retention days): returns *retention_days*
    - SUM queries (storage size): returns 0
    """
    database = MagicMock()

    def query_side_effect(statement: object, session: object, **kwargs: object) -> MagicMock:
        """Route query results based on statement content."""
        statement_str = str(statement)
        result = MagicMock()

        if "c_file_retention" in statement_str:
            # Retention days lookup -- returns a row with .retention_days attribute
            if retention_days is not None:
                retention_row = MagicMock()
                retention_row.retention_days = retention_days
                result.scalars.return_value.first.return_value = retention_row
            else:
                # None retention_days = PERMANENT category (row exists but days is NULL)
                retention_row = MagicMock()
                retention_row.retention_days = None
                result.scalars.return_value.first.return_value = retention_row
        elif "coalesce" in statement_str.lower() or "sum" in statement_str.lower():
            # Storage size query
            result.scalar_one.return_value = 0
        else:
            # Default: dedup / file listing queries
            result.scalars.return_value.first.return_value = None
            result.scalars.return_value.all.return_value = []
            result.scalar_one.return_value = 0

        return result

    database.query.side_effect = query_side_effect
    return database


def _make_manager(
    tmp_path: Path,
    *,
    database: MagicMock | None = None,
    deduplicate: bool = True,
    directory_depth: str = "daily",
) -> StorageManager:
    """Create a StorageManager with a tmp_path-based backend."""
    resolved_database = database or _make_database()
    settings = StorageSettings(
        root=tmp_path,
        deduplicate=deduplicate,
        directory_depth=directory_depth,
    )
    return StorageManager(
        resolved_database,
        "hr",
        "registry",
        "companies",
        runtime_id="test-runtime-001",
        settings=settings,
    )


class TestStorageManagerInit:
    """Tests for StorageManager construction."""

    def test_app_directory_structure(self, tmp_path: Path) -> None:
        manager = _make_manager(tmp_path)

        assert manager.app_directory == tmp_path / "hr" / "registry" / "companies"

    def test_today_directory_daily_depth(self, tmp_path: Path) -> None:
        manager = _make_manager(tmp_path, directory_depth="daily")

        today = manager.today_directory
        # Should end with a YYYY-MM-DD pattern
        date_part = today.name
        assert len(date_part) == 10
        assert date_part[4] == "-"
        assert date_part[7] == "-"

    def test_today_directory_flat_depth(self, tmp_path: Path) -> None:
        manager = _make_manager(tmp_path, directory_depth="flat")

        assert manager.today_directory == manager.app_directory


class TestStore:
    """Tests for StorageManager.store()."""

    def test_creates_daily_directory(self, tmp_path: Path) -> None:
        manager = _make_manager(tmp_path)
        session = MagicMock()

        manager.store(b"content", "pdf", original_filename="doc.pdf", session=session)

        # The file should be in a YYYY-MM-DD subdirectory
        stored_files = list(tmp_path.rglob("*.pdf"))
        assert len(stored_files) == 1
        # Parent should be a date directory
        assert len(stored_files[0].parent.name) == 10

    def test_hash_named_file(self, tmp_path: Path) -> None:
        content = b"test content for hashing"
        expected_hash = hashlib.sha256(content).hexdigest()
        manager = _make_manager(tmp_path)
        session = MagicMock()

        result = manager.store(content, "txt", original_filename="original.txt", session=session)

        assert result.stem == expected_hash
        assert result.suffix == ".txt"

    def test_writes_correct_content(self, tmp_path: Path) -> None:
        content = b"binary file content here"
        manager = _make_manager(tmp_path)
        session = MagicMock()

        result = manager.store(content, "bin", original_filename="data.bin", session=session)

        assert result.read_bytes() == content

    def test_inserts_stored_file_row(self, tmp_path: Path) -> None:
        database = _make_database()
        manager = _make_manager(tmp_path, database=database)
        session = MagicMock()

        manager.store(b"content", "pdf", original_filename="report.pdf", session=session)

        database.add.assert_called_once()
        stored_file = database.add.call_args[0][0]
        assert isinstance(stored_file, StoredFile)
        assert stored_file.original_filename == "report.pdf"
        assert stored_file.file_extension == "pdf"
        assert stored_file.file_size == 7
        assert stored_file.location == "local"

    def test_dedup_skips_duplicate(self, tmp_path: Path) -> None:
        database = _make_database()
        content = b"duplicate content"

        # First store: no duplicate found
        manager = _make_manager(tmp_path, database=database)
        session = MagicMock()
        first_path = manager.store(content, "txt", original_filename="first.txt", session=session)

        # Simulate duplicate found on second call
        existing_record = MagicMock(spec=StoredFile)
        existing_record.stored_path = str(first_path.relative_to(tmp_path))

        def dedup_side_effect(statement: object, session: object, **kwargs: object) -> MagicMock:
            statement_str = str(statement)
            result = MagicMock()
            if "c_file_retention" in statement_str:
                retention_row = MagicMock()
                retention_row.retention_days = 365
                result.scalars.return_value.first.return_value = retention_row
            else:
                result.scalars.return_value.first.return_value = existing_record
                result.scalars.return_value.all.return_value = []
            return result

        database.query.side_effect = dedup_side_effect

        second_path = manager.store(content, "txt", original_filename="second.txt", session=session)

        # Should return existing path, not store again
        assert second_path == tmp_path / str(existing_record.stored_path)
        # add() should have been called only once (for the first store)
        assert database.add.call_count == 1

    def test_dedup_allows_different_content(self, tmp_path: Path) -> None:
        database = _make_database()
        manager = _make_manager(tmp_path, database=database)
        session = MagicMock()

        manager.store(b"content A", "txt", original_filename="a.txt", session=session)
        manager.store(b"content B", "txt", original_filename="b.txt", session=session)

        assert database.add.call_count == 2

    def test_dedup_disabled(self, tmp_path: Path) -> None:
        database = _make_database()
        manager = _make_manager(tmp_path, database=database, deduplicate=False)
        session = MagicMock()

        manager.store(b"same", "txt", original_filename="a.txt", session=session)
        manager.store(b"same", "txt", original_filename="b.txt", session=session)

        # Both stores should insert (no dedup check)
        assert database.add.call_count == 2

    def test_flat_depth_no_date_folder(self, tmp_path: Path) -> None:
        manager = _make_manager(tmp_path, directory_depth="flat")
        session = MagicMock()

        result = manager.store(b"content", "txt", original_filename="doc.txt", session=session)

        # File should be in app_dir/txt/ (extension folder, no date folder)
        assert result.parent == tmp_path / "hr" / "registry" / "companies" / "txt"

    def test_monthly_depth(self, tmp_path: Path) -> None:
        manager = _make_manager(tmp_path, directory_depth="monthly")
        session = MagicMock()

        result = manager.store(b"content", "pdf", original_filename="doc.pdf", session=session)

        # Date folder should be YYYY-MM (7 chars)
        date_folder = result.parent.name
        assert len(date_folder) == 7
        assert date_folder[4] == "-"
        # Extension folder is the grandparent
        assert result.parent.parent.name == "pdf"

    def test_yearly_depth(self, tmp_path: Path) -> None:
        manager = _make_manager(tmp_path, directory_depth="yearly")
        session = MagicMock()

        result = manager.store(b"content", "csv", original_filename="data.csv", session=session)

        # Date folder should be YYYY (4 chars)
        date_folder = result.parent.name
        assert len(date_folder) == 4
        assert result.parent.parent.name == "csv"

    def test_hourly_depth(self, tmp_path: Path) -> None:
        manager = _make_manager(tmp_path, directory_depth="hourly")
        session = MagicMock()

        result = manager.store(b"content", "txt", original_filename="log.txt", session=session)

        # Date folder should be YYYY-MM-DD-HH (13 chars)
        date_folder = result.parent.name
        assert len(date_folder) == 13
        assert result.parent.parent.name == "txt"

    def test_strips_leading_dot_from_extension(self, tmp_path: Path) -> None:
        manager = _make_manager(tmp_path)
        session = MagicMock()

        result = manager.store(b"content", ".pdf", original_filename="doc.pdf", session=session)

        assert result.suffix == ".pdf"
        assert ".." not in result.name

    def test_custom_retention_category_id(self, tmp_path: Path) -> None:
        database = _make_database(retention_days=5475)
        manager = _make_manager(tmp_path, database=database)
        session = MagicMock()

        manager.store(
            b"audit content", "pdf",
            original_filename="audit.pdf",
            retention_category=10,  # Custom company category
            session=session,
        )

        stored_file = database.add.call_args[0][0]
        assert stored_file.retention_category == 10
        assert stored_file.expiration_date is not None

    def test_rejects_invalid_extension(self, tmp_path: Path) -> None:
        manager = _make_manager(tmp_path)
        session = MagicMock()

        import pytest

        with pytest.raises(ValueError, match="Invalid file extension"):
            manager.store(b"content", "txt/../../etc", original_filename="attack.txt", session=session)

    def test_rejects_empty_extension(self, tmp_path: Path) -> None:
        manager = _make_manager(tmp_path)
        session = MagicMock()

        import pytest

        with pytest.raises(ValueError, match="Invalid file extension"):
            manager.store(b"content", "", original_filename="noext", session=session)


class TestStoreFile:
    """Tests for StorageManager.store_file()."""

    def test_copies_file_content(self, tmp_path: Path) -> None:
        source_dir = tmp_path / "source"
        source_dir.mkdir()
        source_file = source_dir / "report.pdf"
        source_file.write_bytes(b"pdf content")

        storage_dir = tmp_path / "storage"
        manager = _make_manager(storage_dir)
        session = MagicMock()

        result = manager.store_file(source_file, session=session)

        assert result.read_bytes() == b"pdf content"
        # Source file should still exist (copy, not move)
        assert source_file.exists()

    def test_uses_source_filename_as_default(self, tmp_path: Path) -> None:
        source_dir = tmp_path / "source"
        source_dir.mkdir()
        source_file = source_dir / "original_name.csv"
        source_file.write_bytes(b"csv data")

        storage_dir = tmp_path / "storage"
        database = _make_database()
        manager = _make_manager(storage_dir, database=database)
        session = MagicMock()

        manager.store_file(source_file, session=session)

        stored_file = database.add.call_args[0][0]
        assert stored_file.original_filename == "original_name.csv"

    def test_custom_filename(self, tmp_path: Path) -> None:
        source_dir = tmp_path / "source"
        source_dir.mkdir()
        source_file = source_dir / "temp.pdf"
        source_file.write_bytes(b"pdf")

        storage_dir = tmp_path / "storage"
        database = _make_database()
        manager = _make_manager(storage_dir, database=database)
        session = MagicMock()

        manager.store_file(
            source_file,
            original_filename="annual_report_2025.pdf",
            session=session,
        )

        stored_file = database.add.call_args[0][0]
        assert stored_file.original_filename == "annual_report_2025.pdf"


class TestTransfer:
    """Tests for StorageManager.transfer()."""

    def test_copies_to_target_backend(self, tmp_path: Path) -> None:
        local_dir = tmp_path / "local"
        remote_dir = tmp_path / "remote"
        local_backend = FilesystemBackend(local_dir, location="local")
        remote_backend = FilesystemBackend(remote_dir, location="fs_market_hr")

        # Store a file on local backend
        relative = Path("hr/registry/companies/2025-01-01/abc123.txt")
        local_backend.store(b"file content", relative)

        database = _make_database()
        settings = StorageSettings(root=local_dir)
        manager = StorageManager(
            database, "hr", "registry", "companies",
            runtime_id="test-runtime",
            settings=settings,
            backend=local_backend,
        )

        stored_file = MagicMock(spec=StoredFile)
        stored_file.stored_path = str(relative)
        session = MagicMock()

        result = manager.transfer(stored_file, remote_backend, delete_source=False, session=session)

        assert (remote_dir / relative).exists()
        assert (remote_dir / relative).read_bytes() == b"file content"
        assert result == remote_dir / relative

    def test_copy_inserts_new_row(self, tmp_path: Path) -> None:
        local_dir = tmp_path / "local"
        remote_dir = tmp_path / "remote"
        local_backend = FilesystemBackend(local_dir, location="local")
        remote_backend = FilesystemBackend(remote_dir, location="fs_market_hr")

        relative = Path("file.txt")
        local_backend.store(b"content", relative)

        database = _make_database()
        settings = StorageSettings(root=local_dir)
        manager = StorageManager(
            database, "hr", "registry", "companies",
            runtime_id="test-runtime",
            settings=settings,
            backend=local_backend,
        )

        stored_file = MagicMock(spec=StoredFile)
        stored_file.stored_path = str(relative)
        stored_file.location = "local"
        stored_file.app_id = "test-app-id"
        stored_file.runtime = "test-runtime"
        stored_file.content_hash = "abc123"
        stored_file.original_filename = "file.txt"
        stored_file.file_extension = "txt"
        stored_file.file_size = 7
        stored_file.retention_category = 3
        stored_file.expiration_date = None
        session = MagicMock()

        manager.transfer(stored_file, remote_backend, delete_source=False, session=session)

        # Original row location unchanged
        assert stored_file.location == "local"
        # New row inserted for the copy
        database.add.assert_called_once()
        copy_record = database.add.call_args[0][0]
        assert isinstance(copy_record, StoredFile)
        assert copy_record.location == "fs_market_hr"
        assert copy_record.content_hash == "abc123"
        assert copy_record.original_filename == "file.txt"

    def test_move_updates_location(self, tmp_path: Path) -> None:
        local_dir = tmp_path / "local"
        remote_dir = tmp_path / "remote"
        local_backend = FilesystemBackend(local_dir, location="local")
        remote_backend = FilesystemBackend(remote_dir, location="fs_market_hr")

        relative = Path("file.txt")
        local_backend.store(b"content", relative)

        database = _make_database()
        settings = StorageSettings(root=local_dir)
        manager = StorageManager(
            database, "hr", "registry", "companies",
            runtime_id="test-runtime",
            settings=settings,
            backend=local_backend,
        )

        stored_file = MagicMock(spec=StoredFile)
        stored_file.stored_path = str(relative)
        session = MagicMock()

        manager.transfer(stored_file, remote_backend, delete_source=True, session=session)

        assert stored_file.location == "fs_market_hr"
        # Move does not insert a new row
        database.add.assert_not_called()

    def test_delete_source_removes_file(self, tmp_path: Path) -> None:
        local_dir = tmp_path / "local"
        remote_dir = tmp_path / "remote"
        local_backend = FilesystemBackend(local_dir, location="local")
        remote_backend = FilesystemBackend(remote_dir, location="fs_market_hr")

        relative = Path("to_move.txt")
        local_backend.store(b"content", relative)

        database = _make_database()
        settings = StorageSettings(root=local_dir)
        manager = StorageManager(
            database, "hr", "registry", "companies",
            runtime_id="test-runtime",
            settings=settings,
            backend=local_backend,
        )

        stored_file = MagicMock(spec=StoredFile)
        stored_file.stored_path = str(relative)
        session = MagicMock()

        manager.transfer(stored_file, remote_backend, delete_source=True, session=session)

        assert not (local_dir / relative).exists()
        assert (remote_dir / relative).exists()

    def test_keep_local_preserves_source(self, tmp_path: Path) -> None:
        local_dir = tmp_path / "local"
        remote_dir = tmp_path / "remote"
        local_backend = FilesystemBackend(local_dir, location="local")
        remote_backend = FilesystemBackend(remote_dir, location="fs_market_hr")

        relative = Path("to_copy.txt")
        local_backend.store(b"content", relative)

        database = _make_database()
        settings = StorageSettings(root=local_dir)
        manager = StorageManager(
            database, "hr", "registry", "companies",
            runtime_id="test-runtime",
            settings=settings,
            backend=local_backend,
        )

        stored_file = MagicMock(spec=StoredFile)
        stored_file.stored_path = str(relative)
        session = MagicMock()

        manager.transfer(stored_file, remote_backend, delete_source=False, session=session)

        assert (local_dir / relative).exists()
        assert (remote_dir / relative).exists()

    def test_move_idempotent_when_source_already_deleted(self, tmp_path: Path) -> None:
        local_dir = tmp_path / "local"
        remote_dir = tmp_path / "remote"
        local_backend = FilesystemBackend(local_dir, location="local")
        remote_backend = FilesystemBackend(remote_dir, location="fs_market_hr")

        relative = Path("already_moved.txt")
        # File already on remote, source already gone
        remote_backend.store(b"content", relative)

        database = _make_database()
        settings = StorageSettings(root=local_dir)
        manager = StorageManager(
            database, "hr", "registry", "companies",
            runtime_id="test-runtime",
            settings=settings,
            backend=local_backend,
        )

        stored_file = MagicMock(spec=StoredFile)
        stored_file.stored_path = str(relative)
        session = MagicMock()

        # Should not raise -- source is already gone, goal achieved
        result = manager.transfer(stored_file, remote_backend, delete_source=True, session=session)

        assert result == remote_dir / relative

    def test_remote_to_local_with_source_backend(self, tmp_path: Path) -> None:
        local_dir = tmp_path / "local"
        remote_dir = tmp_path / "remote"
        local_backend = FilesystemBackend(local_dir, location="local")
        remote_backend = FilesystemBackend(remote_dir, location="fs_market_hr")

        # File exists only on remote
        relative = Path("remote_file.txt")
        remote_backend.store(b"remote content", relative)

        database = _make_database()
        settings = StorageSettings(root=local_dir)
        manager = StorageManager(
            database, "hr", "registry", "companies",
            runtime_id="test-runtime",
            settings=settings,
            backend=local_backend,
        )

        stored_file = MagicMock(spec=StoredFile)
        stored_file.stored_path = str(relative)
        stored_file.location = "fs_market_hr"
        stored_file.app_id = "test-app-id"
        stored_file.runtime = "test-runtime"
        stored_file.content_hash = "abc123"
        stored_file.original_filename = "remote_file.txt"
        stored_file.file_extension = "txt"
        stored_file.file_size = 14
        stored_file.retention_category = 3
        stored_file.expiration_date = None
        session = MagicMock()

        result = manager.transfer(
            stored_file, local_backend,
            source_backend=remote_backend,
            delete_source=False,
            session=session,
        )

        # File copied to local
        assert (local_dir / relative).exists()
        assert (local_dir / relative).read_bytes() == b"remote content"
        # Remote source preserved
        assert (remote_dir / relative).exists()
        assert result == local_dir / relative


class TestComputeContentHash:
    """Tests for StorageManager.compute_content_hash()."""

    def test_deterministic(self) -> None:
        content = b"deterministic content"

        hash_a = StorageManager.compute_content_hash(content)
        hash_b = StorageManager.compute_content_hash(content)

        assert hash_a == hash_b

    def test_different_content_different_hash(self) -> None:
        hash_a = StorageManager.compute_content_hash(b"content A")
        hash_b = StorageManager.compute_content_hash(b"content B")

        assert hash_a != hash_b

    def test_hash_length(self) -> None:
        result = StorageManager.compute_content_hash(b"test")

        assert len(result) == 64
        assert all(character in "0123456789abcdef" for character in result)

    def test_matches_hashlib_sha256(self) -> None:
        content = b"verify against hashlib"
        expected = hashlib.sha256(content).hexdigest()

        assert StorageManager.compute_content_hash(content) == expected


class TestGetStorageSize:
    """Tests for StorageManager.get_storage_size()."""

    def test_returns_sum_from_database(self, tmp_path: Path) -> None:
        database = _make_database()

        def size_side_effect(statement: object, session: object, **kwargs: object) -> MagicMock:
            result = MagicMock()
            result.scalar_one.return_value = 1048576
            return result

        database.query.side_effect = size_side_effect

        manager = _make_manager(tmp_path, database=database)
        session = MagicMock()

        result = manager.get_storage_size(session=session)

        assert result == 1048576

    def test_returns_zero_when_empty(self, tmp_path: Path) -> None:
        database = _make_database()

        def size_side_effect(statement: object, session: object, **kwargs: object) -> MagicMock:
            result = MagicMock()
            result.scalar_one.return_value = 0
            return result

        database.query.side_effect = size_side_effect

        manager = _make_manager(tmp_path, database=database)
        session = MagicMock()

        result = manager.get_storage_size(session=session)

        assert result == 0


class TestRetentionExpiration:
    """Tests for DB-driven expiration date computation."""

    def test_permanent_retention_null_expiration(self, tmp_path: Path) -> None:
        database = _make_database(retention_days=None)
        manager = _make_manager(tmp_path, database=database)
        session = MagicMock()

        manager.store(
            b"permanent content", "pdf",
            original_filename="keep_forever.pdf",
            retention_category=FileRetention.PERMANENT,
            session=session,
        )

        stored_file = database.add.call_args[0][0]
        assert stored_file.expiration_date is None

    def test_transient_retention_has_expiration(self, tmp_path: Path) -> None:
        database = _make_database(retention_days=7)
        manager = _make_manager(tmp_path, database=database)
        session = MagicMock()

        manager.store(
            b"temp content", "txt",
            original_filename="temp.txt",
            retention_category=FileRetention.TRANSIENT,
            session=session,
        )

        stored_file = database.add.call_args[0][0]
        assert stored_file.expiration_date is not None

    def test_standard_retention_has_expiration(self, tmp_path: Path) -> None:
        database = _make_database(retention_days=365)
        manager = _make_manager(tmp_path, database=database)
        session = MagicMock()

        manager.store(
            b"standard content", "csv",
            original_filename="data.csv",
            retention_category=FileRetention.STANDARD,
            session=session,
        )

        stored_file = database.add.call_args[0][0]
        assert stored_file.expiration_date is not None
        assert stored_file.retention_category == FileRetention.STANDARD

class TestResolveBackend:
    """Tests for StorageManager.resolve_backend() and string backend parameter."""

    def test_resolves_active_backend(self, tmp_path: Path) -> None:
        database = MagicMock()
        backend_row = MagicMock()
        backend_row.location_name = "fs_market_hr"
        backend_row.root_path = str(tmp_path / "remote")
        backend_row.is_active = True

        mock_result = MagicMock()
        mock_result.scalars.return_value.first.return_value = backend_row
        database.query.return_value = mock_result

        session = MagicMock()
        backend = StorageManager.resolve_backend(database, "fs_market_hr", session)

        assert backend.location_name == "fs_market_hr"
        assert backend.root == tmp_path / "remote"

    def test_raises_for_missing_backend(self) -> None:
        database = MagicMock()
        mock_result = MagicMock()
        mock_result.scalars.return_value.first.return_value = None
        database.query.return_value = mock_result

        session = MagicMock()
        import pytest

        with pytest.raises(ValueError, match="No storage backend found"):
            StorageManager.resolve_backend(database, "nonexistent", session)

    def test_raises_for_inactive_backend(self) -> None:
        database = MagicMock()
        backend_row = MagicMock()
        backend_row.location_name = "fs_disabled"
        backend_row.root_path = "/disabled"
        backend_row.is_active = False

        mock_result = MagicMock()
        mock_result.scalars.return_value.first.return_value = backend_row
        database.query.return_value = mock_result

        session = MagicMock()
        import pytest

        with pytest.raises(ValueError, match="inactive"):
            StorageManager.resolve_backend(database, "fs_disabled", session)

    def test_string_backend_in_constructor(self, tmp_path: Path) -> None:
        database = _make_database()
        backend_row = MagicMock()
        backend_row.location_name = "fs_market_hr"
        backend_row.root_path = str(tmp_path / "remote")
        backend_row.is_active = True

        def init_query_side_effect(statement: object, session: object, **kwargs: object) -> MagicMock:
            statement_str = str(statement)
            result = MagicMock()
            if "storage_backend" in statement_str:
                result.scalars.return_value.first.return_value = backend_row
            elif "c_file_retention" in statement_str:
                retention_row = MagicMock()
                retention_row.retention_days = 365
                result.scalars.return_value.first.return_value = retention_row
            else:
                result.scalars.return_value.first.return_value = None
                result.scalars.return_value.all.return_value = []
                result.scalar_one.return_value = 0
            return result

        database.query.side_effect = init_query_side_effect
        database.create_session.return_value.__enter__ = MagicMock(return_value=MagicMock())
        database.create_session.return_value.__exit__ = MagicMock(return_value=False)

        settings = StorageSettings(root=tmp_path)
        manager = StorageManager(
            database, "hr", "registry", "companies",
            runtime_id="test-runtime",
            settings=settings,
            backend="fs_market_hr",
        )

        assert manager._backend.location_name == "fs_market_hr"
