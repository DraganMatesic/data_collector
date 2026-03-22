"""Tests for storage backend interface and filesystem implementation."""

from pathlib import Path

import pytest

from data_collector.storage.backend import FilesystemBackend


class TestFilesystemStore:
    """Tests for FilesystemBackend.store()."""

    def test_creates_parent_directories(self, tmp_path: Path) -> None:
        backend = FilesystemBackend(tmp_path)
        relative = Path("deep/nested/dir/file.txt")

        backend.store(b"content", relative)

        assert (tmp_path / relative).exists()

    def test_writes_correct_content(self, tmp_path: Path) -> None:
        backend = FilesystemBackend(tmp_path)
        content = b"hello world binary content"
        relative = Path("test.bin")

        backend.store(content, relative)

        assert (tmp_path / relative).read_bytes() == content

    def test_returns_absolute_path(self, tmp_path: Path) -> None:
        backend = FilesystemBackend(tmp_path)
        relative = Path("output/file.pdf")

        result = backend.store(b"pdf bytes", relative)

        assert result == tmp_path / relative
        assert result.is_absolute()

    def test_overwrites_existing_file(self, tmp_path: Path) -> None:
        backend = FilesystemBackend(tmp_path)
        relative = Path("overwrite.txt")

        backend.store(b"first", relative)
        backend.store(b"second", relative)

        assert (tmp_path / relative).read_bytes() == b"second"


class TestFilesystemRetrieve:
    """Tests for FilesystemBackend.retrieve()."""

    def test_reads_stored_content(self, tmp_path: Path) -> None:
        backend = FilesystemBackend(tmp_path)
        content = b"stored content for retrieval"
        relative = Path("retrieve_test.bin")
        backend.store(content, relative)

        result = backend.retrieve(relative)

        assert result == content


class TestFilesystemDelete:
    """Tests for FilesystemBackend.delete()."""

    def test_removes_existing_file(self, tmp_path: Path) -> None:
        backend = FilesystemBackend(tmp_path)
        relative = Path("to_delete.txt")
        backend.store(b"delete me", relative)

        result = backend.delete(relative)

        assert result is True
        assert not (tmp_path / relative).exists()

    def test_returns_false_for_missing_file(self, tmp_path: Path) -> None:
        backend = FilesystemBackend(tmp_path)

        result = backend.delete(Path("nonexistent.txt"))

        assert result is False


class TestFilesystemExists:
    """Tests for FilesystemBackend.exists()."""

    def test_true_for_existing_file(self, tmp_path: Path) -> None:
        backend = FilesystemBackend(tmp_path)
        relative = Path("exists.txt")
        backend.store(b"exists", relative)

        assert backend.exists(relative) is True

    def test_false_for_missing_file(self, tmp_path: Path) -> None:
        backend = FilesystemBackend(tmp_path)

        assert backend.exists(Path("missing.txt")) is False

    def test_false_for_directory(self, tmp_path: Path) -> None:
        backend = FilesystemBackend(tmp_path)
        (tmp_path / "subdir").mkdir()

        assert backend.exists(Path("subdir")) is False


class TestFilesystemGetSize:
    """Tests for FilesystemBackend.get_size()."""

    def test_returns_correct_byte_count(self, tmp_path: Path) -> None:
        backend = FilesystemBackend(tmp_path)
        content = b"exactly 25 bytes of data!"
        relative = Path("sized.bin")
        backend.store(content, relative)

        assert backend.get_size(relative) == 25


class TestFilesystemProperties:
    """Tests for FilesystemBackend properties."""

    def test_root_property(self, tmp_path: Path) -> None:
        backend = FilesystemBackend(tmp_path)

        assert backend.root == tmp_path

    def test_default_location_name(self, tmp_path: Path) -> None:
        backend = FilesystemBackend(tmp_path)

        assert backend.location_name == "local"

    def test_custom_location_name(self, tmp_path: Path) -> None:
        backend = FilesystemBackend(tmp_path, location="fs_market_hr")

        assert backend.location_name == "fs_market_hr"


class TestPathValidation:
    """Tests for path escape prevention."""

    def test_rejects_parent_directory_traversal(self, tmp_path: Path) -> None:
        backend = FilesystemBackend(tmp_path)

        with pytest.raises(ValueError, match="escapes"):
            backend.store(b"data", Path("../../etc/passwd"))

    def test_rejects_absolute_path_escape(self, tmp_path: Path) -> None:
        backend = FilesystemBackend(tmp_path)

        with pytest.raises(ValueError, match="escapes"):
            backend.retrieve(Path("../../outside.txt"))

    def test_allows_nested_relative_path(self, tmp_path: Path) -> None:
        backend = FilesystemBackend(tmp_path)
        relative = Path("deep/nested/dir/file.txt")

        backend.store(b"content", relative)

        assert backend.exists(relative)
