"""Tests for StorageSettings environment variable loading and defaults."""

from pathlib import Path

import pytest

from data_collector.settings.storage import StorageSettings


class TestDefaults:
    """Tests for StorageSettings default values."""

    def test_root_default(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("DC_STORAGE_ROOT", raising=False)
        settings = StorageSettings()
        assert settings.root == Path("./storage")

    def test_deduplicate_default(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("DC_STORAGE_DEDUPLICATE", raising=False)
        settings = StorageSettings()
        assert settings.deduplicate is True

    def test_directory_depth_default(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("DC_STORAGE_DIRECTORY_DEPTH", raising=False)
        settings = StorageSettings()
        assert settings.directory_depth == "daily"

    def test_default_retention_default(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("DC_STORAGE_DEFAULT_RETENTION", raising=False)
        settings = StorageSettings()
        assert settings.default_retention == 3  # FileRetention.STANDARD

    def test_min_free_disk_gb_default(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("DC_STORAGE_MIN_FREE_DISK_GB", raising=False)
        settings = StorageSettings()
        assert settings.min_free_disk_gb == 10.0

    def test_max_storage_alert_gb_default(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("DC_STORAGE_MAX_STORAGE_ALERT_GB", raising=False)
        settings = StorageSettings()
        assert settings.max_storage_alert_gb is None


class TestEnvPrefix:
    """Tests for DC_STORAGE_ environment variable prefix."""

    def test_root_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DC_STORAGE_ROOT", "/data/storage")
        settings = StorageSettings()
        assert settings.root == Path("/data/storage")

    def test_deduplicate_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DC_STORAGE_DEDUPLICATE", "false")
        settings = StorageSettings()
        assert settings.deduplicate is False

    def test_directory_depth_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DC_STORAGE_DIRECTORY_DEPTH", "monthly")
        settings = StorageSettings()
        assert settings.directory_depth == "monthly"

    def test_default_retention_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DC_STORAGE_DEFAULT_RETENTION", "9")
        settings = StorageSettings()
        assert settings.default_retention == 9  # FileRetention.PERMANENT

    def test_min_free_disk_gb_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DC_STORAGE_MIN_FREE_DISK_GB", "50.0")
        settings = StorageSettings()
        assert settings.min_free_disk_gb == 50.0

    def test_max_storage_alert_gb_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DC_STORAGE_MAX_STORAGE_ALERT_GB", "500.0")
        settings = StorageSettings()
        assert settings.max_storage_alert_gb == 500.0


class TestDirectConstruction:
    """Tests for direct keyword construction."""

    def test_all_fields(self) -> None:
        settings = StorageSettings(
            root=Path("/custom/path"),
            deduplicate=False,
            directory_depth="hourly",
            default_retention=9,
            min_free_disk_gb=25.0,
            max_storage_alert_gb=50.0,
        )

        assert settings.root == Path("/custom/path")
        assert settings.deduplicate is False
        assert settings.directory_depth == "hourly"
        assert settings.default_retention == 9
        assert settings.min_free_disk_gb == 25.0
        assert settings.max_storage_alert_gb == 50.0

    def test_root_path_coercion(self) -> None:
        settings = StorageSettings(root="/string/path")  # type: ignore[arg-type]
        assert isinstance(settings.root, Path)
        assert settings.root == Path("/string/path")


class TestValidation:
    """Tests for StorageSettings field validation."""

    def test_directory_depth_rejects_invalid_value(self) -> None:
        with pytest.raises(ValueError, match="directory_depth must be one of"):
            StorageSettings(directory_depth="invalid_depth")

    def test_directory_depth_accepts_all_valid_values(self) -> None:
        for depth in ["flat", "yearly", "monthly", "daily", "hourly"]:
            settings = StorageSettings(directory_depth=depth)
            assert settings.directory_depth == depth
