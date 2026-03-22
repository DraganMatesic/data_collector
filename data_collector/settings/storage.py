"""Pydantic settings for file storage management."""

from __future__ import annotations

from pathlib import Path

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from data_collector.enums.storage import FileRetention

_VALID_DIRECTORY_DEPTHS = {"flat", "yearly", "monthly", "daily", "hourly"}


class StorageSettings(BaseSettings):
    """Storage management settings loaded from environment variables.

    Environment variables follow the ``DC_STORAGE_`` prefix pattern:

        DC_STORAGE_ROOT                 -- Base directory for file storage.
        DC_STORAGE_DEDUPLICATE          -- Enable SHA-256 hash-based deduplication.
        DC_STORAGE_DIRECTORY_DEPTH      -- Date folder granularity (flat/yearly/monthly/daily/hourly).
        DC_STORAGE_DEFAULT_RETENTION   -- Default FileRetention category (integer ID).
        DC_STORAGE_MIN_FREE_DISK_GB    -- Alert when physical disk free space drops below (GB).
        DC_STORAGE_MAX_STORAGE_ALERT_GB -- Optional: alert when per-backend stored data exceeds (GB).

    Alerting thresholds can be overridden per-backend in the ``StorageBackend``
    database table.  These settings serve as global defaults for backends that
    do not specify their own thresholds.

    Examples:
        From environment variables::

            settings = StorageSettings()
            manager = StorageManager(database, "hr", "registry", "companies", runtime_id=rid, settings=settings)

        Direct construction (testing, overrides)::

            settings = StorageSettings(root=Path("/data/storage"), directory_depth="monthly")
    """

    model_config = SettingsConfigDict(env_prefix="DC_STORAGE_")

    # -- Storage root --
    root: Path = Path("./storage")

    # -- Deduplication --
    deduplicate: bool = True

    # -- Directory organization --
    directory_depth: str = "daily"

    # -- Retention default --
    default_retention: int = FileRetention.STANDARD

    # -- Alerting (global defaults, overridden per-backend in StorageBackend table) --
    min_free_disk_gb: float = 10.0
    max_storage_alert_gb: float | None = None

    @field_validator("directory_depth")
    @classmethod
    def validate_directory_depth(cls, value: str) -> str:
        """Validate that directory_depth is one of the supported values."""
        if value not in _VALID_DIRECTORY_DEPTHS:
            message = f"directory_depth must be one of {sorted(_VALID_DIRECTORY_DEPTHS)}, got '{value}'"
            raise ValueError(message)
        return value
