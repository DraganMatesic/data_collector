"""Periodic storage maintenance: file retention enforcement and disk space monitoring.

The Manager composes ``StorageJanitor`` and calls :meth:`run_maintenance`
on a configurable interval within the main tick loop.  Each cycle iterates
all active backends from the ``StorageBackend`` table, enforces per-file
retention based on ``expiration_date``, monitors total storage usage against
per-backend budget thresholds, and checks physical disk free space.

Alert thresholds are resolved per-backend: each ``StorageBackend`` row can
override the global defaults from ``StorageSettings``.
"""

from __future__ import annotations

import logging
import shutil
from pathlib import Path

from sqlalchemy import func, select

from data_collector.settings.manager import ManagerSettings
from data_collector.settings.storage import StorageSettings
from data_collector.storage.backend import FilesystemBackend
from data_collector.storage.retention import enforce_retention_by_database
from data_collector.tables.storage import StorageBackend, StoredFile
from data_collector.utilities.database.main import Database


class StorageJanitor:
    """Periodic storage maintenance across all registered backends.

    Responsibilities:
        1. **Retention enforcement** -- delete expired files and their
           ``StoredFile`` rows based on ``expiration_date``.
        2. **Disk free space monitoring** (mandatory) -- check physical disk
           free space via ``shutil.disk_usage()``.  Threshold resolved
           per-backend from ``StorageBackend.min_free_disk_gb``, falling
           back to ``StorageSettings.min_free_disk_gb``.
        3. **Storage budget monitoring** (optional) -- compute total stored
           bytes per backend and compare against budget.  Threshold from
           ``StorageBackend.max_storage_alert_gb``, falling back to
           ``StorageSettings.max_storage_alert_gb`` (``None`` = disabled).

    Composed into the Manager and called synchronously within the
    ``_tick()`` loop.  Any exception is caught by the Manager wrapper --
    storage janitor failures never affect app scheduling or commands.

    Args:
        database: Database instance for StoredFile queries and deletions.
        manager_settings: Manager configuration (janitor interval/toggle).
        storage_settings: Storage configuration (alert thresholds).
        logger: Structured logger for janitor messages.
    """

    def __init__(
        self,
        database: Database,
        manager_settings: ManagerSettings,
        storage_settings: StorageSettings,
        *,
        logger: logging.Logger,
    ) -> None:
        self._database = database
        self._manager_settings = manager_settings
        self._storage_settings = storage_settings
        self._logger = logger

    def run_maintenance(self) -> None:
        """Run one maintenance cycle across all active backends.

        1. Load active backends with their configuration rows.
        2. For each backend: enforce retention, check disk free space,
           check storage budget.
        3. Log summary of deleted files.
        """
        backend_pairs = self._load_active_backends()
        if not backend_pairs:
            self._logger.debug("Storage janitor: no active backends registered")
            return

        total_deleted = 0
        for backend, backend_row in backend_pairs:
            try:
                deleted = self._enforce_backend_retention(backend)
                total_deleted += deleted

                free_disk_threshold = self._resolve_min_free_disk_gb(backend_row)
                self._check_disk_free_space(backend, free_disk_threshold)

                budget_threshold = self._resolve_max_storage_alert_gb(backend_row)
                if budget_threshold is not None:
                    self._check_storage_usage(backend, budget_threshold)
            except Exception:
                self._logger.exception(
                    f"Storage janitor: error processing backend '{backend.location_name}', skipping",
                )

        if total_deleted > 0:
            self._logger.info(
                f"Storage janitor: deleted {total_deleted} expired file(s) across {len(backend_pairs)} backend(s)",
            )

    def _load_active_backends(self) -> list[tuple[FilesystemBackend, StorageBackend | None]]:
        """Load all active backends: registered backends from the database plus the default local backend.

        The default local backend (``StorageSettings.root``, location ``"local"``)
        is always included even when no ``storage_backend`` row exists for it.
        This ensures the zero-config deployment scenario gets retention
        enforcement and disk monitoring without manual DBA setup.

        Returns:
            List of (FilesystemBackend, StorageBackend row or None) pairs.
            The row is ``None`` for the auto-included local backend when no
            matching ``storage_backend`` row exists.
        """
        with self._database.create_session() as session:
            statement = select(StorageBackend).where(
                StorageBackend.is_active.is_(True),
            )
            backend_rows: list[StorageBackend] = list(
                self._database.query(statement, session).scalars().all()
            )
            for row in backend_rows:
                session.expunge(row)

        registered_locations: set[str] = set()
        pairs: list[tuple[FilesystemBackend, StorageBackend | None]] = []
        for row in backend_rows:
            location = str(row.location_name)
            registered_locations.add(location)
            backend = FilesystemBackend(
                root=Path(str(row.root_path)),
                location=location,
                logger=self._logger,
            )
            pairs.append((backend, row))

        # Auto-include default local backend if not already registered
        if "local" not in registered_locations:
            local_backend = FilesystemBackend(
                root=self._storage_settings.root,
                location="local",
                logger=self._logger,
            )
            pairs.append((local_backend, None))

        return pairs

    def _enforce_backend_retention(self, backend: FilesystemBackend) -> int:
        """Enforce DB-driven retention for a single backend.

        Args:
            backend: The storage backend to clean.

        Returns:
            Number of files deleted.
        """
        return enforce_retention_by_database(
            self._database,
            backend,
            logger=self._logger,
        )

    def _check_disk_free_space(self, backend: FilesystemBackend, threshold_gb: float) -> None:
        """Check physical disk free space and log warning if below threshold.

        Uses ``shutil.disk_usage()`` which works on local paths, UNC paths
        (Windows), and mounted paths (Linux).

        Args:
            backend: The storage backend whose root directory to check.
            threshold_gb: Minimum free space in GB before alerting.
        """
        try:
            usage = shutil.disk_usage(backend.root)
        except OSError:
            self._logger.warning(
                f"Storage janitor: cannot check disk for backend '{backend.location_name}' at {backend.root}",
            )
            return

        free_gb = usage.free / (1024 ** 3)
        if free_gb < threshold_gb:
            self._logger.warning(
                f"Storage janitor: backend '{backend.location_name}' disk free {free_gb:.2f} GB "
                f"below {threshold_gb:.2f} GB threshold",
            )
        else:
            self._logger.debug(
                f"Storage janitor: backend '{backend.location_name}' disk free {free_gb:.2f} GB "
                f"(threshold {threshold_gb:.2f} GB)",
            )

    def _check_storage_usage(self, backend: FilesystemBackend, threshold_gb: float) -> None:
        """Check total stored data for a backend against a budget threshold.

        Queries ``SUM(file_size)`` from ``StoredFile`` for the given
        location.  Only called when the budget threshold is not ``None``.

        Args:
            backend: The storage backend to check.
            threshold_gb: Maximum allowed storage in GB.
        """
        with self._database.create_session() as session:
            statement = select(
                func.coalesce(func.sum(StoredFile.file_size), 0)
            ).where(
                StoredFile.location == backend.location_name,
            )
            total_bytes = int(self._database.query(statement, session).scalar_one())

        total_gb = total_bytes / (1024 ** 3)
        if total_gb >= threshold_gb:
            self._logger.warning(
                f"Storage janitor: backend '{backend.location_name}' usage {total_gb:.2f} GB "
                f"exceeds budget {threshold_gb:.2f} GB",
            )
        else:
            self._logger.debug(
                f"Storage janitor: backend '{backend.location_name}' usage {total_gb:.2f} GB "
                f"(budget {threshold_gb:.2f} GB)",
            )

    def _resolve_min_free_disk_gb(self, backend_row: StorageBackend | None) -> float:
        """Resolve the minimum free disk space threshold for a backend.

        Per-backend value from ``StorageBackend.min_free_disk_gb`` takes
        precedence.  Falls back to ``StorageSettings.min_free_disk_gb``.
        When ``backend_row`` is ``None`` (auto-included local backend),
        the global default is used.

        Args:
            backend_row: The StorageBackend configuration row, or ``None``.

        Returns:
            Threshold in GB.
        """
        if backend_row is not None:
            per_backend_value = getattr(backend_row, "min_free_disk_gb", None)
            if per_backend_value is not None:
                return float(per_backend_value)
        return self._storage_settings.min_free_disk_gb

    def _resolve_max_storage_alert_gb(self, backend_row: StorageBackend | None) -> float | None:
        """Resolve the storage budget threshold for a backend.

        Per-backend value from ``StorageBackend.max_storage_alert_gb`` takes
        precedence.  Falls back to ``StorageSettings.max_storage_alert_gb``.
        When ``backend_row`` is ``None`` (auto-included local backend),
        the global default is used.

        Args:
            backend_row: The StorageBackend configuration row, or ``None``.

        Returns:
            Threshold in GB, or ``None`` if budget alerting is disabled.
        """
        if backend_row is not None:
            per_backend_value = getattr(backend_row, "max_storage_alert_gb", None)
            if per_backend_value is not None:
                return float(per_backend_value)
        return self._storage_settings.max_storage_alert_gb
