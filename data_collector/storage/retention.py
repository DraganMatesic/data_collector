"""File retention policies and enforcement.

Provides filesystem-based retention (by file age) and database-driven
retention (by per-file ``expiration_date`` computed from
``CodebookFileRetention.retention_days``).

The database-driven approach is the primary mechanism:  each file's
retention category determines its ``expiration_date`` at store time,
and ``enforce_retention_by_database()`` deletes expired files along
with their ``StoredFile`` rows.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

from sqlalchemy import delete, select

from data_collector.enums.storage import FileRetention
from data_collector.tables.storage import CodebookFileRetention, StoredFile

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from data_collector.settings.storage import StorageSettings
    from data_collector.storage.backend import BaseStorageBackend
    from data_collector.utilities.database.main import Database


# -- Auto-derived from FileRetention enum: maps lowercase name to member --
_RETENTION_NAME_TO_ENUM: dict[str, FileRetention] = {
    member.name.lower(): member for member in FileRetention
}


@dataclass(frozen=True)
class RetentionPolicy:
    """Configuration for filesystem-based file retention enforcement.

    Args:
        max_age_days: Maximum file age in days before deletion.  ``None``
            means permanent -- no files are deleted.
    """

    max_age_days: int | None

    @classmethod
    def from_settings(
        cls,
        settings: StorageSettings,
        database: Database,
        session: Session,
    ) -> RetentionPolicy:
        """Create a RetentionPolicy from StorageSettings and the codebook table.

        Resolves ``settings.default_retention`` to a ``FileRetention`` enum
        value, then queries ``CodebookFileRetention.retention_days`` from
        the database.

        Args:
            settings: Storage configuration with ``default_retention`` name.
            database: Database instance for codebook lookup.
            session: Active SQLAlchemy session.

        Returns:
            A RetentionPolicy with ``max_age_days`` from the database.
        """
        retention_name = settings.default_retention.lower()
        retention_enum = _RETENTION_NAME_TO_ENUM.get(retention_name)
        if retention_enum is None:
            return cls(max_age_days=None)

        statement = select(CodebookFileRetention.retention_days).where(
            CodebookFileRetention.id == retention_enum.value,
        )
        retention_days = database.query(statement, session).scalar_one_or_none()
        return cls(max_age_days=retention_days)


def enforce_retention(
    backend: BaseStorageBackend,
    policy: RetentionPolicy,
    *,
    logger: logging.Logger | None = None,
) -> int:
    """Delete files older than the retention policy allows.

    Walks all files under the backend root, deletes those whose
    modification time exceeds ``policy.max_age_days``, and removes
    empty subdirectories after cleanup.

    Args:
        backend: Storage backend whose root directory to scan.
        policy: Retention policy defining the maximum file age.
        logger: Optional logger for retention messages.

    Returns:
        Number of files deleted.
    """
    retention_logger = logger or logging.getLogger(__name__)

    if policy.max_age_days is None:
        retention_logger.debug("Retention policy is permanent, skipping enforcement")
        return 0

    root = backend.root
    if not root.is_dir():
        retention_logger.debug(f"Retention root does not exist: {root}")
        return 0

    cutoff_timestamp = (datetime.now(UTC) - timedelta(days=policy.max_age_days)).timestamp()
    deleted_count = 0

    for file_path in _walk_files(root):
        try:
            file_mtime = file_path.stat().st_mtime
            if file_mtime < cutoff_timestamp:
                file_path.unlink()
                deleted_count += 1
        except PermissionError:
            retention_logger.warning(f"Retention: permission denied, skipping {file_path}")
        except OSError:
            retention_logger.warning(f"Retention: failed to process {file_path}", exc_info=True)

    _remove_empty_directories(root, logger=retention_logger)

    if deleted_count > 0:
        retention_logger.info(f"Retention: deleted {deleted_count} file(s) from {root}")

    return deleted_count


def enforce_retention_by_database(
    database: Database,
    backend: BaseStorageBackend,
    *,
    logger: logging.Logger | None = None,
) -> int:
    """Delete expired files using database-driven retention.

    Queries ``StoredFile`` rows whose ``expiration_date`` has passed and
    whose ``location`` matches the backend, deletes the physical files
    via the backend, then removes the corresponding database rows.

    This is the primary retention mechanism.  Each file's expiration is
    computed at store time from ``CodebookFileRetention.retention_days``.

    Args:
        database: Database instance for querying and deleting StoredFile rows.
        backend: Storage backend holding the physical files.
        logger: Optional logger for retention messages.

    Returns:
        Number of files deleted.
    """
    retention_logger = logger or logging.getLogger(__name__)
    now = datetime.now(UTC)

    with database.create_session() as session:
        statement = select(StoredFile).where(
            StoredFile.expiration_date.isnot(None),
            StoredFile.expiration_date <= now,
            StoredFile.location == backend.location_name,
        )
        expired_files: list[StoredFile] = list(
            database.query(statement, session).scalars().all()
        )

        if not expired_files:
            return 0

        expired_ids: list[int] = []
        for stored_file in expired_files:
            relative_path = Path(str(stored_file.stored_path))
            try:
                file_deleted = backend.delete(relative_path)
            except PermissionError:
                retention_logger.warning(f"Retention: permission denied, skipping {relative_path}")
                continue
            except OSError:
                retention_logger.warning(f"Retention: failed to delete {relative_path}", exc_info=True)
                continue

            if file_deleted or not backend.exists(relative_path):
                # File was deleted or was already absent -- clean up the DB row
                expired_ids.append(int(stored_file.id))  # type: ignore[arg-type]

        if expired_ids:
            delete_statement = delete(StoredFile).where(
                StoredFile.id.in_(expired_ids),  # type: ignore[union-attr]
            )
            database.run(delete_statement, session)
            session.commit()

    retention_logger.info(
        f"Retention: deleted {len(expired_ids)} expired file(s) from {backend.location_name}",
    )

    _remove_empty_directories(backend.root, logger=retention_logger)
    return len(expired_ids)


def _walk_files(directory: Path) -> list[Path]:
    """Collect all files under *directory* recursively.

    Args:
        directory: Root directory to walk.

    Returns:
        List of file paths found.
    """
    files: list[Path] = []
    for root, _directories, filenames in os.walk(directory, followlinks=False):
        root_path = Path(root)
        for filename in filenames:
            files.append(root_path / filename)
    return files


def _remove_empty_directories(
    root: Path,
    *,
    logger: logging.Logger,
) -> None:
    """Remove empty subdirectories under *root* (bottom-up).

    The root directory itself is never removed.

    Args:
        root: Base directory to scan for empty subdirectories.
        logger: Logger for error reporting.
    """
    for dirpath, dirnames, filenames in os.walk(root, topdown=False, followlinks=False):
        directory = Path(dirpath)
        if directory == root:
            continue
        if not filenames and not dirnames:
            try:
                directory.rmdir()
                logger.debug(f"Retention: removed empty directory {directory}")
            except OSError:
                logger.warning(f"Retention: failed to remove directory {directory}", exc_info=True)
