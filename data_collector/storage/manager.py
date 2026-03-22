"""Centralized file storage with deduplication, retention, and multi-backend transfer.

``StorageManager`` organizes files by extension and configurable date depth,
names them by SHA-256 content hash for deduplication, tracks metadata in the
``StoredFile`` table, and supports bidirectional transfers between storage
backends (local disk, network shares).

Directory structure: ``{root}/{group}/{parent}/{app}/{ext}/{date}/{hash}.{ext}``
"""

from __future__ import annotations

import hashlib
import logging
import re
from datetime import UTC, datetime, timedelta
from pathlib import Path

from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from data_collector.enums.storage import FileRetention
from data_collector.settings.storage import StorageSettings
from data_collector.storage.backend import BaseStorageBackend, FilesystemBackend
from data_collector.storage.retention import enforce_retention_by_database
from data_collector.tables.storage import CodebookFileRetention, StorageBackend, StoredFile
from data_collector.utilities.database.main import Database
from data_collector.utilities.functions.runtime import get_app_id

# Date folder format strings keyed by directory_depth setting value.
# Empty string means no date folder (flat mode).
_DATE_FORMATS: dict[str, str] = {
    "flat": "",
    "yearly": "%Y",
    "monthly": "%Y-%m",
    "daily": "%Y-%m-%d",
    "hourly": "%Y-%m-%d-%H",
}


class StorageManager:
    """File storage with daily directory organization, hash-based deduplication, and retention.

    Files are stored using their SHA-256 content hash as the filename
    (``{hash}.{ext}``).  The original filename is preserved in the
    ``StoredFile`` database record for display and query purposes.

    Retention categories are read from the ``CodebookFileRetention`` table
    at runtime, not hardcoded.  Companies can add custom categories by
    inserting rows into the codebook table.  The ``retention_category``
    parameter accepts both ``FileRetention`` enum values and custom
    integer category IDs.

    Args:
        database: Database instance for StoredFile operations.
        group: Application group (e.g., ``"hr"``, ``"examples"``).
        parent: Application parent (e.g., ``"registry"``, ``"scraping"``).
        app_name: Application name (e.g., ``"company_scraper"``).
        runtime_id: Runtime session identifier for StoredFile.runtime FK.
        settings: Storage configuration.  Defaults to ``StorageSettings()``.
        backend: Storage backend.  Defaults to ``FilesystemBackend(settings.root)``.
        logger: Structured logger.  Defaults to module-level logger.

    Examples:
        Basic usage::

            manager = StorageManager(
                database, "hr", "registry", "companies",
                runtime_id=runtime_id,
            )
            path = manager.store(
                pdf_bytes, "pdf",
                original_filename="annual_report_2025.pdf",
                session=session,
            )

        With custom retention category::

            # Using a custom category ID added by company DBA
            path = manager.store(
                pdf_bytes, "pdf",
                original_filename="tax_filing.pdf",
                retention_category=10,  # Custom 'Regulatory 15Y'
                session=session,
            )

        With custom backend::

            remote = FilesystemBackend(Path("//fileserver/share"), location="fs_hr")
            manager = StorageManager(
                database, "hr", "registry", "companies",
                runtime_id=runtime_id,
                backend=remote,
            )
    """

    def __init__(
        self,
        database: Database,
        group: str,
        parent: str,
        app_name: str,
        *,
        runtime_id: str,
        settings: StorageSettings | None = None,
        backend: BaseStorageBackend | str | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self._database = database
        self._group = group
        self._parent = parent
        self._app_name = app_name
        self._runtime_id = runtime_id
        self._settings = settings or StorageSettings()
        self._logger = logger or logging.getLogger(__name__)

        if isinstance(backend, str):
            with database.create_session() as session:
                self._backend = self.resolve_backend(database, backend, session, logger=self._logger)
        elif backend is not None:
            self._backend = backend
        else:
            self._backend = FilesystemBackend(self._settings.root, location="local")
        self._app_id = get_app_id(group, parent, app_name)
        self._app_relative_root = Path(group) / parent / app_name
        self._retention_cache: dict[int, int | None] = {}

    @property
    def app_directory(self) -> Path:
        """Absolute path to the application storage directory."""
        return self._backend.root / self._app_relative_root

    @property
    def today_directory(self) -> Path:
        """Absolute path to the current date directory for a given extension.

        Returns the app directory with the date folder appended based on
        ``directory_depth``.  In flat mode, returns the app directory itself.
        Does not include the extension segment -- use the full path from
        ``store()`` for extension-specific directories.
        """
        date_format = _DATE_FORMATS.get(self._settings.directory_depth, _DATE_FORMATS["daily"])
        if date_format:
            return self.app_directory / datetime.now(UTC).strftime(date_format)
        return self.app_directory

    def store(
        self,
        content: bytes,
        extension: str,
        *,
        original_filename: str,
        retention_category: int = FileRetention.STANDARD,
        session: Session,
    ) -> Path:
        """Store file content with hash-based naming and deduplication.

        Computes the SHA-256 hash of *content*, checks for an existing file
        with the same hash in the database (if deduplication is enabled),
        and stores the file as ``{hash}.{ext}`` in the daily directory.

        A ``StoredFile`` row is inserted for metadata tracking.

        Args:
            content: Raw file bytes.
            extension: File extension without leading dot (e.g., ``"pdf"``).
            original_filename: Original filename for display and queries.
            retention_category: Retention category ID from
                ``CodebookFileRetention``.  Accepts ``FileRetention`` enum
                values or custom integer IDs.
            session: Active SQLAlchemy session for database operations.

        Returns:
            Absolute path of the stored (or existing deduplicated) file.
        """
        content_hash = self.compute_content_hash(content)
        extension = extension.lstrip(".")
        if not re.match(r"^[a-zA-Z0-9._-]{1,32}$", extension):
            message = f"Invalid file extension: '{extension}'"
            raise ValueError(message)

        if self._settings.deduplicate:
            existing_path = self._find_duplicate(content_hash, session)
            if existing_path is not None:
                self._logger.debug(
                    f"Dedup: file with hash {content_hash[:12]} already exists at {existing_path}",
                )
                return existing_path

        relative_path = self._build_relative_path(content_hash, extension)
        absolute_path = self._backend.store(content, relative_path)

        expiration_date = self._compute_expiration_date(retention_category, session)
        stored_file = StoredFile(
            app_id=self._app_id,
            runtime=self._runtime_id,
            content_hash=content_hash,
            original_filename=original_filename,
            stored_path=str(relative_path),
            file_extension=extension,
            file_size=len(content),
            location=self._backend.location_name,
            retention_category=retention_category,
            expiration_date=expiration_date,
        )
        try:
            self._database.add(stored_file, session)
            session.flush()
        except IntegrityError:
            # Concurrent insert won the race -- unique constraint on
            # (app_id, content_hash, location) prevents duplicates.
            session.rollback()
            self._logger.debug(
                f"Dedup: concurrent insert for hash {content_hash[:12]} on {self._backend.location_name}",
            )
            return absolute_path

        self._logger.info(
            f"Stored {original_filename} ({len(content)} bytes) as {relative_path} "
            f"[retention_category={retention_category}]",
        )
        return absolute_path

    def store_file(
        self,
        source_path: Path,
        *,
        original_filename: str | None = None,
        retention_category: int = FileRetention.STANDARD,
        session: Session,
    ) -> Path:
        """Store an existing file from disk.

        Reads the source file and delegates to :meth:`store`.

        Args:
            source_path: Path to the source file to store.
            original_filename: Original filename.  Defaults to the source
                file's name.
            retention_category: Retention category ID from
                ``CodebookFileRetention``.
            session: Active SQLAlchemy session for database operations.

        Returns:
            Absolute path of the stored file.
        """
        content = source_path.read_bytes()
        resolved_filename = original_filename or source_path.name
        extension = source_path.suffix.lstrip(".")
        return self.store(
            content,
            extension,
            original_filename=resolved_filename,
            retention_category=retention_category,
            session=session,
        )

    def transfer(
        self,
        stored_file: StoredFile,
        target_backend: BaseStorageBackend,
        *,
        source_backend: BaseStorageBackend | None = None,
        delete_source: bool = False,
        retention_category: int | None = None,
        session: Session,
    ) -> Path:
        """Transfer a file between any two storage backends.

        Reads from the source backend and writes to the target backend.
        Supports any direction: local-to-remote, remote-to-local, or
        cross-server transfers.

        **Copy mode** (``delete_source=False``): creates a new ``StoredFile``
        row for the target location.  Both the original and the copy are
        tracked independently -- each has its own retention enforcement,
        storage size accounting, and location.

        **Move mode** (``delete_source=True``): deletes the source file and
        updates the existing ``StoredFile`` row to point to the target.

        An optional ``retention_category`` overrides the retention policy
        for the target copy.  This enables different retention rules per
        location (e.g., TRANSIENT locally, PERMANENT on the file server).

        Args:
            stored_file: The ``StoredFile`` record to transfer.
            target_backend: Destination storage backend.
            source_backend: Backend that currently holds the file.
                Defaults to ``self._backend`` when ``None``.
            delete_source: If ``True``, delete the source file and update
                the existing row (move).  If ``False``, keep the source
                and insert a new row for the copy.
            retention_category: Optional retention category ID for the
                target.  When ``None``, inherits the source file's
                retention.  In move mode, updates the existing row.
            session: Active SQLAlchemy session for database operations.

        Returns:
            Absolute path of the file on the target backend.

        Examples:
            Local to remote (source defaults to self._backend)::

                manager.transfer(stored_file, remote_backend, session=session)

            Remote to local (pull for processing)::

                manager.transfer(stored_file, local_backend,
                    source_backend=remote_backend, session=session)

            Cross-server with different retention::

                manager.transfer(stored_file, archive_backend,
                    source_backend=prod_backend,
                    retention_category=FileRetention.REGULATORY_10Y,
                    session=session)
        """
        resolved_source = source_backend or self._backend
        source_relative_path = Path(str(stored_file.stored_path))
        target_absolute_path = target_backend.root / source_relative_path

        if not target_backend.exists(source_relative_path):
            content = resolved_source.retrieve(source_relative_path)
            target_absolute_path = target_backend.store(content, source_relative_path)

        target_retention = retention_category if retention_category is not None else int(stored_file.retention_category)  # type: ignore[arg-type]
        target_expiration = (
            self._compute_expiration_date(target_retention, session)
            if retention_category is not None
            else stored_file.expiration_date
        )

        if delete_source:
            resolved_source.delete(source_relative_path)

            # Check if the target already has a row (e.g., from a previous move or copy)
            existing_on_target = select(StoredFile).where(
                StoredFile.app_id == str(stored_file.app_id),
                StoredFile.content_hash == str(stored_file.content_hash),
                StoredFile.location == target_backend.location_name,
            ).limit(1)
            target_row = self._database.query(existing_on_target, session).scalars().first()

            if target_row is not None:
                # Target row exists -- delete the source row instead of updating
                session.delete(stored_file)
                session.flush()
                self._logger.info(
                    f"Moved: {source_relative_path} -> {target_backend.location_name} "
                    f"(source row deleted, target row exists)",
                )
            else:
                # No target row -- update the source row to point to the target
                stored_file.location = target_backend.location_name  # type: ignore[assignment]
                if retention_category is not None:
                    stored_file.retention_category = target_retention  # type: ignore[assignment]
                    stored_file.expiration_date = target_expiration  # type: ignore[assignment]
                session.flush()
                self._logger.info(
                    f"Moved: {source_relative_path} -> {target_backend.location_name}",
                )
        else:
            # Check if a copy already exists on the target backend
            existing_copy = select(StoredFile).where(
                StoredFile.app_id == str(stored_file.app_id),
                StoredFile.content_hash == str(stored_file.content_hash),
                StoredFile.location == target_backend.location_name,
            ).limit(1)
            if self._database.query(existing_copy, session).scalars().first() is not None:
                self._logger.debug(
                    f"Copy already exists on {target_backend.location_name}: {source_relative_path}",
                )
            else:
                copy_record = StoredFile(
                    app_id=str(stored_file.app_id),
                    runtime=str(stored_file.runtime),
                    content_hash=str(stored_file.content_hash),
                    original_filename=str(stored_file.original_filename),
                    stored_path=str(stored_file.stored_path),
                    file_extension=str(stored_file.file_extension),
                    file_size=int(stored_file.file_size),  # type: ignore[arg-type]
                    location=target_backend.location_name,
                    retention_category=target_retention,
                    expiration_date=target_expiration,
                )
                self._database.add(copy_record, session)
                self._logger.info(
                    f"Copied: {source_relative_path} -> {target_backend.location_name}",
                )

        return target_absolute_path

    @classmethod
    def resolve_backend(
        cls,
        database: Database,
        location_name: str,
        session: Session,
        *,
        logger: logging.Logger | None = None,
    ) -> FilesystemBackend:
        """Create a FilesystemBackend from a ``StorageBackend`` database row.

        Looks up the named backend in the ``storage_backend`` configuration
        table and returns a ``FilesystemBackend`` configured with the stored
        root path.

        Args:
            database: Database instance for the lookup query.
            location_name: Backend identifier to look up (e.g., ``"fs_market_hr"``).
            session: Active SQLAlchemy session.
            logger: Optional logger for the created backend.

        Returns:
            FilesystemBackend configured from the database row.

        Raises:
            ValueError: If no active backend with that name exists.
        """
        statement = select(StorageBackend).where(
            StorageBackend.location_name == location_name,
        )
        backend_row = database.query(statement, session).scalars().first()

        if backend_row is None:
            message = f"No storage backend found with location_name='{location_name}'"
            raise ValueError(message)

        if not backend_row.is_active:
            message = f"Storage backend '{location_name}' is inactive"
            raise ValueError(message)

        return FilesystemBackend(
            root=Path(str(backend_row.root_path)),
            location=str(backend_row.location_name),
            logger=logger,
        )

    @staticmethod
    def compute_content_hash(content: bytes) -> str:
        """Compute the SHA-256 hex digest of file content.

        Args:
            content: Raw file bytes.

        Returns:
            64-character lowercase hexadecimal string.
        """
        return hashlib.sha256(content).hexdigest()

    def get_stored_files(
        self,
        *,
        session: Session,
        date: str | None = None,
    ) -> list[StoredFile]:
        """List files tracked in the database for this application.

        Args:
            session: Active SQLAlchemy session.
            date: Optional date filter in ``YYYY-MM-DD`` format.

        Returns:
            List of ``StoredFile`` records.
        """
        statement = select(StoredFile).where(StoredFile.app_id == self._app_id)

        if date is not None:
            filter_date = datetime.strptime(date, "%Y-%m-%d").replace(tzinfo=UTC)
            next_day = filter_date + timedelta(days=1)
            statement = statement.where(
                StoredFile.date_created >= filter_date,
                StoredFile.date_created < next_day,
            )

        result = self._database.query(statement, session)
        return list(result.scalars().all())

    def get_storage_size(self, *, session: Session) -> int:
        """Return total storage size in bytes for this application.

        Args:
            session: Active SQLAlchemy session.

        Returns:
            Total file size in bytes, or 0 if no files are tracked.
        """
        statement = (
            select(func.coalesce(func.sum(StoredFile.file_size), 0))
            .where(
                StoredFile.app_id == self._app_id,
                StoredFile.location == self._backend.location_name,
            )
        )
        result = self._database.query(statement, session).scalar_one()
        return int(result)

    def enforce_retention(self, *, session: Session) -> int:
        """Enforce retention policy for this application's files.

        Delegates to :func:`enforce_retention_by_database` for
        database-driven retention.

        Args:
            session: Active SQLAlchemy session.

        Returns:
            Number of files deleted.
        """
        return enforce_retention_by_database(
            self._database,
            self._backend,
            logger=self._logger,
        )

    def _find_duplicate(self, content_hash: str, session: Session) -> Path | None:
        """Check the database for an existing file with the same content hash on this backend.

        Only matches files on the **current** backend's location.  If the
        same content exists on a different backend, it is not considered a
        duplicate -- the caller must store it on this backend separately
        (each location tracks its own ``StoredFile`` row).

        Args:
            content_hash: SHA-256 hex digest to search for.
            session: Active SQLAlchemy session.

        Returns:
            Absolute path of the existing file on this backend, or
            ``None`` if no duplicate exists on this backend.
        """
        statement = select(StoredFile).where(
            StoredFile.app_id == self._app_id,
            StoredFile.content_hash == content_hash,
            StoredFile.location == self._backend.location_name,
        ).limit(1)
        existing = self._database.query(statement, session).scalars().first()
        if existing is not None:
            return self._backend.root / Path(str(existing.stored_path))
        return None

    def _build_relative_path(self, content_hash: str, extension: str) -> Path:
        """Build the relative storage path for a file.

        Structure: ``{group}/{parent}/{app}/{ext}/{date}/{hash}.{ext}``

        The extension folder groups files by type.  The date folder
        granularity is controlled by ``directory_depth`` (flat, yearly,
        monthly, daily, hourly).

        Args:
            content_hash: SHA-256 hex digest used as the filename.
            extension: File extension without leading dot.

        Returns:
            Relative path from the backend root.
        """
        filename = f"{content_hash}.{extension}"
        parts = self._app_relative_root / extension
        date_format = _DATE_FORMATS.get(self._settings.directory_depth, _DATE_FORMATS["daily"])
        if date_format:
            parts = parts / datetime.now(UTC).strftime(date_format)
        return parts / filename

    def _compute_expiration_date(self, retention_category: int, session: Session) -> datetime | None:
        """Compute the expiration date from the codebook retention_days.

        Queries ``CodebookFileRetention`` for the ``retention_days`` value
        of the given category.  Results are cached per instance to avoid
        repeated database queries.

        Args:
            retention_category: Retention category ID.
            session: Active SQLAlchemy session.

        Returns:
            Expiration datetime, or ``None`` for permanent files.
        """
        retention_days = self._get_retention_days(retention_category, session)
        if retention_days is None:
            return None
        return datetime.now(UTC) + timedelta(days=retention_days)

    def _get_retention_days(self, retention_category: int, session: Session) -> int | None:
        """Look up retention_days from the CodebookFileRetention table.

        Results are cached per ``StorageManager`` instance to avoid
        repeated queries for the same category within a session.

        Args:
            retention_category: Retention category ID.
            session: Active SQLAlchemy session.

        Returns:
            Number of days to retain, or ``None`` for permanent.
        """
        if retention_category in self._retention_cache:
            return self._retention_cache[retention_category]

        # Query the full row to distinguish "category exists with NULL days"
        # (PERMANENT) from "category does not exist" (misconfiguration).
        statement = select(CodebookFileRetention).where(
            CodebookFileRetention.id == retention_category,
        )
        row = self._database.query(statement, session).scalars().first()
        if row is None:
            self._logger.warning(
                f"Retention category {retention_category} not found in codebook, "
                f"treating as permanent (no expiration)",
            )
            self._retention_cache[retention_category] = None
            return None

        retention_days: int | None = int(row.retention_days) if row.retention_days is not None else None  # type: ignore[arg-type]
        self._retention_cache[retention_category] = retention_days
        return retention_days
