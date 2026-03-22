"""Storage codebook, file metadata, and backend configuration tables.

``CodebookFileRetention`` defines retention categories seeded from the
``FileRetention`` IntEnum.  Each category specifies a ``retention_days``
value (NULL for permanent files that are never deleted).

``StoredFile`` tracks every file managed by ``StorageManager``, enabling
centralized deduplication, retention enforcement, storage analytics, and
multi-location tracking across local and remote backends.

``StorageBackend`` is an operational configuration table that serves as a
central registry of named storage backends.  DBAs add or modify backends
at runtime without code changes.
"""

from sqlalchemy import BigInteger, Boolean, Column, DateTime, Float, ForeignKey, Index, String, UnicodeText, text
from sqlalchemy.sql import func

from data_collector.tables.shared import Base
from data_collector.utilities.database.columns import auto_increment_column


class CodebookFileRetention(Base):
    """Codebook for file retention categories.

    Seeded from ``data_collector.enums.storage.FileRetention``.  The
    ``retention_days`` column drives automated retention enforcement:
    files whose ``expiration_date`` (computed at store time from this value)
    has passed are deleted by ``enforce_retention_by_database()``.
    """

    __tablename__ = "c_file_retention"
    id = Column(BigInteger, primary_key=True, comment="Retention category ID from FileRetention enum")
    description = Column(String(128), comment="Retention category description")
    retention_days = Column(BigInteger, nullable=True, comment="Days to retain files, NULL for permanent")
    sha = Column(String(64), comment="Hash for merge-based seeding")
    archive = Column(DateTime(timezone=True), comment="Soft delete timestamp")
    date_created = Column(DateTime(timezone=True), server_default=func.now())


class StoredFile(Base):
    """Centralized file metadata for storage management.

    Each row represents one file managed by ``StorageManager``.  Files are
    named by their SHA-256 content hash (``{hash}.{ext}``), with the original
    filename preserved in ``original_filename`` for display purposes.

    The ``location`` column tracks which storage backend currently holds the
    file (e.g. ``"local"``, ``"fs_market_hr"``).  After a ``transfer()``
    call, ``location`` and ``stored_path`` are updated to reflect the new
    backend.

    Retention enforcement queries ``expiration_date`` (computed at store time
    from ``CodebookFileRetention.retention_days``).  Files with
    ``expiration_date=NULL`` (PERMANENT category) are never deleted.
    """

    __tablename__ = "stored_file"

    id = auto_increment_column()
    app_id = Column(
        String(64),
        index=True,
        nullable=False,
        comment="Application identifier (SHA-256 of group|parent|name)",
    )
    runtime = Column(
        String(64),
        index=True,
        nullable=False,
        comment="Runtime session identifier",
    )
    content_hash = Column(String(64), nullable=False, comment="SHA-256 hex digest of file content")
    original_filename = Column(UnicodeText, nullable=False, comment="Original filename before hash-based renaming")
    stored_path = Column(UnicodeText, nullable=False, comment="Relative path from backend root")
    file_extension = Column(String(32), comment="File extension without leading dot")
    file_size = Column(BigInteger, nullable=False, comment="File size in bytes")
    location = Column(
        String(256),
        nullable=False,
        server_default=text("'local'"),
        comment="Storage backend identifier (e.g. local, fs_market_hr)",
    )
    retention_category = Column(
        BigInteger,
        ForeignKey("c_file_retention.id", ondelete="RESTRICT"),
        nullable=False,
        comment="Retention category from CodebookFileRetention",
    )
    expiration_date = Column(
        DateTime(timezone=True),
        nullable=True,
        comment="Computed expiration timestamp, NULL for permanent files",
    )
    date_created = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    __table_args__ = (
        Index("ix_stored_file_content_hash", "content_hash"),
        Index("ix_stored_file_app_date", "app_id", "date_created"),
        Index("ix_stored_file_expiration", "expiration_date"),
        Index("ix_stored_file_location", "location"),
    )


class StorageBackend(Base):
    """Central registry of named storage backends.

    Operational configuration table (not a codebook).  DBAs add or modify
    backends at runtime without code changes or redeployment.  Each row
    defines a named backend with a root path that ``FilesystemBackend``
    uses to store and retrieve files.

    The ``location_name`` value is stored in ``StoredFile.location`` to
    track which backend holds each file.

    Examples:
        DBA adds a remote file server::

            INSERT INTO storage_backend (location_name, root_path, description)
            VALUES ('fs_market_hr', '\\\\fileserver\\hr_share', 'Croatia market file server');

        Application resolves the backend::

            manager = StorageManager(database, group, parent, app_name,
                runtime_id=rid, backend="fs_market_hr")
    """

    __tablename__ = "storage_backend"

    id = auto_increment_column()
    location_name = Column(
        String(128),
        unique=True,
        nullable=False,
        comment="Unique backend identifier (e.g. local, fs_market_hr)",
    )
    root_path = Column(
        UnicodeText,
        nullable=False,
        comment="Backend root directory (local path, UNC path, or mount point)",
    )
    description = Column(String(256), comment="Human-readable description of this backend")
    is_active = Column(
        Boolean,
        nullable=False,
        server_default=text("true"),
        comment="Whether this backend is available for use",
    )
    min_free_disk_gb: Column[float] = Column(
        Float,
        nullable=True,
        comment="Alert when disk free space drops below this (GB). NULL = use global default",
    )
    max_storage_alert_gb: Column[float] = Column(
        Float,
        nullable=True,
        comment="Alert when stored data exceeds this (GB). NULL = use global default or disabled",
    )
    date_created = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
