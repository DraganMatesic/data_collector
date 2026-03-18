"""Pipeline ORM tables for event-driven task processing.

Events captures inbound work items from any producer (WatchService, scrapers,
API endpoints, manual INSERT).  EventProcessingStatus tracks which events have
been dispatched to Dramatiq actors, preventing double-dispatch via a unique
constraint.  PipelineTask provides full lifecycle tracking of a document as it
moves through pipeline stages.  DeadLetter stores messages that exhausted all
retry attempts for manual inspection and reprocessing.

CodebookPipelineStatus and CodebookPipelineStage are standard codebook tables
seeded from the corresponding IntEnums in ``data_collector.enums.pipeline``.
"""

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    UnicodeText,
    UniqueConstraint,
    text,
)
from sqlalchemy import false as sa_false
from sqlalchemy import true as sa_true
from sqlalchemy.sql import func

from data_collector.tables.shared import Base
from data_collector.utilities.database.columns import auto_increment_column


class CodebookPipelineStatus(Base):
    """Codebook for pipeline task execution states."""

    __tablename__ = "c_pipeline_status"
    id = Column(BigInteger, primary_key=True, comment="Pipeline status ID")
    description = Column(String(128), comment="Status description")
    sha = Column(String(64), comment="Hash for merge-based seeding")
    archive = Column(DateTime(timezone=True), comment="Soft delete timestamp")
    date_created = Column(DateTime(timezone=True), server_default=func.now())


class CodebookPipelineStage(Base):
    """Codebook for pipeline processing stages."""

    __tablename__ = "c_pipeline_stage"
    id = Column(BigInteger, primary_key=True, comment="Pipeline stage ID")
    description = Column(String(128), comment="Stage description")
    sha = Column(String(64), comment="Hash for merge-based seeding")
    archive = Column(DateTime(timezone=True), comment="Soft delete timestamp")
    date_created = Column(DateTime(timezone=True), server_default=func.now())


class Events(Base):
    """Inbound event record for pipeline dispatch.

    Any producer (WatchService, scraper, API, manual INSERT) writes an event
    row.  The TaskDispatcher polls for unprocessed events and publishes
    Dramatiq messages to the correct exchange/queue based on ``worker_path``.
    """

    __tablename__ = "events"

    id = auto_increment_column()
    worker_path = Column(
        String(512),
        nullable=False,
        comment="Python module path for dynamic import of TopicExchangeQueue definition",
    )
    file_path = Column(UnicodeText, nullable=True, comment="Source document or file path")
    document_type = Column(String(128), nullable=True, index=True, comment="Document type identifier")
    metadata_json = Column(UnicodeText, nullable=True, comment="Arbitrary JSON payload for the event")
    event_type = Column(Integer, nullable=False, index=True, comment="EventType enum value (1=CREATED, 2=MODIFIED)")
    path_hash = Column(String(64), nullable=False, index=True, comment="SHA hash of normalized file path")
    country = Column(String(8), nullable=False, index=True, comment="Country code for pipeline routing")
    watch_group = Column(String(128), nullable=False, index=True, comment="Logical watch group (e.g. ocr, ingest)")
    file_size = Column(BigInteger, nullable=True, comment="Final file size in bytes (set on stabilization)")
    stable = Column(
        Boolean, nullable=True, server_default=sa_true(), comment="False while streaming, True when stable",
    )
    stabilized_date = Column(DateTime(timezone=True), nullable=True, comment="When file became stable")
    corrupted = Column(
        Boolean, nullable=True, server_default=sa_false(), comment="True if worker detected file corruption",
    )
    destination_path = Column(UnicodeText, nullable=True, comment="Permanent storage path after worker moves file")
    app_id = Column(String(64), index=True, nullable=True, comment="Producer application identifier")
    archive = Column(DateTime(timezone=True), nullable=True, comment="Soft delete timestamp")
    date_created = Column(DateTime(timezone=True), server_default=func.now())


class WatchRoots(Base):
    """Persistent configuration for watched directory roots.

    Each row defines a directory that WatchService monitors for new files.
    The ``worker_path`` links detected events to the Dramatiq actor module
    that processes them.  Rows with ``active=True`` and ``archive IS NULL``
    are loaded on startup and periodically refreshed at runtime.
    """

    __tablename__ = "watch_roots"

    id = auto_increment_column()
    root_path = Column(String(1024), nullable=False, comment="Absolute path to the watched directory")
    rel_path = Column(String(512), nullable=False, comment="Relative path identifier for routing")
    country = Column(String(8), nullable=False, index=True, comment="Country code for pipeline routing")
    watch_group = Column(String(128), nullable=False, index=True, comment="Logical watch group (e.g. ocr, ingest)")
    worker_path = Column(String(512), nullable=False, comment="Python module path for Dramatiq actor import")
    extensions = Column(
        UnicodeText, nullable=True, comment="JSON array of allowed extensions (e.g. [\".pdf\", \".zip\"])",
    )
    recursive = Column(Boolean, nullable=False, server_default=sa_true(), comment="Whether to watch subdirectories")
    active = Column(
        Boolean, nullable=False, server_default=sa_true(), comment="Whether this root is currently monitored",
    )
    archive = Column(DateTime(timezone=True), nullable=True, comment="Soft delete timestamp")
    date_created = Column(DateTime(timezone=True), server_default=func.now())


class EventProcessingStatus(Base):
    """Tracks dispatch status per event to prevent double-dispatch.

    A row is created when the TaskDispatcher successfully publishes a
    Dramatiq message for an event.  The unique constraint on
    ``(event_id, actor_name)`` ensures each event is dispatched to each
    actor at most once.
    """

    __tablename__ = "event_processing_status"

    id = auto_increment_column()
    event_id = Column(
        BigInteger,
        ForeignKey("events.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
        comment="Reference to the source event",
    )
    actor_name = Column(String(128), nullable=False, comment="Dramatiq actor that received this event")
    dispatched_date = Column(
        DateTime(timezone=True), nullable=False, comment="When the message was published to RabbitMQ",
    )
    date_created = Column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        UniqueConstraint("event_id", "actor_name", name="uq_event_processing_status_event_id_actor_name"),
    )


class PipelineTask(Base):
    """Tracks a document through pipeline stages.

    Each row represents one unit of work moving through a multi-stage
    pipeline.  The ``task_id`` is a SHA hash identifying the logical task,
    while ``current_stage`` and ``status`` track progress.  ``stage_history``
    stores a JSON array of completed stage transitions for auditability.
    """

    __tablename__ = "pipeline_task"

    id = auto_increment_column()
    task_id = Column(String(64), unique=True, nullable=False, index=True, comment="Unique task hash identifier")
    document_type = Column(String(128), nullable=True, index=True, comment="Document type being processed")
    source_path = Column(UnicodeText, nullable=True, comment="Source file or document path")
    current_stage = Column(
        Integer,
        ForeignKey("c_pipeline_stage.id", ondelete="SET NULL"),
        nullable=True,
        comment="Current pipeline stage (PipelineStage enum)",
    )
    status = Column(
        Integer,
        ForeignKey("c_pipeline_status.id", ondelete="SET NULL"),
        nullable=False,
        server_default=text("0"),
        comment="Current task status (PipelineStatus enum)",
    )
    stage_history = Column(UnicodeText, nullable=True, comment="JSON array of completed stage transitions")
    error_message = Column(UnicodeText, nullable=True, comment="Last error message if status is FAILED")
    error_stage = Column(String(64), nullable=True, comment="Stage where the error occurred")
    retry_count = Column(Integer, server_default=text("0"), nullable=False, comment="Number of retry attempts")
    worker_id = Column(String(128), nullable=True, comment="Dramatiq worker/actor identifier")
    app_id = Column(String(64), nullable=True, index=True, comment="Application that owns this task")
    runtime = Column(String(64), nullable=True, index=True, comment="Runtime identifier for the processing run")
    start_time = Column(DateTime(timezone=True), nullable=True, comment="When processing began")
    end_time = Column(DateTime(timezone=True), nullable=True, comment="When processing finished")
    archive = Column(DateTime(timezone=True), nullable=True, comment="Soft delete timestamp")
    date_created = Column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        Index("ix_pipeline_task_status", "status"),
    )


class DeadLetter(Base):
    """Failed Dramatiq messages stored for manual inspection and reprocessing.

    Messages that exhaust all retry attempts are forwarded to the dead-letter
    actor, which inserts a record here with the full payload, error details,
    and traceback.
    """

    __tablename__ = "dead_letter"

    id = auto_increment_column()
    queue_name = Column(String(256), nullable=True, comment="Original RabbitMQ queue name")
    actor_name = Column(String(128), nullable=True, comment="Dramatiq actor that failed")
    message_args = Column(UnicodeText, nullable=True, comment="JSON-serialized positional arguments")
    message_kwargs = Column(UnicodeText, nullable=True, comment="JSON-serialized keyword arguments")
    error_type = Column(String(256), nullable=True, comment="Exception class name")
    error_message = Column(UnicodeText, nullable=True, comment="Exception message")
    traceback = Column(UnicodeText, nullable=True, comment="Full traceback string")
    original_message_id = Column(String(128), nullable=True, comment="Dramatiq message ID")
    date_created = Column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        Index("ix_dead_letter_actor_name", "actor_name"),
    )


class RateLimiterState(Base):
    """Key-value state for Dramatiq rate limiters.

    Used by ``DatabaseRateLimiterBackend`` to implement
    ``ConcurrentRateLimiter`` (mutex, concurrency caps) and
    ``WindowRateLimiter`` (time-windowed request limits).  Each row
    represents a rate limiter key with an atomic integer value and an
    expiration timestamp for TTL-based cleanup.
    """

    __tablename__ = "rate_limiter_state"

    key = Column(String(256), primary_key=True, comment="Rate limiter key")
    value = Column(Integer, nullable=False, server_default=text("0"), comment="Current counter value")
    expiration_date = Column(DateTime(timezone=True), nullable=True, comment="Key expiration timestamp (UTC)")
