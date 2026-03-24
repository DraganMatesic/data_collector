"""Logging ORM tables and related codebooks."""


from sqlalchemy import BigInteger, Boolean, Column, DateTime, ForeignKey, Integer, String, UnicodeText, func, text

from data_collector.tables.apps import AppFunctions, Apps
from data_collector.tables.runtime import Runtime
from data_collector.tables.shared import Base
from data_collector.utilities.database.columns import auto_increment_column


class CodebookLogLevel(Base):
    """
    Codebook for log levels
    """
    __tablename__ = 'c_log_level'
    id = Column(BigInteger, primary_key=True, comment="log level ID")
    description = Column(String(128), comment="Log level description")
    sha = Column(String(64), comment="Hash for merge-based seeding")
    archive = Column(DateTime(timezone=True), comment="Soft delete timestamp")
    date_created = Column(DateTime(timezone=True), server_default=func.now())


class Logs(Base):
    """Application log records persisted in database."""

    __tablename__ = 'logs'

    id = auto_increment_column()
    app_id = Column(
        String(length=64),
        ForeignKey(Apps.app, ondelete="CASCADE"),
        index=True,
        doc="App id mapped in apps table. Root caller of all logs."
    )

    module_name = Column(String(length=256), doc="Module name (.py) from where logging is coming")
    module_path = Column(UnicodeText, doc="Module path from where logging is coming.")
    function_name = Column(String(length=256), doc="Function name from where logging is coming")
    function_id = Column(
        String(length=64),
        ForeignKey(AppFunctions.function_hash, ondelete="CASCADE"),
        index=True,
        doc="SHA3-256 hash of app_id + function_name, references app_functions.function_hash",
    )


    call_chain = Column(UnicodeText, doc="Contains call chain from root caller to actual logging caller")
    thread_id = Column(BigInteger, doc="OS thread ID from threading.get_ident(), auto-bound by @fun_watch")
    lineno = Column(Integer, doc="Indicates line number where logging function was called")

    log_level= Column(Integer,
                      ForeignKey(CodebookLogLevel.id, ondelete="CASCADE"),
                      index=True,
                      doc="Default numeric python logging levels")

    msg = Column(UnicodeText, doc="Logging message that is emitted")
    context_json = Column(UnicodeText, nullable=True, doc="Arbitrary structured context from structured logging")
    runtime = Column(String(length=64), ForeignKey(Runtime.runtime, ondelete="CASCADE"), index=True)
    date_created = Column(DateTime(timezone=True), server_default=func.now())


class FunctionLog(Base):
    """Per-invocation execution metrics recorded by @fun_watch."""

    __tablename__ = "function_log"

    id = auto_increment_column()
    function_hash = Column(
        String(64),
        ForeignKey(AppFunctions.function_hash, ondelete="CASCADE"),
        index=True,
    )
    execution_order = Column(BigInteger, nullable=False)
    thread_execution_order = Column(BigInteger, nullable=False, server_default=text("0"))
    log_role = Column(String(16), nullable=False, server_default=text("'single'"))
    parent_log_id = Column(BigInteger)
    main_app = Column(String(64), index=True)
    app_id = Column(String(64), index=True)
    thread_id = Column(BigInteger)
    task_size = Column(BigInteger)
    solved = Column(Integer, server_default=text("0"))
    failed = Column(Integer, server_default=text("0"))
    processed_count = Column(BigInteger, nullable=False, server_default=text("0"))
    is_success = Column(Boolean, nullable=False, server_default=text("true"))
    start_time = Column(DateTime(timezone=True))
    end_time = Column(DateTime(timezone=True))
    totals = Column(Integer)
    totalm = Column(Integer)
    totalh = Column(Integer)
    runtime = Column(
        String(64),
        ForeignKey(Runtime.runtime, ondelete="CASCADE"),
        index=True,
    )
    date_created = Column(DateTime(timezone=True), server_default=func.now())


class FunctionLogError(Base):
    """Error details for failed @fun_watch invocations."""

    __tablename__ = "function_log_error"

    id = auto_increment_column()
    function_log_id = Column(
        BigInteger,
        ForeignKey(FunctionLog.id, ondelete="CASCADE"),
        nullable=False,
        unique=True,
        index=True,
    )
    error_type = Column(String(256), doc="Exception class name")
    error_message = Column(UnicodeText, doc="Exception message string")
    item_error_count = Column(
        Integer, nullable=False, server_default=text("0"),
        doc="Count of items with typed errors via mark_failed(error_type=...)",
    )
    item_error_types_json = Column(UnicodeText, doc="JSON: error type -> count mapping")
    item_error_samples_json = Column(UnicodeText, doc="JSON: error type -> sample messages (max 5 per type)")
    date_created = Column(DateTime(timezone=True), server_default=func.now())
