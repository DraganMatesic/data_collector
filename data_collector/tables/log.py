"""Logging ORM tables and related codebooks."""


from sqlalchemy import BigInteger, Column, DateTime, ForeignKey, Integer, String, Text, func, text

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
    archive = Column(DateTime, comment="Soft delete timestamp")
    date_created = Column(DateTime, server_default=func.now())


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
    module_path = Column(Text, doc="Module path from where logging is coming.")
    function_name = Column(String(length=256), doc="Function name from where logging is coming")
    function_id = Column(String(length=64), index=True,
                         doc="It is hashed value of app_id, module_name and function_name ")


    call_chain = Column(Text, doc="Contains call chain from root caller to actual logging caller")
    instance_id = Column(Integer, doc="ID of instance from what logging data in specified function is coming")
    lineno = Column(Integer, doc="Indicates line number where logging function was called")

    log_level= Column(Integer,
                      ForeignKey(CodebookLogLevel.id, ondelete="CASCADE"),
                      index=True,
                      doc="Default numeric python logging levels")

    msg = Column(Text, doc="Logging message that is emitted")
    runtime = Column(String(length=64), ForeignKey(Runtime.runtime, ondelete="CASCADE"), index=True)
    date_created = Column(DateTime, server_default=text("NOW()"))


class FunctionLog(Base):
    """Per-invocation execution metrics recorded by @fun_watch."""

    __tablename__ = "function_log"

    id = auto_increment_column()
    function_hash = Column(
        String(64),
        ForeignKey(AppFunctions.function_hash, ondelete="CASCADE"),
        index=True,
    )
    execution_order = Column(Integer)
    main_app = Column(String(64), index=True)
    app_id = Column(String(64), index=True)
    thread_id = Column(Integer)
    task_size = Column(BigInteger)
    solved = Column(Integer, server_default=text("0"))
    failed = Column(Integer, server_default=text("0"))
    start_time = Column(DateTime)
    end_time = Column(DateTime)
    totals = Column(Integer)
    totalm = Column(Integer)
    totalh = Column(Integer)
    runtime = Column(
        String(64),
        ForeignKey(Runtime.runtime, ondelete="CASCADE"),
        index=True,
    )
    sha = Column(String(64), index=True)
    archive = Column(DateTime, comment="Soft delete timestamp")
    date_created = Column(DateTime, server_default=func.now())
    date_modified = Column(DateTime)
