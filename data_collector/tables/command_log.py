"""Command audit trail ORM model."""

from sqlalchemy import Column, DateTime, Integer, String, func, text

from data_collector.tables.shared import Base
from data_collector.utilities.database.columns import auto_increment_column


class CommandLog(Base):
    """Audit trail for commands dispatched to the orchestration manager.

    Records who issued each command, when it was issued, and when it was
    executed.  Unlike the ``cmd_*`` columns on :class:`Apps` (which store
    only the *latest* command), this table preserves the full history.
    """

    __tablename__ = "command_log"

    id = auto_increment_column()
    app_id = Column(String(64), index=True, nullable=False, comment="Target application identifier")
    cmd_by = Column(String(128), comment="User or system that issued the command")
    cmd_name = Column(String(128), comment="Command name (e.g. START, STOP)")
    cmd_time = Column(DateTime, comment="When the command was received")
    cmd_flag = Column(Integer, server_default=text("0"), comment="Command status (CmdFlag enum)")
    cmd_exec = Column(DateTime, comment="When the command was executed")
    date_created = Column(DateTime, server_default=func.now())
