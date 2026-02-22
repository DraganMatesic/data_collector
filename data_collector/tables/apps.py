from sqlalchemy import (
    Column, String, BigInteger, ForeignKey, ForeignKeyConstraint,
    PrimaryKeyConstraint, Boolean, DateTime, Integer, Text, text,
    func
)


from sqlalchemy.orm import relationship
from data_collector.tables.shared import Base
from data_collector.utilities.database.main import auto_increment_column


class CodebookCommandFlags(Base):
    """
    Codebook for command flags
    """
    __tablename__ = 'c_cmd_flags'
    id = Column(BigInteger, primary_key=True, comment="Command flag ID")
    description = Column(String(128), comment="Command flag description")
    sha = Column(String(64), comment="Hash for merge-based seeding")
    archive = Column(DateTime, comment="Soft delete timestamp")
    date_created = Column(DateTime, server_default=func.now())


class CodebookCommandList(Base):
    """
    Codebook for command list that are in use
    """
    __tablename__ = 'c_cmd_list'
    id = Column(BigInteger, primary_key=True, comment="Command ID")
    name = Column(String(10), unique=True, comment="Name of command")
    description = Column(String(128), comment="Command description")
    sha = Column(String(64), comment="Hash for merge-based seeding")
    archive = Column(DateTime, comment="Soft delete timestamp")
    date_created = Column(DateTime, server_default=func.now())


class CodebookFatalFlags(Base):
    """
    Codebook for fatal flags
    """
    __tablename__ = 'c_fatal_flags'
    id = Column(BigInteger, primary_key=True, comment="Fatal flag ID")
    description = Column(String(128), comment="Fatal flag description")
    sha = Column(String(64), comment="Hash for merge-based seeding")
    archive = Column(DateTime, comment="Soft delete timestamp")
    date_created = Column(DateTime, server_default=func.now())


class CodebookRunStatus(Base):
    """
    Codebook for app run status
    """
    __tablename__ = 'c_run_status'
    id = Column(BigInteger, primary_key=True, comment="Run status ID")
    description = Column(String(128), comment="Run status description")
    sha = Column(String(64), comment="Hash for merge-based seeding")
    archive = Column(DateTime, comment="Soft delete timestamp")
    date_created = Column(DateTime, server_default=func.now())


class AppGroups(Base):
    """
    Table contains logical grouping for applications.
    """
    __tablename__ = 'app_groups'

    # Auto-incrementing primary key using a database sequence
    id = auto_increment_column()

    # Unique name of the group
    name = Column(String(256), unique=True)


class AppParents(Base):
    """
    Table represents a parent grouping within an AppGroup (e.g., services in a group).
    """
    __tablename__ = 'app_parents'

    # Auto-incrementing ID
    id = auto_increment_column(primary_key=False)

    parent = Column(String(64), unique=True, comment="Parent identifier (unique across all) - hash value")
    name = Column(String(256), primary_key=True, nullable=False)
    group_name = Column(String, ForeignKey(AppGroups.name, ondelete="CASCADE"), index=True,
                        primary_key=True, nullable=False)

    # Composite primary key ensures group/parent/app are unique
    __table_args__ = (
        PrimaryKeyConstraint("name", "group_name"),
    )


class Apps(Base):
    """
    Core table tracking individual application configurations and runtime states.
    """
    __tablename__ = 'apps'

    # Auto-incrementing ID
    id = auto_increment_column(primary_key=False)

    # Application identifier unique hash value
    app = Column(String(64), unique=True, index=True, nullable=False)

    # Composite primary key: defines path from root to app (group > parent > app)
    group_name = Column(String(256), primary_key=True, nullable=False)
    parent_name = Column(String(256), primary_key=True, nullable=False)
    app_name = Column(String(256), primary_key=True, nullable=False)

    # Links to AppParents.parent; cascades on delete
    parent_id = Column(String, ForeignKey(AppParents.parent, ondelete="CASCADE"), index=True)

    # Scheduling fields
    last_run = Column(DateTime, comment="Timestamp of last app run")
    next_run = Column(DateTime, comment="Timestamp of next app run")

    # Runtime data
    app_pids = Column(Text, comment="List of process IDs connected to app")
    run_status = Column(
        Integer,
        ForeignKey(CodebookRunStatus.id, ondelete="RESTRICT"),
        server_default=text("0"),
        comment="Run status of app. FK to c_run_status"
    )
    progress = Column(Text, comment="Optional progress text")

    # Fatal error tracking
    fatal_flag = Column(
        Integer,
        ForeignKey(CodebookFatalFlags.id, ondelete="RESTRICT"),
        server_default=text("0"),
        comment="Fatal error flag. FK to c_fatal_flags"
    )
    fatal_msg = Column(Text, comment="Reason why app is set to fatal status")
    fatal_time = Column(DateTime, comment="Time of last fatal flag")

    # Optional runtime ID linkage
    runtime_id = Column(String(64), comment="ID linking to runtime metadata")

    # Command handling metadata
    cmd_by = Column(String(128), comment="Last user who initiated the command on targeted app")
    cmd_name = Column(
        String(10),
        ForeignKey(CodebookCommandList.name, ondelete="RESTRICT"),
        comment="Name of the last command. FK to c_cmd_list.name"
    )
    cmd_name_obj = relationship(
        "CodebookCommandList",
        primaryjoin="Apps.cmd_name == CodebookCommandList.name",
        lazy="joined",
        uselist=False
    )
    cmd_time = Column(DateTime, comment="When command was issued")
    cmd_flag = Column(
        Integer,
        ForeignKey(CodebookCommandFlags.id, ondelete="RESTRICT"),
        server_default=text("0"),
        comment="Command status flag. FK to c_cmd_flags"
    )
    cmd_exec = Column(DateTime, comment="When command was executed")

    # Disable flag (default: true = disabled)
    disable = Column(Boolean, server_default=text("true"), comment="disable flag for app (default: true = disabled)")

    # Composite foreign key ensures group/parent match in AppParents
    # Composite primary key ensures group/parent/app are unique
    __table_args__ = (
        PrimaryKeyConstraint("group_name", "parent_name", "app_name"),
        ForeignKeyConstraint(
            ('group_name', 'parent_name'),
            ['app_parents.group_name', 'app_parents.name'],
            ondelete="CASCADE"
        ),
    )


class AppDbObjects(Base):
    """
    Contains mapped database objects that app uses
    """
    __tablename__ = 'app_db_objects'

    # Auto-incrementing ID
    id = auto_increment_column()

    # Application identifier unique hash value
    app_id = Column(String(64), index=True, nullable=False)

    # Data regarding db object
    server_type = Column(String(50))
    server_name  = Column(String(50))
    server_ip = Column(String(50))
    database_name = Column(String(50))
    database_schema = Column(String(50))
    object_name = Column(String(75))
    object_type = Column(String(20))
    last_use_date = Column(DateTime, comment="when the last time DDL action was done")
    sha = Column(String(64), nullable=False, index=True)

    archive = Column(DateTime, comment="data and time this database object was removed from usage")
    date_created = Column(DateTime, server_default=func.now())  # DateCreated
