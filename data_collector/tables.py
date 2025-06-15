import logging

from sqlalchemy import (
    Column, String, BigInteger, ForeignKey, ForeignKeyConstraint,
    Boolean, DateTime, Integer, Text, ARRAY, text, func, Identity,
    Date
)

from enum import IntEnum
from dataclasses import dataclass, field
from sqlalchemy.orm import (declarative_base, relationship)
from data_collector.utilities.database import main
from data_collector.settings.main import MainDatabaseSettings

# Base class for all ORM models
Base = declarative_base(cls=main.BaseModel)


class RunStatus(IntEnum):
    NOT_RUNNING = 0
    RUNNING = 1
    STOPPED = 2


class CommandFlag(IntEnum):
    PENDING = 0
    EXECUTED = 1
    NOT_EXECUTED = 2


class CommandList(IntEnum):
    START = 1
    STOP = 2
    RESTART = 3
    ENABLE = 4
    DISABLE = 5


class FatalFlag(IntEnum):
    FAILED_TO_START = 1
    ALERT_SENT = 2
    UNEXPECTED_BEHAVIOR = 3


@dataclass
class SeedData:
    data: list
    data_label: str
    compare_key: list = field(default_factory=lambda: ['id', 'description'])


class CodebookCommandFlags(Base):
    """
    Codebook for command flags
    """
    __tablename__ = 'c_cmd_flags'
    id = Column(BigInteger, primary_key=True, comment="Command flag ID")
    description = Column(String(128), comment="Command flag description")


class CodebookCommandList(Base):
    """
    Codebook for command list that are in use
    """
    __tablename__ = 'c_cmd_list'
    id = Column(BigInteger, primary_key=True, comment="Command ID")
    name = Column(String(10), unique=True, comment="Name of command")
    description = Column(String(128), comment="Command description")


class CodebookFatalFlags(Base):
    """
    Codebook for fatal flags
    """
    __tablename__ = 'c_fatal_flags'
    id = Column(BigInteger, primary_key=True, comment="Fatal flag ID")
    description = Column(String(128), comment="Fatal flag description")


class CodebookRunStatus(Base):
    """
    Codebook for app run status
    """
    __tablename__ = 'c_run_status'
    id = Column(BigInteger, primary_key=True, comment="Run status ID")
    description = Column(String(128), comment="Run status description")


class AppGroups(Base):
    """
    Table contains logical grouping for applications.
    """
    __tablename__ = 'app_groups'

    # Auto-incrementing primary key using a database sequence
    id = Column(BigInteger, Identity(), primary_key=True)

    # Unique name of the group
    name = Column(String(256), unique=True)


class AppParents(Base):
    """
    Table represents a parent grouping within an AppGroup (e.g., services in a group).
    """
    __tablename__ = 'app_parents'

    # Auto-incrementing ID
    id = Column(BigInteger, Identity())

    parent = Column(String(64), unique=True, comment="Parent identifier (unique across all) - hash value")
    name = Column(String(256), primary_key=True, comment="Name and group together form a composite primary key")
    group_name = Column(String, ForeignKey('app_groups.name', ondelete="CASCADE"), index=True, primary_key=True,
                        comment="References AppGroups.name; cascades on delete")


class Apps(Base):
    """
    Core table tracking individual application configurations and runtime states.
    """
    __tablename__ = 'apps'

    # Auto-incrementing ID
    id = Column(BigInteger, Identity())

    # Application identifier unique hash value
    app = Column(String(64), unique=True, index=True, nullable=False)

    # Composite primary key: defines path from root to app (group > parent > app)
    group_name = Column(String(256), primary_key=True)
    parent_name = Column(String(256), primary_key=True)
    app_name = Column(String(256), primary_key=True)

    # Links to AppParents.parent; cascades on delete
    parent_id = Column(String, ForeignKey('app_parents.parent', ondelete="CASCADE"), index=True)

    # Scheduling fields
    last_run = Column(DateTime, comment="Timestamp of last app run")
    next_run = Column(DateTime, comment="Timestamp of next app run")

    # Runtime data
    app_pids = Column(ARRAY(Integer), comment="List of process IDs connected to app")
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

    # Composite foreign key: ensures group/parent match in AppParents
    __table_args__ = (
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
    id = Column(BigInteger, Identity(), primary_key=True)

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


class ExampleTable(Base):
    """
    Used for examples described in documentation
    """
    __tablename__ = 'example_table'

    # Auto-incrementing ID
    id = Column(BigInteger, Identity(), primary_key=True)
    company_id = Column(Integer)
    person_id = Column(Integer)
    name = Column(String(15))
    surname = Column(String(25))
    birth_date = Column(Date)
    sha = Column(String(64))
    archive = Column(DateTime, comment="data and time this database object was removed from usage")
    date_created = Column(DateTime, server_default=func.now())  # DateCreated


def create_tables():
    """
    Create or recreate table that manager.py depends on
    """
    Base.metadata.drop_all(database.engine)
    Base.metadata.create_all(database.engine)



def populate_tables():
    """
    Populate codebook data to created tables
    """
    with database.create_session() as session:
        seed_data = list()
        # populate data for cmd flags in manager.py
        cmd_flags = [CodebookCommandFlags(id=CommandFlag.PENDING, description="Command pending"),
                     CodebookCommandFlags(id=CommandFlag.EXECUTED, description="Command Executed"),
                     CodebookCommandFlags(id=CommandFlag.NOT_EXECUTED, description="Command not executed, conditions not meet")]
        seed_data.append(SeedData(data=cmd_flags, data_label="cmd_flags"))


        # populate data for command list in manager.py
        cmd_list = [CodebookCommandList(id=CommandList.START, name="start", description="Start app"),
                    CodebookCommandList(id=CommandList.STOP, name="stop", description="Stop app"),
                    CodebookCommandList(id=CommandList.RESTART, name="restart", description="Restart app"),
                    CodebookCommandList(id=CommandList.ENABLE, name="enable", description="Enable app"),
                    CodebookCommandList(id=CommandList.DISABLE, name="disable", description="Disable app"),
                    ]
        seed_data.append(SeedData(data=cmd_list, data_label="cmd_list", compare_key=['id', 'name', 'description']))

        # populate data for fatal flags in manager.py
        fatal_flags = [CodebookFatalFlags(id=FatalFlag.FAILED_TO_START, description="Failed to start"),
                       CodebookFatalFlags(id=FatalFlag.ALERT_SENT, description="App stopped, alert sent"),
                       CodebookFatalFlags(id=FatalFlag.UNEXPECTED_BEHAVIOR, description="Unexpected behaviour"),
                       ]
        seed_data.append(SeedData(data=fatal_flags, data_label="fatal_flags"))

        run_status = [CodebookRunStatus(id=RunStatus.NOT_RUNNING, description="App not running"),
                      CodebookRunStatus(id=RunStatus.RUNNING, description="App is running"),
                      CodebookRunStatus(id=RunStatus.STOPPED, description="App is stopped. Send command start or restart to start again."),
                      ]
        seed_data.append(SeedData(data=run_status, data_label="run_status"))

        for sd in seed_data:
            try:
                database.merge(sd.data, session, delete=True, compare_key=sd.compare_key)
            except Exception as e:
                logger.error(f"Failed to populate {sd.data_label}: {e}")


if __name__ == '__main__':
    # used in development environment
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    database = main.Database(MainDatabaseSettings())

    # todo add in manager.py to check if tables are created and if not to create them before populating with data
    create_tables()
    populate_tables()




