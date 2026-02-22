import logging
from dataclasses import dataclass
from data_collector.tables.shared import Base
from data_collector.utilities.database.main import Database
from data_collector.utilities.functions.runtime import bulk_hash
from data_collector.settings.main import MainDatabaseSettings

# App Codebooks
from data_collector.tables.apps import (CodebookCommandFlags,
                                        CodebookCommandList,
                                        CodebookFatalFlags,
                                        CodebookRunStatus)

# Logging Codebooks
from data_collector.tables.log import CodebookLogLevel

# Runtime Codebooks
from data_collector.tables.runtime import CodebookRuntimeCodes

# Notification Codebooks
from data_collector.tables.notifications import CodebookAlertSeverity

# Enum classes
from data_collector.enums import (CmdFlag,
                                  CmdName,
                                  FatalFlag,
                                  RunStatus,
                                  LogLevel,
                                  RuntimeExitCode,
                                  AlertSeverity)


@dataclass
class SeedData:
    data: list
    data_label: str


class Deploy:
    def __init__(self):
        self.database = Database(MainDatabaseSettings())
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)


    def create_tables(self):
        """
        Create tables defined in Base metadata
        """
        Base.metadata.create_all(self.database.engine)

    def drop_tables(self):
        """
        Drops tables defined in Base metadata
        """
        Base.metadata.drop_all(self.database.engine)

    def recreate_tables(self):
        """
        Drops and created tables defined in Base metadata
        """
        self.drop_tables()
        self.create_tables()

    def populate_tables(self):
        """
        Add or remove codebook data to created tables
        """
        with self.database.create_session() as session:
            seed_data = list()
            # Populate data for cmd flags
            cmd_flags = [CodebookCommandFlags(id=CmdFlag.PENDING.value, description="Command pending"),
                         CodebookCommandFlags(id=CmdFlag.EXECUTED.value, description="Command Executed"),
                         CodebookCommandFlags(id=CmdFlag.NOT_EXECUTED.value, description="Command not executed, conditions not meet")]
            seed_data.append(SeedData(data=cmd_flags, data_label="cmd_flags"))

            # Populate data for command list
            cmd_list = [CodebookCommandList(id=CmdName.START.value, name="start", description="Start app"),
                        CodebookCommandList(id=CmdName.STOP.value, name="stop", description="Stop app"),
                        CodebookCommandList(id=CmdName.RESTART.value, name="restart", description="Restart app"),
                        CodebookCommandList(id=CmdName.ENABLE.value, name="enable", description="Enable app"),
                        CodebookCommandList(id=CmdName.DISABLE.value, name="disable", description="Disable app"),
                        ]
            seed_data.append(SeedData(data=cmd_list, data_label="cmd_list"))

            # Populate data for fatal flags
            fatal_flags = [CodebookFatalFlags(id=FatalFlag.FAILED_TO_START.value, description="Failed to start"),
                           CodebookFatalFlags(id=FatalFlag.APP_STOPPED_ALERT_SENT.value, description="App stopped, alert sent"),
                           CodebookFatalFlags(id=FatalFlag.UNEXPECTED_BEHAVIOUR.value, description="Unexpected behaviour"),
                           ]
            seed_data.append(SeedData(data=fatal_flags, data_label="fatal_flags"))

            # Populate data for run status
            run_status = [CodebookRunStatus(id=RunStatus.NOT_RUNNING.value, description="App not running"),
                          CodebookRunStatus(id=RunStatus.RUNNING.value, description="App is running"),
                          CodebookRunStatus(id=RunStatus.STOPPED.value, description="App is stopped. Send command start or restart to start again."),
                          ]
            seed_data.append(SeedData(data=run_status, data_label="run_status"))

            # Populate data for logging levels
            log_levels = [CodebookLogLevel(id=member.value, description=member.name)
                          for member in LogLevel]
            seed_data.append(SeedData(data=log_levels, data_label="log_level"))

            # Populate data for runtime exit codes
            runtime_codes = [CodebookRuntimeCodes(id=member.value, description=member.name)
                             for member in RuntimeExitCode]
            seed_data.append(SeedData(data=runtime_codes, data_label="runtime_codes"))

            # Populate data for alert severity
            alert_severity = [CodebookAlertSeverity(id=member.value, description=member.name)
                              for member in AlertSeverity]
            seed_data.append(SeedData(data=alert_severity, data_label="alert_severity"))

            # Perform insertion/update of all codebooks via SHA-based merge
            for sd in seed_data:
                try:
                    bulk_hash(sd.data)
                    self.database.merge(sd.data, session)
                except Exception as e:
                    self.logger.error(f"Failed to populate {sd.data_label}: {e}")
