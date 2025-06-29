import logging
from typing import Dict, List
from dataclasses import dataclass, field
from data_collector.tables.shared import Base
from data_collector.utilities.database.main import Database
from data_collector.settings.main import MainDatabaseSettings

# App Codebooks
from data_collector.tables.apps import (CodebookCommandFlags,
                                        CodebookCommandList,
                                        CodebookFatalFlags,
                                        CodebookRunStatus)

# App Enum classes
from data_collector.tables.apps import (CommandFlag,
                                        CommandList,
                                        FatalFlag,
                                        RunStatus)

# Logging Codebooks
from data_collector.tables.log import (CodebookLogLevel)


@dataclass
class SeedData:
    data: list
    data_label: str
    compare_key: list = field(default_factory=lambda: ['id', 'description'])


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
            # Populate data for cmd flags in manager.py
            cmd_flags = [CodebookCommandFlags(id=CommandFlag.PENDING.value, description="Command pending"),
                         CodebookCommandFlags(id=CommandFlag.EXECUTED.value, description="Command Executed"),
                         CodebookCommandFlags(id=CommandFlag.NOT_EXECUTED.value, description="Command not executed, conditions not meet")]
            seed_data.append(SeedData(data=cmd_flags, data_label="cmd_flags"))


            # Populate data for command list in manager.py
            cmd_list = [CodebookCommandList(id=CommandList.START.value, name="start", description="Start app"),
                        CodebookCommandList(id=CommandList.STOP.value, name="stop", description="Stop app"),
                        CodebookCommandList(id=CommandList.RESTART.value, name="restart", description="Restart app"),
                        CodebookCommandList(id=CommandList.ENABLE.value, name="enable", description="Enable app"),
                        CodebookCommandList(id=CommandList.DISABLE.value, name="disable", description="Disable app"),
                        ]
            seed_data.append(SeedData(data=cmd_list, data_label="cmd_list", compare_key=['id', 'name', 'description']))

            # Populate data for fatal flags in manager.py
            fatal_flags = [CodebookFatalFlags(id=FatalFlag.FAILED_TO_START.value, description="Failed to start"),
                           CodebookFatalFlags(id=FatalFlag.ALERT_SENT.value, description="App stopped, alert sent"),
                           CodebookFatalFlags(id=FatalFlag.UNEXPECTED_BEHAVIOR.value, description="Unexpected behaviour"),
                           ]
            seed_data.append(SeedData(data=fatal_flags, data_label="fatal_flags"))

            run_status = [CodebookRunStatus(id=RunStatus.NOT_RUNNING.value, description="App not running"),
                          CodebookRunStatus(id=RunStatus.RUNNING.value, description="App is running"),
                          CodebookRunStatus(id=RunStatus.STOPPED.value, description="App is stopped. Send command start or restart to start again."),
                          ]
            seed_data.append(SeedData(data=run_status, data_label="run_status"))

            # Populate data for logging levels
            log_level_names: Dict[str, int] = logging.getLevelNamesMapping()
            log_level_unique: Dict[int, str] = {v:k for k,v in log_level_names.items()}
            log_level_mapping: List[CodebookLogLevel] = [CodebookLogLevel(id=k, description=v)
                                                         for k, v in log_level_unique.items()]
            seed_data.append(SeedData(data=log_level_mapping, data_label="log_level"))

            # Perform insertion/update of all append codebooks to SeedData class
            for sd in seed_data:
                try:
                    self.database.merge(sd.data, session, delete=True, compare_key=sd.compare_key)
                except Exception as e:
                    self.logger.error(f"Failed to populate {sd.data_label}: {e}")
