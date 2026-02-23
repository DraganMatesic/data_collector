"""Table deployment and seed-data utilities for framework codebooks."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from data_collector.enums import AlertSeverity, CmdFlag, CmdName, FatalFlag, LogLevel, RunStatus, RuntimeExitCode
from data_collector.settings.main import MainDatabaseSettings
from data_collector.tables.apps import CodebookCommandFlags, CodebookCommandList, CodebookFatalFlags, CodebookRunStatus
from data_collector.tables.log import CodebookLogLevel
from data_collector.tables.notifications import CodebookAlertSeverity
from data_collector.tables.runtime import CodebookRuntimeCodes
from data_collector.tables.shared import Base
from data_collector.utilities.database.main import Database
from data_collector.utilities.functions.runtime import bulk_hash


@dataclass
class SeedData:
    """Seed payload + label pair."""

    data: list[Any]
    data_label: str


class Deploy:
    """Create/drop framework tables and seed codebooks."""

    def __init__(self) -> None:
        self.database = Database(MainDatabaseSettings())
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)

    def create_tables(self) -> None:
        """Create tables defined in `Base` metadata."""
        Base.metadata.create_all(self.database.engine)

    def drop_tables(self) -> None:
        """Drop tables defined in `Base` metadata."""
        Base.metadata.drop_all(self.database.engine)

    def recreate_tables(self) -> None:
        """Drop then create framework tables."""
        self.drop_tables()
        self.create_tables()

    def populate_tables(self) -> None:
        """Insert/update codebook seed rows using SHA merge flow."""
        with self.database.create_session() as session:
            seed_data: list[SeedData] = []

            cmd_flags = [
                CodebookCommandFlags(id=CmdFlag.PENDING.value, description="Command pending"),
                CodebookCommandFlags(id=CmdFlag.EXECUTED.value, description="Command Executed"),
                CodebookCommandFlags(
                    id=CmdFlag.NOT_EXECUTED.value,
                    description="Command not executed, conditions not meet",
                ),
            ]
            seed_data.append(SeedData(data=cmd_flags, data_label="cmd_flags"))

            cmd_list = [
                CodebookCommandList(id=CmdName.START.value, name="start", description="Start app"),
                CodebookCommandList(id=CmdName.STOP.value, name="stop", description="Stop app"),
                CodebookCommandList(id=CmdName.RESTART.value, name="restart", description="Restart app"),
                CodebookCommandList(id=CmdName.ENABLE.value, name="enable", description="Enable app"),
                CodebookCommandList(id=CmdName.DISABLE.value, name="disable", description="Disable app"),
            ]
            seed_data.append(SeedData(data=cmd_list, data_label="cmd_list"))

            fatal_flags = [
                CodebookFatalFlags(id=FatalFlag.FAILED_TO_START.value, description="Failed to start"),
                CodebookFatalFlags(id=FatalFlag.APP_STOPPED_ALERT_SENT.value, description="App stopped, alert sent"),
                CodebookFatalFlags(id=FatalFlag.UNEXPECTED_BEHAVIOUR.value, description="Unexpected behaviour"),
            ]
            seed_data.append(SeedData(data=fatal_flags, data_label="fatal_flags"))

            run_status = [
                CodebookRunStatus(id=RunStatus.NOT_RUNNING.value, description="App not running"),
                CodebookRunStatus(id=RunStatus.RUNNING.value, description="App is running"),
                CodebookRunStatus(
                    id=RunStatus.STOPPED.value,
                    description="App is stopped. Send command start or restart to start again.",
                ),
            ]
            seed_data.append(SeedData(data=run_status, data_label="run_status"))

            log_levels = [CodebookLogLevel(id=member.value, description=member.name) for member in LogLevel]
            seed_data.append(SeedData(data=log_levels, data_label="log_level"))

            runtime_codes = [
                CodebookRuntimeCodes(id=member.value, description=member.name)
                for member in RuntimeExitCode
            ]
            seed_data.append(SeedData(data=runtime_codes, data_label="runtime_codes"))

            alert_severity = [
                CodebookAlertSeverity(id=member.value, description=member.name)
                for member in AlertSeverity
            ]
            seed_data.append(SeedData(data=alert_severity, data_label="alert_severity"))

            for seed in seed_data:
                try:
                    bulk_hash(seed.data)
                    self.database.merge(seed.data, session)
                except Exception as exc:
                    self.logger.error("Failed to populate %s: %s", seed.data_label, exc)
