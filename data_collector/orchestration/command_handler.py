"""Unified command dispatch for the orchestration manager.

Bridges two command sources into a single thread-safe queue:
- **Database polling:** reads ``cmd_flag=PENDING`` rows from the Apps table.
- **RabbitMQ consumer:** receives ``CommandMessage`` objects on a daemon thread.

The Manager drains the queue on each tick and executes commands sequentially.
"""

from __future__ import annotations

import logging
import queue
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select

from data_collector.enums import CmdFlag, CmdName
from data_collector.messaging.connection import RabbitMQConnection
from data_collector.messaging.consumer import CommandConsumer
from data_collector.messaging.models import CommandMessage
from data_collector.settings.rabbitmq import RabbitMQSettings
from data_collector.tables.apps import Apps
from data_collector.tables.command_log import CommandLog
from data_collector.utilities.database.main import Database


@dataclass(frozen=True)
class PendingCommand:
    """Unified command representation from either DB or RabbitMQ.

    Attributes:
        app_id: 64-char SHA-256 target app identifier.
        command: The command to execute.
        issued_by: User or system that issued the command.
        timestamp: When the command was created (synthesised from
            ``datetime.now(UTC)`` when the source row has no ``cmd_time``).
        source: Origin of the command (``"database"`` or ``"rabbitmq"``).
        args: Optional key-value arguments for the command.
        source_cmd_time: Raw ``cmd_time`` from the Apps row.  ``None`` when
            the DB column was NULL or the command came from RabbitMQ.
    """

    app_id: str
    command: CmdName
    issued_by: str
    timestamp: datetime
    source: str
    args: dict[str, Any] | None = None
    source_cmd_time: datetime | None = None


class CommandHandler:
    """Collect commands from DB polling and RabbitMQ into a unified queue.

    Args:
        database: Database instance connected to the main schema.
        logger: Structured logger for command-related messages.
        rabbitmq_connection: Optional RabbitMQ connection (``None`` disables
            the consumer).
        rabbitmq_settings: Required when ``rabbitmq_connection`` is provided.
    """

    def __init__(
        self,
        database: Database,
        *,
        logger: logging.Logger,
        rabbitmq_connection: RabbitMQConnection | None = None,
        rabbitmq_settings: RabbitMQSettings | None = None,
    ) -> None:
        self._database = database
        self._logger = logger
        self._command_queue: queue.Queue[PendingCommand] = queue.Queue()
        self._rabbitmq_connection = rabbitmq_connection
        self._rabbitmq_settings = rabbitmq_settings
        self._consumer: CommandConsumer | None = None

    def start(self) -> None:
        """Start the RabbitMQ command consumer if configured.

        No-op when ``rabbitmq_connection`` is ``None``.
        """
        if self._rabbitmq_connection is None or self._rabbitmq_settings is None:
            self._logger.info("RabbitMQ not configured, using DB polling only")
            return

        self._consumer = CommandConsumer(
            self._rabbitmq_connection,
            self._rabbitmq_settings,
            self._on_rabbitmq_message,
        )
        self._consumer.start()
        self._logger.info("RabbitMQ command consumer started")

    def stop(self) -> None:
        """Stop the RabbitMQ command consumer if running."""
        if self._consumer is not None:
            self._consumer.stop()
            self._consumer = None
            self._logger.info("RabbitMQ command consumer stopped")

    def poll_database_commands(self) -> None:
        """Query Apps table for pending commands and enqueue them.

        For each app with ``cmd_flag=PENDING`` and ``cmd_name`` set,
        creates a :class:`PendingCommand` and updates ``cmd_flag`` to
        ``EXECUTED`` in the database.
        """
        with self._database.create_session() as session:
            statement = (
                select(Apps)
                .where(
                    Apps.cmd_flag == CmdFlag.PENDING,
                    Apps.cmd_name.isnot(None),
                )
            )
            result = self._database.query(statement, session)
            pending_apps: list[Apps] = list(result.scalars().all())

            for app in pending_apps:
                try:
                    command_value = CmdName(int(app.cmd_name_obj.id)) if app.cmd_name_obj else None
                except (ValueError, TypeError):
                    command_value = None

                if command_value is None:
                    self._logger.warning(
                        "Unknown cmd_name '%s' for app %s, marking NOT_EXECUTED",
                        app.cmd_name, app.app,
                    )
                    app.cmd_flag = CmdFlag.NOT_EXECUTED  # type: ignore[assignment]
                    app.cmd_exec = datetime.now(UTC)  # type: ignore[assignment]
                    session.commit()
                    continue

                raw_cmd_time = app.cmd_time if isinstance(app.cmd_time, datetime) else None
                pending = PendingCommand(
                    app_id=str(app.app),
                    command=command_value,
                    issued_by=str(app.cmd_by) if app.cmd_by is not None else "unknown",  # type: ignore[redundant-expr]
                    timestamp=raw_cmd_time if raw_cmd_time is not None else datetime.now(UTC),
                    source="database",
                    source_cmd_time=raw_cmd_time,
                )
                self._command_queue.put(pending)

            # Leave cmd_flag as PENDING until the command is actually
            # executed. mark_database_command_executed() is called by the
            # Manager after execution so that a crash between poll and
            # execution does not silently lose the command.

    def get_pending_commands(self) -> list[PendingCommand]:
        """Drain the command queue and return all pending commands.

        Non-blocking: returns an empty list if the queue is empty.
        """
        commands: list[PendingCommand] = []
        while True:
            try:
                commands.append(self._command_queue.get_nowait())
            except queue.Empty:
                break
        return commands

    def log_command(
        self,
        command: PendingCommand,
        *,
        executed: bool,
    ) -> None:
        """Write an audit trail record and update the source row.

        For database-sourced commands, marks the Apps row as EXECUTED or
        NOT_EXECUTED only after the command has actually been processed.

        Args:
            command: The command that was processed.
            executed: Whether the command was successfully executed.
        """
        flag = CmdFlag.EXECUTED if executed else CmdFlag.NOT_EXECUTED

        with self._database.create_session() as session:
            record = CommandLog(
                app_id=command.app_id,
                cmd_by=command.issued_by,
                cmd_name=command.command.name,
                cmd_time=command.timestamp,
                cmd_flag=flag,
                cmd_exec=datetime.now(UTC),
            )
            session.add(record)

            # Mark the source Apps row only if it still represents the
            # processed command.  A newer PENDING command written between
            # poll_database_commands() and this point must not be overwritten.
            if command.source == "database":
                time_filter = (
                    Apps.cmd_time == command.source_cmd_time
                    if command.source_cmd_time is not None
                    else Apps.cmd_time.is_(None)
                )
                statement = select(Apps).where(
                    Apps.app == command.app_id,
                    Apps.cmd_flag == CmdFlag.PENDING,
                    time_filter,
                )
                row = self._database.query(statement, session).scalar_one_or_none()
                if row is not None:
                    row.cmd_flag = flag  # type: ignore[assignment]
                    row.cmd_exec = datetime.now(UTC)  # type: ignore[assignment]

            session.commit()

    def _on_rabbitmq_message(self, message: CommandMessage) -> None:
        """Callback invoked by the RabbitMQ consumer daemon thread.

        Converts the :class:`CommandMessage` into a :class:`PendingCommand`
        and places it on the thread-safe queue for the main loop to process.
        """
        pending = PendingCommand(
            app_id=message.app_id,
            command=message.command,
            issued_by=message.issued_by,
            timestamp=message.timestamp,
            source="rabbitmq",
            args=message.args,
        )
        self._command_queue.put(pending)
        self._logger.info(
            "RabbitMQ command received: %s for app %s from %s",
            message.command.name, message.app_id, message.issued_by,
        )
