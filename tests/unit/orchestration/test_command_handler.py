# pyright: reportPrivateUsage=false
"""Unit tests for the CommandHandler class."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

from data_collector.enums import CmdFlag, CmdName
from data_collector.orchestration.command_handler import CommandHandler, PendingCommand

_MODULE = "data_collector.orchestration.command_handler"


def _make_pending(
    *,
    app_id: str = "test_app_hash",
    command: CmdName = CmdName.START,
    issued_by: str = "test_user",
    source: str = "database",
) -> PendingCommand:
    return PendingCommand(
        app_id=app_id,
        command=command,
        issued_by=issued_by,
        timestamp=datetime.now(UTC),
        source=source,
    )


class TestPollDatabaseCommands:
    """Test CommandHandler.poll_database_commands()."""

    def test_enqueues_pending_commands(self) -> None:
        mock_database = MagicMock()
        mock_session = MagicMock()
        mock_database.create_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_database.create_session.return_value.__exit__ = MagicMock(return_value=False)

        mock_app = MagicMock()
        mock_app.app = "test_hash"
        mock_app.cmd_flag = CmdFlag.PENDING
        mock_app.cmd_name_obj = MagicMock()
        mock_app.cmd_name_obj.id = CmdName.START.value
        mock_app.cmd_by = "admin"
        mock_app.cmd_time = datetime.now(UTC)

        mock_database.query.return_value.scalars.return_value.all.return_value = [mock_app]

        handler = CommandHandler(mock_database, logger=MagicMock())
        handler.poll_database_commands()

        commands = handler.get_pending_commands()
        assert len(commands) == 1
        assert commands[0].app_id == "test_hash"
        assert commands[0].command == CmdName.START
        assert commands[0].source == "database"

    def test_marks_invalid_command_not_executed(self) -> None:
        mock_database = MagicMock()
        mock_session = MagicMock()
        mock_database.create_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_database.create_session.return_value.__exit__ = MagicMock(return_value=False)

        mock_app = MagicMock()
        mock_app.app = "test_hash"
        mock_app.cmd_flag = CmdFlag.PENDING
        mock_app.cmd_name_obj = None
        mock_app.cmd_by = "admin"
        mock_app.cmd_time = datetime.now(UTC)

        mock_database.query.return_value.scalars.return_value.all.return_value = [mock_app]

        handler = CommandHandler(mock_database, logger=MagicMock())
        handler.poll_database_commands()

        # Invalid command should not be enqueued
        commands = handler.get_pending_commands()
        assert len(commands) == 0
        assert mock_app.cmd_flag == CmdFlag.NOT_EXECUTED

    def test_no_pending_commands(self) -> None:
        mock_database = MagicMock()
        mock_session = MagicMock()
        mock_database.create_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_database.create_session.return_value.__exit__ = MagicMock(return_value=False)
        mock_database.query.return_value.scalars.return_value.all.return_value = []

        handler = CommandHandler(mock_database, logger=MagicMock())
        handler.poll_database_commands()

        commands = handler.get_pending_commands()
        assert len(commands) == 0


class TestGetPendingCommands:
    """Test CommandHandler.get_pending_commands() queue drain."""

    def test_drains_queue(self) -> None:
        handler = CommandHandler(MagicMock(), logger=MagicMock())

        # Manually enqueue commands
        handler._command_queue.put(_make_pending(app_id="app1"))
        handler._command_queue.put(_make_pending(app_id="app2"))

        commands = handler.get_pending_commands()
        assert len(commands) == 2
        assert commands[0].app_id == "app1"
        assert commands[1].app_id == "app2"

    def test_empty_queue_returns_empty_list(self) -> None:
        handler = CommandHandler(MagicMock(), logger=MagicMock())
        commands = handler.get_pending_commands()
        assert commands == []

    def test_queue_is_empty_after_drain(self) -> None:
        handler = CommandHandler(MagicMock(), logger=MagicMock())
        handler._command_queue.put(_make_pending())

        handler.get_pending_commands()
        assert handler.get_pending_commands() == []


class TestLogCommand:
    """Test CommandHandler.log_command() audit trail."""

    def test_log_command_creates_record(self) -> None:
        mock_database = MagicMock()
        mock_session = MagicMock()
        mock_database.create_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_database.create_session.return_value.__exit__ = MagicMock(return_value=False)

        handler = CommandHandler(mock_database, logger=MagicMock())
        command = _make_pending()

        handler.log_command(command, executed=True)

        mock_session.add.assert_called_once()
        mock_session.commit.assert_called_once()
        record = mock_session.add.call_args[0][0]
        assert record.app_id == "test_app_hash"
        assert record.cmd_name == "START"
        assert record.cmd_flag == CmdFlag.EXECUTED

    def test_log_command_not_executed(self) -> None:
        mock_database = MagicMock()
        mock_session = MagicMock()
        mock_database.create_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_database.create_session.return_value.__exit__ = MagicMock(return_value=False)

        handler = CommandHandler(mock_database, logger=MagicMock())
        command = _make_pending()

        handler.log_command(command, executed=False)

        record = mock_session.add.call_args[0][0]
        assert record.cmd_flag == CmdFlag.NOT_EXECUTED

    def test_log_command_skips_update_when_newer_command_pending(self) -> None:
        """Guard: a newer PENDING command must not be overwritten."""
        mock_database = MagicMock()
        mock_session = MagicMock()
        mock_database.create_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_database.create_session.return_value.__exit__ = MagicMock(return_value=False)
        # Simulate no matching row (newer command changed cmd_time/cmd_flag)
        mock_database.query.return_value.scalar_one_or_none.return_value = None

        handler = CommandHandler(mock_database, logger=MagicMock())
        command = _make_pending(source="database")

        handler.log_command(command, executed=True)

        # CommandLog audit record is still written
        mock_session.add.assert_called_once()
        # But no Apps row attributes were mutated (no row returned)
        mock_session.commit.assert_called_once()


class TestRabbitMQIntegration:
    """Test RabbitMQ consumer start/stop and message callback."""

    def test_start_without_rabbitmq_is_noop(self) -> None:
        handler = CommandHandler(MagicMock(), logger=MagicMock())
        handler.start()
        assert handler._consumer is None

    @patch(f"{_MODULE}.CommandConsumer")
    def test_start_with_rabbitmq_creates_consumer(
        self, mock_consumer_cls: MagicMock,
    ) -> None:
        mock_connection = MagicMock()
        mock_settings = MagicMock()

        handler = CommandHandler(
            MagicMock(),
            logger=MagicMock(),
            rabbitmq_connection=mock_connection,
            rabbitmq_settings=mock_settings,
        )
        handler.start()

        mock_consumer_cls.assert_called_once()
        assert handler._consumer is not None
        handler._consumer.start.assert_called_once()  # type: ignore[union-attr]

    def test_stop_without_consumer_is_noop(self) -> None:
        handler = CommandHandler(MagicMock(), logger=MagicMock())
        handler.stop()  # Should not raise

    @patch(f"{_MODULE}.CommandConsumer")
    def test_stop_stops_consumer(self, mock_consumer_cls: MagicMock) -> None:
        mock_connection = MagicMock()
        mock_settings = MagicMock()

        handler = CommandHandler(
            MagicMock(),
            logger=MagicMock(),
            rabbitmq_connection=mock_connection,
            rabbitmq_settings=mock_settings,
        )
        handler.start()
        handler.stop()

        assert handler._consumer is None

    def test_rabbitmq_message_enqueued(self) -> None:
        handler = CommandHandler(MagicMock(), logger=MagicMock())

        mock_message = MagicMock()
        mock_message.app_id = "rmq_app"
        mock_message.command = CmdName.STOP
        mock_message.issued_by = "rmq_user"
        mock_message.timestamp = datetime.now(UTC)
        mock_message.args = {"key": "value"}

        handler._on_rabbitmq_message(mock_message)

        commands = handler.get_pending_commands()
        assert len(commands) == 1
        assert commands[0].app_id == "rmq_app"
        assert commands[0].command == CmdName.STOP
        assert commands[0].source == "rabbitmq"
        assert commands[0].args == {"key": "value"}
