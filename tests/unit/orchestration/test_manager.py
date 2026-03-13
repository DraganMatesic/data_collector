# pyright: reportPrivateUsage=false
"""Unit tests for the Manager class."""

from __future__ import annotations

import time
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

from data_collector.enums import CmdName, FatalFlag
from data_collector.enums.runtime import RuntimeExitCode
from data_collector.orchestration.command_handler import PendingCommand
from data_collector.orchestration.manager import Manager
from data_collector.orchestration.process_tracker import TrackedProcess

_MODULE = "data_collector.orchestration.manager"


def _make_settings(**overrides: object) -> MagicMock:
    defaults: dict[str, object] = {
        "polling_interval": 10,
        "command_poll_interval": 5,
        "process_check_interval": 30,
        "startup_grace_period": 10,
        "max_start_failures": 3,
        "retention_enabled": False,
        "retention_check_interval": 3600,
        "shutdown_timeout": 30,
    }
    defaults.update(overrides)
    settings = MagicMock()
    for key, value in defaults.items():
        setattr(settings, key, value)
    return settings


def _make_manager(
    *,
    settings: MagicMock | None = None,
    notification_dispatcher: MagicMock | None = None,
) -> Manager:
    mock_database = MagicMock()
    if settings is None:
        settings = _make_settings()
    return Manager(
        mock_database,
        settings,
        logger=MagicMock(),
        notification_dispatcher=notification_dispatcher,
    )


def _make_pending_command(
    *,
    app_id: str = "test_app_hash",
    command: CmdName = CmdName.START,
) -> PendingCommand:
    return PendingCommand(
        app_id=app_id,
        command=command,
        issued_by="test_user",
        timestamp=datetime.now(UTC),
        source="database",
    )


def _make_tracked(
    *,
    app_id: str = "test_app_hash",
    return_code: int | None = 0,
) -> TrackedProcess:
    tracked = MagicMock(spec=TrackedProcess)
    tracked.app_id = app_id
    tracked.group_name = "test_group"
    tracked.parent_name = "test_parent"
    tracked.app_name = "test_app"
    tracked.return_code = return_code
    tracked.runtime_id = "a" * 32
    return tracked


class TestStopEvent:
    """Test Manager.stop() sets the stop event."""

    def test_stop_sets_event(self) -> None:
        manager = _make_manager()
        assert not manager._stop_event.is_set()
        manager.stop()
        assert manager._stop_event.is_set()


class TestTick:
    """Test Manager._tick() dispatches to subsystems."""

    def test_tick_processes_commands(self) -> None:
        manager = _make_manager()
        manager._command_handler = MagicMock()
        manager._command_handler.get_pending_commands.return_value = []
        manager._process_tracker = MagicMock()
        manager._process_tracker.check_processes.return_value = []
        manager._scheduler = MagicMock()
        manager._scheduler.get_ready_apps.return_value = []

        # Force command poll by setting last poll far in the past
        manager._last_command_poll = 0.0
        manager._last_process_check = time.monotonic() + 9999

        manager._tick()

        manager._command_handler.poll_database_commands.assert_called_once()
        manager._command_handler.get_pending_commands.assert_called_once()

    def test_tick_checks_processes(self) -> None:
        manager = _make_manager()
        manager._command_handler = MagicMock()
        manager._command_handler.get_pending_commands.return_value = []
        manager._process_tracker = MagicMock()
        manager._process_tracker.check_processes.return_value = []
        manager._scheduler = MagicMock()
        manager._scheduler.get_ready_apps.return_value = []

        # Force process check
        manager._last_command_poll = time.monotonic() + 9999
        manager._last_process_check = 0.0

        manager._tick()

        manager._process_tracker.check_processes.assert_called_once()

    def test_tick_spawns_ready_apps(self) -> None:
        manager = _make_manager()
        manager._command_handler = MagicMock()
        manager._command_handler.get_pending_commands.return_value = []
        manager._process_tracker = MagicMock()
        manager._process_tracker.check_processes.return_value = []
        manager._process_tracker.is_tracked.return_value = False

        mock_app = MagicMock()
        mock_app.app = "ready_app"
        manager._scheduler = MagicMock()
        manager._scheduler.get_ready_apps.return_value = [mock_app]

        manager._last_command_poll = time.monotonic() + 9999
        manager._last_process_check = time.monotonic() + 9999

        manager._tick()

        manager._process_tracker.spawn.assert_called_once_with(mock_app, app_args=None)

    def test_tick_skips_already_tracked_app(self) -> None:
        manager = _make_manager()
        manager._command_handler = MagicMock()
        manager._command_handler.get_pending_commands.return_value = []
        manager._process_tracker = MagicMock()
        manager._process_tracker.check_processes.return_value = []
        manager._process_tracker.is_tracked.return_value = True

        mock_app = MagicMock()
        mock_app.app = "already_running"
        manager._scheduler = MagicMock()
        manager._scheduler.get_ready_apps.return_value = [mock_app]

        manager._last_command_poll = time.monotonic() + 9999
        manager._last_process_check = time.monotonic() + 9999

        manager._tick()

        manager._process_tracker.spawn.assert_not_called()

    def test_tick_runs_retention_when_enabled(self) -> None:
        settings = _make_settings(retention_enabled=True, retention_check_interval=0)
        manager = _make_manager(settings=settings)
        manager._command_handler = MagicMock()
        manager._command_handler.get_pending_commands.return_value = []
        manager._process_tracker = MagicMock()
        manager._process_tracker.check_processes.return_value = []
        manager._scheduler = MagicMock()
        manager._scheduler.get_ready_apps.return_value = []
        manager._retention_cleaner = MagicMock()

        manager._last_command_poll = time.monotonic() + 9999
        manager._last_process_check = time.monotonic() + 9999
        manager._last_retention_run = 0.0

        manager._tick()

        manager._retention_cleaner.run_cleanup.assert_called_once()


class TestExecuteCommand:
    """Test Manager._execute_command() dispatch."""

    @patch.object(Manager, "_get_app")
    def test_cmd_start(self, mock_get_app: MagicMock) -> None:
        manager = _make_manager()
        manager._process_tracker = MagicMock()
        manager._process_tracker.is_tracked.return_value = False

        mock_app = MagicMock()
        mock_app.disable = False
        mock_get_app.return_value = mock_app

        result = manager._execute_command(_make_pending_command(command=CmdName.START))
        assert result is True
        manager._process_tracker.spawn.assert_called_once()

    def test_cmd_start_already_running(self) -> None:
        manager = _make_manager()
        manager._process_tracker = MagicMock()
        manager._process_tracker.is_tracked.return_value = True

        result = manager._execute_command(_make_pending_command(command=CmdName.START))
        assert result is False

    @patch.object(Manager, "_get_app")
    def test_cmd_start_disabled_app(self, mock_get_app: MagicMock) -> None:
        manager = _make_manager()
        manager._process_tracker = MagicMock()
        manager._process_tracker.is_tracked.return_value = False

        mock_app = MagicMock()
        mock_app.disable = True
        mock_get_app.return_value = mock_app

        result = manager._execute_command(_make_pending_command(command=CmdName.START))
        assert result is False

    def test_cmd_stop(self) -> None:
        manager = _make_manager()
        manager._process_tracker = MagicMock()
        manager._process_tracker.is_tracked.return_value = True
        manager._process_tracker.terminate_process.return_value = True

        result = manager._execute_command(_make_pending_command(command=CmdName.STOP))
        assert result is True
        manager._process_tracker.terminate_process.assert_called_once_with(
            "test_app_hash", exit_code=RuntimeExitCode.CMD_STOP,
        )

    def test_cmd_stop_not_running(self) -> None:
        manager = _make_manager()
        manager._process_tracker = MagicMock()
        manager._process_tracker.is_tracked.return_value = False

        result = manager._execute_command(_make_pending_command(command=CmdName.STOP))
        assert result is False

    @patch(f"{_MODULE}.update_app_status")
    def test_cmd_enable(self, mock_update: MagicMock) -> None:
        manager = _make_manager()

        result = manager._execute_command(_make_pending_command(command=CmdName.ENABLE))
        assert result is True
        mock_update.assert_called_once()
        call_kwargs = mock_update.call_args[1]
        assert call_kwargs["disable"] is False
        assert call_kwargs["fatal_flag"] == FatalFlag.NONE

    @patch(f"{_MODULE}.update_app_status")
    def test_cmd_disable(self, mock_update: MagicMock) -> None:
        manager = _make_manager()
        manager._process_tracker = MagicMock()
        manager._process_tracker.is_tracked.return_value = False

        result = manager._execute_command(_make_pending_command(command=CmdName.DISABLE))
        assert result is True
        mock_update.assert_called_once()
        call_kwargs = mock_update.call_args[1]
        assert call_kwargs["disable"] is True

    @patch(f"{_MODULE}.update_app_status")
    def test_cmd_disable_terminates_running(self, mock_update: MagicMock) -> None:
        manager = _make_manager()
        manager._process_tracker = MagicMock()
        manager._process_tracker.is_tracked.return_value = True

        manager._execute_command(_make_pending_command(command=CmdName.DISABLE))

        manager._process_tracker.terminate_process.assert_called_once_with(
            "test_app_hash", exit_code=RuntimeExitCode.CMD_DISABLE,
        )

    @patch.object(Manager, "_get_app")
    def test_cmd_restart(self, mock_get_app: MagicMock) -> None:
        manager = _make_manager()
        manager._process_tracker = MagicMock()
        # First call (STOP check): running. Second call (START check): not running after stop.
        manager._process_tracker.is_tracked.side_effect = [True, False]
        manager._process_tracker.terminate_process.return_value = True

        mock_app = MagicMock()
        mock_app.disable = False
        mock_get_app.return_value = mock_app

        result = manager._execute_command(_make_pending_command(command=CmdName.RESTART))
        assert result is True
        manager._process_tracker.terminate_process.assert_called_once()
        manager._process_tracker.spawn.assert_called_once()


class TestHandleCompletedProcesses:
    """Test Manager._handle_completed_processes() failure tracking."""

    @patch.object(Manager, "_get_app")
    def test_successful_completion_resets_failure_count(self, mock_get_app: MagicMock) -> None:
        manager = _make_manager()
        manager._scheduler = MagicMock()
        mock_get_app.return_value = MagicMock()
        manager._failure_counts["test_app_hash"] = 2

        tracked = _make_tracked(return_code=0)
        manager._handle_completed_processes([tracked])

        assert "test_app_hash" not in manager._failure_counts

    @patch.object(Manager, "_get_app")
    def test_crash_increments_failure_count(self, mock_get_app: MagicMock) -> None:
        manager = _make_manager()
        manager._scheduler = MagicMock()
        mock_get_app.return_value = MagicMock()

        tracked = _make_tracked(return_code=1)
        manager._handle_completed_processes([tracked])

        assert manager._failure_counts["test_app_hash"] == 1

    @patch(f"{_MODULE}.update_app_status")
    @patch.object(Manager, "_get_app")
    def test_max_failures_triggers_fatal(self, mock_get_app: MagicMock, mock_update: MagicMock) -> None:
        settings = _make_settings(max_start_failures=2)
        manager = _make_manager(settings=settings)
        manager._scheduler = MagicMock()
        mock_get_app.return_value = MagicMock()

        # Already at 1 failure
        manager._failure_counts["test_app_hash"] = 1

        tracked = _make_tracked(return_code=1)
        manager._handle_completed_processes([tracked])

        # Should have called update_app_status with fatal flag
        mock_update.assert_called_once()
        call_kwargs = mock_update.call_args[1]
        assert call_kwargs["fatal_flag"] == FatalFlag.FAILED_TO_START
        assert call_kwargs["disable"] is True

    @patch.object(Manager, "_get_app")
    def test_sets_fallback_next_run(self, mock_get_app: MagicMock) -> None:
        manager = _make_manager()
        manager._scheduler = MagicMock()
        mock_app = MagicMock()
        mock_get_app.return_value = mock_app

        tracked = _make_tracked(return_code=0)
        manager._handle_completed_processes([tracked])

        manager._scheduler.set_fallback_next_run.assert_called_once_with(
            "test_app_hash", mock_app,
        )


class TestHandleFatal:
    """Test Manager._handle_fatal() notification dispatch."""

    @patch(f"{_MODULE}.update_app_status")
    def test_handle_fatal_without_notifications(self, mock_update: MagicMock) -> None:
        manager = _make_manager(notification_dispatcher=None)

        manager._handle_fatal(
            "test_hash", "group", "parent", "app",
            FatalFlag.FAILED_TO_START, "Test error",
        )

        mock_update.assert_called_once()
        call_kwargs = mock_update.call_args[1]
        assert call_kwargs["fatal_flag"] == FatalFlag.FAILED_TO_START
        assert call_kwargs["disable"] is True

    @patch(f"{_MODULE}.update_app_status")
    def test_handle_fatal_with_notifications(self, mock_update: MagicMock) -> None:
        mock_dispatcher = MagicMock()
        manager = _make_manager(notification_dispatcher=mock_dispatcher)

        manager._handle_fatal(
            "test_hash", "group", "parent", "app",
            FatalFlag.FAILED_TO_START, "Test error",
        )

        mock_dispatcher.send.assert_called_once()
        notification = mock_dispatcher.send.call_args[0][0]
        assert notification.app_id == "test_hash"
        assert "Test error" in notification.message

    @patch(f"{_MODULE}.update_app_status")
    def test_handle_fatal_notification_failure_logged(self, mock_update: MagicMock) -> None:
        mock_dispatcher = MagicMock()
        mock_dispatcher.send.side_effect = RuntimeError("Connection failed")
        mock_logger = MagicMock()
        manager = _make_manager(notification_dispatcher=mock_dispatcher)
        manager._logger = mock_logger

        manager._handle_fatal(
            "test_hash", "group", "parent", "app",
            FatalFlag.FAILED_TO_START, "Test error",
        )

        # Should not propagate the exception; should log it
        mock_logger.exception.assert_called_once()


class TestShutdown:
    """Test Manager._shutdown() cleanup."""

    def test_shutdown_stops_handler_and_tracker(self) -> None:
        manager = _make_manager()
        manager._command_handler = MagicMock()
        manager._process_tracker = MagicMock()

        manager._shutdown()

        manager._command_handler.stop.assert_called_once()
        manager._process_tracker.terminate_all.assert_called_once_with(
            exit_code=RuntimeExitCode.MANAGER_EXIT,
            timeout=30,
        )


class TestSpawnApp:
    """Test Manager._spawn_app() error handling."""

    @patch(f"{_MODULE}.update_app_status")
    def test_spawn_failure_triggers_fatal(self, mock_update: MagicMock) -> None:
        manager = _make_manager()
        manager._process_tracker = MagicMock()
        manager._process_tracker.spawn.side_effect = OSError("No such file")

        mock_app = MagicMock()
        mock_app.app = "failing_app"
        mock_app.group_name = "group"
        mock_app.parent_name = "parent"
        mock_app.app_name = "app"

        result = manager._spawn_app(mock_app)
        assert result is False

        mock_update.assert_called_once()
        call_kwargs = mock_update.call_args[1]
        assert call_kwargs["fatal_flag"] == FatalFlag.FAILED_TO_START
