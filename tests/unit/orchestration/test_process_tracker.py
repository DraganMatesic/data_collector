"""Unit tests for the ProcessTracker class."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from data_collector.enums import RunStatus
from data_collector.enums.runtime import RuntimeExitCode
from data_collector.orchestration.process_tracker import ProcessTracker


def _make_mock_app(
    *,
    app_id: str = "test_app_hash",
    group_name: str = "test_group",
    parent_name: str = "test_parent",
    app_name: str = "test_app",
    app_pids: str | None = None,
) -> MagicMock:
    app = MagicMock()
    app.app = app_id
    app.group_name = group_name
    app.parent_name = parent_name
    app.app_name = app_name
    app.app_pids = app_pids
    return app


_MODULE = "data_collector.orchestration.process_tracker"


class TestSpawn:
    """Test ProcessTracker.spawn() subprocess creation."""

    @patch(f"{_MODULE}.subprocess.Popen")
    @patch(f"{_MODULE}.update_app_status")
    def test_spawn_creates_subprocess(
        self, mock_update: MagicMock, mock_popen: MagicMock,
    ) -> None:
        mock_process = MagicMock()
        mock_process.pid = 12345
        mock_popen.return_value = mock_process

        mock_database = MagicMock()
        mock_session = MagicMock()
        mock_database.create_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_database.create_session.return_value.__exit__ = MagicMock(return_value=False)

        tracker = ProcessTracker(mock_database, logger=MagicMock())
        app = _make_mock_app()

        tracked = tracker.spawn(app)

        assert tracked.app_id == "test_app_hash"
        assert tracked.process is mock_process
        assert len(tracked.runtime_id) == 32
        assert tracker.is_tracked("test_app_hash")

    @patch(f"{_MODULE}.subprocess.Popen")
    @patch(f"{_MODULE}.update_app_status")
    def test_spawn_no_shell_true(
        self, mock_update: MagicMock, mock_popen: MagicMock,
    ) -> None:
        mock_popen.return_value = MagicMock(pid=1)

        mock_database = MagicMock()
        mock_session = MagicMock()
        mock_database.create_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_database.create_session.return_value.__exit__ = MagicMock(return_value=False)

        tracker = ProcessTracker(mock_database, logger=MagicMock())
        tracker.spawn(_make_mock_app())

        popen_call = mock_popen.call_args
        # shell should not be True (either absent or False)
        assert popen_call.kwargs.get("shell") is not True

    @patch(f"{_MODULE}.subprocess.Popen")
    @patch(f"{_MODULE}.update_app_status")
    def test_spawn_updates_app_status(
        self, mock_update: MagicMock, mock_popen: MagicMock,
    ) -> None:
        mock_popen.return_value = MagicMock(pid=999)

        mock_database = MagicMock()
        mock_session = MagicMock()
        mock_database.create_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_database.create_session.return_value.__exit__ = MagicMock(return_value=False)

        tracker = ProcessTracker(mock_database, logger=MagicMock())
        tracker.spawn(_make_mock_app())

        mock_update.assert_called_once()
        call_kwargs = mock_update.call_args[1]
        assert call_kwargs["run_status"] == RunStatus.RUNNING
        assert call_kwargs["app_pids"] == "999"

    @patch(f"{_MODULE}.subprocess.Popen")
    @patch(f"{_MODULE}.update_app_status")
    def test_spawn_with_args(
        self, mock_update: MagicMock, mock_popen: MagicMock,
    ) -> None:
        mock_popen.return_value = MagicMock(pid=1)

        mock_database = MagicMock()
        mock_session = MagicMock()
        mock_database.create_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_database.create_session.return_value.__exit__ = MagicMock(return_value=False)

        tracker = ProcessTracker(mock_database, logger=MagicMock())
        tracker.spawn(_make_mock_app(), app_args={"key": "value"})

        cmd = mock_popen.call_args[0][0]
        assert "--args" in cmd


class TestCheckProcesses:
    """Test ProcessTracker.check_processes() polling."""

    @patch(f"{_MODULE}.subprocess.Popen")
    @patch(f"{_MODULE}.update_app_status")
    def test_completed_process_returned(
        self, mock_update: MagicMock, mock_popen: MagicMock,
    ) -> None:
        mock_process = MagicMock()
        mock_process.pid = 1
        mock_process.poll.return_value = 0
        mock_process.returncode = 0
        mock_popen.return_value = mock_process

        mock_database = MagicMock()
        mock_session = MagicMock()
        mock_database.create_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_database.create_session.return_value.__exit__ = MagicMock(return_value=False)
        mock_database.query.return_value.scalar_one_or_none.return_value = MagicMock()

        tracker = ProcessTracker(mock_database, logger=MagicMock())
        tracker.spawn(_make_mock_app())

        completed = tracker.check_processes()
        assert len(completed) == 1
        assert completed[0].return_code == 0
        assert not tracker.is_tracked("test_app_hash")

    @patch(f"{_MODULE}.subprocess.Popen")
    @patch(f"{_MODULE}.update_app_status")
    def test_running_process_not_returned(
        self, mock_update: MagicMock, mock_popen: MagicMock,
    ) -> None:
        mock_process = MagicMock()
        mock_process.pid = 1
        mock_process.poll.return_value = None
        mock_popen.return_value = mock_process

        mock_database = MagicMock()
        mock_session = MagicMock()
        mock_database.create_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_database.create_session.return_value.__exit__ = MagicMock(return_value=False)

        tracker = ProcessTracker(mock_database, logger=MagicMock())
        tracker.spawn(_make_mock_app())

        completed = tracker.check_processes()
        assert len(completed) == 0
        assert tracker.is_tracked("test_app_hash")


class TestTerminate:
    """Test ProcessTracker.terminate_process() and terminate_all()."""

    @patch(f"{_MODULE}.subprocess.Popen")
    @patch(f"{_MODULE}.update_app_status")
    def test_terminate_process(
        self, mock_update: MagicMock, mock_popen: MagicMock,
    ) -> None:
        mock_process = MagicMock()
        mock_process.pid = 1
        mock_process.returncode = 0
        mock_popen.return_value = mock_process

        mock_database = MagicMock()
        mock_session = MagicMock()
        mock_database.create_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_database.create_session.return_value.__exit__ = MagicMock(return_value=False)
        mock_database.query.return_value.scalar_one_or_none.return_value = MagicMock()

        tracker = ProcessTracker(mock_database, logger=MagicMock())
        tracker.spawn(_make_mock_app())

        result = tracker.terminate_process("test_app_hash", exit_code=RuntimeExitCode.CMD_STOP)
        assert result is True
        assert not tracker.is_tracked("test_app_hash")
        mock_process.terminate.assert_called_once()

    def test_terminate_nonexistent_returns_false(self) -> None:
        tracker = ProcessTracker(MagicMock(), logger=MagicMock())
        result = tracker.terminate_process("nonexistent", exit_code=RuntimeExitCode.CMD_STOP)
        assert result is False

    def test_active_count(self) -> None:
        tracker = ProcessTracker(MagicMock(), logger=MagicMock())
        assert tracker.active_count == 0
