# pyright: reportPrivateUsage=false
"""Tests for Manager integration with WatchService and TaskDispatcher."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from data_collector.messaging.watchservice import Root
from data_collector.orchestration.manager import Manager

_MODULE = "data_collector.orchestration.manager"


def _make_settings() -> MagicMock:
    settings = MagicMock()
    settings.polling_interval = 10
    settings.command_poll_interval = 5
    settings.process_check_interval = 30
    settings.startup_grace_period = 10
    settings.max_start_failures = 3
    settings.retention_enabled = False
    settings.retention_check_interval = 3600
    settings.shutdown_timeout = 30
    return settings


def _make_manager(
    *,
    dramatiq_broker: MagicMock | None = None,
) -> Manager:
    mock_database = MagicMock()
    return Manager(
        mock_database,
        _make_settings(),
        logger=MagicMock(),
        dramatiq_broker=dramatiq_broker,
    )


def _make_root(root_id: int = 1) -> Root:
    return Root(
        root_id=root_id,
        root_path=f"/ingest/hr/dir{root_id}",
        rel_path=f"dir{root_id}",
        country="HR",
        watch_group="ocr",
        worker_path="data_collector.croatia.gazette.ocr.main",
    )


# ---------------------------------------------------------------------------
# WatchService auto-detection
# ---------------------------------------------------------------------------


class TestWatchServiceAutoDetect:
    """Manager starts WatchService when active WatchRoots exist."""

    @patch(f"{_MODULE}.WatchService")
    @patch(f"{_MODULE}.IngestEventHandler")
    @patch(f"{_MODULE}.load_roots_from_database")
    def test_starts_when_roots_exist(
        self,
        mock_load_roots: MagicMock,
        mock_handler_class: MagicMock,
        mock_ws_class: MagicMock,
    ) -> None:
        mock_load_roots.return_value = [_make_root()]
        mock_ws_instance = MagicMock()
        mock_ws_class.return_value = mock_ws_instance

        manager = _make_manager()
        manager._start_watch_service()

        mock_load_roots.assert_called_once()
        mock_ws_class.assert_called_once()
        mock_ws_instance.start.assert_called_once()
        assert manager._watch_service is mock_ws_instance

    @patch(f"{_MODULE}.load_roots_from_database")
    def test_not_started_when_no_roots(self, mock_load_roots: MagicMock) -> None:
        mock_load_roots.return_value = []

        manager = _make_manager()
        manager._start_watch_service()

        assert manager._watch_service is None

    @patch(f"{_MODULE}.load_roots_from_database")
    def test_handles_db_error_gracefully(self, mock_load_roots: MagicMock) -> None:
        mock_load_roots.side_effect = Exception("DB unreachable")

        manager = _make_manager()
        manager._start_watch_service()

        assert manager._watch_service is None


# ---------------------------------------------------------------------------
# TaskDispatcher auto-detection
# ---------------------------------------------------------------------------


class TestTaskDispatcherAutoDetect:
    """Manager starts TaskDispatcher when DramatiqBroker is provided."""

    @patch(f"{_MODULE}.TaskDispatcher")
    def test_starts_when_broker_provided(self, mock_td_class: MagicMock) -> None:
        mock_td_instance = MagicMock()
        mock_td_class.return_value = mock_td_instance
        mock_broker = MagicMock()

        manager = _make_manager(dramatiq_broker=mock_broker)
        manager._start_task_dispatcher()

        mock_td_class.assert_called_once()
        mock_td_instance.start.assert_called_once()
        assert manager._task_dispatcher is mock_td_instance

    def test_not_started_when_no_broker(self) -> None:
        manager = _make_manager(dramatiq_broker=None)
        manager._start_task_dispatcher()

        assert manager._task_dispatcher is None


# ---------------------------------------------------------------------------
# Shutdown order
# ---------------------------------------------------------------------------


class TestShutdownOrder:
    """TaskDispatcher stops before WatchService (stop consuming before stop producing)."""

    def test_shutdown_stops_both_in_order(self) -> None:
        manager = _make_manager()
        mock_td = MagicMock()
        mock_ws = MagicMock()
        manager._task_dispatcher = mock_td
        manager._watch_service = mock_ws

        shutdown_order: list[str] = []
        mock_td.stop.side_effect = lambda: shutdown_order.append("task_dispatcher")
        mock_ws.stop.side_effect = lambda: shutdown_order.append("watch_service")

        manager._shutdown()

        assert shutdown_order == ["task_dispatcher", "watch_service"]

    def test_shutdown_handles_none_services(self) -> None:
        manager = _make_manager()
        assert manager._task_dispatcher is None
        assert manager._watch_service is None

        # Should not crash
        manager._shutdown()
