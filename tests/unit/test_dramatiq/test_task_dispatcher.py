"""Tests for TaskDispatcher."""

from __future__ import annotations

import threading
from unittest.mock import MagicMock, patch

import pytest

from data_collector.dramatiq.task_dispatcher import TaskDispatcher
from data_collector.settings.dramatiq import TaskDispatcherSettings


def _make_dispatcher(
    batch_size: int = 10,
    poll_interval: int = 1,
) -> tuple[TaskDispatcher, MagicMock, MagicMock]:
    """Create a TaskDispatcher with mocked dependencies."""
    mock_database = MagicMock()
    mock_broker = MagicMock()
    settings = TaskDispatcherSettings(batch_size=batch_size, poll_interval=poll_interval)
    dispatcher = TaskDispatcher(mock_database, mock_broker, settings)
    return dispatcher, mock_database, mock_broker


class TestConstruction:
    """Tests for TaskDispatcher construction."""

    def test_stores_settings(self) -> None:
        dispatcher, _, _ = _make_dispatcher(batch_size=50, poll_interval=5)
        assert dispatcher._batch_size == 50
        assert dispatcher._poll_interval == 5

    def test_calculates_yield_per(self) -> None:
        dispatcher, _, _ = _make_dispatcher(batch_size=100)
        assert dispatcher._yield_per == 10

    def test_yield_per_minimum_one(self) -> None:
        dispatcher, _, _ = _make_dispatcher(batch_size=5)
        assert dispatcher._yield_per >= 1

    def test_default_settings(self) -> None:
        mock_database = MagicMock()
        mock_broker = MagicMock()
        dispatcher = TaskDispatcher(mock_database, mock_broker)
        assert dispatcher._batch_size == 100
        assert dispatcher._poll_interval == 10


class TestStartStop:
    """Tests for thread lifecycle."""

    def test_start_creates_daemon_thread(self) -> None:
        dispatcher, _, _ = _make_dispatcher()
        block_event = threading.Event()

        def blocking_loop() -> None:
            block_event.wait(timeout=5.0)

        with patch.object(dispatcher, "_dispatch_loop", side_effect=blocking_loop):
            dispatcher.start()
            try:
                assert dispatcher.is_running is True
                assert dispatcher._dispatcher_thread is not None
                assert dispatcher._dispatcher_thread.daemon is True
            finally:
                block_event.set()
                dispatcher.stop(timeout=2.0)

    def test_start_raises_if_already_running(self) -> None:
        dispatcher, _, _ = _make_dispatcher()
        block_event = threading.Event()

        def blocking_loop() -> None:
            block_event.wait(timeout=5.0)

        with patch.object(dispatcher, "_dispatch_loop", side_effect=blocking_loop):
            dispatcher.start()
            try:
                with pytest.raises(RuntimeError, match="already running"):
                    dispatcher.start()
            finally:
                block_event.set()
                dispatcher.stop(timeout=2.0)

    def test_stop_sets_event(self) -> None:
        dispatcher, _, _ = _make_dispatcher()
        dispatcher.stop()
        assert dispatcher._stop_event.is_set()

    def test_is_running_false_before_start(self) -> None:
        dispatcher, _, _ = _make_dispatcher()
        assert dispatcher.is_running is False


class TestDispatchBatch:
    """Tests for batch dispatch logic."""

    def test_returns_zero_when_no_events(self) -> None:
        dispatcher, mock_database, _ = _make_dispatcher()

        mock_session = MagicMock()
        mock_database.query.return_value.scalar.return_value = 0
        mock_database.create_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_database.create_session.return_value.__exit__ = MagicMock(return_value=False)

        result = dispatcher._dispatch_batch()
        assert result == 0

    def test_dispatches_events_and_returns_count(self) -> None:
        dispatcher, mock_database, mock_broker = _make_dispatcher()

        # Mock events
        mock_event_1 = MagicMock()
        mock_event_1.id = 1
        mock_event_1.app_path = "some.module"
        mock_event_2 = MagicMock()
        mock_event_2.id = 2
        mock_event_2.app_path = "some.module"

        mock_session = MagicMock()
        # First query: COUNT query returns 2
        # Second query: events query returns scalars iterator
        events_result = MagicMock()
        events_result.yield_per.return_value.scalars.return_value = [mock_event_1, mock_event_2]
        mock_database.query.side_effect = [
            MagicMock(scalar=MagicMock(return_value=2)),
            events_result,
        ]
        mock_database.create_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_database.create_session.return_value.__exit__ = MagicMock(return_value=False)

        with patch.object(dispatcher, "_dispatch_event", return_value=True) as mock_dispatch:
            result = dispatcher._dispatch_batch()

        assert result == 2
        assert mock_dispatch.call_count == 2


class TestDispatchEvent:
    """Tests for single event dispatch."""

    @patch("data_collector.dramatiq.task_dispatcher.importlib.import_module")
    def test_dispatches_event_successfully(self, mock_import: MagicMock) -> None:
        dispatcher, mock_database, mock_broker = _make_dispatcher()

        mock_queue_def = MagicMock()
        mock_queue_def.name = "dc_test_queue"
        mock_queue_def.actor_name = "test_actor"
        mock_queue_def.exchange_name = "dc_test_exchange"
        mock_queue_def.routing_key = "test.key"

        mock_module = MagicMock()
        mock_module.MAIN_EXCHANGE_QUEUE = mock_queue_def
        mock_import.return_value = mock_module

        mock_event = MagicMock()
        mock_event.id = 42
        mock_event.app_path = "some.topic.module"

        mock_session = MagicMock()

        result = dispatcher._dispatch_event(mock_event, mock_session)

        assert result is True
        mock_broker.create_message.assert_called_once()
        mock_broker.publish.assert_called_once()
        mock_database.add.assert_called_once()
        mock_session.commit.assert_called_once()

    @patch("data_collector.dramatiq.task_dispatcher.importlib.import_module")
    def test_handles_import_error(self, mock_import: MagicMock) -> None:
        dispatcher, _, _ = _make_dispatcher()

        mock_import.side_effect = ImportError("No module named 'bad.module'")

        mock_event = MagicMock()
        mock_event.id = 1
        mock_event.app_path = "bad.module"

        mock_session = MagicMock()

        result = dispatcher._dispatch_event(mock_event, mock_session)
        assert result is False

    @patch("data_collector.dramatiq.task_dispatcher.importlib.import_module")
    def test_handles_missing_queue_constant(self, mock_import: MagicMock) -> None:
        dispatcher, _, _ = _make_dispatcher()

        mock_module = MagicMock(spec=[])  # No MAIN_EXCHANGE_QUEUE attribute
        mock_import.return_value = mock_module

        mock_event = MagicMock()
        mock_event.id = 1
        mock_event.app_path = "some.module"

        mock_session = MagicMock()

        result = dispatcher._dispatch_event(mock_event, mock_session)
        assert result is False

    @patch("data_collector.dramatiq.task_dispatcher.importlib.import_module")
    def test_rollback_on_publish_error(self, mock_import: MagicMock) -> None:
        dispatcher, _, mock_broker = _make_dispatcher()

        mock_queue_def = MagicMock()
        mock_module = MagicMock()
        mock_module.MAIN_EXCHANGE_QUEUE = mock_queue_def
        mock_import.return_value = mock_module

        mock_broker.publish.side_effect = ConnectionError("broker down")

        mock_event = MagicMock()
        mock_event.id = 1
        mock_event.app_path = "some.module"

        mock_session = MagicMock()

        result = dispatcher._dispatch_event(mock_event, mock_session)
        assert result is False
        mock_session.rollback.assert_called_once()


class TestDispatchLoop:
    """Tests for the main dispatch loop."""

    def test_loop_exits_on_stop_event(self) -> None:
        dispatcher, _, _ = _make_dispatcher(poll_interval=1)

        with patch.object(dispatcher, "_dispatch_batch", return_value=0):
            dispatcher._stop_event.set()
            dispatcher._dispatch_loop()  # Should return immediately

    def test_loop_waits_when_no_events(self) -> None:
        dispatcher, _, _ = _make_dispatcher(poll_interval=1)

        call_count = 0

        def counting_dispatch() -> int:
            nonlocal call_count
            call_count += 1
            if call_count >= 2:
                dispatcher._stop_event.set()
            return 0

        with patch.object(dispatcher, "_dispatch_batch", side_effect=counting_dispatch):
            dispatcher._dispatch_loop()

        assert call_count >= 2

    def test_loop_recovers_from_error(self) -> None:
        dispatcher, _, _ = _make_dispatcher(poll_interval=1)

        call_count = 0

        def error_then_stop() -> int:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ConnectionError("transient error")
            dispatcher._stop_event.set()
            return 0

        with patch.object(dispatcher, "_dispatch_batch", side_effect=error_then_stop):
            dispatcher._dispatch_loop()

        assert call_count >= 2
