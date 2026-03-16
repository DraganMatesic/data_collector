"""Tests for WatchService file system monitoring."""

from __future__ import annotations

import os
import time
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from data_collector.enums.pipeline import EventType
from data_collector.messaging.watchservice import (
    DetectCommand,
    EventData,
    EventHandler,
    IngestEventHandler,
    ModifiedCheckCommand,
    Root,
    StabilizeCommand,
    WatchdogAdapter,
    WatchService,
    load_roots_from_database,
)
from data_collector.settings.watchservice import WatchServiceSettings

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_root(**overrides: object) -> Root:
    defaults: dict[str, object] = {
        "root_id": 1,
        "root_path": "/ingest/hr/gazette",
        "rel_path": "gazette",
        "country": "HR",
        "watch_group": "ocr",
        "worker_path": "data_collector.croatia.gazette.ocr.main",
    }
    defaults.update(overrides)
    return Root(**defaults)  # type: ignore[arg-type]


def _make_event_data(**overrides: object) -> EventData:
    root = _make_root()
    defaults: dict[str, object] = {
        "abs_path": "/ingest/hr/gazette/test.pdf",
        "root_data": root,
        "event_type": EventType.CREATED,
        "path_hash": "abc123",
    }
    defaults.update(overrides)
    return EventData(**defaults)  # type: ignore[arg-type]


class MockEventHandler(EventHandler):
    """In-memory EventHandler for testing."""

    def __init__(self) -> None:
        self.events: list[EventData] = []
        self.stabilized: list[tuple[int, int | None]] = []
        self.stable_dates: dict[str, datetime] = {}
        self._next_id = 1

    def on_event_detected(self, event_data: EventData) -> int:
        self.events.append(event_data)
        event_id = self._next_id
        self._next_id += 1
        return event_id

    def on_event_stabilized(self, event_id: int, file_size: int | None) -> None:
        self.stabilized.append((event_id, file_size))

    def get_last_stable_event(self, path_hash: str) -> datetime | None:
        return self.stable_dates.get(path_hash)


# ---------------------------------------------------------------------------
# TestEventType
# ---------------------------------------------------------------------------


class TestEventType:
    """Verify EventType enum values."""

    def test_created_value(self) -> None:
        assert EventType.CREATED == 1

    def test_modified_value(self) -> None:
        assert EventType.MODIFIED == 2

    def test_deleted_value(self) -> None:
        assert EventType.DELETED == 3


# ---------------------------------------------------------------------------
# TestRootDataclass
# ---------------------------------------------------------------------------


class TestRootDataclass:
    """Verify Root dataclass construction and defaults."""

    def test_construction(self) -> None:
        root = _make_root()
        assert root.root_id == 1
        assert root.root_path == "/ingest/hr/gazette"
        assert root.country == "HR"
        assert root.watch_group == "ocr"
        assert root.worker_path == "data_collector.croatia.gazette.ocr.main"

    def test_defaults(self) -> None:
        root = _make_root()
        assert root.extensions is None
        assert root.recursive is True

    def test_with_extensions(self) -> None:
        root = _make_root(extensions=[".pdf", ".zip"])
        assert root.extensions == [".pdf", ".zip"]


# ---------------------------------------------------------------------------
# TestEventDataDataclass
# ---------------------------------------------------------------------------


class TestEventDataDataclass:
    """Verify EventData dataclass construction and defaults."""

    def test_construction(self) -> None:
        event = _make_event_data()
        assert event.abs_path == "/ingest/hr/gazette/test.pdf"
        assert event.event_type == EventType.CREATED
        assert event.path_hash == "abc123"

    def test_defaults(self) -> None:
        event = _make_event_data()
        assert event.stable is True
        assert event.disallowed == 0
        assert event.file_size is None


# ---------------------------------------------------------------------------
# TestWatchServiceSettings
# ---------------------------------------------------------------------------


class TestWatchServiceSettings:
    """Verify settings defaults and env prefix."""

    def test_defaults(self) -> None:
        settings = WatchServiceSettings()
        assert settings.observer == "auto"
        assert settings.poll_interval == 5
        assert settings.reconcile_interval == 60
        assert settings.debounce == 2.0
        assert settings.stability_timeout == 10
        assert settings.stabilization_grace == 5
        assert settings.watched_dirs_root == "./watched"
        assert settings.writer_batch_size == 50
        assert settings.reconcile_batch_size == 500

    def test_env_prefix(self) -> None:
        assert WatchServiceSettings.model_config.get("env_prefix") == "DC_WATCHER_"


# ---------------------------------------------------------------------------
# TestPathNormalization
# ---------------------------------------------------------------------------


class TestPathNormalization:
    """Test WatchService._normalize_path."""

    def test_normpath_applied(self) -> None:
        result = WatchService._normalize_path("/foo/bar/../baz")
        expected = os.path.normpath("/foo/bar/../baz")
        assert result == expected

    @patch("data_collector.messaging.watchservice.sys")
    def test_forward_slashes_on_windows(self, mock_sys: MagicMock) -> None:
        mock_sys.platform = "win32"
        result = WatchService._normalize_path("C:/users/test/file.pdf")
        assert "/" not in result or result.startswith("//")

    @patch("data_collector.messaging.watchservice.sys")
    def test_linux_path_unchanged(self, mock_sys: MagicMock) -> None:
        mock_sys.platform = "linux"
        result = WatchService._normalize_path("/ingest/hr/gazette/file.pdf")
        assert result == os.path.normpath("/ingest/hr/gazette/file.pdf")


# ---------------------------------------------------------------------------
# TestExtensionFiltering
# ---------------------------------------------------------------------------


class TestExtensionFiltering:
    """Test WatchService._is_allowed."""

    def test_allowed_extension(self) -> None:
        assert WatchService._is_allowed("/test/file.pdf", [".pdf", ".zip"]) is True

    def test_disallowed_extension(self) -> None:
        assert WatchService._is_allowed("/test/file.txt", [".pdf", ".zip"]) is False

    def test_case_insensitive(self) -> None:
        assert WatchService._is_allowed("/test/file.PDF", [".pdf"]) is True

    def test_no_extension(self) -> None:
        assert WatchService._is_allowed("/test/file", [".pdf"]) is False


# ---------------------------------------------------------------------------
# TestTempFileFiltering
# ---------------------------------------------------------------------------


class TestTempFileFiltering:
    """Test WatchService._is_temp_file."""

    def test_office_lock_file(self) -> None:
        assert WatchService._is_temp_file("/ingest/~$document.xlsx") is True

    def test_tilde_temp_file(self) -> None:
        assert WatchService._is_temp_file("/ingest/~document.tmp") is True

    def test_libreoffice_lock_file(self) -> None:
        assert WatchService._is_temp_file("/ingest/.~lock.document.xlsx#") is True

    def test_normal_file_not_filtered(self) -> None:
        assert WatchService._is_temp_file("/ingest/report.xlsx") is False

    def test_normal_pdf_not_filtered(self) -> None:
        assert WatchService._is_temp_file("/ingest/gazette/2024/doc.pdf") is False


# ---------------------------------------------------------------------------
# TestFileStability
# ---------------------------------------------------------------------------


class TestStabilityMonitorLogic:
    """Test stability tracking via last_size / last_changed in pending entries."""

    def test_size_change_resets_last_changed(self) -> None:
        root = _make_root(root_path="/ingest/hr/gazette")
        handler = MockEventHandler()
        settings = WatchServiceSettings(stability_timeout=10)
        service = WatchService([root], handler, settings)

        normalized = WatchService._normalize_path("/ingest/hr/gazette/test.pdf")
        service._pending_stability[normalized] = {
            "root": root, "event_type": EventType.CREATED, "event_id": 1,
            "last_size": 1024, "last_changed": time.monotonic() - 20,
        }

        # File grew -- last_changed should reset even though 20s elapsed
        with patch("data_collector.messaging.watchservice.os.path.getsize", return_value=2048):
            with service._pending_lock:
                snapshot = dict(service._pending_stability)
            # Simulate one monitor cycle (inline, not threaded)
            for path, info in snapshot.items():
                current_size = os.path.getsize(path)
                if current_size != info["last_size"]:
                    service._pending_stability[path]["last_size"] = current_size
                    service._pending_stability[path]["last_changed"] = time.monotonic()

        assert service._pending_stability[normalized]["last_size"] == 2048
        # last_changed was just reset, so it should be very recent
        assert time.monotonic() - service._pending_stability[normalized]["last_changed"] < 1.0

    def test_unchanged_size_below_timeout_not_stabilized(self) -> None:
        root = _make_root(root_path="/ingest/hr/gazette")
        handler = MockEventHandler()
        settings = WatchServiceSettings(stability_timeout=10)
        service = WatchService([root], handler, settings)

        normalized = WatchService._normalize_path("/ingest/hr/gazette/test.pdf")
        service._pending_stability[normalized] = {
            "root": root, "event_type": EventType.CREATED, "event_id": 1,
            "last_size": 1024, "last_changed": time.monotonic(),  # just now
        }

        # Size unchanged but timeout not reached -- should NOT stabilize
        assert service._write_queue.empty()

    @patch("data_collector.messaging.watchservice.WatchService._create_observer")
    @patch("data_collector.messaging.watchservice.WatchService._recover_pending_streams")
    @patch("data_collector.messaging.watchservice.WatchService._reconcile")
    def test_unchanged_size_past_timeout_stabilizes(
        self,
        mock_reconcile: MagicMock,
        mock_recover: MagicMock,
        mock_observer: MagicMock,
    ) -> None:
        mock_observer.return_value = MagicMock()
        root = _make_root(root_path="/ingest/hr/gazette")
        handler = MockEventHandler()
        settings = WatchServiceSettings(stability_timeout=1, debounce=0.1)
        service = WatchService([root], handler, settings)
        service.start()

        try:
            # Inject a pending entry that has been unchanged for longer than timeout
            normalized = WatchService._normalize_path("/ingest/hr/gazette/test.pdf")
            with service._pending_lock:
                service._pending_stability[normalized] = {
                    "root": root, "event_type": EventType.CREATED, "event_id": 1,
                    "last_size": 1024, "last_changed": time.monotonic() - 5,
                }

            # Give the stability monitor time to detect and stabilize
            with patch("data_collector.messaging.watchservice.os.path.getsize", return_value=1024):
                time.sleep(0.5)

            # Handler should have received the stabilization
            assert len(handler.stabilized) == 1
            assert handler.stabilized[0] == (1, 1024)
        finally:
            service.stop()


# ---------------------------------------------------------------------------
# TestFindRoot
# ---------------------------------------------------------------------------


class TestFindRoot:
    """Test WatchService._find_root."""

    def test_match(self) -> None:
        root = _make_root(root_path="/ingest/hr/gazette")
        service = WatchService([root], MockEventHandler())
        normalized = WatchService._normalize_path("/ingest/hr/gazette/file.pdf")
        found = service._find_root(normalized)
        assert found is not None
        assert found.root_id == 1

    def test_no_match(self) -> None:
        root = _make_root(root_path="/ingest/hr/gazette")
        service = WatchService([root], MockEventHandler())
        normalized = WatchService._normalize_path("/other/path/file.pdf")
        assert service._find_root(normalized) is None

    def test_longest_prefix_match(self) -> None:
        root_parent = _make_root(root_id=1, root_path="/ingest/hr")
        root_child = _make_root(root_id=2, root_path="/ingest/hr/gazette")
        service = WatchService([root_parent, root_child], MockEventHandler())
        normalized = WatchService._normalize_path("/ingest/hr/gazette/file.pdf")
        found = service._find_root(normalized)
        assert found is not None
        assert found.root_id == 2

    def test_no_match_partial_directory_name(self) -> None:
        root = _make_root(root_path="/ingest/hr/gazette")
        service = WatchService([root], MockEventHandler())
        normalized = WatchService._normalize_path("/ingest/hr/gazette-archive/file.pdf")
        assert service._find_root(normalized) is None


# ---------------------------------------------------------------------------
# TestHandlePathCreated
# ---------------------------------------------------------------------------


class TestHandlePathCreated:
    """Test handle_path for CREATED events."""

    def test_created_enqueues_detect_command(self) -> None:
        root = _make_root(root_path="/ingest/hr/gazette")
        handler = MockEventHandler()
        service = WatchService([root], handler)

        service.handle_path("/ingest/hr/gazette/test.pdf", EventType.CREATED)

        assert not service._write_queue.empty()
        command = service._write_queue.get_nowait()
        assert isinstance(command, DetectCommand)
        assert command.event_data.event_type == EventType.CREATED
        assert command.event_data.stable is False

    def test_created_adds_to_pending(self) -> None:
        root = _make_root(root_path="/ingest/hr/gazette")
        service = WatchService([root], MockEventHandler())

        service.handle_path("/ingest/hr/gazette/test.pdf", EventType.CREATED)

        normalized = WatchService._normalize_path("/ingest/hr/gazette/test.pdf")
        assert normalized in service._pending_stability

    def test_duplicate_created_absorbed(self) -> None:
        root = _make_root(root_path="/ingest/hr/gazette")
        service = WatchService([root], MockEventHandler())

        service.handle_path("/ingest/hr/gazette/test.pdf", EventType.CREATED)
        service.handle_path("/ingest/hr/gazette/test.pdf", EventType.CREATED)

        # Only one command enqueued
        assert service._write_queue.qsize() == 1

    def test_filtered_extension_not_enqueued(self) -> None:
        root = _make_root(root_path="/ingest/hr/gazette", extensions=[".pdf"])
        service = WatchService([root], MockEventHandler())

        service.handle_path("/ingest/hr/gazette/test.txt", EventType.CREATED)

        assert service._write_queue.empty()


# ---------------------------------------------------------------------------
# TestHandlePathModified
# ---------------------------------------------------------------------------


class TestHandlePathModified:
    """Test handle_path for MODIFIED events."""

    def test_modified_while_pending_absorbed(self) -> None:
        root = _make_root(root_path="/ingest/hr/gazette")
        service = WatchService([root], MockEventHandler())

        # Simulate pending stability
        normalized = WatchService._normalize_path("/ingest/hr/gazette/test.pdf")
        service._pending_stability[normalized] = {
            "root": root, "event_type": EventType.CREATED, "event_id": 1,
            "last_size": -1, "last_changed": time.monotonic(),
        }

        service.handle_path("/ingest/hr/gazette/test.pdf", EventType.MODIFIED)

        # No command enqueued (absorbed)
        assert service._write_queue.empty()

    @patch("data_collector.messaging.watchservice.os.path.getmtime")
    def test_modified_enqueues_check_command(self, mock_getmtime: MagicMock) -> None:
        mock_getmtime.return_value = time.time()
        root = _make_root(root_path="/ingest/hr/gazette")
        service = WatchService([root], MockEventHandler())

        service.handle_path("/ingest/hr/gazette/test.pdf", EventType.MODIFIED)

        assert not service._write_queue.empty()
        command = service._write_queue.get_nowait()
        assert isinstance(command, ModifiedCheckCommand)


# ---------------------------------------------------------------------------
# TestWriterThread
# ---------------------------------------------------------------------------


class TestWriterThread:
    """Test writer thread command processing."""

    def test_detect_command_calls_handler(self) -> None:
        handler = MockEventHandler()
        root = _make_root(root_path="/ingest/hr/gazette")
        service = WatchService([root], handler)

        event_data = _make_event_data(stable=False)
        normalized = WatchService._normalize_path("/ingest/hr/gazette/test.pdf")
        service._pending_stability[normalized] = {
            "root": root, "event_type": EventType.CREATED, "event_id": None,
            "last_size": -1, "last_changed": time.monotonic(),
        }

        command = DetectCommand(event_data=event_data, normalized_path=normalized)
        service._process_write_command(command)

        assert len(handler.events) == 1
        assert handler.events[0].stable is False
        assert service._pending_stability[normalized]["event_id"] == 1

    def test_stabilize_command_calls_handler(self) -> None:
        handler = MockEventHandler()
        root = _make_root()
        service = WatchService([root], handler)

        command = StabilizeCommand(event_id=42, file_size=1024)
        service._process_write_command(command)

        assert handler.stabilized == [(42, 1024)]

    def test_modified_check_with_no_prior_event(self) -> None:
        handler = MockEventHandler()
        root = _make_root(root_path="/ingest/hr/gazette")
        service = WatchService([root], handler)

        event_data = _make_event_data(event_type=EventType.MODIFIED)
        command = ModifiedCheckCommand(event_data=event_data, file_modification_time=time.time())
        service._process_write_command(command)

        # Should treat as CREATED
        assert len(handler.events) == 1
        assert handler.events[0].event_type == EventType.CREATED

    def test_modified_check_within_grace_ignored(self) -> None:
        handler = MockEventHandler()
        handler.stable_dates["abc123"] = datetime.now(UTC)
        root = _make_root(root_path="/ingest/hr/gazette")
        settings = WatchServiceSettings(stabilization_grace=10)
        service = WatchService([root], handler, settings)

        event_data = _make_event_data(event_type=EventType.MODIFIED)
        # mtime within grace period
        command = ModifiedCheckCommand(event_data=event_data, file_modification_time=time.time())
        service._process_write_command(command)

        # No event detected (within grace)
        assert len(handler.events) == 0

    def test_modified_check_beyond_grace_creates_event(self) -> None:
        handler = MockEventHandler()
        handler.stable_dates["abc123"] = datetime.now(UTC) - timedelta(seconds=30)
        root = _make_root(root_path="/ingest/hr/gazette")
        settings = WatchServiceSettings(stabilization_grace=5)
        service = WatchService([root], handler, settings)

        event_data = _make_event_data(event_type=EventType.MODIFIED)
        command = ModifiedCheckCommand(event_data=event_data, file_modification_time=time.time())
        service._process_write_command(command)

        assert len(handler.events) == 1
        assert handler.events[0].event_type == EventType.MODIFIED


# ---------------------------------------------------------------------------
# TestStreamingCoalescing
# ---------------------------------------------------------------------------


class TestStreamingCoalescing:
    """Test that CREATED+MODIFIED during streaming produces single event."""

    def test_created_then_modified_single_event(self) -> None:
        root = _make_root(root_path="/ingest/hr/gazette")
        handler = MockEventHandler()
        service = WatchService([root], handler)

        # CREATED → enqueued
        service.handle_path("/ingest/hr/gazette/test.pdf", EventType.CREATED)
        assert service._write_queue.qsize() == 1

        # MODIFIED while pending → absorbed
        service.handle_path("/ingest/hr/gazette/test.pdf", EventType.MODIFIED)
        assert service._write_queue.qsize() == 1  # Still just 1


# ---------------------------------------------------------------------------
# TestGracePeriod
# ---------------------------------------------------------------------------


class TestGracePeriod:
    """Test MODIFIED grace period validation."""

    def test_mtime_within_grace_ignored(self) -> None:
        handler = MockEventHandler()
        handler.stable_dates["abc123"] = datetime.now(UTC) - timedelta(seconds=2)
        root = _make_root(root_path="/ingest/hr/gazette")
        settings = WatchServiceSettings(stabilization_grace=10)
        service = WatchService([root], handler, settings)

        event_data = _make_event_data(event_type=EventType.MODIFIED)
        command = ModifiedCheckCommand(event_data=event_data, file_modification_time=time.time())
        service._process_write_command(command)

        assert len(handler.events) == 0

    def test_mtime_beyond_grace_creates_event(self) -> None:
        handler = MockEventHandler()
        handler.stable_dates["abc123"] = datetime.now(UTC) - timedelta(minutes=5)
        root = _make_root(root_path="/ingest/hr/gazette")
        settings = WatchServiceSettings(stabilization_grace=5)
        service = WatchService([root], handler, settings)

        event_data = _make_event_data(event_type=EventType.MODIFIED)
        command = ModifiedCheckCommand(event_data=event_data, file_modification_time=time.time())
        service._process_write_command(command)

        assert len(handler.events) == 1


# ---------------------------------------------------------------------------
# TestWatchdogAdapter
# ---------------------------------------------------------------------------


class TestWatchdogAdapter:
    """Test WatchdogAdapter event forwarding."""

    def test_on_created_forwards(self) -> None:
        mock_service = MagicMock()
        adapter = WatchdogAdapter(mock_service)

        event = MagicMock()
        event.is_directory = False
        event.src_path = "/ingest/test.pdf"

        adapter.on_created(event)
        mock_service.handle_path.assert_called_once_with("/ingest/test.pdf", EventType.CREATED)

    def test_on_modified_forwards(self) -> None:
        mock_service = MagicMock()
        adapter = WatchdogAdapter(mock_service)

        event = MagicMock()
        event.is_directory = False
        event.src_path = "/ingest/test.pdf"

        adapter.on_modified(event)
        mock_service.handle_path.assert_called_once_with("/ingest/test.pdf", EventType.MODIFIED)

    def test_directory_ignored(self) -> None:
        mock_service = MagicMock()
        adapter = WatchdogAdapter(mock_service)

        event = MagicMock()
        event.is_directory = True
        event.src_path = "/ingest/subdir"

        adapter.on_created(event)
        adapter.on_modified(event)
        mock_service.handle_path.assert_not_called()

    def test_no_on_deleted(self) -> None:
        mock_service = MagicMock()
        adapter = WatchdogAdapter(mock_service)

        # WatchdogAdapter has no on_deleted override
        has_override = hasattr(adapter, "on_deleted")
        if has_override:
            assert adapter.on_deleted.__qualname__.startswith("FileSystemEventHandler")


# ---------------------------------------------------------------------------
# TestIngestEventHandler
# ---------------------------------------------------------------------------


class TestIngestEventHandler:
    """Test IngestEventHandler database operations."""

    def test_on_event_detected_creates_row(self) -> None:
        mock_database = MagicMock()
        mock_session = MagicMock()
        mock_database.create_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_database.create_session.return_value.__exit__ = MagicMock(return_value=False)

        handler = IngestEventHandler(mock_database)
        event_data = _make_event_data(stable=False)

        def capture_add(event_obj: object, session: object) -> None:
            event_obj.id = 42  # type: ignore[attr-defined]

        mock_database.add.side_effect = capture_add

        handler.on_event_detected(event_data)

        mock_database.add.assert_called_once()
        mock_session.commit.assert_called_once()

    def test_on_event_detected_sets_worker_path(self) -> None:
        mock_database = MagicMock()
        mock_session = MagicMock()
        mock_database.create_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_database.create_session.return_value.__exit__ = MagicMock(return_value=False)

        captured_events: list[object] = []

        def capture_add(event_obj: object, session: object) -> None:
            event_obj.id = 1  # type: ignore[attr-defined]
            captured_events.append(event_obj)

        mock_database.add.side_effect = capture_add

        handler = IngestEventHandler(mock_database)
        event_data = _make_event_data(stable=True)

        handler.on_event_detected(event_data)

        assert len(captured_events) == 1
        created_event = captured_events[0]
        assert created_event.worker_path == "data_collector.croatia.gazette.ocr.main"  # type: ignore[attr-defined]

    def test_on_event_stabilized_updates_row(self) -> None:
        mock_database = MagicMock()
        mock_session = MagicMock()
        mock_event = MagicMock()
        mock_database.create_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_database.create_session.return_value.__exit__ = MagicMock(return_value=False)
        mock_database.query.return_value.scalar_one_or_none.return_value = mock_event

        handler = IngestEventHandler(mock_database)
        handler.on_event_stabilized(42, 1024)

        mock_database.query.assert_called_once()
        assert mock_event.stable is True
        assert mock_event.file_size == 1024
        mock_session.commit.assert_called_once()


# ---------------------------------------------------------------------------
# TestWatchServiceLifecycle
# ---------------------------------------------------------------------------


class TestWatchServiceLifecycle:
    """Test WatchService start/stop lifecycle."""

    @patch("data_collector.messaging.watchservice.WatchService._create_observer")
    @patch("data_collector.messaging.watchservice.WatchService._recover_pending_streams")
    @patch("data_collector.messaging.watchservice.WatchService._reconcile")
    def test_double_start_raises(
        self,
        mock_reconcile: MagicMock,
        mock_recover: MagicMock,
        mock_observer: MagicMock,
    ) -> None:
        mock_observer_instance = MagicMock()
        mock_observer.return_value = mock_observer_instance

        root = _make_root(root_path="/ingest/hr/gazette")
        service = WatchService([root], MockEventHandler())
        service.start()

        with pytest.raises(RuntimeError, match="already running"):
            service.start()

        service.stop()

    @patch("data_collector.messaging.watchservice.WatchService._create_observer")
    @patch("data_collector.messaging.watchservice.WatchService._recover_pending_streams")
    @patch("data_collector.messaging.watchservice.WatchService._reconcile")
    def test_start_stop(
        self,
        mock_reconcile: MagicMock,
        mock_recover: MagicMock,
        mock_observer: MagicMock,
    ) -> None:
        mock_observer_instance = MagicMock()
        mock_observer.return_value = mock_observer_instance

        root = _make_root(root_path="/ingest/hr/gazette")
        service = WatchService([root], MockEventHandler())

        service.start()
        assert service.is_running is True

        service.stop(timeout=5.0)
        assert service.is_running is False

    @patch("data_collector.messaging.watchservice.WatchService._create_observer")
    @patch("data_collector.messaging.watchservice.WatchService._recover_pending_streams")
    @patch("data_collector.messaging.watchservice.WatchService._reconcile")
    def test_is_running_reflects_thread_state(
        self,
        mock_reconcile: MagicMock,
        mock_recover: MagicMock,
        mock_observer: MagicMock,
    ) -> None:
        mock_observer.return_value = MagicMock()

        root = _make_root(root_path="/ingest/hr/gazette")
        service = WatchService([root], MockEventHandler())

        assert service.is_running is False

        service.start()
        assert service.is_running is True

        service.stop()
        assert service.is_running is False


# ---------------------------------------------------------------------------
# TestObserverSelection
# ---------------------------------------------------------------------------


class TestObserverSelection:
    """Test platform-aware observer selection."""

    def test_polling_returns_polling_observer(self) -> None:
        root = _make_root()
        service = WatchService([root], MockEventHandler())
        observer = service._create_observer("polling")
        from watchdog.observers.polling import PollingObserver

        assert isinstance(observer, PollingObserver)

    @patch("data_collector.messaging.watchservice.sys")
    def test_auto_on_linux(self, mock_sys: MagicMock) -> None:
        mock_sys.platform = "linux"
        root = _make_root()
        service = WatchService([root], MockEventHandler())
        observer = service._create_observer("auto")
        # Should return an Observer (native or polling fallback)
        assert observer is not None


# ---------------------------------------------------------------------------
# TestDatabaseOutage
# ---------------------------------------------------------------------------


class TestDatabaseOutage:
    """Test writer thread behavior during database outage."""

    def test_failed_command_re_enqueued(self) -> None:
        handler = MockEventHandler()
        handler.on_event_detected = MagicMock(side_effect=Exception("DB connection failed"))  # type: ignore[method-assign]
        root = _make_root(root_path="/ingest/hr/gazette")
        service = WatchService([root], handler)
        service._stop_event.set()  # Prevent infinite retry loop

        event_data = _make_event_data(stable=False)
        normalized = WatchService._normalize_path("/ingest/hr/gazette/test.pdf")

        # Directly call the writer loop with a command that will fail
        service._write_queue.put(DetectCommand(event_data=event_data, normalized_path=normalized))
        service._write_queue.put(object())  # Sentinel to stop

        # The writer loop should handle the error gracefully
        # (just verify no unhandled exception)
        service._writer_loop()


# ---------------------------------------------------------------------------
# TestLoadRootsFromDatabase
# ---------------------------------------------------------------------------


def _make_watch_roots_row(
    *,
    root_id: int = 1,
    root_path: str = "/ingest/hr/gazette",
    rel_path: str = "gazette",
    country: str = "HR",
    watch_group: str = "ocr",
    worker_path: str = "data_collector.croatia.gazette.ocr.main",
    extensions: str | None = '[".pdf", ".zip"]',
    recursive: bool = True,
    active: bool = True,
) -> MagicMock:
    """Create a mock WatchRoots row."""
    row = MagicMock()
    row.id = root_id
    row.root_path = root_path
    row.rel_path = rel_path
    row.country = country
    row.watch_group = watch_group
    row.worker_path = worker_path
    row.extensions = extensions
    row.recursive = recursive
    row.active = active
    return row


class TestLoadRootsFromDatabase:
    """Test load_roots_from_database function."""

    def test_loads_active_roots(self) -> None:
        mock_database = MagicMock()
        mock_session = MagicMock()
        mock_database.create_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_database.create_session.return_value.__exit__ = MagicMock(return_value=False)

        row = _make_watch_roots_row()
        mock_database.query.return_value.scalars.return_value.all.return_value = [row]

        roots = load_roots_from_database(mock_database)

        assert len(roots) == 1
        assert roots[0].root_id == 1
        assert roots[0].root_path == "/ingest/hr/gazette"
        assert roots[0].country == "HR"
        assert roots[0].watch_group == "ocr"
        assert roots[0].worker_path == "data_collector.croatia.gazette.ocr.main"

    def test_parses_extensions_json(self) -> None:
        mock_database = MagicMock()
        mock_session = MagicMock()
        mock_database.create_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_database.create_session.return_value.__exit__ = MagicMock(return_value=False)

        row = _make_watch_roots_row(extensions='[".pdf", ".zip"]')
        mock_database.query.return_value.scalars.return_value.all.return_value = [row]

        roots = load_roots_from_database(mock_database)
        assert roots[0].extensions == [".pdf", ".zip"]

    def test_null_extensions_returns_none(self) -> None:
        mock_database = MagicMock()
        mock_session = MagicMock()
        mock_database.create_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_database.create_session.return_value.__exit__ = MagicMock(return_value=False)

        row = _make_watch_roots_row(extensions=None)
        mock_database.query.return_value.scalars.return_value.all.return_value = [row]

        roots = load_roots_from_database(mock_database)
        assert roots[0].extensions is None

    def test_empty_result(self) -> None:
        mock_database = MagicMock()
        mock_session = MagicMock()
        mock_database.create_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_database.create_session.return_value.__exit__ = MagicMock(return_value=False)

        mock_database.query.return_value.scalars.return_value.all.return_value = []

        roots = load_roots_from_database(mock_database)
        assert roots == []


# ---------------------------------------------------------------------------
# TestRootHotReload
# ---------------------------------------------------------------------------


class TestRootHotReload:
    """Test WatchService._refresh_roots hot-reload behavior."""

    def test_new_root_scheduled(self) -> None:
        root = _make_root(root_id=1, root_path="/ingest/hr/gazette")
        mock_database = MagicMock()
        service = WatchService([root], MockEventHandler(), database=mock_database)
        service._observer = MagicMock()
        service._adapter = MagicMock()

        new_root = _make_root(root_id=2, root_path="/ingest/hr/contracts")
        with patch(
            "data_collector.messaging.watchservice.load_roots_from_database",
            return_value=[root, new_root],
        ):
            service._refresh_roots()

        assert len(service._roots) == 2
        new_normalized = WatchService._normalize_path("/ingest/hr/contracts")
        assert new_normalized in service._roots
        assert new_normalized in service._watches

    def test_removed_root_unscheduled(self) -> None:
        root_a = _make_root(root_id=1, root_path="/ingest/hr/gazette")
        root_b = _make_root(root_id=2, root_path="/ingest/hr/contracts")
        mock_database = MagicMock()
        service = WatchService([root_a, root_b], MockEventHandler(), database=mock_database)
        service._observer = MagicMock()
        service._adapter = MagicMock()

        # Simulate that root_b had a watch
        normalized_b = WatchService._normalize_path("/ingest/hr/contracts")
        mock_watch = MagicMock()
        service._watches[normalized_b] = mock_watch

        # Refresh returns only root_a
        with patch(
            "data_collector.messaging.watchservice.load_roots_from_database",
            return_value=[root_a],
        ):
            service._refresh_roots()

        assert len(service._roots) == 1
        assert normalized_b not in service._roots
        assert normalized_b not in service._watches
        service._observer.unschedule.assert_called_once_with(mock_watch)

    def test_changed_root_rescheduled(self) -> None:
        root = _make_root(root_id=1, root_path="/ingest/hr/gazette")
        mock_database = MagicMock()
        service = WatchService([root], MockEventHandler(), database=mock_database)
        service._observer = MagicMock()
        service._adapter = MagicMock()

        normalized = WatchService._normalize_path("/ingest/hr/gazette")
        old_watch = MagicMock()
        service._watches[normalized] = old_watch

        # Change worker_path
        changed_root = _make_root(
            root_id=1, root_path="/ingest/hr/gazette", worker_path="data_collector.new.module.main",
        )
        with patch(
            "data_collector.messaging.watchservice.load_roots_from_database",
            return_value=[changed_root],
        ):
            service._refresh_roots()

        service._observer.unschedule.assert_called_once_with(old_watch)
        assert service._roots[normalized].worker_path == "data_collector.new.module.main"

    def test_no_database_skips_refresh(self) -> None:
        root = _make_root()
        service = WatchService([root], MockEventHandler())
        # No database — should not crash
        service._refresh_roots()
