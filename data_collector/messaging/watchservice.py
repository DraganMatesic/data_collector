"""File system monitoring service for pipeline event production.

WatchService monitors directories for new and modified files, writing events
to the ``Events`` table.  It is an event producer -- the ``TaskDispatcher``
polls the Events table and handles Dramatiq dispatch.

The service uses a non-blocking architecture with four daemon threads:
watchdog observer (OS events), writer (DB operations), stability monitor
(deferred file checks), and reconciliation scanner (catch-all safety net).

Watched directories are hot folders -- temporary holding areas.  Workers
move processed files to permanent storage and delete the source from the
hot folder.
"""

from __future__ import annotations

import json
import logging
import os
import queue
import sys
import threading
import time
from abc import ABC, abstractmethod
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

from sqlalchemy import select
from watchdog.events import FileSystemEvent, FileSystemEventHandler
from watchdog.observers import Observer
from watchdog.observers.polling import PollingObserver

from data_collector.enums.pipeline import EventType
from data_collector.settings.watchservice import WatchServiceSettings
from data_collector.tables.pipeline import Events, WatchRoots
from data_collector.utilities.database.main import Database
from data_collector.utilities.functions.runtime import make_hash

logger = logging.getLogger(__name__)

# Sentinel object to signal the writer thread to drain and exit
_SENTINEL = object()


# ---------------------------------------------------------------------------
# Root loading
# ---------------------------------------------------------------------------


def load_roots_from_database(database: Database) -> list[Root]:
    """Query active WatchRoots and construct Root dataclass instances.

    Loads rows where ``active=True`` and ``archive IS NULL`` from the
    ``watch_roots`` table.  Extensions stored as a JSON array in the
    ``extensions`` text column are parsed into ``list[str]``.

    Args:
        database: Database instance for session creation.

    Returns:
        List of Root instances for active, non-archived watch roots.
    """
    with database.create_session() as session:
        statement = select(WatchRoots).where(
            WatchRoots.active.is_(True),
            WatchRoots.archive.is_(None),
        )
        rows = database.query(statement, session).scalars().all()

    roots: list[Root] = []
    for row in rows:
        record: Any = row
        extensions_raw: str | None = str(record.extensions) if record.extensions is not None else None
        parsed_extensions: list[str] | None = None
        if extensions_raw is not None:
            parsed_extensions = json.loads(extensions_raw)

        roots.append(
            Root(
                root_id=int(record.id),
                root_path=str(record.root_path),
                rel_path=str(record.rel_path),
                country=str(record.country),
                watch_group=str(record.watch_group),
                worker_path=str(record.worker_path),
                extensions=parsed_extensions,
                recursive=bool(record.recursive),
            )
        )
    return roots


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------


@dataclass
class Root:
    """Configuration for a single watched directory root.

    Args:
        root_id: Unique numeric identifier for this root.
        root_path: Absolute path to the watched directory.
        rel_path: Relative path identifier for routing.
        country: Country code for pipeline routing (e.g., "HR").
        watch_group: Logical grouping (e.g., "ocr", "ingest").
        worker_path: Python module path for Dramatiq actor import (e.g.,
            "data_collector.croatia.gazette.ocr.main").  TaskDispatcher
            dynamically imports this module to read ``MAIN_EXCHANGE_QUEUE``.
        extensions: Allowed file extensions (None = all files).
        recursive: Whether to watch subdirectories.
    """

    root_id: int
    root_path: str
    rel_path: str
    country: str
    watch_group: str
    worker_path: str
    extensions: list[str] | None = None
    recursive: bool = True


@dataclass
class EventData:
    """Rich event context passed between WatchService layers.

    Args:
        abs_path: Absolute path to the detected file.
        root_data: Root configuration that owns this file.
        event_type: Type of file system event.
        path_hash: SHA hash of the normalized file path.
        stable: Whether the file has finished writing.
        disallowed: 1 if extension is not in root's allow list.
        file_size: File size in bytes (None if unknown).
    """

    abs_path: str
    root_data: Root
    event_type: EventType
    path_hash: str
    stable: bool = True
    disallowed: int = 0
    file_size: int | None = None


# ---------------------------------------------------------------------------
# Write commands (internal queue protocol)
# ---------------------------------------------------------------------------


@dataclass
class DetectCommand:
    """Enqueued by handle_path for CREATED events."""

    event_data: EventData
    normalized_path: str


@dataclass
class StabilizeCommand:
    """Enqueued by stability monitor when a file becomes stable."""

    event_id: int
    file_size: int | None


@dataclass
class ModifiedCheckCommand:
    """Enqueued by handle_path for MODIFIED events (needs DB query)."""

    event_data: EventData
    file_modification_time: float


# ---------------------------------------------------------------------------
# EventHandler ABC
# ---------------------------------------------------------------------------


class EventHandler(ABC):
    """Application-level handler for file system events.

    Supports the streaming lifecycle: events are detected (potentially
    unstable), then stabilized once the file finishes writing.
    """

    @abstractmethod
    def on_event_detected(self, event_data: EventData) -> int:
        """Create an initial event record in the Events table.

        Args:
            event_data: Rich event context including path, root, and stability.

        Returns:
            The database ID of the created event row.
        """

    @abstractmethod
    def on_event_stabilized(self, event_id: int, file_size: int | None) -> None:
        """Mark an event as stable with final metadata.

        Args:
            event_id: The database ID of the event to update.
            file_size: Final file size in bytes.
        """

    @abstractmethod
    def get_last_stable_event(self, path_hash: str) -> datetime | None:
        """Return the stabilized_date for the most recent stable event.

        Args:
            path_hash: SHA hash of the normalized file path.

        Returns:
            The stabilized_date timestamp, or None if no stable event exists.
        """


# ---------------------------------------------------------------------------
# IngestEventHandler
# ---------------------------------------------------------------------------


class IngestEventHandler(EventHandler):
    """Writes file events to the Events table for TaskDispatcher pickup.

    Each method creates its own database session via ``Database.create_session()``,
    making all operations thread-safe.

    Args:
        database: Database instance for session creation.
        app_id: Producer application identifier written to every Events row.
    """

    def __init__(self, database: Database, app_id: str | None = None) -> None:
        self.database = database
        self.app_id = app_id

    def on_event_detected(self, event_data: EventData) -> int:
        """Create an Events row for a detected file.

        If the event is immediately stable, ``stabilized_date`` is set to now.
        If unstable (streaming), ``stabilized_date`` is left as None.
        ``document_type`` is derived from the file extension.
        """
        now = datetime.now(UTC)
        _, extension = os.path.splitext(event_data.abs_path)
        document_type = extension.lstrip(".").lower() if extension else None

        with self.database.create_session() as session:
            event = Events(
                worker_path=event_data.root_data.worker_path,
                file_path=event_data.abs_path,
                document_type=document_type,
                event_type=event_data.event_type,
                path_hash=event_data.path_hash,
                country=event_data.root_data.country,
                watch_group=event_data.root_data.watch_group,
                file_size=event_data.file_size,
                stable=event_data.stable,
                stabilized_date=now if event_data.stable else None,
                app_id=self.app_id,
            )
            self.database.add(event, session)
            session.commit()
            event_id = cast(int, event.id)
            logger.debug(
                "Event detected (event_id=%d, stable=%s, path=%s)",
                event_id,
                event_data.stable,
                event_data.abs_path,
            )
            return event_id

    def on_event_stabilized(self, event_id: int, file_size: int | None) -> None:
        """Update an Events row to mark it as stable."""
        now = datetime.now(UTC)
        with self.database.create_session() as session:
            event = self.database.query(
                select(Events).where(Events.id == event_id), session,
            ).scalar_one_or_none()
            if event is None:
                logger.warning("Cannot stabilize event_id=%d: row not found", event_id)
                return
            record: Any = event
            record.stable = True
            record.stabilized_date = now
            record.file_size = file_size
            session.commit()
            logger.debug("Event stabilized (event_id=%d, file_size=%s)", event_id, file_size)

    def get_last_stable_event(self, path_hash: str) -> datetime | None:
        """Query the most recent stabilized_date for a given path_hash."""
        with self.database.create_session() as session:
            statement = (
                select(Events.stabilized_date)
                .where(
                    Events.path_hash == path_hash,
                    Events.stable.is_(True),
                    Events.archive.is_(None),
                )
                .order_by(Events.stabilized_date.desc())
                .limit(1)
            )
            result = self.database.query(statement, session).scalar_one_or_none()
            return result


# ---------------------------------------------------------------------------
# WatchdogAdapter
# ---------------------------------------------------------------------------


class WatchdogAdapter(FileSystemEventHandler):
    """Stateless adapter forwarding watchdog events to WatchService.

    Only forwards CREATED and MODIFIED events for files (not directories).
    DELETE events are ignored -- downstream actors need file content.

    Args:
        watch_service: The WatchService instance to forward events to.
    """

    def __init__(self, watch_service: WatchService) -> None:
        super().__init__()
        self.watch_service = watch_service

    def on_created(self, event: FileSystemEvent) -> None:
        """Forward file creation events."""
        if not event.is_directory:
            self.watch_service.handle_path(str(event.src_path), EventType.CREATED)

    def on_modified(self, event: FileSystemEvent) -> None:
        """Forward file modification events."""
        if not event.is_directory:
            self.watch_service.handle_path(str(event.src_path), EventType.MODIFIED)


# ---------------------------------------------------------------------------
# WatchService
# ---------------------------------------------------------------------------


class WatchService:
    """Multi-root directory watcher with streaming-aware event production.

    Monitors hot folder directories for new and modified files. Events are
    written to the Events table for TaskDispatcher pickup.  The service uses
    a non-blocking architecture -- the watchdog thread is never blocked by
    DB operations or stability checks.

    Args:
        roots: List of Root configurations for watched directories.
        event_handler: Handler for writing events to the database.
        settings: WatchService configuration.  Uses defaults if not provided.
        database: Database instance for reconciliation and startup recovery.
            Optional -- reconciliation is disabled when None.
    """

    def __init__(
        self,
        roots: list[Root],
        event_handler: EventHandler,
        settings: WatchServiceSettings | None = None,
        database: Database | None = None,
    ) -> None:
        resolved_settings = settings or WatchServiceSettings()
        self._roots: dict[str, Root] = {}
        for root in roots:
            normalized_root = self._normalize_path(root.root_path)
            self._roots[normalized_root] = root

        self._event_handler = event_handler
        self._database = database
        self._observer_type = resolved_settings.observer
        self._poll_interval = resolved_settings.poll_interval
        self._reconcile_interval = resolved_settings.reconcile_interval
        self._debounce = resolved_settings.debounce
        self._stability_timeout = resolved_settings.stability_timeout
        self._stabilization_grace = resolved_settings.stabilization_grace
        self._reconcile_batch_size = resolved_settings.reconcile_batch_size

        self._pending_stability: dict[str, dict[str, Any]] = {}
        self._pending_lock = threading.Lock()
        self._write_queue: queue.Queue[DetectCommand | StabilizeCommand | ModifiedCheckCommand | object] = queue.Queue()
        self._stop_event = threading.Event()

        self._observer: Any = None
        self._watches: dict[str, Any] = {}
        self._adapter: WatchdogAdapter | None = None
        self._writer_thread: threading.Thread | None = None
        self._stability_thread: threading.Thread | None = None
        self._reconciliation_thread: threading.Thread | None = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Start watching directories and processing events.

        Creates the watchdog observer, starts all daemon threads, recovers
        pending streams from the database, and runs an initial reconciliation
        scan to catch files added during downtime.

        Raises:
            RuntimeError: If the service is already running.
        """
        if self._writer_thread is not None and self._writer_thread.is_alive():
            raise RuntimeError("WatchService is already running")

        self._stop_event.clear()

        # 1. Start writer thread first (other threads enqueue to it)
        self._writer_thread = threading.Thread(
            target=self._writer_loop,
            daemon=True,
            name="watchservice-writer",
        )
        self._writer_thread.start()

        # 2. Recover pending streams from DB
        self._recover_pending_streams()

        # 3. Run initial reconciliation (catches files added during downtime)
        self._reconcile()

        # 4. Create and start observer
        self._observer = self._create_observer(self._observer_type)
        self._adapter = WatchdogAdapter(self)
        for normalized_path, root in self._roots.items():
            watch = self._observer.schedule(self._adapter, root.root_path, recursive=root.recursive)
            self._watches[normalized_path] = watch
            logger.info(
                "Watching directory: %s (group=%s, country=%s)",
                root.root_path,
                root.watch_group,
                root.country,
            )
        self._observer.start()

        # 5. Start background threads
        self._stability_thread = threading.Thread(
            target=self._monitor_stability,
            daemon=True,
            name="watchservice-stability",
        )
        self._stability_thread.start()

        self._reconciliation_thread = threading.Thread(
            target=self._reconciliation_loop,
            daemon=True,
            name="watchservice-reconciliation",
        )
        self._reconciliation_thread.start()

        logger.info("WatchService started (%d roots, observer=%s)", len(self._roots), self._observer_type)

    def stop(self, timeout: float = 10.0) -> None:
        """Stop watching and gracefully drain the write queue.

        Args:
            timeout: Maximum seconds to wait for each thread to exit.
        """
        self._stop_event.set()

        # Stop observer
        if self._observer is not None:
            self._observer.stop()
            self._observer.join(timeout=timeout)
            self._observer = None

        # Signal writer to drain and exit
        self._write_queue.put(_SENTINEL)

        # Join threads
        for thread_name, thread in [
            ("writer", self._writer_thread),
            ("stability", self._stability_thread),
            ("reconciliation", self._reconciliation_thread),
        ]:
            if thread is not None:
                thread.join(timeout=timeout)
                if thread.is_alive():
                    logger.warning("WatchService %s thread did not exit within %.1f seconds", thread_name, timeout)

        self._writer_thread = None
        self._stability_thread = None
        self._reconciliation_thread = None

        logger.info("WatchService stopped")

    @property
    def is_running(self) -> bool:
        """Whether the writer thread is alive."""
        return self._writer_thread is not None and self._writer_thread.is_alive()

    # ------------------------------------------------------------------
    # Event handling (called on watchdog thread -- must be non-blocking)
    # ------------------------------------------------------------------

    def handle_path(self, file_path: str, event_type: EventType) -> None:
        """Main entry point for file system events.

        Called by WatchdogAdapter on the watchdog thread.  Performs fast
        in-memory checks and enqueues commands to the writer thread for
        DB operations.  Never blocks on I/O.

        Args:
            file_path: Absolute path to the detected file.
            event_type: Type of file system event.
        """
        normalized_path = self._normalize_path(file_path)

        root = self._find_root(normalized_path)
        if root is None:
            return

        if self._is_temp_file(normalized_path):
            return

        if root.extensions and not self._is_allowed(normalized_path, root.extensions):
            return

        path_hash = cast(str, make_hash(normalized_path))

        if event_type == EventType.CREATED:
            with self._pending_lock:
                if normalized_path in self._pending_stability:
                    return  # Absorb duplicate CREATED while streaming
                self._pending_stability[normalized_path] = {
                    "root": root,
                    "event_type": event_type,
                    "event_id": None,
                    "last_size": -1,
                    "last_changed": time.monotonic(),
                }

            event_data = EventData(
                abs_path=file_path,
                root_data=root,
                event_type=event_type,
                path_hash=path_hash,
                stable=False,
            )
            self._write_queue.put(DetectCommand(event_data=event_data, normalized_path=normalized_path))

        elif event_type == EventType.MODIFIED:
            with self._pending_lock:
                if normalized_path in self._pending_stability:
                    return  # Absorb MODIFIED during streaming

            try:
                file_modification_time = os.path.getmtime(file_path)
            except OSError:
                return

            event_data = EventData(
                abs_path=file_path,
                root_data=root,
                event_type=event_type,
                path_hash=path_hash,
                stable=True,
            )
            self._write_queue.put(
                ModifiedCheckCommand(event_data=event_data, file_modification_time=file_modification_time)
            )

    # ------------------------------------------------------------------
    # Writer thread
    # ------------------------------------------------------------------

    def _writer_loop(self) -> None:
        """Drain the write queue and perform all DB operations.

        Runs as a daemon thread.  On DB connection failure, enters
        exponential backoff.  On sentinel, drains remaining items and exits.
        """
        reconnect_delay = 1

        while True:
            try:
                command = self._write_queue.get(timeout=1.0)
            except queue.Empty:
                if self._stop_event.is_set():
                    break
                continue

            if command is _SENTINEL:
                # Drain remaining items before exiting
                while not self._write_queue.empty():
                    try:
                        remaining = self._write_queue.get_nowait()
                        if remaining is not _SENTINEL:
                            self._process_write_command(remaining)
                    except queue.Empty:
                        break
                break

            try:
                self._process_write_command(command)
                reconnect_delay = 1
            except Exception:
                if self._stop_event.is_set():
                    break
                logger.exception("Writer thread DB error, retrying in %d seconds", reconnect_delay)
                self._stop_event.wait(timeout=reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, 30)
                # Re-enqueue the failed command for retry
                self._write_queue.put(command)

    def _process_write_command(self, command: object) -> None:
        """Process a single write command."""
        if isinstance(command, DetectCommand):
            event_id = self._event_handler.on_event_detected(command.event_data)
            with self._pending_lock:
                if command.normalized_path in self._pending_stability:
                    self._pending_stability[command.normalized_path]["event_id"] = event_id

        elif isinstance(command, StabilizeCommand):
            self._event_handler.on_event_stabilized(command.event_id, command.file_size)

        elif isinstance(command, ModifiedCheckCommand):
            stabilized_date = self._event_handler.get_last_stable_event(command.event_data.path_hash)
            if stabilized_date is None:
                # No prior event -- treat as CREATED
                with self._pending_lock:
                    normalized_path = self._normalize_path(command.event_data.abs_path)
                    if normalized_path in self._pending_stability:
                        return
                    self._pending_stability[normalized_path] = {
                        "root": command.event_data.root_data,
                        "event_type": EventType.CREATED,
                        "event_id": None,
                        "last_size": -1,
                    "last_changed": time.monotonic(),
                    }
                detect_data = EventData(
                    abs_path=command.event_data.abs_path,
                    root_data=command.event_data.root_data,
                    event_type=EventType.CREATED,
                    path_hash=command.event_data.path_hash,
                    stable=False,
                )
                detect_event_id = self._event_handler.on_event_detected(detect_data)
                with self._pending_lock:
                    normalized_path = self._normalize_path(command.event_data.abs_path)
                    if normalized_path in self._pending_stability:
                        self._pending_stability[normalized_path]["event_id"] = detect_event_id
            else:
                grace_cutoff = stabilized_date + timedelta(seconds=self._stabilization_grace)
                file_modification_datetime = datetime.fromtimestamp(command.file_modification_time, tz=UTC)
                if file_modification_datetime > grace_cutoff:
                    # Legitimate modification -- create new MODIFIED event (stable=True)
                    self._event_handler.on_event_detected(command.event_data)

    # ------------------------------------------------------------------
    # Stability monitor thread
    # ------------------------------------------------------------------

    def _monitor_stability(self) -> None:
        """Background thread checking deferred files until size stops changing.

        On each cycle the monitor reads the current file size for every
        pending entry.  If the size changed since the last check, the
        ``last_changed`` timestamp is reset.  When the size has not changed
        for ``stability_timeout`` seconds (default 10), the file is
        declared stable.  Files that disappear from disk are stabilized
        immediately with ``file_size=None``.
        """
        while not self._stop_event.is_set():
            # Phase 1: snapshot pending items under lock (fast)
            with self._pending_lock:
                snapshot = dict(self._pending_stability)

            # Phase 2: check sizes outside lock (no sleep, just stat calls)
            completed_paths: list[str] = []
            size_updates: dict[str, dict[str, int | float]] = {}
            now = time.monotonic()

            for normalized_path, info in snapshot.items():
                event_id = info["event_id"]
                if event_id is None:
                    continue  # Writer hasn't processed DetectCommand yet

                try:
                    current_size = os.path.getsize(normalized_path)
                except OSError:
                    # File disappeared -- stabilize with unknown size
                    self._write_queue.put(StabilizeCommand(event_id=event_id, file_size=None))
                    completed_paths.append(normalized_path)
                    continue

                last_size = info["last_size"]
                last_changed = info["last_changed"]

                if current_size != last_size:
                    # File still growing -- reset the clock
                    size_updates[normalized_path] = {
                        "last_size": current_size,
                        "last_changed": now,
                    }
                elif current_size > 0 and (now - last_changed) >= self._stability_timeout:
                    # Size unchanged for stability_timeout seconds -- stable
                    self._write_queue.put(StabilizeCommand(event_id=event_id, file_size=current_size))
                    completed_paths.append(normalized_path)

            # Phase 3: update sizes and remove completed under lock
            if size_updates or completed_paths:
                with self._pending_lock:
                    for path, updates in size_updates.items():
                        if path in self._pending_stability:
                            self._pending_stability[path]["last_size"] = updates["last_size"]
                            self._pending_stability[path]["last_changed"] = updates["last_changed"]
                    for path in completed_paths:
                        self._pending_stability.pop(path, None)

            self._stop_event.wait(timeout=self._debounce)

    # ------------------------------------------------------------------
    # Reconciliation
    # ------------------------------------------------------------------

    def _reconciliation_loop(self) -> None:
        """Background thread running periodic reconciliation and config refresh."""
        while not self._stop_event.is_set():
            self._stop_event.wait(timeout=self._reconcile_interval)
            if self._stop_event.is_set():
                break
            try:
                self._refresh_roots()
            except Exception:
                logger.exception("Root config refresh failed")
            try:
                self._reconcile()
            except Exception:
                logger.exception("Reconciliation scan failed")

    def _refresh_roots(self) -> None:
        """Reload root configurations from the database and adjust the observer.

        Compares the current in-memory roots with the database state.
        New roots are scheduled on the observer, removed roots are
        unscheduled, and changed roots are re-scheduled.
        """
        if self._database is None or self._observer is None or self._adapter is None:
            return

        fresh_roots = load_roots_from_database(self._database)
        fresh_map: dict[str, Root] = {}
        for root in fresh_roots:
            normalized = self._normalize_path(root.root_path)
            fresh_map[normalized] = root

        current_keys = set(self._roots.keys())
        fresh_keys = set(fresh_map.keys())

        # Removed roots
        for removed_key in current_keys - fresh_keys:
            if removed_key in self._watches:
                self._observer.unschedule(self._watches[removed_key])
                del self._watches[removed_key]
                logger.info("Stopped watching removed root: %s", removed_key)
            del self._roots[removed_key]

        # New roots
        for added_key in fresh_keys - current_keys:
            root = fresh_map[added_key]
            watch = self._observer.schedule(self._adapter, root.root_path, recursive=root.recursive)
            self._watches[added_key] = watch
            self._roots[added_key] = root
            logger.info(
                "Started watching new root: %s (group=%s, country=%s)",
                root.root_path,
                root.watch_group,
                root.country,
            )

        # Changed roots (same path but different config)
        for common_key in current_keys & fresh_keys:
            old_root = self._roots[common_key]
            new_root = fresh_map[common_key]
            if (
                old_root.worker_path != new_root.worker_path
                or old_root.extensions != new_root.extensions
                or old_root.recursive != new_root.recursive
                or old_root.country != new_root.country
                or old_root.watch_group != new_root.watch_group
            ):
                if common_key in self._watches:
                    self._observer.unschedule(self._watches[common_key])
                watch = self._observer.schedule(self._adapter, new_root.root_path, recursive=new_root.recursive)
                self._watches[common_key] = watch
                self._roots[common_key] = new_root
                logger.info("Re-scheduled changed root: %s", common_key)

    def _reconcile(self) -> None:
        """Full directory scan to catch files missed by the watcher.

        Scans all watched directories, computes path hashes, and queries
        the Events table in batches.  Files not found in Events are
        enqueued as new CREATED events.
        """
        if self._database is None:
            return

        for root in self._roots.values():
            if self._stop_event.is_set():
                return

            root_path = Path(root.root_path)
            if not root_path.exists():
                logger.warning("Reconciliation: root path does not exist: %s", root.root_path)
                continue

            file_iterator: Iterator[Path] = root_path.rglob("*") if root.recursive else root_path.iterdir()

            batch_paths: list[tuple[str, str, str]] = []  # (normalized_path, abs_path, path_hash)

            for file_path in file_iterator:
                if self._stop_event.is_set():
                    return
                if not file_path.is_file():
                    continue
                if self._is_temp_file(str(file_path)):
                    continue
                if root.extensions and file_path.suffix.lower() not in root.extensions:
                    continue

                abs_path = str(file_path)
                normalized_path = self._normalize_path(abs_path)
                path_hash = cast(str, make_hash(normalized_path))
                batch_paths.append((normalized_path, abs_path, path_hash))

                if len(batch_paths) >= self._reconcile_batch_size:
                    self._reconcile_batch(batch_paths, root)
                    batch_paths = []

            if batch_paths:
                self._reconcile_batch(batch_paths, root)

    def _reconcile_batch(
        self,
        batch_paths: list[tuple[str, str, str]],
        root: Root,
    ) -> None:
        """Process a batch of file paths against the Events table.

        Args:
            batch_paths: List of (normalized_path, abs_path, path_hash) tuples.
            root: The Root configuration for these paths.
        """
        if self._database is None:
            return

        path_hashes = [path_hash for _, _, path_hash in batch_paths]

        with self._database.create_session() as session:
            statement = (
                select(Events.path_hash)
                .where(
                    Events.path_hash.in_(path_hashes),
                    Events.archive.is_(None),
                )
            )
            existing_hashes = set(self._database.query(statement, session).scalars().all())

        for normalized_path, abs_path, path_hash in batch_paths:
            if path_hash not in existing_hashes:
                with self._pending_lock:
                    if normalized_path in self._pending_stability:
                        continue
                    self._pending_stability[normalized_path] = {
                        "root": root,
                        "event_type": EventType.CREATED,
                        "event_id": None,
                        "last_size": -1,
                    "last_changed": time.monotonic(),
                    }
                event_data = EventData(
                    abs_path=abs_path,
                    root_data=root,
                    event_type=EventType.CREATED,
                    path_hash=path_hash,
                    stable=False,
                )
                self._write_queue.put(DetectCommand(event_data=event_data, normalized_path=normalized_path))
                logger.debug("Reconciliation: found untracked file %s", abs_path)

    # ------------------------------------------------------------------
    # Startup recovery
    # ------------------------------------------------------------------

    def _recover_pending_streams(self) -> None:
        """Recover unstable events from the database on startup.

        Queries Events rows where ``stable=False`` and re-adds them to
        ``_pending_stability`` so the stability monitor can finalize them.
        """
        if self._database is None:
            return

        with self._database.create_session() as session:
            statement = (
                select(Events)
                .where(
                    Events.stable.is_(False),
                    Events.archive.is_(None),
                )
            )
            unstable_events = self._database.query(statement, session).scalars().all()

        recovered_count = 0
        for event in unstable_events:
            file_path = str(event.file_path)
            normalized_path = self._normalize_path(file_path)
            event_id = cast(int, event.id)

            root = self._find_root(normalized_path)
            if root is None:
                logger.warning("Recovered event has no matching root, finalizing: %s", file_path)
                self._write_queue.put(StabilizeCommand(event_id=event_id, file_size=None))
                continue

            if os.path.exists(file_path):
                with self._pending_lock:
                    self._pending_stability[normalized_path] = {
                        "root": root,
                        "event_type": EventType.CREATED,
                        "event_id": event_id,
                        "last_size": -1,
                    "last_changed": time.monotonic(),
                    }
                recovered_count += 1
            else:
                # File no longer exists -- finalize the event as-is
                self._write_queue.put(StabilizeCommand(event_id=event_id, file_size=None))

        if recovered_count > 0:
            logger.info("Recovered %d pending streams from database", recovered_count)

    # ------------------------------------------------------------------
    # Observer selection
    # ------------------------------------------------------------------

    def _create_observer(self, observer_type: str) -> Any:
        """Create a platform-aware watchdog observer.

        Args:
            observer_type: ``auto`` for native OS API, ``polling`` for fallback.

        Returns:
            A watchdog Observer instance.
        """
        if observer_type == "polling":
            return PollingObserver(timeout=self._poll_interval)

        try:
            return Observer()
        except Exception:
            logger.warning("Native observer unavailable, falling back to PollingObserver")
            return PollingObserver(timeout=self._poll_interval)

    # ------------------------------------------------------------------
    # Path utilities
    # ------------------------------------------------------------------

    @staticmethod
    def _normalize_path(path: str) -> str:
        """Normalize a file path for consistent hashing and deduplication.

        Handles Windows UNC paths, forward/backward slashes, and long paths.

        Args:
            path: Raw file path from the OS or user input.

        Returns:
            Normalized path string.
        """
        if sys.platform == "win32":
            path = path.replace("/", "\\")
            if path.startswith("\\\\") and len(path) > 248:
                path = "\\\\?" + path[1:]
        return os.path.normpath(path)

    def _find_root(self, normalized_path: str) -> Root | None:
        """Find which root a file path belongs to (longest-prefix match).

        Args:
            normalized_path: Normalized file path.

        Returns:
            The matching Root, or None if the path is not under any root.
        """
        best_match: Root | None = None
        best_length = 0

        for root_path, root in self._roots.items():
            if (
                normalized_path.startswith(root_path)
                and len(root_path) > best_length
                and (len(normalized_path) == len(root_path) or normalized_path[len(root_path)] == os.sep)
            ):
                best_match = root
                best_length = len(root_path)

        return best_match

    @staticmethod
    def _is_temp_file(file_path: str) -> bool:
        """Check if a file is a temporary or lock file that should be skipped.

        Matches patterns created by common applications:
        - ``~$document.xlsx`` (Microsoft Office lock files)
        - ``~document.tmp`` (general temp files with tilde prefix)
        - ``.~lock.document#`` (LibreOffice lock files)

        Args:
            file_path: Normalized path to check.

        Returns:
            True if the filename matches a known temp/lock file pattern.
        """
        filename = os.path.basename(file_path)
        return filename.startswith(("~$", "~", ".~"))

    @staticmethod
    def _is_allowed(file_path: str, extensions: list[str]) -> bool:
        """Check if a file's extension is in the allowed list.

        Args:
            file_path: Path to check.
            extensions: List of allowed extensions (e.g., [".pdf", ".zip"]).

        Returns:
            True if the file extension is in the allowed list (case-insensitive).
        """
        _, extension = os.path.splitext(file_path)
        return extension.lower() in [ext.lower() for ext in extensions]
