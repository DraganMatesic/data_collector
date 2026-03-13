"""Subprocess lifecycle management for the orchestration manager.

Handles spawning app processes, tracking PIDs, detecting completion
or crash, terminating processes on command, and cleaning up orphan PIDs
left over from a previous Manager session.
"""

from __future__ import annotations

import contextlib
import ctypes
import json
import logging
import os
import signal
import subprocess
import sys
import threading
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select

from data_collector.enums import RunStatus
from data_collector.enums.runtime import RuntimeExitCode
from data_collector.tables.apps import Apps
from data_collector.tables.runtime import Runtime
from data_collector.utilities.app_status import update_app_status
from data_collector.utilities.database.main import Database
from data_collector.utilities.functions.math import get_totalh, get_totalm, get_totals


@dataclass
class TrackedProcess:
    """State for a single tracked subprocess.

    Attributes:
        process: The subprocess.Popen handle.
        app_id: 64-char SHA-256 app identifier.
        runtime_id: UUID4 hex string for this execution.
        start_time: UTC timestamp when the process was spawned.
        group_name: App group for module path construction.
        parent_name: App parent for module path construction.
        app_name: App name for module path construction.
    """

    process: subprocess.Popen[bytes]
    app_id: str
    runtime_id: str
    start_time: datetime
    group_name: str
    parent_name: str
    app_name: str
    return_code: int | None = field(default=None, init=False)


class ProcessTracker:
    """Track and manage app subprocesses.

    Thread-safe: all access to the internal ``_tracked`` dictionary is
    guarded by a lock.

    Args:
        database: Database instance connected to the main schema.
        logger: Structured logger for process-related messages.
        startup_grace_period: Seconds to wait before first PID check after spawn.
    """

    def __init__(
        self,
        database: Database,
        *,
        logger: logging.Logger,
        startup_grace_period: int = 10,
    ) -> None:
        self._database = database
        self._logger = logger
        self._startup_grace_period = startup_grace_period
        self._tracked: dict[str, TrackedProcess] = {}
        self._lock = threading.Lock()

    def spawn(
        self,
        app: Apps,
        *,
        app_args: dict[str, Any] | None = None,
    ) -> TrackedProcess:
        """Launch an app as a subprocess and begin tracking it.

        Builds the command using ``sys.executable -m`` to inherit the
        current virtual environment.  Never uses ``shell=True``.

        Args:
            app: Apps ORM instance to launch.
            app_args: Optional arguments passed via ``--args`` JSON.

        Returns:
            The TrackedProcess descriptor for the spawned process.

        Raises:
            OSError: If the subprocess cannot be started.
        """
        runtime_id = uuid.uuid4().hex
        module_path = f"data_collector.{app.group_name}.{app.parent_name}.{app.app_name}.main"
        command: list[str] = [sys.executable, "-m", module_path]

        if app_args is not None:
            command.extend(["--args", json.dumps(app_args)])

        start_time = datetime.now(UTC)

        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        tracked = TrackedProcess(
            process=process,
            app_id=str(app.app),
            runtime_id=runtime_id,
            start_time=start_time,
            group_name=str(app.group_name),
            parent_name=str(app.parent_name),
            app_name=str(app.app_name),
        )

        with self._lock:
            self._tracked[str(app.app)] = tracked

        # Update Apps table: mark as running
        update_app_status(
            self._database,
            str(app.app),
            run_status=RunStatus.RUNNING,
            app_pids=str(process.pid),
            runtime_id=runtime_id,
        )

        # Create initial Runtime record
        with self._database.create_session() as session:
            runtime_record = Runtime(
                runtime=runtime_id,
                app_id=app.app,
                start_time=start_time,
            )
            session.add(runtime_record)
            session.commit()

        self._logger.info(
            "Spawned %s/%s/%s (PID %d, runtime %s)",
            app.group_name, app.parent_name, app.app_name, process.pid, runtime_id,
        )
        return tracked

    def check_processes(self) -> list[TrackedProcess]:
        """Poll all tracked processes and return those that have completed.

        Completed processes are finalized (Runtime record updated, Apps
        status cleared) and removed from tracking.

        Returns:
            List of TrackedProcess instances that have finished.
        """
        completed: list[TrackedProcess] = []

        with self._lock:
            app_ids = list(self._tracked.keys())

        for app_id in app_ids:
            with self._lock:
                tracked = self._tracked.get(app_id)
            if tracked is None:
                continue

            return_code = tracked.process.poll()
            if return_code is None:
                continue

            tracked.return_code = return_code
            self._finalize_process(tracked, return_code)

            with self._lock:
                self._tracked.pop(app_id, None)

            completed.append(tracked)

        return completed

    def terminate_process(
        self,
        app_id: str,
        *,
        exit_code: RuntimeExitCode,
        timeout: int = 10,
    ) -> bool:
        """Terminate a tracked process by app_id.

        Sends ``terminate()`` first, waits up to ``timeout`` seconds, then
        sends ``kill()`` if the process is still alive.

        Args:
            app_id: The 64-char SHA-256 app identifier.
            exit_code: RuntimeExitCode to record in the Runtime table.
            timeout: Seconds to wait after terminate before force-killing.

        Returns:
            ``True`` if the process was found and terminated.
        """
        with self._lock:
            tracked = self._tracked.pop(app_id, None)
        if tracked is None:
            return False

        return_code = self._kill_process(tracked, timeout=timeout)
        self._finalize_process(tracked, return_code, exit_code_override=exit_code.value)

        self._logger.info(
            "Terminated %s/%s/%s (PID %d, exit_code=%s)",
            tracked.group_name, tracked.parent_name, tracked.app_name,
            tracked.process.pid, exit_code.name,
        )
        return True

    def terminate_all(
        self,
        *,
        exit_code: RuntimeExitCode,
        timeout: int = 30,
    ) -> None:
        """Terminate all tracked processes during Manager shutdown.

        Args:
            exit_code: RuntimeExitCode to record (typically MANAGER_EXIT).
            timeout: Per-process timeout for graceful termination.
        """
        with self._lock:
            app_ids = list(self._tracked.keys())

        for app_id in app_ids:
            self.terminate_process(app_id, exit_code=exit_code, timeout=timeout)

    def cleanup_orphan_pids(self) -> None:
        """Find and clean up apps with stale PIDs from a previous session.

        Called once at Manager startup. For each app that has ``app_pids``
        set in the database:
        - If the PID is alive, terminate it with ``ORPHAN_PID`` exit code.
        - If the PID is dead, clear the stale state.
        """
        with self._database.create_session() as session:
            statement = select(Apps).where(Apps.app_pids.isnot(None))
            result = self._database.query(statement, session)
            orphan_apps: list[Apps] = list(result.scalars().all())
            session.expunge_all()

        for app in orphan_apps:
            pid_str = str(app.app_pids) if app.app_pids is not None else ""  # type: ignore[redundant-expr]
            app_id = str(app.app)
            if not pid_str:
                continue

            try:
                pid = int(pid_str.strip())
            except ValueError:
                self._logger.warning(
                    "Invalid PID value '%s' for app %s, clearing", pid_str, app_id,
                )
                update_app_status(
                    self._database, app_id,
                    run_status=RunStatus.NOT_RUNNING, app_pids=None,
                )
                continue

            if self.is_pid_alive(pid):
                self._logger.warning(
                    "Orphan process PID %d found for %s/%s/%s, terminating",
                    pid, app.group_name, app.parent_name, app.app_name,
                )
                self.kill_pid(pid)

            update_app_status(
                self._database, app_id,
                run_status=RunStatus.NOT_RUNNING, app_pids=None,
            )
            self._logger.info("Cleared orphan state for %s/%s/%s", app.group_name, app.parent_name, app.app_name)

    @property
    def active_count(self) -> int:
        """Number of currently tracked processes."""
        with self._lock:
            return len(self._tracked)

    def is_tracked(self, app_id: str) -> bool:
        """Check whether an app is currently tracked."""
        with self._lock:
            return app_id in self._tracked

    def _finalize_process(
        self,
        tracked: TrackedProcess,
        return_code: int,
        *,
        exit_code_override: int | None = None,
    ) -> None:
        """Update Runtime record and Apps status for a completed process."""
        end_time = datetime.now(UTC)
        effective_exit_code = exit_code_override if exit_code_override is not None else return_code

        with self._database.create_session() as session:
            statement = select(Runtime).where(Runtime.runtime == tracked.runtime_id)
            runtime_record = self._database.query(statement, session).scalar_one_or_none()
            if runtime_record is not None:
                runtime_record.end_time = end_time
                runtime_record.totals = get_totals(tracked.start_time, end_time)
                runtime_record.totalm = get_totalm(tracked.start_time, end_time)
                runtime_record.totalh = get_totalh(tracked.start_time, end_time)
                runtime_record.exit_code = effective_exit_code
                session.commit()

        # Clear Apps runtime state; the app itself may have set next_run
        update_app_status(
            self._database, tracked.app_id,
            run_status=RunStatus.NOT_RUNNING,
        )

        # Clear app_pids separately (update_app_status skips None values,
        # but we need to explicitly write NULL)
        with self._database.create_session() as session:
            statement = select(Apps).where(Apps.app == tracked.app_id)
            row = self._database.query(statement, session).scalar_one_or_none()
            if row is not None:
                row.app_pids = None  # type: ignore[assignment]
                session.commit()

    @staticmethod
    def _kill_process(tracked: TrackedProcess, *, timeout: int = 10) -> int:
        """Terminate a process, force-kill if it doesn't exit in time."""
        try:
            tracked.process.terminate()
            tracked.process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            tracked.process.kill()
            tracked.process.wait(timeout=5)
        return tracked.process.returncode

    @staticmethod
    def is_pid_alive(pid: int) -> bool:
        """Check whether a process with the given PID is still running."""
        if sys.platform == "win32":
            kernel32 = ctypes.windll.kernel32  # type: ignore[attr-defined]
            process_query_limited_information = 0x1000
            handle = kernel32.OpenProcess(process_query_limited_information, False, pid)
            if handle:
                kernel32.CloseHandle(handle)
                return True
            return False

        # Unix: send signal 0 to check existence
        try:
            os.kill(pid, signal.SIG_DFL)
            return True
        except ProcessLookupError:
            return False
        except PermissionError:
            # Process exists but we lack permission -- still alive
            return True

    @staticmethod
    def kill_pid(pid: int, *, timeout: int = 10) -> None:
        """Terminate an orphan process by PID (not tracked by this Manager)."""
        if sys.platform == "win32":
            kernel32 = ctypes.windll.kernel32  # type: ignore[attr-defined]
            process_terminate = 0x0001
            handle = kernel32.OpenProcess(process_terminate, False, pid)
            if handle:
                kernel32.TerminateProcess(handle, 1)
                kernel32.CloseHandle(handle)
        else:
            with contextlib.suppress(ProcessLookupError):
                os.kill(pid, signal.SIGTERM)
