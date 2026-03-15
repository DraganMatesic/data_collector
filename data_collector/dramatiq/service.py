"""Cross-platform service wrapper for Dramatiq worker processes.

Provides a Windows service (via ``pywin32``) and a Linux systemd unit
file generator for running Dramatiq workers as managed background
services that auto-start on boot.

Windows usage::

    python -m data_collector.dramatiq.service install
    python -m data_collector.dramatiq.service start
    python -m data_collector.dramatiq.service stop
    python -m data_collector.dramatiq.service remove

Linux usage::

    python -m data_collector.dramatiq.service generate-systemd > /etc/systemd/system/dc-workers.service
    systemctl daemon-reload
    systemctl enable dc-workers
    systemctl start dc-workers
"""

from __future__ import annotations

import argparse
import contextlib
import logging
import logging.handlers
import os
import site
import subprocess
import sys
import textwrap
import threading
from pathlib import Path

# pythonservice.exe runs outside the venv context -- it does not activate
# the virtual environment or set up sys.path for the project. We must
# manually add the venv site-packages and project root so that
# ``data_collector`` can be imported.
_SERVICE_DIR = Path(__file__).resolve().parent
_PROJECT_ROOT = _SERVICE_DIR.parent.parent
_VENV_SITE_PACKAGES = _PROJECT_ROOT / ".venv" / "Lib" / "site-packages"

if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))
if _VENV_SITE_PACKAGES.exists() and str(_VENV_SITE_PACKAGES) not in sys.path:
    site.addsitedir(str(_VENV_SITE_PACKAGES))

from data_collector.settings.dramatiq import DramatiqSettings  # noqa: E402

logger = logging.getLogger(__name__)

_ACTOR_MODULE: str = "data_collector.dramatiq.actors"
"""Module passed to the ``dramatiq`` CLI for actor discovery."""


def build_dramatiq_command(settings: DramatiqSettings | None = None) -> list[str]:
    """Build the ``dramatiq`` CLI command from settings.

    Uses ``data_collector.dramatiq.cli_wrapper`` instead of ``dramatiq``
    directly. The wrapper patches ``StreamablePipe`` before Dramatiq's
    main process spawns workers, preventing the ``concurrent send_bytes()``
    crash on Python 3.12+ / Windows.

    Args:
        settings: Dramatiq configuration. Uses defaults if not provided.

    Returns:
        Command list suitable for ``subprocess.Popen``.
    """
    resolved_settings = settings or DramatiqSettings()
    command = [
        sys.executable, "-m", "data_collector.dramatiq.cli_wrapper",
        _ACTOR_MODULE,
        "-p", str(resolved_settings.processes),
        "-t", str(resolved_settings.workers),
    ]

    if resolved_settings.queues:
        queue_list = [queue.strip() for queue in resolved_settings.queues.split(",") if queue.strip()]
        for queue_name in queue_list:
            command.extend(["-Q", queue_name])

    return command


def generate_systemd_unit(settings: DramatiqSettings | None = None) -> str:
    """Generate a systemd unit file for Dramatiq workers.

    The generated unit uses the current Python interpreter path
    (``sys.executable``) to ensure the correct virtual environment
    is used.

    Args:
        settings: Dramatiq configuration for process/thread counts.

    Returns:
        Complete systemd unit file content as a string.
    """
    resolved_settings = settings or DramatiqSettings()
    command_parts = build_dramatiq_command(resolved_settings)
    exec_start = " ".join(command_parts)

    return textwrap.dedent(f"""\
        [Unit]
        Description=Data Collector Dramatiq Workers
        After=network.target rabbitmq-server.service postgresql.service
        Wants=rabbitmq-server.service

        [Service]
        Type=simple
        User=datacollector
        WorkingDirectory=/opt/data-collector
        ExecStart={exec_start}
        Restart=on-failure
        RestartSec=10
        KillSignal=SIGTERM
        TimeoutStopSec=30

        [Install]
        WantedBy=multi-user.target
    """)


# ---------------------------------------------------------------------------
# Windows service
# ---------------------------------------------------------------------------

_PYWIN32_AVAILABLE: bool = False

if sys.platform == "win32":
    try:
        import win32event  # pyright: ignore[reportMissingModuleSource]
        import win32service  # pyright: ignore[reportMissingModuleSource]
        import win32serviceutil  # pyright: ignore[reportMissingModuleSource]

        _PYWIN32_AVAILABLE = True  # pyright: ignore[reportConstantRedefinition]

        class DramatiqWorkerService(win32serviceutil.ServiceFramework):  # pyright: ignore[reportPossiblyUnboundVariable, reportGeneralTypeIssues]
            """Windows service that manages Dramatiq worker processes.

            Spawns the ``dramatiq`` CLI as a subprocess and monitors it.
            On service stop, sends SIGTERM for graceful shutdown.

            Install and manage via::

                python -m data_collector.dramatiq.service install
                python -m data_collector.dramatiq.service start
                python -m data_collector.dramatiq.service stop
                python -m data_collector.dramatiq.service remove
            """

            _svc_name_ = "DataCollectorWorkers"
            _svc_display_name_ = "Data Collector Dramatiq Workers"
            _svc_description_ = "Background task processing for data collection pipelines"
            _exe_name_ = str(Path(sys.executable).resolve().parent / "pythonservice.exe")

            _SHUTDOWN_TIMEOUT: int = 30

            def __init__(self, args: list[str]) -> None:
                win32serviceutil.ServiceFramework.__init__(self, args)  # pyright: ignore[reportPossiblyUnboundVariable, reportUnknownMemberType]
                self.stop_event = win32event.CreateEvent(None, 0, 0, None)  # pyright: ignore[reportPossiblyUnboundVariable, reportUnknownMemberType]
                self._process: subprocess.Popen[bytes] | None = None

            def SvcStop(self) -> None:
                """Handle service stop request.

                Kills the entire process tree (main Dramatiq CLI process +
                all multiprocessing worker subprocesses) to prevent orphaned
                workers from consuming messages with stale code.
                """
                self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)  # pyright: ignore[reportPossiblyUnboundVariable, reportUnknownMemberType]
                logger.info("Service stop requested")

                if self._process is not None and self._process.poll() is None:
                    self._kill_process_tree(self._process.pid)
                    try:
                        self._process.wait(timeout=self._SHUTDOWN_TIMEOUT)
                        logger.info("Dramatiq workers stopped gracefully")
                    except subprocess.TimeoutExpired:
                        self._process.kill()
                        logger.warning("Dramatiq workers killed after timeout")

                win32event.SetEvent(self.stop_event)  # pyright: ignore[reportPossiblyUnboundVariable, reportUnknownMemberType]

            @staticmethod
            def _pipe_to_rotating_log(
                process: subprocess.Popen[bytes],
                handler: logging.handlers.RotatingFileHandler,
            ) -> None:
                """Read subprocess stdout line by line and write to a RotatingFileHandler.

                Runs in a daemon thread for the lifetime of the subprocess.
                The handler rotates the log file automatically when it
                exceeds ``maxBytes``.
                """
                pipe = process.stdout
                if pipe is None:
                    return
                try:
                    for line in iter(pipe.readline, b""):
                        text = line.decode("utf-8", errors="replace").rstrip("\n\r")
                        if text:
                            record = logging.LogRecord(
                                name="dramatiq.service.output",
                                level=logging.INFO,
                                pathname="",
                                lineno=0,
                                msg=text,
                                args=(),
                                exc_info=None,
                            )
                            handler.emit(record)
                except (OSError, ValueError):
                    pass

            @staticmethod
            def _kill_process_tree(parent_pid: int) -> None:
                """Terminate a process and all its descendants.

                Uses ``taskkill /T /F /PID`` which recursively kills the
                entire process tree on Windows.  This prevents orphaned
                Dramatiq worker subprocesses when the service stops.
                """
                with contextlib.suppress(subprocess.TimeoutExpired, OSError):
                    subprocess.run(
                        ["taskkill", "/T", "/F", "/PID", str(parent_pid)],
                        capture_output=True,
                        timeout=10,
                    )

            def SvcDoRun(self) -> None:
                """Start Dramatiq workers and wait for stop signal."""
                logger.info("Service starting")
                settings = DramatiqSettings()
                command = build_dramatiq_command(settings)

                # Resolve the project root from the venv Python path.
                # sys.executable: D:\project\.venv\Scripts\python.exe
                # Project root is two levels up from Scripts/.
                project_root = str(Path(sys.executable).resolve().parent.parent.parent)

                # Disable colorama ANSI wrapping to prevent
                # "concurrent send_bytes()" crash on Python 3.13 + Windows.
                worker_environment = {**os.environ, "NO_COLOR": "1", "TERM": "dumb"}

                # sys.executable inside pythonservice.exe points to
                # pythonservice.exe itself, not the venv Python. Resolve
                # the correct Python interpreter from the venv Scripts dir.
                venv_python = str(_PROJECT_ROOT / ".venv" / "Scripts" / "python.exe")
                command[0] = venv_python

                log_path = Path(settings.log_file)
                if not log_path.is_absolute():
                    log_path = _PROJECT_ROOT / log_path
                log_path.parent.mkdir(parents=True, exist_ok=True)

                rotating_handler = logging.handlers.RotatingFileHandler(
                    filename=str(log_path),
                    maxBytes=settings.log_max_bytes,
                    backupCount=settings.log_backup_count,
                    encoding="utf-8",
                )

                logger.info("Starting Dramatiq workers: %s (cwd=%s)", " ".join(command), project_root)
                self._process = subprocess.Popen(
                    command,
                    cwd=project_root,
                    env=worker_environment,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                )

                # Background thread reads subprocess output line by line
                # and writes through the RotatingFileHandler. This ensures
                # log rotation happens continuously, even if the service
                # runs for months without a restart.
                pipe_thread = threading.Thread(
                    target=self._pipe_to_rotating_log,
                    args=(self._process, rotating_handler),
                    daemon=True,
                )
                pipe_thread.start()

                try:
                    win32event.WaitForSingleObject(  # pyright: ignore[reportPossiblyUnboundVariable, reportUnknownMemberType]
                        self.stop_event,
                        win32event.INFINITE,  # pyright: ignore[reportPossiblyUnboundVariable, reportUnknownMemberType]
                    )
                finally:
                    rotating_handler.close()

    except ImportError:
        pass


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def _handle_generate_systemd() -> None:
    """Print a systemd unit file to stdout."""
    print(generate_systemd_unit())


def main() -> None:
    """Parse CLI arguments and dispatch to the appropriate handler."""
    if sys.platform == "win32":
        if not _PYWIN32_AVAILABLE:
            print(
                "pywin32 is required for Windows service management.\n"
                "Install it with: pip install pywin32",
                file=sys.stderr,
            )
            sys.exit(1)
        win32serviceutil.HandleCommandLine(DramatiqWorkerService)  # pyright: ignore[reportPossiblyUnboundVariable, reportUnknownMemberType]
    else:
        parser = argparse.ArgumentParser(
            prog="data_collector.dramatiq.service",
            description="Data Collector Dramatiq worker service management",
        )
        parser.add_argument(
            "action",
            choices=["generate-systemd"],
            help="Action to perform",
        )
        arguments = parser.parse_args()

        if arguments.action == "generate-systemd":
            _handle_generate_systemd()


if __name__ == "__main__":
    main()
