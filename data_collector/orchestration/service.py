"""Cross-platform service wrapper for the orchestration Manager.

Provides a Windows service (via ``pywin32``) and a Linux systemd unit
file generator for running the Manager as a managed background service
that auto-starts on boot.

Windows usage::

    python -m data_collector.orchestration.service install
    python -m data_collector.orchestration.service start
    python -m data_collector.orchestration.service stop
    python -m data_collector.orchestration.service remove

Linux usage::

    python -m data_collector.orchestration.service generate-systemd > /etc/systemd/system/dc-manager.service
    systemctl daemon-reload
    systemctl enable dc-manager
    systemctl start dc-manager
"""

from __future__ import annotations

import argparse
import logging
import logging.handlers
import os
import site
import sys
import textwrap
import threading
import traceback
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

# Resolve the service log directory from DC_LOG_ERROR_FILE (if set).
# At module-load time, Pydantic is not yet available, so we read the
# env var directly.  The same resolution logic as LogSettings applies:
# if the value is a directory (or has no file extension), use it as-is;
# if it is a file path, use its parent directory.
_log_error_env = os.environ.get("DC_LOG_ERROR_FILE", "")
if _log_error_env:
    _log_error_path = Path(_log_error_env)
    if _log_error_path.is_dir() or (not _log_error_path.suffix and not _log_error_path.name.endswith(".log")):
        _SERVICE_LOG_DIR = _log_error_path
    else:
        _SERVICE_LOG_DIR = _log_error_path.parent
else:
    _SERVICE_LOG_DIR = _PROJECT_ROOT / "logs"

# Early boot log -- captures errors before LoggingService is available.
_BOOT_LOG = _SERVICE_LOG_DIR / "manager_service_boot.log"
try:
    _SERVICE_LOG_DIR.mkdir(parents=True, exist_ok=True)
    with open(_BOOT_LOG, "a", encoding="utf-8") as _boot_file:
        _boot_file.write(
            f"--- Module loading: __file__={__file__}, "
            f"PROJECT_ROOT={_PROJECT_ROOT}, "
            f"VENV={_VENV_SITE_PACKAGES}, "
            f"exists={_VENV_SITE_PACKAGES.exists()}\n"
            f"    sys.path[:5]={sys.path[:5]}\n"
        )
except Exception:
    pass

logger = logging.getLogger(__name__)


def generate_systemd_unit() -> str:
    """Generate a systemd unit file for the Manager.

    The generated unit uses the current Python interpreter path
    (``sys.executable``) to ensure the correct virtual environment
    is used.

    Returns:
        Complete systemd unit file content as a string.
    """
    return textwrap.dedent(f"""\
        [Unit]
        Description=Data Collector Manager
        After=network.target postgresql.service rabbitmq-server.service
        Wants=postgresql.service

        [Service]
        Type=simple
        User=datacollector
        WorkingDirectory=/opt/data-collector
        ExecStart={sys.executable} -m data_collector.orchestration
        Restart=on-failure
        RestartSec=10
        KillSignal=SIGTERM
        TimeoutStopSec=60

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

        class ManagerService(win32serviceutil.ServiceFramework):  # pyright: ignore[reportPossiblyUnboundVariable, reportGeneralTypeIssues]
            """Windows service that runs the orchestration Manager.

            Unlike the Dramatiq worker service (which spawns a subprocess),
            this service runs the Manager directly in-process.  The Manager's
            main loop blocks until ``manager.stop()`` is called.  The service
            stop handler sets a ``threading.Event`` that the Manager monitors
            via a background watcher thread.

            Install and manage via::

                python -m data_collector.orchestration.service install
                python -m data_collector.orchestration.service start
                python -m data_collector.orchestration.service stop
                python -m data_collector.orchestration.service remove
            """

            _svc_name_ = "DataCollectorManager"
            _svc_display_name_ = "Data Collector Manager"
            _svc_description_ = "Orchestration manager for data collection pipelines"
            _exe_name_ = str(Path(sys.executable).resolve().parent / "pythonservice.exe")

            def __init__(self, args: list[str]) -> None:
                win32serviceutil.ServiceFramework.__init__(self, args)  # pyright: ignore[reportPossiblyUnboundVariable, reportUnknownMemberType]
                self._win32_stop_event = win32event.CreateEvent(None, 0, 0, None)  # pyright: ignore[reportPossiblyUnboundVariable, reportUnknownMemberType]
                self._manager_stop_event = threading.Event()

            def SvcStop(self) -> None:
                """Handle service stop request.

                Sets the ``threading.Event`` that the Manager's stop-watcher
                thread monitors.  The watcher calls ``manager.stop()`` which
                triggers the graceful shutdown sequence.
                """
                self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)  # pyright: ignore[reportPossiblyUnboundVariable, reportUnknownMemberType]
                logger.info("Service stop requested")
                self._manager_stop_event.set()
                win32event.SetEvent(self._win32_stop_event)  # pyright: ignore[reportPossiblyUnboundVariable, reportUnknownMemberType]

            def SvcDoRun(self) -> None:
                """Start the Manager and block until service stop."""
                # Configure file-based logging for service diagnostics.
                # LoggingService is not available yet (it requires DB init),
                # so we use a RotatingFileHandler for early boot errors.
                service_log = _SERVICE_LOG_DIR / "manager_service.log"
                _SERVICE_LOG_DIR.mkdir(parents=True, exist_ok=True)
                file_handler = logging.handlers.RotatingFileHandler(
                    filename=str(service_log),
                    maxBytes=5_242_880,
                    backupCount=3,
                    encoding="utf-8",
                )
                file_handler.setFormatter(logging.Formatter(
                    "%(asctime)s %(levelname)s %(name)s %(message)s"
                ))
                logging.getLogger().addHandler(file_handler)
                logging.getLogger().setLevel(logging.DEBUG)

                logger.info("Service starting (project_root=%s)", _PROJECT_ROOT)

                try:
                    logger.info("Importing run_manager")
                    from data_collector.orchestration.__main__ import run_manager
                    logger.info("Starting run_manager")
                    run_manager(external_stop_event=self._manager_stop_event)
                    logger.info("run_manager returned normally")
                except SystemExit as system_exit:
                    logger.error("Manager called sys.exit(%s)", system_exit.code)
                except Exception:
                    logger.exception("Manager exited with error")
                finally:
                    logger.info("Service stopped")
                    file_handler.close()

    except ImportError:
        pass
    except Exception as _service_error:
        try:
            with open(_BOOT_LOG, "a", encoding="utf-8") as _err_file:
                _err_file.write("--- Service class definition failed:\n")
                traceback.print_exc(file=_err_file)
        except Exception:
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
        win32serviceutil.HandleCommandLine(ManagerService)  # pyright: ignore[reportPossiblyUnboundVariable, reportUnknownMemberType]
    else:
        parser = argparse.ArgumentParser(
            prog="data_collector.orchestration.service",
            description="Data Collector Manager service management",
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
