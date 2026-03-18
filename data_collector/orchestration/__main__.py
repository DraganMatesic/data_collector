"""Entry point for the orchestration manager.

Usage:
    python -m data_collector.orchestration              # Start the manager
    python -m data_collector.orchestration --stop-apps  # Terminate all running apps
"""

from __future__ import annotations

import argparse
import logging
import signal
import threading
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import structlog
import urllib3
from sqlalchemy import select

from data_collector.dramatiq.broker import DramatiqBroker
from data_collector.enums import RunStatus
from data_collector.enums.runtime import RuntimeExitCode
from data_collector.messaging.connection import RabbitMQConnection
from data_collector.notifications.dispatcher import NotificationDispatcher
from data_collector.orchestration.manager import Manager
from data_collector.orchestration.process_tracker import ProcessTracker
from data_collector.settings.dramatiq import DramatiqSettings, TaskDispatcherSettings
from data_collector.settings.main import LogSettings, MainDatabaseSettings
from data_collector.settings.manager import ManagerSettings
from data_collector.settings.notification import NotificationSettings
from data_collector.settings.rabbitmq import RabbitMQSettings
from data_collector.settings.watchservice import WatchServiceSettings
from data_collector.tables.apps import Apps
from data_collector.tables.runtime import Runtime
from data_collector.utilities.app_registration import ensure_service_app
from data_collector.utilities.app_status import update_app_status
from data_collector.utilities.database.main import Database
from data_collector.utilities.functions.math import get_totalh, get_totalm, get_totals
from data_collector.utilities.log.main import LoggingService


class _ManagerContextFilter(logging.Filter):
    """Inject app_id and runtime into every LogRecord.

    Attached to the ``"data_collector"`` logger so that all log records
    from any thread (including daemon threads for WatchService,
    TaskDispatcher, and RabbitMQ consumer) carry the Manager's identity.
    """

    def __init__(self, app_id: str, runtime_id: str) -> None:
        super().__init__()
        self._app_id = app_id
        self._runtime_id = runtime_id

    def filter(self, record: logging.LogRecord) -> bool:
        """Add context fields to the record if not already present."""
        if not getattr(record, "app_id", None):
            record.app_id = self._app_id  # type: ignore[attr-defined]
        if not getattr(record, "runtime", None):
            record.runtime = self._runtime_id  # type: ignore[attr-defined]
        if not getattr(record, "module_name", None):
            record.module_name = Path(record.pathname).name  # type: ignore[attr-defined]
        if not getattr(record, "module_path", None):
            record.module_path = record.pathname  # type: ignore[attr-defined]
        if not getattr(record, "function_name", None):
            record.function_name = record.funcName  # type: ignore[attr-defined]
        if not getattr(record, "thread_id", None):
            record.thread_id = record.thread  # type: ignore[attr-defined]
        return True


def run_manager(external_stop_event: threading.Event | None = None) -> None:
    """Start the orchestration manager main loop.

    Args:
        external_stop_event: Optional event set by an external caller (e.g. a
            Windows service) to request graceful shutdown.  When provided, a
            background thread monitors the event and calls ``manager.stop()``
            when it fires.  When ``None``, shutdown is driven by SIGINT/SIGTERM.
    """
    settings = ManagerSettings()
    database = Database(MainDatabaseSettings())

    # --- Phase 1: Establish identity BEFORE logging ---
    # Logs table has FK constraints on app_id and runtime.  Both rows must
    # exist before DatabaseHandler writes any records, otherwise FK violations
    # silently drop all log entries.
    manager_app_id = ensure_service_app(database, "manager")
    manager_runtime_id = uuid.uuid4().hex
    with database.create_session() as session:
        session.merge(Runtime(
            runtime=manager_runtime_id,
            app_id=manager_app_id,
            start_time=datetime.now(UTC),
        ))
        session.commit()

    # --- Phase 2: Optional integrations (no logging yet) ---
    notification_dispatcher: NotificationDispatcher | None = None
    if settings.notifications_enabled:
        notification_settings = NotificationSettings()
        if notification_settings.notifications_enabled:
            notification_dispatcher = NotificationDispatcher.from_settings(notification_settings)

    rabbitmq_connection: RabbitMQConnection | None = None
    rabbitmq_settings: RabbitMQSettings | None = None
    dramatiq_broker: DramatiqBroker | None = None
    if settings.rabbitmq_enabled:
        rabbitmq_settings = RabbitMQSettings()
        rabbitmq_connection = RabbitMQConnection(rabbitmq_settings)
        rabbitmq_connection.connect()
        dramatiq_settings = DramatiqSettings()
        dramatiq_broker = DramatiqBroker(rabbitmq_settings, dramatiq_settings)

    # --- Phase 3: Logging (identity already exists) ---
    log_settings = LogSettings()
    log_error_directory = Path(log_settings.log_error_file).parent
    try:
        log_error_directory.mkdir(parents=True, exist_ok=True)
    except PermissionError:
        raise RuntimeError(f"Cannot create log directory: {log_error_directory}") from None

    service = LoggingService(
        "data_collector",
        settings=log_settings,
        db_engine=database.engine,
    )
    logger = service.configure_logger()

    # Bind identity to structlog context and stdlib handler filter.
    # Every log record from this point onward carries app_id + runtime.
    structlog.contextvars.bind_contextvars(app_id=manager_app_id, runtime=manager_runtime_id)
    context_filter = _ManagerContextFilter(manager_app_id, manager_runtime_id)
    for handler in logging.getLogger("data_collector").handlers:
        handler.addFilter(context_filter)

    # --- Phase 4: Manager construction and run ---
    manager = Manager(
        database,
        settings,
        logger=cast(logging.Logger, logger),
        notification_dispatcher=notification_dispatcher,
        rabbitmq_connection=rabbitmq_connection,
        rabbitmq_settings=rabbitmq_settings,
        dramatiq_broker=dramatiq_broker,
        watch_service_settings=WatchServiceSettings(),
        task_dispatcher_settings=TaskDispatcherSettings(),
        manager_app_id=manager_app_id,
    )

    # External stop event support (Windows service mode)
    if external_stop_event is not None:
        def _wait_for_external_stop() -> None:
            external_stop_event.wait()
            manager.stop()

        stop_watcher = threading.Thread(
            target=_wait_for_external_stop,
            daemon=True,
            name="service-stop-watcher",
        )
        stop_watcher.start()
    else:
        # Signal handlers for graceful shutdown (CLI mode only).
        # Cannot register signals from non-main threads (e.g. Windows service).
        def handle_shutdown(signum: int, frame: object) -> None:
            manager.stop()

        signal.signal(signal.SIGTERM, handle_shutdown)
        signal.signal(signal.SIGINT, handle_shutdown)

    exit_code = RuntimeExitCode.FINISHED
    try:
        manager.run()
    except Exception:
        exit_code = RuntimeExitCode.MANAGER_EXIT
        cast(logging.Logger, logger).exception("Manager crashed")
    finally:
        # Finalize Runtime record with end_time, duration, and exit code.
        end_time = datetime.now(UTC)
        with database.create_session() as session:
            runtime_row = database.query(
                select(Runtime).where(Runtime.runtime == manager_runtime_id), session,
            ).scalar_one_or_none()
            if runtime_row is not None:
                runtime_row.end_time = end_time
                start = runtime_row.start_time
                if start is not None:
                    runtime_row.totals = get_totals(start, end_time)
                    runtime_row.totalm = get_totalm(start, end_time)
                    runtime_row.totalh = get_totalh(start, end_time)
                runtime_row.except_cnt = manager.exception_count
                runtime_row.exit_code = exit_code
                session.commit()

        if rabbitmq_connection is not None:
            rabbitmq_connection.close()
        service.stop()


def _stop_apps() -> None:
    """Terminate all running app processes directly from DB PIDs.

    Reads ``app_pids`` from the Apps table and terminates each process.
    Works independently of the Manager process -- use before service
    restart to prevent orphan processes.
    """
    database = Database(MainDatabaseSettings())
    logger = logging.getLogger("orchestration.stop_apps")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    with database.create_session() as session:
        statement = select(Apps).where(Apps.app_pids.isnot(None))
        result = database.query(statement, session)
        apps_with_pids: list[Apps] = list(result.scalars().all())
        session.expunge_all()

    if not apps_with_pids:
        logger.info("No apps with active PIDs found")
        return

    logger.info("Found %d app(s) with PIDs to stop", len(apps_with_pids))

    stopped_count = 0
    for app in apps_with_pids:
        app_record: Any = app
        pid_str = str(app_record.app_pids) if app_record.app_pids is not None else ""
        app_id = str(app_record.app)
        if not pid_str:
            continue

        try:
            pid = int(pid_str.strip())
        except ValueError:
            logger.warning("Invalid PID '%s' for %s/%s/%s, clearing",
                           pid_str, app.group_name, app.parent_name, app.app_name)
            update_app_status(database, app_id, run_status=RunStatus.NOT_RUNNING)
            _clear_app_pids(database, app_id)
            continue

        if ProcessTracker.is_pid_alive(pid):
            logger.info("Terminating PID %d (%s/%s/%s)",
                         pid, app.group_name, app.parent_name, app.app_name)
            ProcessTracker.kill_pid(pid)
            stopped_count += 1

        update_app_status(database, app_id, run_status=RunStatus.NOT_RUNNING)
        _clear_app_pids(database, app_id)

    logger.info("Stopped %d app(s)", stopped_count)


def _clear_app_pids(database: Database, app_id: str) -> None:
    """Set app_pids to NULL for the given app."""
    with database.create_session() as session:
        statement = select(Apps).where(Apps.app == app_id)
        row = database.query(statement, session).scalar_one_or_none()
        if row is not None:
            app_row: Any = row
            app_row.app_pids = None
            session.commit()


def main() -> None:
    """Parse CLI arguments and dispatch to the appropriate handler."""
    parser = argparse.ArgumentParser(
        prog="data_collector.orchestration",
        description="Data Collector orchestration manager",
    )
    parser.add_argument(
        "--stop-apps",
        action="store_true",
        help="Terminate all running app processes and exit",
    )
    parser.add_argument(
        "--suppress-tls-warnings",
        action="store_true",
        help="Suppress urllib3 InsecureRequestWarning (for self-signed certificates)",
    )
    arguments = parser.parse_args()

    if arguments.suppress_tls_warnings:
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    if arguments.stop_apps:
        _stop_apps()
    else:
        run_manager()


if __name__ == "__main__":
    main()
