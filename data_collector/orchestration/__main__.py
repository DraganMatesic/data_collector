"""Entry point for the orchestration manager.

Usage:
    python -m data_collector.orchestration              # Start the manager
    python -m data_collector.orchestration --stop-apps  # Terminate all running apps
"""

from __future__ import annotations

import argparse
import logging
import signal
from typing import Any, cast

from sqlalchemy import select

from data_collector.dramatiq.broker import DramatiqBroker
from data_collector.enums import RunStatus
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
from data_collector.utilities.app_status import update_app_status
from data_collector.utilities.database.main import Database
from data_collector.utilities.log.main import LoggingService


def _run_manager() -> None:
    """Start the orchestration manager main loop."""
    settings = ManagerSettings()
    database = Database(MainDatabaseSettings())

    # Optional: notification dispatcher
    notification_dispatcher: NotificationDispatcher | None = None
    if settings.notifications_enabled:
        notification_settings = NotificationSettings()
        if notification_settings.notifications_enabled:
            notification_dispatcher = NotificationDispatcher.from_settings(notification_settings)

    # Optional: RabbitMQ connection + Dramatiq broker
    rabbitmq_connection: RabbitMQConnection | None = None
    rabbitmq_settings: RabbitMQSettings | None = None
    dramatiq_broker: DramatiqBroker | None = None
    if settings.rabbitmq_enabled:
        rabbitmq_settings = RabbitMQSettings()
        rabbitmq_connection = RabbitMQConnection(rabbitmq_settings)
        rabbitmq_connection.connect()
        dramatiq_settings = DramatiqSettings()
        dramatiq_broker = DramatiqBroker(rabbitmq_settings, dramatiq_settings)

    # Logging
    log_settings = LogSettings()
    service = LoggingService(
        "orchestration.manager",
        settings=log_settings,
        db_engine=database.engine,
    )
    logger = service.configure_logger()

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
    )

    # Signal handlers for graceful shutdown
    def handle_shutdown(signum: int, frame: object) -> None:
        manager.stop()

    signal.signal(signal.SIGTERM, handle_shutdown)
    signal.signal(signal.SIGINT, handle_shutdown)

    try:
        manager.run()
    finally:
        if rabbitmq_connection is not None:
            rabbitmq_connection.close()


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
    arguments = parser.parse_args()

    if arguments.stop_apps:
        _stop_apps()
    else:
        _run_manager()


if __name__ == "__main__":
    main()
