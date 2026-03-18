"""Central orchestration manager for data collection applications.

The Manager is the "brain" of the framework: a long-lived process that
continuously polls the Apps table, launches apps when their ``next_run``
arrives, processes commands, detects crashes, sends alerts, and cleans
up old records.
"""

from __future__ import annotations

import logging
import threading
import time
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select

from data_collector.dramatiq.broker import DramatiqBroker
from data_collector.dramatiq.task_dispatcher import TaskDispatcher
from data_collector.enums import CmdName, FatalFlag
from data_collector.enums.notifications import AlertSeverity
from data_collector.enums.runtime import RuntimeExitCode
from data_collector.messaging.connection import RabbitMQConnection
from data_collector.messaging.watchservice import IngestEventHandler, WatchService, load_roots_from_database
from data_collector.notifications.dispatcher import NotificationDispatcher
from data_collector.notifications.models import Notification
from data_collector.orchestration.command_handler import CommandHandler, PendingCommand
from data_collector.orchestration.process_tracker import ProcessTracker, TrackedProcess
from data_collector.orchestration.retention import RetentionCleaner
from data_collector.orchestration.scheduler import Scheduler
from data_collector.settings.dramatiq import TaskDispatcherSettings
from data_collector.settings.manager import ManagerSettings
from data_collector.settings.rabbitmq import RabbitMQSettings
from data_collector.settings.watchservice import WatchServiceSettings
from data_collector.tables.apps import Apps
from data_collector.utilities.app_registration import ensure_service_app
from data_collector.utilities.app_status import update_app_status
from data_collector.utilities.database.main import Database


class Manager:
    """Central orchestrator that schedules and controls app processes.

    Composes :class:`Scheduler`, :class:`ProcessTracker`,
    :class:`CommandHandler`, and :class:`RetentionCleaner` into a single
    main loop that runs until a stop signal is received.

    Args:
        database: Database instance connected to the main schema.
        settings: Manager configuration from environment variables.
        logger: Structured logger for orchestration messages.
        notification_dispatcher: Optional dispatcher for fatal alerts.
        rabbitmq_connection: Optional RabbitMQ connection for command consumption.
        rabbitmq_settings: Required when ``rabbitmq_connection`` is provided.
        dramatiq_broker: Optional broker for TaskDispatcher dispatch.
            When provided, TaskDispatcher starts automatically.
        watch_service_settings: Optional WatchService configuration.
        task_dispatcher_settings: Optional TaskDispatcher configuration.
    """

    def __init__(
        self,
        database: Database,
        settings: ManagerSettings,
        *,
        logger: logging.Logger,
        notification_dispatcher: NotificationDispatcher | None = None,
        rabbitmq_connection: RabbitMQConnection | None = None,
        rabbitmq_settings: RabbitMQSettings | None = None,
        dramatiq_broker: DramatiqBroker | None = None,
        watch_service_settings: WatchServiceSettings | None = None,
        task_dispatcher_settings: TaskDispatcherSettings | None = None,
        manager_app_id: str | None = None,
    ) -> None:
        self._database = database
        self._settings = settings
        self._logger = logger
        self._notification_dispatcher = notification_dispatcher
        self._stop_event = threading.Event()

        self._scheduler = Scheduler(database, logger=logger)
        self._process_tracker = ProcessTracker(
            database,
            logger=logger,
            startup_grace_period=settings.startup_grace_period,
        )
        self._command_handler = CommandHandler(
            database,
            logger=logger,
            rabbitmq_connection=rabbitmq_connection,
            rabbitmq_settings=rabbitmq_settings,
        )
        self._retention_cleaner = RetentionCleaner(
            database, settings, logger=logger,
        )

        # Pipeline services (auto-detected from DB state)
        self._dramatiq_broker = dramatiq_broker
        self._watch_service_settings = watch_service_settings
        self._task_dispatcher_settings = task_dispatcher_settings
        self._watch_service: WatchService | None = None
        self._task_dispatcher: TaskDispatcher | None = None

        # In-memory failure tracking per app (reset on Manager restart)
        self._failure_counts: dict[str, int] = {}

        # Pre-computed Manager app_id (set by caller before logging starts)
        self._manager_app_id = manager_app_id

        # Timing trackers for periodic operations
        self._last_command_poll: float = 0.0
        self._last_process_check: float = 0.0
        self._last_retention_run: float = 0.0

    def run(self) -> None:
        """Start the main loop. Blocks until :meth:`stop` is called."""
        self._logger.info("Manager starting")

        # Clean up stale PIDs from previous session
        self._process_tracker.cleanup_orphan_pids()

        # Start RabbitMQ consumer if configured
        self._command_handler.start()

        # Auto-detect: WatchService (starts if active WatchRoots exist)
        self._start_watch_service()

        # Auto-detect: TaskDispatcher (starts if DramatiqBroker is available)
        self._start_task_dispatcher()

        self._logger.info(
            f"Manager running (poll={self._settings.polling_interval}s, "
            f"cmd_poll={self._settings.command_poll_interval}s, "
            f"process_check={self._settings.process_check_interval}s)",
        )

        try:
            while not self._stop_event.is_set():
                self._tick()
                self._stop_event.wait(timeout=self._settings.polling_interval)
        finally:
            self._shutdown()

    def stop(self) -> None:
        """Signal the main loop to stop."""
        self._logger.info("Manager stop requested")
        self._stop_event.set()

    def _tick(self) -> None:
        """Execute one iteration of the main loop."""
        now = time.monotonic()

        # 1. Commands (on command_poll_interval)
        if now - self._last_command_poll >= self._settings.command_poll_interval:
            self._process_commands()
            self._last_command_poll = now

        # 2. Process check (on process_check_interval)
        if now - self._last_process_check >= self._settings.process_check_interval:
            self._check_processes()
            self._last_process_check = now

        # 3. Schedule ready apps (every tick)
        self._spawn_ready_apps()

        # 4. Retention cleanup (on retention_check_interval)
        if (
            self._settings.retention_enabled
            and now - self._last_retention_run >= self._settings.retention_check_interval
        ):
            self._run_retention()
            self._last_retention_run = now

    def _process_commands(self) -> None:
        """Poll DB for commands, drain queue, execute each."""
        self._command_handler.poll_database_commands()
        commands = self._command_handler.get_pending_commands()
        for command in commands:
            executed = self._execute_command(command)
            self._command_handler.log_command(command, executed=executed)

    def _execute_command(self, command: PendingCommand) -> bool:
        """Execute a single command against the target app.

        Returns:
            ``True`` if the command was executed successfully.
        """
        app_id = command.app_id
        self._logger.info(
            "Executing command %s for app %s (source=%s, by=%s)",
            command.command.name, app_id, command.source, command.issued_by,
        )

        if command.command == CmdName.START:
            return self._cmd_start(app_id, app_args=command.args)

        if command.command == CmdName.STOP:
            return self._cmd_stop(app_id)

        if command.command == CmdName.RESTART:
            self._cmd_stop(app_id)
            return self._cmd_start(app_id, app_args=command.args)

        if command.command == CmdName.ENABLE:
            return self._cmd_enable(app_id)

        if command.command == CmdName.DISABLE:
            return self._cmd_disable(app_id)

        self._logger.warning("Unknown command %s for app %s", command.command, app_id)
        return False

    def _cmd_start(self, app_id: str, *, app_args: dict[str, Any] | None = None) -> bool:
        """Start an app if it is not already running and not disabled."""
        if self._process_tracker.is_tracked(app_id):
            self._logger.warning("START: app %s is already running", app_id)
            return False

        app = self._get_app(app_id)
        if app is None:
            return False

        if bool(app.disable):
            self._logger.warning("START: app %s is disabled", app_id)
            return False

        return self._spawn_app(app, app_args=app_args)

    def _cmd_stop(self, app_id: str) -> bool:
        """Stop a running app."""
        if not self._process_tracker.is_tracked(app_id):
            self._logger.warning("STOP: app %s is not running", app_id)
            return False

        return self._process_tracker.terminate_process(
            app_id, exit_code=RuntimeExitCode.CMD_STOP,
        )

    def _cmd_enable(self, app_id: str) -> bool:
        """Re-enable a disabled or fatal app."""
        update_app_status(
            self._database, app_id,
            disable=False,
            fatal_flag=FatalFlag.NONE,
            fatal_msg="",
        )
        self._failure_counts.pop(app_id, None)
        self._logger.info("ENABLE: app %s re-enabled", app_id)
        return True

    def _cmd_disable(self, app_id: str) -> bool:
        """Disable an app and terminate if running."""
        update_app_status(self._database, app_id, disable=True)

        if self._process_tracker.is_tracked(app_id):
            self._process_tracker.terminate_process(
                app_id, exit_code=RuntimeExitCode.CMD_DISABLE,
            )

        self._logger.info("DISABLE: app %s disabled", app_id)
        return True

    def _check_processes(self) -> None:
        """Check tracked processes for completion or crash."""
        completed = self._process_tracker.check_processes()
        self._handle_completed_processes(completed)

    def _handle_completed_processes(self, completed: list[TrackedProcess]) -> None:
        """Evaluate completed processes for crashes and update scheduling."""
        for tracked in completed:
            return_code = tracked.return_code if tracked.return_code is not None else -1

            if return_code == 0:
                # Successful completion -- reset failure counter
                self._failure_counts.pop(tracked.app_id, None)
                self._logger.info(
                    "App %s/%s/%s completed successfully",
                    tracked.group_name, tracked.parent_name, tracked.app_name,
                )
            else:
                # Crash or non-zero exit
                count = self._failure_counts.get(tracked.app_id, 0) + 1
                self._failure_counts[tracked.app_id] = count
                self._logger.warning(
                    "App %s/%s/%s exited with code %d (failure %d/%d)",
                    tracked.group_name, tracked.parent_name, tracked.app_name,
                    return_code, count, self._settings.max_start_failures,
                )

                if count >= self._settings.max_start_failures:
                    self._handle_fatal(
                        tracked.app_id,
                        tracked.group_name,
                        tracked.parent_name,
                        tracked.app_name,
                        FatalFlag.FAILED_TO_START,
                        f"App failed {count} consecutive times (last exit code: {return_code})",
                    )

            # Set fallback next_run if app didn't set its own
            app = self._get_app(tracked.app_id)
            if app is not None:
                self._scheduler.set_fallback_next_run(tracked.app_id, app)

    def _spawn_ready_apps(self) -> None:
        """Query for apps ready to run and spawn each."""
        ready_apps = self._scheduler.get_ready_apps()

        for app in ready_apps:
            if self._process_tracker.is_tracked(str(app.app)):
                continue
            self._spawn_app(app)

    def _spawn_app(
        self,
        app: Apps,
        *,
        app_args: dict[str, Any] | None = None,
    ) -> bool:
        """Attempt to spawn an app process.

        Returns:
            ``True`` if the app was spawned successfully.
        """
        try:
            self._process_tracker.spawn(app, app_args=app_args)
            return True
        except OSError as error:
            self._logger.error(
                "Failed to spawn %s/%s/%s: %s",
                app.group_name, app.parent_name, app.app_name, error,
            )
            self._handle_fatal(
                str(app.app),
                str(app.group_name),
                str(app.parent_name),
                str(app.app_name),
                FatalFlag.FAILED_TO_START,
                f"Failed to start process: {error}",
            )
            return False

    def _handle_fatal(
        self,
        app_id: str,
        group_name: str,
        parent_name: str,
        app_name: str,
        fatal_flag: FatalFlag,
        message: str,
    ) -> None:
        """Set fatal state, disable the app, and send a notification."""
        now = datetime.now(UTC)
        update_app_status(
            self._database, app_id,
            fatal_flag=fatal_flag,
            fatal_msg=message,
            fatal_time=now,
            disable=True,
        )
        self._logger.error(
            "FATAL [%s] %s/%s/%s: %s",
            fatal_flag.name, group_name, parent_name, app_name, message,
        )

        if self._notification_dispatcher is not None:
            notification = Notification(
                severity=AlertSeverity.CRITICAL,
                title=f"{group_name}/{parent_name}/{app_name}",
                message=message,
                app_id=app_id,
            )
            try:
                self._notification_dispatcher.send(notification)
            except Exception:
                self._logger.exception("Failed to send fatal notification for %s", app_id)

    def _run_retention(self) -> None:
        """Run periodic retention cleanup."""
        try:
            self._retention_cleaner.run_cleanup()
        except Exception:
            self._logger.exception("Retention cleanup failed")

    def _get_app(self, app_id: str) -> Apps | None:
        """Fetch a fresh Apps record from the database."""
        with self._database.create_session() as session:
            statement = select(Apps).where(Apps.app == app_id)
            app = self._database.query(statement, session).scalar_one_or_none()
            if app is not None:
                session.expunge(app)
            return app

    def _shutdown(self) -> None:
        """Graceful shutdown: stop pipeline services, consumer, terminate all processes."""
        self._logger.info("Manager shutting down")

        # Stop TaskDispatcher first (stop consuming), then WatchService (stop producing)
        if self._task_dispatcher is not None:
            self._task_dispatcher.stop()
            self._logger.info("TaskDispatcher stopped")

        if self._watch_service is not None:
            self._watch_service.stop()
            self._logger.info("WatchService stopped")

        self._command_handler.stop()
        self._process_tracker.terminate_all(
            exit_code=RuntimeExitCode.MANAGER_EXIT,
            timeout=self._settings.shutdown_timeout,
        )
        self._logger.info("Manager shutdown complete")

    def _register_service_app(self, service_name: str) -> str:
        """Register a framework service as an app in the Apps hierarchy.

        Delegates to the shared ``ensure_service_app`` utility.

        Args:
            service_name: Service identifier (e.g., "watchservice", "task_dispatcher").

        Returns:
            The computed app_id (64-char SHA-256 hex).
        """
        return ensure_service_app(self._database, service_name)

    def _start_watch_service(self) -> None:
        """Start WatchService regardless of current root count.

        If no active roots exist, the service starts in standby and
        polls for newly registered roots every ``reconcile_interval``
        seconds via hot-reload.
        """
        try:
            roots = load_roots_from_database(self._database)
        except Exception:
            self._logger.exception("Failed to load watch roots from database")
            roots = []

        watch_service_app_id = self._register_service_app("watchservice")
        event_handler = IngestEventHandler(self._database, app_id=watch_service_app_id)
        self._watch_service = WatchService(
            roots,
            event_handler,
            settings=self._watch_service_settings,
            database=self._database,
        )
        self._watch_service.start()
        if roots:
            self._logger.info("WatchService started with %d root(s)", len(roots))
        else:
            self._logger.info("WatchService started, waiting for roots to be registered")

    def _start_task_dispatcher(self) -> None:
        """Start TaskDispatcher if a DramatiqBroker is available."""
        if self._dramatiq_broker is None:
            self._logger.debug("No DramatiqBroker provided, TaskDispatcher not started")
            return

        self._register_service_app("task_dispatcher")
        self._task_dispatcher = TaskDispatcher(
            self._database,
            self._dramatiq_broker,
            settings=self._task_dispatcher_settings,
        )
        self._task_dispatcher.start()
        self._logger.info("TaskDispatcher started")
