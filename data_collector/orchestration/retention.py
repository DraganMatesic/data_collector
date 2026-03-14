"""Periodic cleanup of old log, runtime, and audit records.

The Manager calls :meth:`RetentionCleaner.run_cleanup` on a configurable
interval to prevent unbounded growth of historical tables. When app purge
is enabled, also deletes apps that have passed their scheduled removal date.
"""

from __future__ import annotations

import logging
import shutil
from datetime import UTC, datetime, timedelta
from pathlib import Path

from sqlalchemy import delete, func, select

from data_collector.settings.manager import ManagerSettings
from data_collector.tables.apps import AppGroups, AppParents, Apps
from data_collector.tables.command_log import CommandLog
from data_collector.tables.log import FunctionLog, FunctionLogError, Logs
from data_collector.tables.runtime import Runtime
from data_collector.utilities.database.main import Database


class RetentionCleaner:
    """Delete records older than configured retention thresholds.

    Args:
        database: Database instance connected to the main schema.
        settings: Manager settings with retention configuration.
        logger: Structured logger for retention-related messages.
    """

    def __init__(
        self,
        database: Database,
        settings: ManagerSettings,
        *,
        logger: logging.Logger,
    ) -> None:
        self._database = database
        self._settings = settings
        self._logger = logger

    def run_cleanup(self) -> None:
        """Delete records exceeding their retention period.

        Tables cleaned (in order to respect FK constraints):
        1. ``function_log_error`` -- FK CASCADE from function_log, but explicit
           deletion is safer for databases without CASCADE configured.
        2. ``function_log``
        3. ``logs``
        4. ``runtime``
        5. ``command_log``
        """
        now = datetime.now(UTC)

        self._delete_older_than(
            FunctionLogError,
            FunctionLogError.date_created,
            now - timedelta(days=self._settings.retention_function_log_days),
            "function_log_error",
        )
        self._delete_older_than(
            FunctionLog,
            FunctionLog.date_created,
            now - timedelta(days=self._settings.retention_function_log_days),
            "function_log",
        )
        self._delete_older_than(
            Logs,
            Logs.date_created,
            now - timedelta(days=self._settings.retention_log_days),
            "logs",
        )
        self._delete_older_than(
            Runtime,
            Runtime.date_created,
            now - timedelta(days=self._settings.retention_runtime_days),
            "runtime",
        )
        self._delete_older_than(
            CommandLog,
            CommandLog.date_created,
            now - timedelta(days=self._settings.retention_command_log_days),
            "command_log",
        )

        if self._settings.retention_app_purge_enabled:
            self._purge_removed_apps(now)

    def _delete_older_than(
        self,
        model: type,
        date_column: object,
        cutoff: datetime,
        table_name: str,
    ) -> None:
        """Execute a DELETE statement for rows older than cutoff."""
        with self._database.create_session() as session:
            statement = delete(model).where(date_column < cutoff)  # type: ignore[arg-type]
            result = session.execute(statement)
            deleted_count = int(result.rowcount)  # type: ignore[arg-type]
            session.commit()

        if deleted_count > 0:
            self._logger.info("Retention: deleted %d rows from %s", deleted_count, table_name)

    def _purge_removed_apps(self, now: datetime) -> None:
        """Delete apps whose ``removal_date`` has passed.

        For each expired app:
        1. Delete the app directory from disk (if it exists).
        2. Delete the ``Apps`` row.
        3. Delete orphaned ``AppParents`` and ``AppGroups`` rows.
        """
        with self._database.create_session() as session:
            statement = select(Apps).where(
                Apps.removal_date.isnot(None),
                Apps.removal_date <= now,
            )
            expired_apps: list[Apps] = list(self._database.query(statement, session).scalars().all())

            if not expired_apps:
                return

            package_root = Path(__file__).resolve().parent.parent

            for app in expired_apps:
                session.delete(app)

            session.flush()

            # Disk cleanup after DB delete succeeds (flush validates FK
            # constraints).  If commit later fails the directories are
            # still present; if commit succeeds but rmtree fails the
            # orphan directories are harmless and can be cleaned manually.
            for app in expired_apps:
                group_name = str(app.group_name)
                parent_name = str(app.parent_name)
                app_name = str(app.app_name)
                app_dir = package_root / group_name / parent_name / app_name

                if app_dir.is_dir():
                    try:
                        shutil.rmtree(app_dir)
                        self._logger.info(
                            "Retention: deleted app directory %s/%s/%s",
                            group_name, parent_name, app_name,
                        )
                    except OSError:
                        self._logger.exception(
                            "Retention: failed to delete app directory %s/%s/%s",
                            group_name, parent_name, app_name,
                        )

            # Clean up orphaned AppParents (no remaining Apps reference them)
            seen_parents: set[tuple[str, str]] = set()
            for app in expired_apps:
                parent_key = (str(app.group_name), str(app.parent_name))
                if parent_key in seen_parents:
                    continue
                seen_parents.add(parent_key)
                remaining_apps = self._database.query(
                    select(func.count()).select_from(Apps).where(
                        Apps.group_name == app.group_name,
                        Apps.parent_name == app.parent_name,
                    ),
                    session,
                ).scalar_one()
                if remaining_apps == 0:
                    parent_row = self._database.query(
                        select(AppParents).where(
                            AppParents.group_name == app.group_name,
                            AppParents.name == app.parent_name,
                        ),
                        session,
                    ).scalar_one_or_none()
                    if parent_row is not None:
                        session.delete(parent_row)

            session.flush()

            # Clean up orphaned AppGroups (no remaining AppParents reference them)
            seen_groups: set[str] = set()
            for app in expired_apps:
                group_name = str(app.group_name)
                if group_name in seen_groups:
                    continue
                seen_groups.add(group_name)
                remaining_parents = self._database.query(
                    select(func.count()).select_from(AppParents).where(
                        AppParents.group_name == group_name,
                    ),
                    session,
                ).scalar_one()
                if remaining_parents == 0:
                    group_row = self._database.query(
                        select(AppGroups).where(AppGroups.name == group_name),
                        session,
                    ).scalar_one_or_none()
                    if group_row is not None:
                        session.delete(group_row)

            session.commit()

        self._logger.info("Retention: purged %d expired app(s)", len(expired_apps))
