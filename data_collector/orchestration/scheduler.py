"""Scheduling logic for the orchestration manager.

Determines which apps are ready to run based on their ``next_run``,
``interval``, and ``cron_expression`` columns. Provides fallback
``next_run`` calculation when an app completes without setting its own.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta

from croniter import croniter
from sqlalchemy import select

from data_collector.enums import AppType, FatalFlag, RunStatus
from data_collector.tables.apps import Apps
from data_collector.utilities.app_status import update_app_status
from data_collector.utilities.database.main import Database


class Scheduler:
    """Query and schedule apps based on their scheduling configuration.

    Args:
        database: Database instance connected to the main schema.
        logger: Structured logger for schedule-related messages.
    """

    def __init__(self, database: Database, *, logger: logging.Logger) -> None:
        self._database = database
        self._logger = logger

    def get_ready_apps(self) -> list[Apps]:
        """Return apps whose ``next_run`` has passed and are eligible to launch.

        Eligibility requires all of:
        - ``disable`` is ``False``
        - ``run_status`` is ``NOT_RUNNING``
        - ``fatal_flag`` is ``NONE``
        - ``next_run`` is not ``NULL`` and <= now
        """
        now = datetime.now(UTC)
        with self._database.create_session() as session:
            statement = (
                select(Apps)
                .where(
                    Apps.app_type == AppType.MANAGED,
                    Apps.disable == False,  # noqa: E712
                    Apps.run_status == RunStatus.NOT_RUNNING,
                    Apps.fatal_flag == FatalFlag.NONE,
                    Apps.next_run.isnot(None),
                    Apps.next_run <= now,
                )
            )
            result = self._database.query(statement, session)
            apps: list[Apps] = list(result.scalars().all())
            # Detach from session so Manager can use them after session closes
            session.expunge_all()
        return apps

    def calculate_next_run(self, app: Apps) -> datetime | None:
        """Calculate the next run time from the app's scheduling configuration.

        Precedence:
        1. ``cron_expression`` -- parsed via croniter
        2. ``interval`` -- added as minutes to current time
        3. Neither set -- returns ``None`` (manual-only app)

        Args:
            app: Apps ORM instance with scheduling columns.

        Returns:
            Next scheduled datetime (UTC), or ``None`` if no schedule configured.
        """
        now = datetime.now(UTC)

        cron_expr = str(app.cron_expression) if app.cron_expression is not None else ""  # type: ignore[redundant-expr]
        if cron_expr.strip():
            try:
                cron = croniter(cron_expr.strip(), now)
                next_time: datetime = cron.get_next(datetime)  # type: ignore[assignment]
                return next_time
            except (ValueError, KeyError):
                self._logger.error(
                    "Invalid cron expression for app %s/%s/%s: %s",
                    app.group_name, app.parent_name, app.app_name, cron_expr,
                )
                return None

        interval_val = int(app.interval) if app.interval is not None else 0  # type: ignore[redundant-expr]
        if interval_val > 0:
            return now + timedelta(minutes=interval_val)

        return None

    def set_fallback_next_run(self, app_id: str, app: Apps) -> None:
        """Set ``next_run`` as a fallback when the app did not set its own.

        Called after an app completes. If ``next_run`` is still in the past
        (meaning the app did not update it during its run), the Manager
        calculates the next run from ``interval`` or ``cron_expression``.

        Args:
            app_id: The 64-char SHA-256 app identifier.
            app: Apps ORM instance (may be stale; next_run is re-read from DB).
        """
        with self._database.create_session() as session:
            statement = select(Apps).where(Apps.app == app_id)
            fresh_app = self._database.query(statement, session).scalar_one_or_none()
            if fresh_app is None:
                return

            now = datetime.now(UTC)
            # If the app already set a future next_run, respect it
            if fresh_app.next_run is not None and fresh_app.next_run > now:
                return

            next_run = self.calculate_next_run(fresh_app)
            if next_run is not None:
                update_app_status(self._database, app_id, next_run=next_run)
                self._logger.info(
                    "Fallback next_run set for %s/%s/%s: %s",
                    fresh_app.group_name, fresh_app.parent_name, fresh_app.app_name, next_run,
                )
