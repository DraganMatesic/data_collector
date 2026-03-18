"""Shared service app registration for framework-internal services.

Ensures the AppGroups, AppParents, and Apps rows exist for a service
(e.g., "manager", "watchservice", "task_dispatcher") and updates its
runtime status.  Used by both the Manager startup sequence and the
Manager's internal service registration for sub-services.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime

from sqlalchemy import select

from data_collector.enums import RunStatus
from data_collector.tables.apps import AppGroups, AppParents, Apps
from data_collector.utilities.app_status import update_app_status
from data_collector.utilities.database.main import Database
from data_collector.utilities.functions.runtime import get_app_id

_FRAMEWORK_GROUP = "data_collector"


def ensure_service_app(
    database: Database,
    service_name: str,
    *,
    set_running: bool = True,
) -> str:
    """Register a framework service in the Apps hierarchy.

    Uses the convention ``group="data_collector"``, ``parent=service_name``,
    ``name=service_name`` for internal services.

    This function is safe to call multiple times -- it uses ``session.merge``
    for the Apps row and only inserts AppGroups/AppParents if missing.

    Args:
        database: Database instance for DB operations.
        service_name: Service identifier (e.g., "manager", "watchservice").
        set_running: If True, update the app's status to RUNNING with the
            current PID and last_run timestamp.

    Returns:
        The computed app_id (64-char SHA-256 hex).
    """
    app_id = get_app_id(_FRAMEWORK_GROUP, service_name, service_name)

    with database.create_session() as session:
        existing_group = database.query(
            select(AppGroups).where(AppGroups.name == _FRAMEWORK_GROUP), session,
        ).scalar_one_or_none()
        if existing_group is None:
            database.add(AppGroups(name=_FRAMEWORK_GROUP), session)
            session.flush()

        existing_parent = database.query(
            select(AppParents).where(
                AppParents.name == service_name,
                AppParents.group_name == _FRAMEWORK_GROUP,
            ),
            session,
        ).scalar_one_or_none()
        if existing_parent is None:
            database.add(AppParents(name=service_name, group_name=_FRAMEWORK_GROUP), session)
            session.flush()

        session.merge(Apps(
            app=app_id,
            group_name=_FRAMEWORK_GROUP,
            parent_name=service_name,
            app_name=service_name,
        ))
        session.commit()

    if set_running:
        update_app_status(
            database,
            app_id,
            last_run=datetime.now(UTC),
            app_pids=str(os.getpid()),
            run_status=RunStatus.RUNNING,
        )

    return app_id
