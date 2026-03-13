"""Shared app status update utility.

Provides update_app_status() for writing runtime state back to the Apps
table. Used by scraping, orchestration, and any other application type
that needs to update app lifecycle columns.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select

from data_collector.enums import RunStatus
from data_collector.tables.apps import Apps
from data_collector.utilities.database.main import Database


def update_app_status(
    database: Database,
    app_id: str,
    *,
    run_status: int | None = None,
    runtime_id: str | None = None,
    next_run: datetime | None = None,
    solved: int | None = None,
    failed: int | None = None,
    task_size: int | None = None,
    progress: int | None = None,
    fatal_flag: int | None = None,
    fatal_msg: str | None = None,
    fatal_time: datetime | None = None,
    last_run: datetime | None = None,
    eta: datetime | None = None,
    disable: bool | None = None,
    app_pids: str | None = None,
) -> None:
    """Update Apps table columns for the given app_id.

    Only non-None arguments are written. No-op if the app_id is not found.

    Args:
        database: Database instance connected to the main schema.
        app_id: The 64-char SHA-256 app identifier (Apps.app column).
        run_status: RunStatus enum value.
        runtime_id: Current runtime hash.
        next_run: Scheduled next execution time.
        solved: Successfully processed item count.
        failed: Error item count.
        task_size: Total items to process.
        progress: Completion percentage (0-100).
        fatal_flag: FatalFlag enum value.
        fatal_msg: Fatal condition description.
        fatal_time: When fatal was detected.
        last_run: Timestamp of last run start. Auto-set to now(UTC) when
            run_status is RUNNING and last_run is not explicitly provided.
        eta: Estimated time of completion.
        disable: Set to True to disable the app (blocker fatals). None leaves
            the column unchanged.
        app_pids: Process ID string to record in the Apps table.
    """
    field_map: dict[str, Any] = {
        "run_status": run_status,
        "runtime_id": runtime_id,
        "next_run": next_run,
        "solved": solved,
        "failed": failed,
        "task_size": task_size,
        "progress": progress,
        "fatal_flag": fatal_flag,
        "fatal_msg": fatal_msg,
        "fatal_time": fatal_time,
        "last_run": last_run,
        "eta": eta,
        "disable": disable,
        "app_pids": app_pids,
    }
    if run_status == RunStatus.RUNNING and last_run is None:
        field_map["last_run"] = datetime.now(UTC)

    updates = {k: v for k, v in field_map.items() if v is not None}
    if not updates:
        return

    with database.create_session() as session:
        stmt = select(Apps).where(Apps.app == app_id)
        row = database.query(stmt, session).scalar_one_or_none()
        if row is None:
            return
        for attr, value in updates.items():
            setattr(row, attr, value)
        if run_status == RunStatus.RUNNING:
            row.eta = None  # type: ignore[assignment]
            row.progress = 0  # type: ignore[assignment]
        session.commit()
