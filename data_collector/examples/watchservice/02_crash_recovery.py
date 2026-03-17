# pyright: reportPrivateUsage=false
"""WatchService crash recovery -- resilience proof of concept.

Demonstrates:
    - Normal operation with files stabilizing correctly
    - Abrupt crash simulation (no graceful shutdown)
    - Files arriving while WatchService is down
    - Automatic recovery on restart (pending streams + reconciliation)
    - Zero data loss verification after recovery

The test runs four phases:
    1. Normal operation: 500 files generated and fully stabilized
    2. Crash mid-stream: WatchService killed while processing 500 more files
    3. Downtime: remaining files written while service is offline
    4. Restart: fresh WatchService recovers unstable events and reconciles disk

Requires:
    DC_DB_MAIN_USERNAME, DC_DB_MAIN_PASSWORD, DC_DB_MAIN_DATABASENAME,
    DC_DB_MAIN_IP, DC_DB_MAIN_PORT environment variables.

Run:
    python -m data_collector.examples run watchservice/02_crash_recovery
"""

from __future__ import annotations

import logging
import os
import shutil
import threading
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from sqlalchemy import func, select

from data_collector.examples.watchservice._file_generator import generate_files
from data_collector.messaging.watchservice import IngestEventHandler, Root, WatchService
from data_collector.settings.main import LogSettings
from data_collector.settings.watchservice import WatchServiceSettings
from data_collector.tables.apps import AppGroups, AppParents, Apps
from data_collector.tables.deploy import Deploy
from data_collector.tables.pipeline import Events, WatchRoots
from data_collector.tables.runtime import Runtime
from data_collector.utilities.database.main import Database
from data_collector.utilities.functions.runtime import AppInfo, get_app_info
from data_collector.utilities.log.main import LoggingService

_REQUIRED_ENV = (
    "DC_DB_MAIN_USERNAME", "DC_DB_MAIN_PASSWORD", "DC_DB_MAIN_DATABASENAME",
    "DC_DB_MAIN_IP", "DC_DB_MAIN_PORT",
)

PHASE_1_FILES = 500
PHASE_2_FILES = 500             # files generated while crash happens mid-stream
TOTAL_FILES = PHASE_1_FILES + PHASE_2_FILES
CRASH_AFTER_ON_DISK = 150       # crash when this many phase-2 files are on disk

EXAMPLE_DIR = Path(__file__).parent
ROOT_A_PATH = EXAMPLE_DIR / "_watched_root_a"
ROOT_B_PATH = EXAMPLE_DIR / "_watched_root_b"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _check_db_env() -> bool:
    """Return True when required DB environment variables are set."""
    missing = [variable for variable in _REQUIRED_ENV if not os.environ.get(variable)]
    if missing:
        print(f"  Skipping: DB env vars not set: {', '.join(missing)}")
        return False
    return True


def _seed_app_hierarchy(database: Database, app_info: AppInfo, runtime_id: str) -> None:
    """Seed AppGroups, AppParents, Apps, and Runtime rows."""
    group = app_info["app_group"]
    parent = app_info["app_parent"]
    app_id = app_info["app_id"]

    with database.create_session() as session:
        existing_group = database.query(
            select(AppGroups).where(AppGroups.name == group), session,
        ).scalar_one_or_none()
        if existing_group is None:
            database.add(AppGroups(name=group), session)
            session.flush()

        existing_parent = database.query(
            select(AppParents).where(AppParents.name == parent, AppParents.group_name == group), session,
        ).scalar_one_or_none()
        if existing_parent is None:
            database.add(AppParents(name=parent, group_name=group), session)
            session.flush()

        session.merge(Apps(
            app=app_id,
            group_name=group,
            parent_name=parent,
            app_name=app_info["app_name"],
        ))
        session.merge(Runtime(
            runtime=runtime_id,
            app_id=app_id,
            start_time=datetime.now(UTC),
        ))
        session.commit()


def _register_watch_roots(database: Database) -> list[Root]:
    """Create watch directories and insert WatchRoots rows."""
    ROOT_A_PATH.mkdir(parents=True, exist_ok=True)
    ROOT_B_PATH.mkdir(parents=True, exist_ok=True)

    root_configs = [
        {
            "root_path": str(ROOT_A_PATH.resolve()),
            "rel_path": "root_a",
            "country": "HR",
            "watch_group": "crash_recovery",
            "worker_path": "data_collector.croatia.gazette.ocr.main",
            "extensions": '[".bin"]',
            "recursive": False,
        },
        {
            "root_path": str(ROOT_B_PATH.resolve()),
            "rel_path": "root_b",
            "country": "DE",
            "watch_group": "crash_recovery",
            "worker_path": "data_collector.germany.contracts.ocr.main",
            "extensions": '[".bin"]',
            "recursive": False,
        },
    ]

    roots: list[Root] = []
    for index, config in enumerate(root_configs):
        with database.create_session() as session:
            watch_root = WatchRoots(**config)
            database.add(watch_root, session)
            session.commit()
            root_id = int(watch_root.id)  # type: ignore[arg-type]

        root_path = str(config["root_path"])
        country = str(config["country"])
        roots.append(Root(
            root_id=root_id,
            root_path=root_path,
            rel_path=str(config["rel_path"]),
            country=country,
            watch_group=str(config["watch_group"]),
            worker_path=str(config["worker_path"]),
            extensions=[".bin"],
            recursive=False,
        ))
        print(f"  Root {index + 1}: {root_path} (country={country}, ext=.bin)")

    return roots


def _create_watch_service(
    roots: list[Root],
    database: Database,
    app_id: str,
    settings: WatchServiceSettings,
) -> WatchService:
    """Create a fresh WatchService instance."""
    event_handler = IngestEventHandler(database, app_id=app_id)
    return WatchService(roots, event_handler, settings=settings, database=database)


def _simulate_crash(watch_service: WatchService) -> None:
    """Kill WatchService without graceful shutdown.

    Sets the stop event and kills the observer, but does NOT drain
    the writer queue or join threads.  This mimics what happens on
    process kill -- queued but unwritten events are lost, and
    unstable events remain in the database.
    """
    watch_service._stop_event.set()
    if watch_service._observer is not None:
        watch_service._observer.stop()
    # No sentinel to writer, no thread joins, no drain.
    # Daemon threads exit on their own via _stop_event check.


def _query_event_counts(database: Database) -> dict[str, int]:
    """Return current Events table counts."""
    with database.create_session() as session:
        total = database.query(
            select(func.count()).select_from(Events).where(Events.archive.is_(None)), session,
        ).scalar() or 0
        stable = database.query(
            select(func.count()).select_from(Events).where(
                Events.archive.is_(None), Events.stable.is_(True),
            ), session,
        ).scalar() or 0
    return {"total": total, "stable": stable, "unstable": total - stable}


def _count_files_on_disk() -> int:
    """Count .bin files across both watch roots."""
    count = 0
    for root_dir in [ROOT_A_PATH, ROOT_B_PATH]:
        if root_dir.exists():
            count += sum(1 for file in root_dir.iterdir() if file.suffix == ".bin")
    return count


def _wait_for_stabilization(database: Database, expected: int, timeout: float = 180.0) -> float:
    """Poll Events table until all events are stable or timeout expires."""
    start = time.monotonic()
    last_print = 0.0

    while (time.monotonic() - start) < timeout:
        with database.create_session() as session:
            total = database.query(
                select(func.count()).select_from(Events).where(Events.archive.is_(None)), session,
            ).scalar() or 0
            stable = database.query(
                select(func.count()).select_from(Events).where(
                    Events.archive.is_(None), Events.stable.is_(True),
                ), session,
            ).scalar() or 0

        now = time.monotonic()
        if now - last_print >= 2.0:
            print(f"\r  Stable: {stable}/{total}", end="", flush=True)
            last_print = now

        if total >= expected and stable >= total:
            elapsed = time.monotonic() - start
            print(f"\r  Stable: {stable}/{total}                    ")
            return elapsed

        time.sleep(0.5)

    elapsed = time.monotonic() - start
    print()
    return elapsed


def _print_results(database: Database, app_id: str, pre_crash_events: int) -> bool:
    """Query Events table and print crash recovery verification results."""
    with database.create_session() as session:
        total_events = database.query(
            select(func.count()).select_from(Events).where(Events.archive.is_(None)), session,
        ).scalar() or 0

        stable_events = database.query(
            select(func.count()).select_from(Events).where(
                Events.archive.is_(None), Events.stable.is_(True),
            ), session,
        ).scalar() or 0

        unstable_events = total_events - stable_events

        duplicate_count = database.query(
            select(func.count()).select_from(
                select(Events.path_hash)
                .where(Events.archive.is_(None))
                .group_by(Events.path_hash)
                .having(func.count() > 1)
                .subquery()
            ), session,
        ).scalar() or 0

        events_root_a = database.query(
            select(func.count()).select_from(Events).where(
                Events.archive.is_(None), Events.country == "HR",
            ), session,
        ).scalar() or 0

        events_root_b = database.query(
            select(func.count()).select_from(Events).where(
                Events.archive.is_(None), Events.country == "DE",
            ), session,
        ).scalar() or 0

        app_row = database.query(
            select(Apps).where(Apps.app == app_id), session,
        ).scalar_one_or_none()

        min_size: Any = database.query(
            select(func.min(Events.file_size)).where(Events.archive.is_(None)), session,
        ).scalar()
        max_size: Any = database.query(
            select(func.max(Events.file_size)).where(Events.archive.is_(None)), session,
        ).scalar()

    recovered_events = total_events - pre_crash_events
    app_verified = app_row is not None
    min_kb = f"{min_size / 1024:.1f} KB" if min_size else "N/A"
    max_kb = f"{max_size / 1024:.1f} KB" if max_size else "N/A"

    print("\n=== Results ===\n")
    print(f"  Total files on disk:      {_count_files_on_disk()}")
    print(f"  Total Events rows:        {total_events}")
    print(f"  Stable events:            {stable_events}")
    print(f"  Unstable events:          {unstable_events}")
    print(f"  Duplicate path_hashes:    {duplicate_count}")
    print(f"  Root A events (HR):       {events_root_a}")
    print(f"  Root B events (DE):       {events_root_b}")
    print(f"  Pre-crash events:         {pre_crash_events}")
    print(f"  Recovered after restart:  {recovered_events}")
    print(f"  Apps row verified:        {'Yes' if app_verified else 'NO'}")
    print(f"  File size range:          {min_kb} - {max_kb}")

    passed = (
        total_events == TOTAL_FILES
        and stable_events == TOTAL_FILES
        and duplicate_count == 0
        and app_verified
    )

    print()
    if passed:
        print("  PASS: All files recovered after crash, zero duplicates.")
    else:
        print("  FAIL: Check results above for mismatches.")

    return passed


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    """Run WatchService crash recovery proof of concept."""
    print("\n=== WatchService Crash Recovery ===\n")

    if not _check_db_env():
        return

    # ---- Step 1: Deploy tables ----
    print("[1/8] Deploying tables (recreate) ...")
    deploy = Deploy()
    deploy.recreate_tables()
    deploy.populate_tables()
    database = deploy.database
    print("  Tables created and codebooks populated.\n")

    # ---- Step 2: Register app hierarchy ----
    print("[2/8] Registering app hierarchy ...")
    app_info = get_app_info(__file__, depth=-3)
    assert isinstance(app_info, dict)
    app_id = app_info["app_id"]
    runtime_id = os.urandom(16).hex()

    _seed_app_hierarchy(database, app_info, runtime_id)
    print(f"  app_id: {app_id}")

    with database.create_session() as session:
        verified = database.query(
            select(Apps).where(Apps.app == app_id), session,
        ).scalar_one_or_none()
    print(f"  Verified: Apps row {'exists' if verified else 'MISSING'}.\n")

    # ---- Step 3: Register watch roots ----
    print("[3/8] Registering watch roots ...")
    roots = _register_watch_roots(database)
    print()

    # Configure logging -- suppress WatchService internals
    log_settings = LogSettings(log_level=logging.ERROR, log_error_file="error.log")
    logging_service = LoggingService(
        logger_name="examples.watchservice.crash_recovery",
        settings=log_settings,
        db_engine=database.engine,
    )
    logging_service.configure_logger()
    logging.getLogger("data_collector.messaging.watchservice").setLevel(logging.ERROR)

    settings = WatchServiceSettings(debounce=0.3, stability_timeout=3, reconcile_interval=120)
    watch_service: WatchService | None = None

    try:
        # ---- Step 4: Phase 1 -- Normal operation ----
        print(f"[4/8] Phase 1: Normal operation ({PHASE_1_FILES} files) ...")
        watch_service = _create_watch_service(roots, database, app_id, settings)
        watch_service.start()
        print("  WatchService started.")

        generate_files(total=PHASE_1_FILES, root_a=ROOT_A_PATH, root_b=ROOT_B_PATH)
        print("  Waiting for stabilization ...")
        _wait_for_stabilization(database, expected=PHASE_1_FILES)

        counts = _query_event_counts(database)
        print(f"  Phase 1 complete: {counts['total']} events, {counts['stable']} stable.\n")

        # ---- Step 5: Phase 2 -- Crash while files keep arriving ----
        print(f"[5/8] Phase 2: Starting {PHASE_2_FILES} more files, crash after ~{CRASH_AFTER_ON_DISK} ...")
        print("  (Simulates files arriving from network share / FTP / external system)")

        generator_thread = threading.Thread(
            target=generate_files,
            kwargs={
                "total": PHASE_2_FILES,
                "root_a": ROOT_A_PATH,
                "root_b": ROOT_B_PATH,
                "start_index": PHASE_1_FILES,
                "progress_interval": 50,
            },
            daemon=True,
        )
        generator_thread.start()

        # Wait for some files to land, then crash
        while _count_files_on_disk() < PHASE_1_FILES + CRASH_AFTER_ON_DISK:
            time.sleep(0.1)

        print("\n  >>> CRASH: WatchService killed abruptly <<<")
        _simulate_crash(watch_service)
        time.sleep(1.0)  # let daemon threads notice stop_event

        counts = _query_event_counts(database)
        detected_before_crash = counts["total"] - PHASE_1_FILES
        print(f"  Events in DB:  {counts['total']}  ({PHASE_1_FILES} pre-crash + {detected_before_crash} before crash)")
        print(f"  Stable:        {counts['stable']}")
        print(f"  Unstable:      {counts['unstable']}")

        pre_crash_events = counts["total"]

        # ---- Step 6: Phase 3 -- Files continue during downtime ----
        print("\n[6/8] Phase 3: WatchService is DOWN, files still arriving ...")

        generator_thread.join()  # wait for all 500 phase-2 files to finish
        print(f"  Files on disk: {_count_files_on_disk()}")
        counts = _query_event_counts(database)
        print(f"  Events in DB:  {counts['total']}  (frozen since crash)\n")

        # ---- Step 7: Phase 4 -- Restart and recovery ----
        print("[7/8] Phase 4: Restart and recovery ...")
        watch_service = _create_watch_service(roots, database, app_id, settings)
        watch_service.start()
        print("  WatchService restarted.")
        print("  Waiting for all events to stabilize ...")

        _wait_for_stabilization(database, expected=TOTAL_FILES, timeout=240.0)

        # ---- Step 8: Results ----
        print("[8/8] Verification ...")
        _print_results(database, app_id, pre_crash_events)

        print()
        input("Press Enter to clean up and exit ...")

    finally:
        print("\nCleaning up ...")
        if watch_service is not None and watch_service.is_running:
            watch_service.stop()
        print("  WatchService stopped.")
        logging_service.stop()

        if ROOT_A_PATH.exists():
            shutil.rmtree(ROOT_A_PATH)
        if ROOT_B_PATH.exists():
            shutil.rmtree(ROOT_B_PATH)
        print("  Watch directories removed.")
        print("  Done.")


if __name__ == "__main__":
    main()
