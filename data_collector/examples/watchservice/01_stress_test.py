"""WatchService stress test -- 1000 files across 2 watch roots.

Demonstrates:
    - WatchService end-to-end lifecycle (start, monitor, stop)
    - Streaming-aware file stability detection
    - Two independent watch roots with different country codes
    - Concurrent file generation (8 writer threads)
    - Verification: all files arrive in Events table, zero duplicates
    - App hierarchy registration in Apps table

Requires:
    DC_DB_MAIN_USERNAME, DC_DB_MAIN_PASSWORD, DC_DB_MAIN_DATABASENAME,
    DC_DB_MAIN_IP, DC_DB_MAIN_PORT environment variables.

Run:
    python -m data_collector.examples run watchservice/01_stress_test
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
from data_collector.tables.deploy import ExampleDeploy
from data_collector.tables.pipeline import Events, WatchRoots
from data_collector.tables.runtime import Runtime
from data_collector.utilities.database.main import Database
from data_collector.utilities.functions.runtime import AppInfo, get_app_info
from data_collector.utilities.log.main import LoggingService

_REQUIRED_ENV = (
    "DC_DB_MAIN_USERNAME", "DC_DB_MAIN_PASSWORD", "DC_DB_MAIN_DATABASENAME",
    "DC_DB_MAIN_IP", "DC_DB_MAIN_PORT",
)

TOTAL_FILES = 1000

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
            "watch_group": "stress_test",
            "worker_path": "data_collector.croatia.gazette.ocr.main",
            "extensions": '[".bin"]',
            "recursive": False,
        },
        {
            "root_path": str(ROOT_B_PATH.resolve()),
            "rel_path": "root_b",
            "country": "DE",
            "watch_group": "stress_test",
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


def _wait_for_stabilization(database: Database, expected: int, timeout: float = 120.0) -> float:
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


def _print_results(database: Database, app_id: str, expected_total: int) -> bool:
    """Query Events table and print verification results. Returns True on pass."""
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

    app_verified = app_row is not None
    min_kb = f"{min_size / 1024:.1f} KB" if min_size else "N/A"
    max_kb = f"{max_size / 1024:.1f} KB" if max_size else "N/A"

    print("\n=== Results ===\n")
    print(f"  Total files generated:    {expected_total}")
    print(f"  Total Events rows:        {total_events}")
    print(f"  Stable events:            {stable_events}")
    print(f"  Unstable events:          {unstable_events}")
    print(f"  Duplicate path_hashes:    {duplicate_count}")
    print(f"  Root A events (HR):       {events_root_a}")
    print(f"  Root B events (DE):       {events_root_b}")
    print(f"  Apps row verified:        {'Yes' if app_verified else 'NO'}")
    print(f"  File size range:          {min_kb} - {max_kb}")

    passed = (
        total_events == expected_total
        and stable_events == expected_total
        and duplicate_count == 0
        and app_verified
    )

    print()
    if passed:
        print("  PASS: All files arrived, zero duplicates, app registered.")
    else:
        print("  FAIL: Check results above for mismatches.")

    return passed


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    """Run WatchService stress test."""
    print("\n=== WatchService Stress Test ===\n")

    if not _check_db_env():
        return

    # ---- Step 1: Deploy tables ----
    print("[1/6] Deploying tables (recreate) ...")
    deploy = ExampleDeploy()
    deploy.recreate_tables()
    deploy.populate_tables()
    database = deploy.database
    print("  Tables created and codebooks populated.\n")

    # ---- Step 2: Register app hierarchy ----
    print("[2/6] Registering app hierarchy ...")
    app_info = get_app_info(__file__, depth=-3)
    assert isinstance(app_info, dict)
    app_id = app_info["app_id"]
    runtime_id = os.urandom(16).hex()

    _seed_app_hierarchy(database, app_info, runtime_id)
    print(f"  app_id:  {app_id}")
    print(f"  group:   {app_info['app_group']}")
    print(f"  parent:  {app_info['app_parent']}")

    with database.create_session() as session:
        verified = database.query(
            select(Apps).where(Apps.app == app_id), session,
        ).scalar_one_or_none()
    print(f"  Verified: Apps row {'exists' if verified else 'MISSING'} in database.\n")

    # ---- Step 3: Register watch roots ----
    print("[3/6] Registering watch roots ...")
    roots = _register_watch_roots(database)
    print()

    # ---- Step 4: Start WatchService ----
    print("[4/6] Starting WatchService ...")

    log_settings = LogSettings(log_level=logging.ERROR, log_error_file="error.log")
    logging_service = LoggingService(
        logger_name="examples.watchservice.stress_test",
        settings=log_settings,
        db_engine=database.engine,
    )
    logging_service.configure_logger()
    logging.getLogger("data_collector.messaging.watchservice").setLevel(logging.ERROR)

    settings = WatchServiceSettings(debounce=0.3, stability_timeout=3, reconcile_interval=120)
    event_handler = IngestEventHandler(database, app_id=app_id)
    watch_service = WatchService(roots, event_handler, settings=settings, database=database)
    watch_service.start()

    print(f"  Observer: {settings.observer} | Debounce: {settings.debounce}s | Timeout: {settings.stability_timeout}s")
    print(f"  WatchService running ({len(roots)} roots).\n")

    try:
        # ---- Step 5: Generate files ----
        print(f"[5/6] Generating {TOTAL_FILES} files across 2 roots ...")
        stop_event = threading.Event()
        generation_start = time.monotonic()

        generator_thread = threading.Thread(
            target=generate_files,
            kwargs={
                "total": TOTAL_FILES,
                "root_a": ROOT_A_PATH,
                "root_b": ROOT_B_PATH,
                "stop_event": stop_event,
            },
            daemon=True,
        )
        generator_thread.start()
        generator_thread.join()

        generation_elapsed = time.monotonic() - generation_start
        print(f"  Generation complete in {generation_elapsed:.1f}s.\n")

        # ---- Step 6: Wait for stabilization ----
        print("[6/6] Waiting for events to stabilize ...")
        stabilization_elapsed = _wait_for_stabilization(database, expected=TOTAL_FILES, timeout=180.0)
        print(f"  All events processed in {stabilization_elapsed:.1f}s.")

        # ---- Results ----
        _print_results(database, app_id, expected_total=TOTAL_FILES)

        # ---- Wait for user ----
        print()
        input("Press Enter to clean up and exit ...")

    finally:
        print("\nCleaning up ...")
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
