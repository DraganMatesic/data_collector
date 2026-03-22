"""Local file storage with hash-based naming, deduplication, and querying.

Demonstrates:
    - Creating a StorageManager with default local backend
    - Storing file content with store() (SHA-256 hash-based naming)
    - Storing from an existing file with store_file()
    - Deduplication: second store of identical content is skipped
    - Querying stored files with get_stored_files()
    - Checking total storage size with get_storage_size()

Requires:
    DC_DB_MAIN_USERNAME, DC_DB_MAIN_PASSWORD, DC_DB_MAIN_DATABASENAME,
    DC_DB_MAIN_IP, DC_DB_MAIN_PORT, DC_STORAGE_ROOT environment variables.

Run:
    python -m data_collector.examples run storage/01_local_storage
"""

from __future__ import annotations

import os
import tempfile
import uuid
from datetime import UTC, datetime
from pathlib import Path

from sqlalchemy import select

from data_collector.enums.storage import FileRetention
from data_collector.settings.main import MainDatabaseSettings
from data_collector.settings.storage import StorageSettings
from data_collector.storage.manager import StorageManager
from data_collector.tables.apps import AppGroups, AppParents, Apps
from data_collector.tables.deploy import Deploy
from data_collector.tables.runtime import Runtime
from data_collector.tables.storage import StoredFile
from data_collector.utilities.database.main import Database
from data_collector.utilities.functions.runtime import AppInfo, get_app_info

_REQUIRED_ENV = (
    "DC_DB_MAIN_USERNAME", "DC_DB_MAIN_PASSWORD", "DC_DB_MAIN_DATABASENAME",
    "DC_DB_MAIN_IP", "DC_DB_MAIN_PORT", "DC_STORAGE_ROOT",
)


def _check_db_env() -> bool:
    """Return True when required environment variables are set."""
    missing = [variable for variable in _REQUIRED_ENV if not os.environ.get(variable)]
    if missing:
        print(f"Skipping: env vars not set: {', '.join(missing)}")
        return False
    return True


def _seed_parent_rows(database: Database, app_info: AppInfo, runtime_id: str) -> None:
    """Seed AppGroups, AppParents, Apps, and Runtime rows required by FK constraints."""
    group = app_info["app_group"]
    parent = app_info["app_parent"]
    app_id = app_info["app_id"]
    with database.create_session() as session:
        if not session.execute(select(AppGroups).where(AppGroups.name == group)).scalar():
            session.add(AppGroups(name=group))
            session.flush()
        if not session.execute(
            select(AppParents).where(AppParents.name == parent, AppParents.group_name == group)
        ).scalar():
            session.add(AppParents(name=parent, group_name=group))
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


def _print_stored_files(files: list[StoredFile]) -> None:
    """Print stored file metadata in a readable format."""
    for stored_file in files:
        print(
            f"  hash={str(stored_file.content_hash)[:12]}... | "
            f"name={stored_file.original_filename} | "
            f"size={stored_file.file_size} bytes | "
            f"ext={stored_file.file_extension} | "
            f"location={stored_file.location}"
        )


def main() -> None:
    """Run local storage example with real DB inserts."""
    if not _check_db_env():
        return

    app_info = get_app_info(__file__, depth=-3)
    assert isinstance(app_info, dict)
    runtime_id = uuid.uuid4().hex

    deploy = Deploy()
    deploy.create_tables()
    deploy.populate_tables()

    database = Database(MainDatabaseSettings())
    _seed_parent_rows(database, app_info, runtime_id)

    settings = StorageSettings()
    manager = StorageManager(
        database,
        app_info["app_group"],
        app_info["app_parent"],
        app_info["app_name"],
        runtime_id=runtime_id,
        settings=settings,
    )

    print(f"Storage root: {settings.root}")
    print(f"App directory: {manager.app_directory}")
    print(f"Today directory: {manager.today_directory}")
    print()

    with database.create_session() as session:
        # -- 1. Store raw content --
        print("=== 1. Storing raw content ===")
        content_a = b"Annual report content for fiscal year 2025."
        path_a = manager.store(
            content_a, "txt",
            original_filename="annual_report_2025.txt",
            retention_category=FileRetention.STANDARD,
            session=session,
        )
        print(f"  Stored at: {path_a}")
        print(f"  File exists: {path_a.exists()}")
        session.commit()

        # -- 2. Store different content --
        print("\n=== 2. Storing different content ===")
        content_b = b"Monthly financial summary for January 2026."
        path_b = manager.store(
            content_b, "txt",
            original_filename="monthly_summary_jan_2026.txt",
            retention_category=FileRetention.REGULATORY_5Y,
            session=session,
        )
        print(f"  Stored at: {path_b}")
        print(f"  Different path: {path_a != path_b}")
        session.commit()

        # -- 3. Deduplication: same content skips storage --
        print("\n=== 3. Deduplication test (same content) ===")
        path_dup = manager.store(
            content_a, "txt",
            original_filename="annual_report_copy.txt",
            session=session,
        )
        print(f"  Returned path: {path_dup}")
        print(f"  Same as original: {path_a == path_dup}")
        session.commit()

        # -- 4. Store from existing file --
        print("\n=== 4. Storing from existing file ===")
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as temp_file:
            temp_file.write(b"id,name,value\n1,alpha,100\n2,beta,200\n")
            temp_path = Path(temp_file.name)
        try:
            path_c = manager.store_file(
                temp_path,
                original_filename="quarterly_data.csv",
                retention_category=FileRetention.REGULATORY_3Y,
                session=session,
            )
            print(f"  Stored at: {path_c}")
            session.commit()
        finally:
            temp_path.unlink(missing_ok=True)

        # -- 5. Query stored files --
        print("\n=== 5. All stored files ===")
        stored_files = manager.get_stored_files(session=session)
        print(f"  Total files tracked: {len(stored_files)}")
        _print_stored_files(stored_files)

        # -- 6. Storage size --
        print("\n=== 6. Storage size ===")
        total_bytes = manager.get_storage_size(session=session)
        print(f"  Total size: {total_bytes} bytes")

    print("\nLocal storage example complete.")


if __name__ == "__main__":
    main()
