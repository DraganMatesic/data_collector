"""Multi-backend file storage and transfer between local and remote backends.

Demonstrates:
    - Creating two FilesystemBackend instances (local + simulated remote)
    - Registering a backend in the StorageBackend database table
    - Resolving a named backend with StorageManager.resolve_backend()
    - Storing files on the local backend (dedup on reruns)
    - Transferring files to the remote backend (copy mode)
    - Transferring files with delete_source=True (move mode)
    - Querying StoredFile.location to verify transfers
    - Idempotent on reruns: dedup prevents duplicate files, transfers skip already-transferred files

Requires:
    DC_DB_MAIN_USERNAME, DC_DB_MAIN_PASSWORD, DC_DB_MAIN_DATABASENAME,
    DC_DB_MAIN_IP, DC_DB_MAIN_PORT, DC_STORAGE_ROOT environment variables.

Run:
    python -m data_collector.examples run storage/02_multi_backend
"""

from __future__ import annotations

import os
import uuid
from datetime import UTC, datetime
from pathlib import Path

from sqlalchemy import select

from data_collector.enums.storage import FileRetention
from data_collector.settings.storage import StorageSettings
from data_collector.storage.manager import StorageManager
from data_collector.tables.apps import AppGroups, AppParents, Apps
from data_collector.tables.deploy import ExampleDeploy
from data_collector.tables.runtime import Runtime
from data_collector.tables.storage import StorageBackend
from data_collector.utilities.database.main import Database
from data_collector.utilities.functions.runtime import AppInfo, get_app_info

_REQUIRED_ENV = (
    "DC_DB_MAIN_USERNAME", "DC_DB_MAIN_PASSWORD", "DC_DB_MAIN_DATABASENAME",
    "DC_DB_MAIN_IP", "DC_DB_MAIN_PORT", "DC_STORAGE_ROOT",
)

_REMOTE_BACKEND_NAME = "fs_example_remote"


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


def _register_remote_backend(database: Database, location_name: str, root_path: Path) -> None:
    """Register a named backend in the StorageBackend table (idempotent)."""
    with database.create_session() as session:
        existing = session.execute(
            select(StorageBackend).where(StorageBackend.location_name == location_name)
        ).scalar()
        if existing is None:
            backend_row = StorageBackend(
                location_name=location_name,
                root_path=str(root_path),
                description=f"Simulated remote backend at {root_path}",
            )
            session.add(backend_row)
            print(f"  Registered backend '{location_name}' -> {root_path}")
        else:
            print(f"  Backend '{location_name}' already registered")
        session.commit()


def main() -> None:
    """Run multi-backend transfer example."""
    if not _check_db_env():
        return

    app_info = get_app_info(__file__, depth=-3)
    assert isinstance(app_info, dict)
    runtime_id = uuid.uuid4().hex

    deploy = ExampleDeploy()
    deploy.create_tables()
    deploy.populate_tables()

    database = deploy.database
    _seed_parent_rows(database, app_info, runtime_id)

    settings = StorageSettings()
    local_root = settings.root
    remote_root = local_root.parent / "remote_fileserver"

    print("=== 1. Backend setup ===")
    print(f"  Local root: {local_root}")
    print(f"  Remote root: {remote_root}")
    _register_remote_backend(database, _REMOTE_BACKEND_NAME, remote_root)

    # -- Create manager with default local backend --
    manager = StorageManager(
        database,
        app_info["app_group"],
        app_info["app_parent"],
        app_info["app_name"],
        runtime_id=runtime_id,
        settings=settings,
    )

    with database.create_session() as session:
        # -- 2. Store files locally (dedup skips if already stored) --
        print("\n=== 2. Storing files on local backend ===")
        path_a = manager.store(
            b"Contract document for client Alpha, signed 2025-12-01.",
            "txt",
            original_filename="contract_alpha.txt",
            retention_category=FileRetention.REGULATORY_5Y,
            session=session,
        )
        print(f"  File A: {path_a}")

        path_b = manager.store(
            b"Invoice #2026-001 for services rendered in Q1 2026.",
            "txt",
            original_filename="invoice_2026_001.txt",
            retention_category=FileRetention.REGULATORY_7Y,
            session=session,
        )
        print(f"  File B: {path_b}")
        session.commit()

        # -- 3. Current state --
        print("\n=== 3. Current stored_file rows ===")
        stored_files = manager.get_stored_files(session=session)
        for stored_file in stored_files:
            print(
                f"  {stored_file.original_filename} | "
                f"location={stored_file.location} | "
                f"path={stored_file.stored_path}"
            )

        # -- 4. Resolve remote backend from database --
        print("\n=== 4. Resolving remote backend from database ===")
        remote_backend = StorageManager.resolve_backend(
            database, _REMOTE_BACKEND_NAME, session,
        )
        print(f"  Resolved: {remote_backend.location_name} -> {remote_backend.root}")

        # -- 5. Transfer file A (copy) -- only if not already on remote --
        local_files = [
            stored_file for stored_file in stored_files
            if stored_file.location == "local"
        ]

        file_a_local = next(
            (stored_file for stored_file in local_files if stored_file.original_filename == "contract_alpha.txt"),
            None,
        )

        print("\n=== 5. Transfer file A (copy mode) ===")
        if file_a_local is not None:
            remote_path_a = manager.transfer(
                file_a_local, remote_backend,
                delete_source=False,
                session=session,
            )
            session.commit()
            print(f"  Remote path: {remote_path_a}")
            print(f"  Remote file exists: {remote_path_a.exists()}")
            local_path_a = settings.root / str(file_a_local.stored_path)
            print(f"  Local copy preserved: {local_path_a.exists()}")
            print(f"  Original row location: {file_a_local.location}  (unchanged)")
            print("  New row inserted for remote copy")
        else:
            print("  Already transferred in a previous run, skipping")

        # -- 6. Transfer file B (move) -- only if still local --
        file_b_local = next(
            (stored_file for stored_file in local_files if stored_file.original_filename == "invoice_2026_001.txt"),
            None,
        )

        print("\n=== 6. Transfer file B (move mode) ===")
        if file_b_local is not None:
            local_path_b = settings.root / str(file_b_local.stored_path)
            print(f"  Local exists before: {local_path_b.exists()}")

            remote_path_b = manager.transfer(
                file_b_local, remote_backend,
                delete_source=True,
                session=session,
            )
            session.commit()
            print(f"  Remote path: {remote_path_b}")
            print(f"  Remote file exists: {remote_path_b.exists()}")
            print(f"  Local deleted: {not local_path_b.exists()}")
            print(f"  Row location: {file_b_local.location}  (updated to remote)")
        else:
            print("  Already moved in a previous run, skipping")

        # -- 7. Final state --
        print("\n=== 7. Final file locations (stored_file table) ===")
        all_files = manager.get_stored_files(session=session)
        for stored_file in all_files:
            print(
                f"  {stored_file.original_filename} | "
                f"location={stored_file.location} | "
                f"size={stored_file.file_size}"
            )

    print("\nMulti-backend example complete.")


if __name__ == "__main__":
    main()
