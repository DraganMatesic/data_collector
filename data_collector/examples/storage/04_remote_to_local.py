"""Pull files from a remote backend to local storage for processing.

Demonstrates:
    - File originates on a remote file server (simulated via a second directory)
    - StorageManager with remote backend as primary (files stored directly on remote)
    - Pulling a local copy with transfer(source_backend=remote, target=local)
    - Original on remote is preserved, local copy tracked as separate StoredFile row
    - Querying stored_file to see both locations for the same content
    - Idempotent on reruns: dedup prevents duplicate files, transfer skips existing copies

Use case:
    A partner delivers ZIP/PDF files to a shared file server. Your app pulls
    a local copy for processing (OCR, extraction, parsing) without touching
    the original. Both the remote original and local working copy are tracked
    in the stored_file table with independent retention policies.

Requires:
    DC_DB_MAIN_USERNAME, DC_DB_MAIN_PASSWORD, DC_DB_MAIN_DATABASENAME,
    DC_DB_MAIN_IP, DC_DB_MAIN_PORT, DC_STORAGE_ROOT environment variables.

    Run storage/02_multi_backend first to ensure the 'fs_example_remote'
    backend is registered, or this example will register it automatically.

Run:
    python -m data_collector.examples run storage/04_remote_to_local
"""

from __future__ import annotations

import os
import uuid
from datetime import UTC, datetime
from pathlib import Path

from sqlalchemy import select

from data_collector.enums.storage import FileRetention
from data_collector.settings.main import MainDatabaseSettings
from data_collector.settings.storage import StorageSettings
from data_collector.storage.backend import FilesystemBackend
from data_collector.storage.manager import StorageManager
from data_collector.tables.apps import AppGroups, AppParents, Apps
from data_collector.tables.deploy import Deploy
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
    """Run remote-to-local transfer example."""
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
    local_root = settings.root
    remote_root = local_root.parent / "remote_fileserver"
    _register_remote_backend(database, _REMOTE_BACKEND_NAME, remote_root)

    # -- 1. Store files directly on the remote backend --
    # Simulates files delivered by a partner to a shared file server.
    print("\n=== 1. Storing files on remote backend (simulating partner delivery) ===")
    remote_manager = StorageManager(
        database,
        app_info["app_group"],
        app_info["app_parent"],
        app_info["app_name"],
        runtime_id=runtime_id,
        settings=settings,
        backend=_REMOTE_BACKEND_NAME,
    )

    with database.create_session() as session:
        path_zip = remote_manager.store(
            b"PK\x03\x04 simulated ZIP archive with financial reports inside",
            "zip",
            original_filename="Q1_2026_financial_reports.zip",
            retention_category=FileRetention.REGULATORY_7Y,
            session=session,
        )
        print(f"  ZIP stored on remote: {path_zip}")

        path_pdf = remote_manager.store(
            b"%PDF-1.4 simulated PDF with signed contract",
            "pdf",
            original_filename="contract_signed_2026_03.pdf",
            retention_category=FileRetention.PERMANENT,
            session=session,
        )
        print(f"  PDF stored on remote: {path_pdf}")
        session.commit()

        # -- 2. Show current state (files only on remote) --
        print("\n=== 2. Current stored_file rows ===")
        remote_files = remote_manager.get_stored_files(session=session)
        for stored_file in remote_files:
            print(
                f"  {stored_file.original_filename} | "
                f"location={stored_file.location}"
            )

        # -- 3. Resolve backends for transfer --
        print("\n=== 3. Resolving backends ===")
        remote_backend = StorageManager.resolve_backend(
            database, _REMOTE_BACKEND_NAME, session,
        )
        local_backend = FilesystemBackend(local_root, location="local")
        print(f"  Remote: {remote_backend.location_name} -> {remote_backend.root}")
        print(f"  Local:  {local_backend.location_name} -> {local_backend.root}")

        # -- 4. Pull ZIP to local for processing (copy, keep remote original) --
        print("\n=== 4. Pull ZIP from remote to local (copy mode) ===")
        zip_files = [
            stored_file for stored_file in remote_files
            if stored_file.original_filename == "Q1_2026_financial_reports.zip"
            and stored_file.location == _REMOTE_BACKEND_NAME
        ]

        if zip_files:
            zip_record = zip_files[0]
            local_path = remote_manager.transfer(
                zip_record,
                local_backend,
                source_backend=remote_backend,
                delete_source=False,
                retention_category=FileRetention.TRANSIENT,
                session=session,
            )
            session.commit()
            print(f"  Local copy: {local_path}")
            print(f"  Local file exists: {local_path.exists()}")
            remote_path = remote_backend.root / Path(str(zip_record.stored_path))
            print(f"  Remote original preserved: {remote_path.exists()}")
            print("  Local retention: TRANSIENT (processing copy, short-lived)")
            print("  Remote retention: REGULATORY_7Y (original, long-lived)")
        else:
            print("  ZIP already pulled in a previous run, skipping")

        # -- 5. Pull PDF to local (copy, keep remote original) --
        print("\n=== 5. Pull PDF from remote to local (copy mode) ===")
        pdf_files = [
            stored_file for stored_file in remote_files
            if stored_file.original_filename == "contract_signed_2026_03.pdf"
            and stored_file.location == _REMOTE_BACKEND_NAME
        ]

        if pdf_files:
            pdf_record = pdf_files[0]
            local_path = remote_manager.transfer(
                pdf_record,
                local_backend,
                source_backend=remote_backend,
                delete_source=False,
                retention_category=FileRetention.SHORT_TERM,
                session=session,
            )
            session.commit()
            print(f"  Local copy: {local_path}")
            print(f"  Local file exists: {local_path.exists()}")
            print("  Local retention: SHORT_TERM (working copy for OCR/extraction)")
            print("  Remote retention: PERMANENT (signed contract, never delete)")
        else:
            print("  PDF already pulled in a previous run, skipping")

        # -- 6. Final state: 4 rows (2 remote originals + 2 local copies) --
        print("\n=== 6. Final file locations (stored_file table) ===")
        all_files = remote_manager.get_stored_files(session=session)
        for stored_file in all_files:
            print(
                f"  {stored_file.original_filename} | "
                f"location={stored_file.location} | "
                f"retention_category={stored_file.retention_category}"
            )

    print("\nRemote-to-local example complete.")


if __name__ == "__main__":
    main()
