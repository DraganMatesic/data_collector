"""Retention categories, expiration dates, and DB-driven retention enforcement.

Demonstrates:
    - Storing files with different retention categories (TRANSIENT, STANDARD, PERMANENT)
    - Querying expiration_date from StoredFile records
    - How expiration_date is computed from CodebookFileRetention.retention_days
    - Adding a custom retention category to the codebook table
    - Storing a file with the custom category
    - DB-driven retention enforcement via manager.enforce_retention()

Requires:
    DC_DB_MAIN_USERNAME, DC_DB_MAIN_PASSWORD, DC_DB_MAIN_DATABASENAME,
    DC_DB_MAIN_IP, DC_DB_MAIN_PORT, DC_STORAGE_ROOT environment variables.

Run:
    python -m data_collector.examples run storage/03_retention
"""

from __future__ import annotations

import os
import uuid
from datetime import UTC, datetime

from sqlalchemy import select

from data_collector.enums.storage import FileRetention
from data_collector.settings.main import MainDatabaseSettings
from data_collector.settings.storage import StorageSettings
from data_collector.storage.manager import StorageManager
from data_collector.tables.apps import AppGroups, AppParents, Apps
from data_collector.tables.deploy import Deploy
from data_collector.tables.runtime import Runtime
from data_collector.tables.storage import CodebookFileRetention
from data_collector.utilities.database.main import Database
from data_collector.utilities.functions.runtime import AppInfo, get_app_info

_REQUIRED_ENV = (
    "DC_DB_MAIN_USERNAME", "DC_DB_MAIN_PASSWORD", "DC_DB_MAIN_DATABASENAME",
    "DC_DB_MAIN_IP", "DC_DB_MAIN_PORT", "DC_STORAGE_ROOT",
)

_CUSTOM_CATEGORY_ID = 20
_CUSTOM_CATEGORY_DAYS = 5475


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


def _register_custom_retention(database: Database) -> None:
    """Insert a custom retention category into the codebook table (idempotent).

    This simulates what a DBA would do post-deployment to add a company-specific
    retention tier not covered by the default FileRetention enum.
    """
    with database.create_session() as session:
        existing = session.execute(
            select(CodebookFileRetention).where(CodebookFileRetention.id == _CUSTOM_CATEGORY_ID)
        ).scalar()
        if existing is None:
            custom_category = CodebookFileRetention(
                id=_CUSTOM_CATEGORY_ID,
                description="Custom audit 15Y -- company-specific compliance requirement",
                retention_days=_CUSTOM_CATEGORY_DAYS,
            )
            session.add(custom_category)
            print(f"  Registered custom category id={_CUSTOM_CATEGORY_ID} ({_CUSTOM_CATEGORY_DAYS} days)")
        else:
            print(f"  Custom category id={_CUSTOM_CATEGORY_ID} already registered")
        session.commit()


def _print_codebook(database: Database) -> None:
    """Print all retention categories from the codebook table."""
    with database.create_session() as session:
        rows = session.execute(
            select(CodebookFileRetention).order_by(CodebookFileRetention.id)
        ).scalars().all()
        for row in rows:
            retention_days_value = int(row.retention_days) if row.retention_days else None  # type: ignore[arg-type]
            days_display = f"{retention_days_value} days" if retention_days_value is not None else "PERMANENT"
            print(f"  id={row.id:>2} | {days_display:>15} | {row.description}")


def main() -> None:
    """Run retention categories and enforcement example."""
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

    # -- 1. Show available retention categories --
    print("=== 1. Retention categories (from database) ===")
    _print_codebook(database)

    # -- 2. Register a custom category --
    print("\n=== 2. Adding custom retention category ===")
    _register_custom_retention(database)

    print("\n=== Updated codebook ===")
    _print_codebook(database)

    with database.create_session() as session:
        # -- 3. Store files with different retention categories --
        print("\n=== 3. Storing files with different retention categories ===")

        now = datetime.now(UTC)

        path_transient = manager.store(
            b"Temporary cache data that can be discarded after 7 days.",
            "tmp",
            original_filename="cache_snapshot.tmp",
            retention_category=FileRetention.TRANSIENT,
            session=session,
        )
        print(f"  TRANSIENT: {path_transient.name}")

        path_standard = manager.store(
            b"Standard business record for general use.",
            "txt",
            original_filename="business_record.txt",
            retention_category=FileRetention.STANDARD,
            session=session,
        )
        print(f"  STANDARD: {path_standard.name}")

        path_permanent = manager.store(
            b"Permanent regulatory filing that must never be deleted.",
            "pdf",
            original_filename="regulatory_filing.pdf",
            retention_category=FileRetention.PERMANENT,
            session=session,
        )
        print(f"  PERMANENT: {path_permanent.name}")

        path_custom = manager.store(
            b"Custom audit report with 15-year retention requirement.",
            "pdf",
            original_filename="audit_report_15y.pdf",
            retention_category=_CUSTOM_CATEGORY_ID,
            session=session,
        )
        print(f"  CUSTOM (15Y): {path_custom.name}")

        session.commit()

        # -- 4. Show expiration dates --
        print("\n=== 4. Expiration dates ===")
        stored_files = manager.get_stored_files(session=session)
        for stored_file in stored_files:
            expiration: datetime | None = stored_file.expiration_date  # type: ignore[assignment]
            if expiration is not None:
                days_until = (expiration - now).days
                expiration_display = f"{expiration.strftime('%Y-%m-%d')} ({days_until} days)"
            else:
                expiration_display = "NEVER (permanent)"
            print(
                f"  {stored_file.original_filename:30s} | "
                f"category={stored_file.retention_category:>2} | "
                f"expires={expiration_display}"
            )

        # -- 5. Retention enforcement --
        print("\n=== 5. Retention enforcement ===")
        print("  (No files are expired yet -- enforcement deletes nothing)")
        deleted_count = manager.enforce_retention(session=session)
        print(f"  Files deleted: {deleted_count}")

        # -- 6. Storage summary --
        print("\n=== 6. Storage summary ===")
        total_bytes = manager.get_storage_size(session=session)
        total_files = len(stored_files)
        print(f"  Total files: {total_files}")
        print(f"  Total size: {total_bytes} bytes")

    print("\nRetention example complete.")


if __name__ == "__main__":
    main()
