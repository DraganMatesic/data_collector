"""CLI entry point for WatchService root management.

Usage:
    python -m data_collector.messaging add-root          # Register a new watch root
    python -m data_collector.messaging list-roots         # List registered watch roots
    python -m data_collector.messaging remove-root        # Soft-delete a watch root
    python -m data_collector.messaging activate-root      # Re-enable a deactivated root
    python -m data_collector.messaging deactivate-root    # Disable a root without deleting
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from typing import Any, cast

from sqlalchemy import select

from data_collector.settings.main import MainDatabaseSettings
from data_collector.tables.pipeline import WatchRoots
from data_collector.utilities.database.main import Database


def _add_root(arguments: argparse.Namespace, database: Database) -> None:
    """Register a new watch root in the database."""
    root_path = arguments.path
    if not root_path or (not root_path.startswith("/") and ":" not in root_path):
        print(f"Error: --path must be an absolute path, got: {root_path}", file=sys.stderr)
        sys.exit(1)

    extensions_json: str | None = None
    if arguments.ext:
        extensions_list = [extension.strip() for extension in arguments.ext.split(",")]
        extensions_json = json.dumps(extensions_list)

    with database.create_session() as session:
        watch_root = WatchRoots(
            root_path=root_path,
            rel_path=arguments.rel_path,
            country=arguments.country,
            watch_group=arguments.group,
            app_path=arguments.app_path,
            extensions=extensions_json,
            recursive=not arguments.no_recursive,
        )
        database.add(watch_root, session)
        session.commit()
        root_id = cast(int, watch_root.id)
        print(f"Added watch root (id={root_id}): {root_path}")


def _list_roots(arguments: argparse.Namespace, database: Database) -> None:
    """List registered watch roots."""
    with database.create_session() as session:
        statement = select(WatchRoots).where(
            WatchRoots.archive.is_(None),
        )
        if arguments.country:
            statement = statement.where(WatchRoots.country == arguments.country)
        if arguments.group:
            statement = statement.where(WatchRoots.watch_group == arguments.group)

        rows = database.query(statement, session).scalars().all()

    if not rows:
        print("No watch roots found.")
        return

    header = f"{'ID':>5}  {'Active':>6}  {'Country':>7}  {'Group':<15}  {'Path':<40}  {'App Path':<50}  {'Ext'}"
    print(header)
    print("-" * len(header))
    for row in rows:
        record: Any = row
        active_marker = "Yes" if record.active else "No"
        extensions_display = str(record.extensions) if record.extensions is not None else ""
        print(
            f"{record.id:>5}  {active_marker:>6}  {record.country:>7}  {str(record.watch_group):<15}  "
            f"{str(record.root_path):<40}  {str(record.app_path):<50}  {extensions_display}"
        )


def _remove_root(arguments: argparse.Namespace, database: Database) -> None:
    """Soft-delete a watch root by setting archive timestamp."""
    with database.create_session() as session:
        statement = select(WatchRoots).where(WatchRoots.id == arguments.root_id)
        row = database.query(statement, session).scalar_one_or_none()
        if row is None:
            print(f"Error: watch root id={arguments.root_id} not found", file=sys.stderr)
            sys.exit(1)
        record: Any = row
        record.archive = datetime.now(UTC)
        session.commit()
        print(f"Removed watch root (id={arguments.root_id}): {record.root_path}")


def _activate_root(arguments: argparse.Namespace, database: Database) -> None:
    """Re-enable a deactivated watch root."""
    with database.create_session() as session:
        statement = select(WatchRoots).where(WatchRoots.id == arguments.root_id)
        row = database.query(statement, session).scalar_one_or_none()
        if row is None:
            print(f"Error: watch root id={arguments.root_id} not found", file=sys.stderr)
            sys.exit(1)
        record: Any = row
        record.active = True
        session.commit()
        print(f"Activated watch root (id={arguments.root_id}): {record.root_path}")


def _deactivate_root(arguments: argparse.Namespace, database: Database) -> None:
    """Disable a watch root without deleting it."""
    with database.create_session() as session:
        statement = select(WatchRoots).where(WatchRoots.id == arguments.root_id)
        row = database.query(statement, session).scalar_one_or_none()
        if row is None:
            print(f"Error: watch root id={arguments.root_id} not found", file=sys.stderr)
            sys.exit(1)
        record: Any = row
        record.active = False
        session.commit()
        print(f"Deactivated watch root (id={arguments.root_id}): {record.root_path}")


def main() -> None:
    """Parse CLI arguments and dispatch to the appropriate handler."""
    parser = argparse.ArgumentParser(
        prog="data_collector.messaging",
        description="WatchService root management CLI",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # add-root
    add_parser = subparsers.add_parser("add-root", help="Register a new watch root")
    add_parser.add_argument("--path", required=True, help="Absolute path to the watched directory")
    add_parser.add_argument("--rel-path", required=True, help="Relative path identifier for routing")
    add_parser.add_argument("--country", required=True, help="Country code (e.g., HR)")
    add_parser.add_argument("--group", required=True, help="Watch group (e.g., ocr, ingest)")
    add_parser.add_argument("--app-path", required=True, help="Python module path for Dramatiq actor import")
    add_parser.add_argument("--ext", default=None, help="Comma-separated allowed extensions (e.g., .pdf,.zip)")
    add_parser.add_argument("--no-recursive", action="store_true", help="Do not watch subdirectories")

    # list-roots
    list_parser = subparsers.add_parser("list-roots", help="List registered watch roots")
    list_parser.add_argument("--country", default=None, help="Filter by country code")
    list_parser.add_argument("--group", default=None, help="Filter by watch group")

    # remove-root
    remove_parser = subparsers.add_parser("remove-root", help="Soft-delete a watch root")
    remove_parser.add_argument("--root-id", type=int, required=True, help="Watch root ID to remove")

    # activate-root
    activate_parser = subparsers.add_parser("activate-root", help="Re-enable a deactivated root")
    activate_parser.add_argument("--root-id", type=int, required=True, help="Watch root ID to activate")

    # deactivate-root
    deactivate_parser = subparsers.add_parser("deactivate-root", help="Disable a root without deleting")
    deactivate_parser.add_argument("--root-id", type=int, required=True, help="Watch root ID to deactivate")

    arguments = parser.parse_args()

    database = Database(MainDatabaseSettings())

    command_handlers = {
        "add-root": _add_root,
        "list-roots": _list_roots,
        "remove-root": _remove_root,
        "activate-root": _activate_root,
        "deactivate-root": _deactivate_root,
    }

    handler = command_handlers[arguments.command]
    handler(arguments, database)


if __name__ == "__main__":
    main()
