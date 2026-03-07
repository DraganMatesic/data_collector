"""Application scaffold generator -- creates directory structure and registers app in DB."""

from __future__ import annotations

import logging
import sys
from pathlib import Path

from data_collector.enums import FatalFlag, RunStatus
from data_collector.scaffold.templates import (
    INIT_TEMPLATE,
    MAIN_ASYNC_TEMPLATE,
    MAIN_SINGLE_TEMPLATE,
    MAIN_THREADED_TEMPLATE,
    PARSER_TEMPLATE,
    TABLES_TEMPLATE,
)
from data_collector.settings.main import MainDatabaseSettings
from data_collector.tables.apps import AppGroups, AppParents, Apps
from data_collector.utilities.database.main import Database
from data_collector.utilities.functions.runtime import get_app_id, get_parent_id

logger = logging.getLogger(__name__)


def to_class_name(name: str) -> str:
    """Convert a snake_case name to PascalCase.

    Args:
        name: Snake-case string (e.g., "company_data").

    Returns:
        PascalCase string (e.g., "CompanyData").
    """
    return "".join(word.capitalize() for word in name.split("_"))


def _write_file(path: Path, content: str) -> None:
    """Write content to a file, creating parent directories as needed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _register_app_in_db(
    group: str,
    parent: str,
    name: str,
    app_id: str,
) -> bool:
    """Register the scaffolded app in the Apps table.

    Creates AppGroups and AppParents rows if they do not exist, then
    inserts the Apps row. Best-effort -- returns False on DB errors.

    Args:
        group: App group name.
        parent: Parent domain name.
        name: Application name.
        app_id: The 64-char SHA-256 app identifier.

    Returns:
        True if registration succeeded, False otherwise.
    """
    try:
        database = Database(MainDatabaseSettings())
    except Exception:
        logger.warning("Could not connect to database. App files created but not registered.")
        return False

    parent_hash = get_parent_id(group, parent)

    try:
        with database.create_session() as session:
            database.update_insert(
                AppGroups(name=group),
                session,
                filter_cols=["name"],
            )

        with database.create_session() as session:
            database.update_insert(
                AppParents(name=parent, group_name=group, parent=parent_hash),
                session,
                filter_cols=["name", "group_name"],
            )

        with database.create_session() as session:
            database.update_insert(
                Apps(
                    app=app_id,
                    group_name=group,
                    parent_name=parent,
                    app_name=name,
                    parent_id=parent_hash,
                    run_status=RunStatus.NOT_RUNNING,
                    fatal_flag=FatalFlag.NONE,
                    disable=True,
                ),
                session,
                filter_cols=["group_name", "parent_name", "app_name"],
            )
        return True
    except Exception:
        logger.exception("Failed to register app in database")
        return False


def _get_package_root() -> Path:
    """Return the data_collector package root directory."""
    return Path(__file__).resolve().parent.parent


def scaffold_app(
    group: str,
    parent: str,
    name: str,
    app_type: str = "single",
    *,
    _package_root: Path | None = None,
) -> None:
    """Generate app directory structure and register in Apps table.

    Creates the standard 3-file app structure (main.py, parser.py, tables.py)
    plus __init__.py under data_collector/{group}/{parent}/{name}/. Also
    ensures __init__.py files exist in the group and parent directories.

    Args:
        group: App group name (directory level 1).
        parent: Parent domain name (directory level 2).
        name: Application name (directory level 3).
        app_type: "single" (default) or "threaded".
        _package_root: Override package root for testing. Do not use in production.
    """
    app_id = get_app_id(group, parent, name)
    class_name = to_class_name(name)

    # Resolve target directory relative to the data_collector package
    package_root = _package_root if _package_root is not None else _get_package_root()
    app_dir = package_root / group / parent / name

    if app_dir.exists():
        print(f"Error: Directory already exists: {app_dir}", file=sys.stderr)
        sys.exit(1)

    # Template context
    ctx = {"group": group, "parent": parent, "name": name, "class_name": class_name}

    # Select main.py template
    template_map = {
        "single": MAIN_SINGLE_TEMPLATE,
        "threaded": MAIN_THREADED_TEMPLATE,
        "async": MAIN_ASYNC_TEMPLATE,
    }
    main_template = template_map[app_type]

    # Create group and parent __init__.py if missing
    group_init = package_root / group / "__init__.py"
    if not group_init.exists():
        _write_file(group_init, f'"""Application group: {group}."""\n')

    parent_init = package_root / group / parent / "__init__.py"
    if not parent_init.exists():
        _write_file(parent_init, f'"""Application parent: {group}.{parent}."""\n')

    # Write app files
    _write_file(app_dir / "__init__.py", INIT_TEMPLATE.format(**ctx))
    _write_file(app_dir / "main.py", main_template.format(**ctx))
    _write_file(app_dir / "parser.py", PARSER_TEMPLATE.format(**ctx))
    _write_file(app_dir / "tables.py", TABLES_TEMPLATE.format(**ctx))

    # Print structure
    type_labels = {"single": "single-threaded", "threaded": "multi-threaded", "async": "async"}
    type_label = type_labels[app_type]
    print("\nCreated app structure:")
    print(f"  data_collector/{group}/{parent}/{name}/")
    print("  ├── __init__.py")
    print(f"  ├── main.py      ({type_label} BaseScraper)")
    print("  ├── parser.py")
    print("  └── tables.py")

    # Register in database
    registered = _register_app_in_db(group, parent, name, app_id)

    if registered:
        print("\nApp registered in database:")
        print(f"  app_id:  {app_id}")
        print("  status:  NOT_RUNNING")
    else:
        print("\nApp NOT registered in database (connection unavailable).")
        print(f"  app_id:  {app_id}  (computed, register manually when DB is available)")

    print("\nNext steps:")
    print("  1. Edit tables.py -- define your ORM models")
    print("  2. Edit parser.py -- implement Parser class methods")
    print("  3. Edit main.py -- set base_url, implement collect() logic")
    print(f"  4. Run: python -m data_collector.{group}.{parent}.{name}.main")
