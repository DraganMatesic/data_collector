"""
CLI entry point for scraping example table management.

Usage:
    python -m data_collector.examples.scraping create      # Create example tables
    python -m data_collector.examples.scraping recreate     # Drop + create example tables (DESTRUCTIVE)
    python -m data_collector.examples.scraping drop         # Drop example tables (DESTRUCTIVE)
"""

from __future__ import annotations

import argparse
import importlib
import pathlib
import sys

from sqlalchemy import Table

from data_collector.examples.scraping import SCHEMA
from data_collector.tables.deploy import Deploy
from data_collector.tables.shared import Base

_EXAMPLES_DIR = pathlib.Path(__file__).parent
for _tables_file in _EXAMPLES_DIR.glob("*/tables.py"):
    _module_name = f"data_collector.examples.scraping.{_tables_file.parent.name}.tables"
    importlib.import_module(_module_name)

_EXAMPLE_TABLES: list[Table] = [
    table for table in Base.metadata.sorted_tables
    if table.schema == SCHEMA
]


def main() -> None:
    """Parse CLI command and execute table operations."""
    parser = argparse.ArgumentParser(
        prog="python -m data_collector.examples.scraping",
        description="Manage scraping example tables",
    )
    parser.add_argument(
        "command",
        choices=["create", "recreate", "drop"],
        help=(
            "create: create example tables, "
            "recreate: drop + create example tables (DESTRUCTIVE), "
            "drop: drop example tables (DESTRUCTIVE)"
        ),
    )
    args = parser.parse_args()

    deploy = Deploy()

    if args.command == "create":
        deploy.create_tables(tables=_EXAMPLE_TABLES, schema=SCHEMA)
        print("Scraping example tables created successfully.")
    elif args.command == "recreate":
        deploy.drop_tables(tables=_EXAMPLE_TABLES)
        deploy.create_tables(tables=_EXAMPLE_TABLES, schema=SCHEMA)
        print("Scraping example tables recreated successfully.")
    elif args.command == "drop":
        deploy.drop_tables(tables=_EXAMPLE_TABLES)
        print("Scraping example tables dropped successfully.")
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
