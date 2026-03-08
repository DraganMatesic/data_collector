"""CLI entry point for scaffolding new applications.

Usage:
    python -m data_collector.scaffold --group country --parent financials --name company_data
    python -m data_collector.scaffold --group country --parent financials --name company_data --type threaded
"""

import argparse

from data_collector.scaffold.generator import scaffold_app


def main() -> None:
    """Parse CLI arguments and generate scaffolded app."""
    parser = argparse.ArgumentParser(
        prog="python -m data_collector.scaffold",
        description="Generate a new data collector application",
    )
    parser.add_argument("--group", required=True, help="App group (e.g., country code)")
    parser.add_argument("--parent", required=True, help="Parent application domain (e.g., financials)")
    parser.add_argument("--name", required=True, help="Application name (e.g., company_data)")
    parser.add_argument(
        "--type",
        choices=["single", "threaded", "async"],
        default="single",
        dest="app_type",
        help="Template type (default: single)",
    )
    args = parser.parse_args()
    scaffold_app(
        group=args.group,
        parent=args.parent,
        name=args.name,
        app_type=args.app_type,
    )


if __name__ == "__main__":
    main()
