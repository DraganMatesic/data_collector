"""CLI entry point for scaffolding and managing application lifecycle.

Usage:
    python -m data_collector.scaffold create   --group country --parent financials --name company_data
    python -m data_collector.scaffold create   --group country --parent financials --name company_data --type threaded
    python -m data_collector.scaffold enable   --group country --parent financials --name company_data
    python -m data_collector.scaffold disable  --group country --parent financials --name company_data
    python -m data_collector.scaffold unmanage --group country --parent financials --name company_data
    python -m data_collector.scaffold remove   --group country --parent financials --name company_data --grace-days 30
"""

import argparse

from data_collector.scaffold.generator import (
    disable_app,
    enable_app,
    remove_app,
    scaffold_app,
    unmanage_app,
)


def _add_common_args(subparser: argparse.ArgumentParser) -> None:
    """Add --group, --parent, --name arguments shared by all subcommands."""
    subparser.add_argument("--group", required=True, help="App group (e.g., country code)")
    subparser.add_argument("--parent", required=True, help="Parent application domain (e.g., financials)")
    subparser.add_argument("--name", required=True, help="Application name (e.g., company_data)")


def main() -> None:
    """Parse CLI arguments and dispatch to the appropriate scaffold command."""
    parser = argparse.ArgumentParser(
        prog="python -m data_collector.scaffold",
        description="Scaffold and manage data collector applications",
    )
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # -- create --
    create_parser = subparsers.add_parser(
        "create", help="Create a new app with directory structure and DB registration",
    )
    _add_common_args(create_parser)
    create_parser.add_argument(
        "--type",
        choices=["single", "threaded", "async"],
        default="single",
        dest="app_type",
        help="Template type (default: single)",
    )

    # -- enable --
    enable_parser = subparsers.add_parser("enable", help="Enable a managed app for scheduling")
    _add_common_args(enable_parser)

    # -- disable --
    disable_parser = subparsers.add_parser("disable", help="Disable an app to prevent scheduling")
    _add_common_args(disable_parser)

    # -- unmanage --
    unmanage_parser = subparsers.add_parser("unmanage", help="Remove an app from Manager oversight")
    _add_common_args(unmanage_parser)

    # -- remove --
    remove_parser = subparsers.add_parser("remove", help="Mark an app for removal after a grace period")
    _add_common_args(remove_parser)
    remove_parser.add_argument(
        "--grace-days",
        type=int,
        default=30,
        help="Days before permanent removal (default: 30)",
    )

    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        return

    if args.command == "create":
        scaffold_app(
            group=args.group,
            parent=args.parent,
            name=args.name,
            app_type=args.app_type,
        )
    elif args.command == "enable":
        enable_app(group=args.group, parent=args.parent, name=args.name)
    elif args.command == "disable":
        disable_app(group=args.group, parent=args.parent, name=args.name)
    elif args.command == "unmanage":
        unmanage_app(group=args.group, parent=args.parent, name=args.name)
    elif args.command == "remove":
        remove_app(group=args.group, parent=args.parent, name=args.name, grace_days=args.grace_days)


if __name__ == "__main__":
    main()
