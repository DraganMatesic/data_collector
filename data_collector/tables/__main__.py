"""
CLI entry point for database deployment.

Usage:
    python -m data_collector.tables create        # Create all tables
    python -m data_collector.tables populate       # Seed codebook data
    python -m data_collector.tables setup          # Create tables + seed data
    python -m data_collector.tables recreate       # Drop + create + seed (DESTRUCTIVE)
    python -m data_collector.tables splunk-setup   # Create Splunk index + sourcetype
    python -m data_collector.tables splunk-clean   # Empty Splunk index data
    python -m data_collector.tables proxy-cleanup  # Delete orphaned proxy reservations
"""
import argparse
import sys

from data_collector.proxy import cleanup_all_reservations
from data_collector.tables.deploy import Deploy


def main() -> None:
    """Parse CLI command and execute deployment actions."""
    parser = argparse.ArgumentParser(
        prog="python -m data_collector.tables",
        description="Database deployment and codebook seeding",
    )
    parser.add_argument(
        "command",
        choices=[
            "create", "populate", "setup", "recreate",
            "splunk-setup", "splunk-clean", "proxy-cleanup",
        ],
        help=(
            "create: create tables, populate: seed codebooks, setup: create + populate, "
            "recreate: drop + create + populate (DESTRUCTIVE), "
            "splunk-setup: create Splunk index + sourcetype, splunk-clean: empty Splunk index data, "
            "proxy-cleanup: delete orphaned proxy reservations"
        ),
    )
    parser.add_argument(
        "--cooldown",
        type=int,
        default=300,
        help="proxy-cleanup: cooldown threshold in seconds for released rows (default: 300)",
    )
    parser.add_argument(
        "--ttl",
        type=int,
        default=1800,
        help="proxy-cleanup: TTL threshold in seconds for crash orphan rows (default: 1800)",
    )
    args = parser.parse_args()

    deploy = Deploy()

    if args.command == "create":
        deploy.create_tables()
        print("Tables created successfully.")
    elif args.command == "populate":
        if deploy.populate_tables():
            print("Codebook data seeded successfully.")
        else:
            print("Codebook seeding completed with errors. Check logs.", file=sys.stderr)
            sys.exit(1)
    elif args.command == "setup":
        deploy.create_tables()
        if deploy.populate_tables():
            print("Tables created and codebook data seeded successfully.")
        else:
            print("Tables created but codebook seeding had errors. Check logs.", file=sys.stderr)
            sys.exit(1)
    elif args.command == "recreate":
        deploy.recreate_tables()
        if deploy.populate_tables():
            print("Tables recreated and codebook data seeded successfully.")
        else:
            print("Tables recreated but codebook seeding had errors. Check logs.", file=sys.stderr)
            sys.exit(1)
    elif args.command == "splunk-setup":
        if deploy.setup_splunk():
            print("Splunk index and sourcetype provisioned successfully.")
        else:
            print("Splunk provisioning failed. Check logs.", file=sys.stderr)
            sys.exit(1)
    elif args.command == "splunk-clean":
        if deploy.clean_splunk():
            print("Splunk index data cleaned successfully.")
        else:
            print("Splunk clean failed. Check logs.", file=sys.stderr)
            sys.exit(1)
    elif args.command == "proxy-cleanup":
        deleted = cleanup_all_reservations(
            deploy.database,
            cooldown_seconds=args.cooldown,
            ttl_seconds=args.ttl,
        )
        print(f"Proxy reservation cleanup complete: {deleted} rows deleted.")


if __name__ == "__main__":
    main()
