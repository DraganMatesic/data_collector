"""
CLI entry point for database deployment.

Usage:
    python -m data_collector.tables create        # Create all tables
    python -m data_collector.tables populate       # Seed codebook data
    python -m data_collector.tables setup          # Create tables + seed data
    python -m data_collector.tables recreate       # Drop + create + seed (DESTRUCTIVE)
    python -m data_collector.tables splunk-setup   # Create Splunk index + sourcetype
    python -m data_collector.tables splunk-clean   # Empty Splunk index data
"""
import argparse
import sys

from data_collector.tables.deploy import Deploy


def main() -> None:
    """Parse CLI command and execute deployment actions."""
    parser = argparse.ArgumentParser(
        prog="python -m data_collector.tables",
        description="Database deployment and codebook seeding",
    )
    parser.add_argument(
        "command",
        choices=["create", "populate", "setup", "recreate", "splunk-setup", "splunk-clean"],
        help=(
            "create: create tables, populate: seed codebooks, setup: create + populate, "
            "recreate: drop + create + populate (DESTRUCTIVE), "
            "splunk-setup: create Splunk index + sourcetype, splunk-clean: empty Splunk index data"
        ),
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


if __name__ == "__main__":
    main()
