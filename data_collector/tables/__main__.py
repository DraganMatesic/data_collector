"""
CLI entry point for database deployment.

Usage:
    python -m data_collector.tables create     # Create all tables
    python -m data_collector.tables populate    # Seed codebook data
    python -m data_collector.tables setup       # Create tables + seed data
"""
import argparse
import sys

from data_collector.tables.deploy import Deploy


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="python -m data_collector.tables",
        description="Database deployment and codebook seeding",
    )
    parser.add_argument(
        "command",
        choices=["create", "populate", "setup"],
        help="create: create tables, populate: seed codebooks, setup: create + populate",
    )
    args = parser.parse_args()

    deploy = Deploy()

    if args.command == "create":
        deploy.create_tables()
        print("Tables created successfully.")
    elif args.command == "populate":
        deploy.populate_tables()
        print("Codebook data seeded successfully.")
    elif args.command == "setup":
        deploy.create_tables()
        deploy.populate_tables()
        print("Tables created and codebook data seeded successfully.")


if __name__ == "__main__":
    main()
