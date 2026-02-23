"""Shared fixtures for integration tests requiring a live PostgreSQL database."""

import os
from collections.abc import Generator

import pytest
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from data_collector.settings.main import MainDatabaseSettings
from data_collector.tables.examples import ExampleTable
from data_collector.tables.shared import Base
from data_collector.utilities.database.main import Database

REQUIRED_ENV = (
    "DC_DB_MAIN_USERNAME",
    "DC_DB_MAIN_PASSWORD",
    "DC_DB_MAIN_DATABASENAME",
    "DC_DB_MAIN_IP",
    "DC_DB_MAIN_PORT",
)


@pytest.fixture(scope="session", autouse=True)
def require_db_env() -> None:  # noqa: PT004
    """Skip the entire integration suite if database env vars are missing."""
    missing = [v for v in REQUIRED_ENV if not os.environ.get(v)]
    if missing:
        pytest.skip(f"Missing DB env vars: {', '.join(missing)}")


@pytest.fixture(scope="session")
def db_settings() -> MainDatabaseSettings:
    """Session-scoped MainDatabaseSettings loaded from environment."""
    return MainDatabaseSettings()


@pytest.fixture(scope="session")
def db(db_settings: MainDatabaseSettings) -> Database:
    """Session-scoped Database instance (app_id=None, no dependency tracking)."""
    return Database(db_settings)


@pytest.fixture(scope="session")
def db_engine(db: Database) -> Generator[Engine]:
    """Creates all tables at session start, drops them at teardown."""
    Base.metadata.create_all(db.engine)
    yield db.engine
    Base.metadata.drop_all(db.engine)


@pytest.fixture()
def session(db: Database, db_engine: Engine) -> Generator[Session]:
    """Function-scoped session with rollback on teardown."""
    _ = db_engine  # Ensure tables are created before any test uses a session
    with db.create_session() as sess:
        yield sess
        sess.rollback()


@pytest.fixture()
def clean_example_table(session: Session) -> None:
    """Deletes all rows from example_table before the test."""
    session.query(ExampleTable).delete()
    session.commit()
