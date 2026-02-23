"""Integration tests for database settings, engine construction, and session lifecycle."""

import os
import warnings

import pytest
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from data_collector.settings.main import DatabaseDriver, DatabaseType, MainDatabaseSettings
from data_collector.utilities.database.main import Database


@pytest.mark.integration
class TestMainDatabaseSettings:
    def test_loads_from_env(self, db_settings: MainDatabaseSettings) -> None:
        assert db_settings.username == os.environ["DC_DB_MAIN_USERNAME"]
        assert db_settings.password == os.environ["DC_DB_MAIN_PASSWORD"]
        assert db_settings.database_name == os.environ["DC_DB_MAIN_DATABASENAME"]
        assert db_settings.ip == os.environ["DC_DB_MAIN_IP"]
        assert str(db_settings.port) == os.environ["DC_DB_MAIN_PORT"]

    def test_has_correct_defaults(self, db_settings: MainDatabaseSettings) -> None:
        assert db_settings.database_type == DatabaseType.POSTGRES
        assert db_settings.database_driver == DatabaseDriver.POSTGRES
        assert db_settings.map_objects is True


@pytest.mark.integration
class TestDatabaseEngine:
    def test_creates_valid_engine(self, db: Database) -> None:
        assert isinstance(db.engine, Engine)

    def test_connectivity(self, db: Database) -> None:
        with db.engine.connect() as conn:
            result = conn.execute(text("SELECT 1")).scalar_one()
        assert result == 1


@pytest.mark.integration
@pytest.mark.usefixtures("db_engine")
class TestDatabaseSession:
    def test_create_session_context_manager(self, db: Database) -> None:
        with db.create_session() as sess:
            assert isinstance(sess, Session)
            value = sess.execute(text("SELECT 1")).scalar_one()
            assert value == 1

    def test_start_session_emits_deprecation_warning(self, db: Database) -> None:
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            sess = db.start_session()
            sess.close()

        deprecation_warnings = [w for w in caught if issubclass(w.category, DeprecationWarning)]
        assert len(deprecation_warnings) == 1
        assert "start_session" in str(deprecation_warnings[0].message)
