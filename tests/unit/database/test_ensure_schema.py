from typing import Any
from unittest.mock import MagicMock, patch

from data_collector.settings.main import DatabaseType
from data_collector.utilities.database.main import Database

_DB_MODULE = "data_collector.utilities.database.main"


def _noop_init(self: Database, *_args: Any, **_kwargs: Any) -> None:
    pass


def _make_database(database_type: DatabaseType) -> Database:
    """Create a Database instance with mocked engine and settings."""
    mock_settings = MagicMock()
    mock_settings.database_type = database_type

    with patch.object(Database, "__init__", _noop_init):
        db = Database(mock_settings)

    db.settings = mock_settings  # type: ignore[assignment]
    db.engine = MagicMock()  # type: ignore[assignment]
    return db


def test_ensure_schema_postgres_executes_create_schema_if_not_exists() -> None:
    db = _make_database(DatabaseType.POSTGRES)
    mock_conn = MagicMock()
    db.engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)  # type: ignore[union-attr]
    db.engine.connect.return_value.__exit__ = MagicMock(return_value=False)  # type: ignore[union-attr]

    db.ensure_schema("scraping")

    mock_conn.execute.assert_called_once()
    sql_text = str(mock_conn.execute.call_args[0][0])
    assert "CREATE SCHEMA IF NOT EXISTS scraping" in sql_text
    mock_conn.commit.assert_called_once()


def test_ensure_schema_mssql_executes_conditional_create() -> None:
    db = _make_database(DatabaseType.MSSQL)
    mock_conn = MagicMock()
    db.engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)  # type: ignore[union-attr]
    db.engine.connect.return_value.__exit__ = MagicMock(return_value=False)  # type: ignore[union-attr]

    db.ensure_schema("scraping")

    mock_conn.execute.assert_called_once()
    sql_text = str(mock_conn.execute.call_args[0][0])
    assert "sys.schemas" in sql_text
    assert "scraping" in sql_text
    mock_conn.commit.assert_called_once()
