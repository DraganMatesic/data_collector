"""Unit tests for Database.query(), Database.run(), and Database.add() methods."""

from unittest.mock import MagicMock

from sqlalchemy import delete, select, text, update

from data_collector.tables.apps import Apps
from data_collector.utilities.database.main import Database, extract_models_from_statement


def _make_mock_database(*, map_objects: bool = False) -> MagicMock:
    """Create a mock Database with settings configured for map_objects."""
    database = MagicMock()
    database.settings.map_objects = map_objects
    database.app_id = "a" * 64 if map_objects else None
    database.logger = MagicMock()
    return database


class TestExtractModelsFromStatement:
    """Tests for extract_models_from_statement function."""

    def test_extracts_model_from_select(self) -> None:
        statement = select(Apps)
        models = extract_models_from_statement(statement)
        assert Apps in models

    def test_extracts_model_from_update(self) -> None:
        statement = update(Apps).where(Apps.app == "test").values(progress=50)
        models = extract_models_from_statement(statement)
        assert Apps in models

    def test_extracts_model_from_delete(self) -> None:
        statement = delete(Apps).where(Apps.app == "test")
        models = extract_models_from_statement(statement)
        assert Apps in models

    def test_returns_empty_set_on_text_statement(self) -> None:
        statement = text("SELECT 1")
        models = extract_models_from_statement(statement)
        assert models == set()


class TestDatabaseQuery:
    """Tests for Database.query() method."""

    def test_delegates_to_session_execute(self) -> None:
        database = _make_mock_database(map_objects=False)
        session = MagicMock()
        statement = select(Apps)

        Database.query(database, statement, session)

        session.execute.assert_called_once_with(statement)

    def test_registers_models_when_map_objects_enabled(self) -> None:
        database = _make_mock_database(map_objects=True)
        session = MagicMock()
        statement = select(Apps)

        Database.query(database, statement, session)

        database.register_models.assert_called_once()
        registered_models = database.register_models.call_args[0][0]
        assert Apps in registered_models

    def test_skips_registration_when_map_objects_disabled(self) -> None:
        database = _make_mock_database(map_objects=False)
        session = MagicMock()
        statement = select(Apps)

        Database.query(database, statement, session)

        database.register_models.assert_not_called()

    def test_skips_registration_when_map_objects_override_false(self) -> None:
        database = _make_mock_database(map_objects=True)
        session = MagicMock()
        statement = select(Apps)

        Database.query(database, statement, session, map_objects=False)

        database.register_models.assert_not_called()


class TestDatabaseRun:
    """Tests for Database.run() method."""

    def test_delegates_to_session_execute(self) -> None:
        database = _make_mock_database(map_objects=False)
        session = MagicMock()
        statement = update(Apps).where(Apps.app == "test").values(progress=50)

        Database.run(database, statement, session)

        session.execute.assert_called_once_with(statement)

    def test_registers_models_when_map_objects_enabled(self) -> None:
        database = _make_mock_database(map_objects=True)
        session = MagicMock()
        statement = update(Apps).where(Apps.app == "test").values(progress=50)

        Database.run(database, statement, session)

        database.register_models.assert_called_once()
        registered_models = database.register_models.call_args[0][0]
        assert Apps in registered_models

    def test_skips_registration_when_map_objects_disabled(self) -> None:
        database = _make_mock_database(map_objects=False)
        session = MagicMock()
        statement = delete(Apps).where(Apps.app == "test")

        Database.run(database, statement, session)

        database.register_models.assert_not_called()

    def test_uses_explicit_models_parameter(self) -> None:
        database = _make_mock_database(map_objects=True)
        session = MagicMock()
        statement = update(Apps).where(Apps.app == "test").values(progress=50)
        explicit_models = {Apps}

        Database.run(database, statement, session, models=explicit_models)

        database.register_models.assert_called_once_with(explicit_models)


class TestDatabaseAdd:
    """Tests for Database.add() method."""

    def test_delegates_to_session_add(self) -> None:
        database = _make_mock_database(map_objects=False)
        session = MagicMock()
        instance = MagicMock()

        Database.add(database, instance, session)

        session.add.assert_called_once_with(instance)

    def test_does_not_flush_by_default(self) -> None:
        database = _make_mock_database(map_objects=False)
        session = MagicMock()
        instance = MagicMock()

        Database.add(database, instance, session)

        session.flush.assert_not_called()

    def test_flush_calls_session_flush(self) -> None:
        database = _make_mock_database(map_objects=False)
        session = MagicMock()
        instance = MagicMock()

        Database.add(database, instance, session, flush=True)

        session.add.assert_called_once_with(instance)
        session.flush.assert_called_once()

    def test_registers_model_when_map_objects_enabled(self) -> None:
        database = _make_mock_database(map_objects=True)
        session = MagicMock()
        instance = MagicMock()

        Database.add(database, instance, session)

        database.register_models.assert_called_once()
