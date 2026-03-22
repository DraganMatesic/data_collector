import threading
from typing import Any
from unittest.mock import MagicMock, patch

from data_collector.tables.deploy import EXAMPLE_SCHEMA, ExampleDeploy
from data_collector.utilities.database.main import Database
from data_collector.utilities.fun_watch import FunWatchRegistry

_DEPLOY_MODULE = "data_collector.tables.deploy"
_DB_MODULE = "data_collector.utilities.database.main"


# ---------------------------------------------------------------------------
# ExampleDeploy tests
# ---------------------------------------------------------------------------


@patch(f"{_DEPLOY_MODULE}.Database")
@patch(f"{_DEPLOY_MODULE}.MainDatabaseSettings")
def test_example_deploy_passes_schema_translate_map_to_database(
    _mock_settings: MagicMock,
    mock_db_cls: MagicMock,
) -> None:
    ExampleDeploy()
    _, kwargs = mock_db_cls.call_args
    assert kwargs["schema_translate_map"] == {None: EXAMPLE_SCHEMA, "scraping": EXAMPLE_SCHEMA}


@patch(f"{_DEPLOY_MODULE}.Database")
@patch(f"{_DEPLOY_MODULE}.MainDatabaseSettings")
def test_example_deploy_create_tables_ensures_dc_example_schema(
    _mock_settings: MagicMock,
    mock_db_cls: MagicMock,
) -> None:
    mock_db = mock_db_cls.return_value
    deploy = ExampleDeploy()

    with patch(f"{_DEPLOY_MODULE}.Base") as mock_base:
        deploy.create_tables()
        mock_db.ensure_schema.assert_called_once_with(EXAMPLE_SCHEMA)
        mock_base.metadata.create_all.assert_called_once_with(mock_db.engine, tables=None)


@patch(f"{_DEPLOY_MODULE}.Database")
@patch(f"{_DEPLOY_MODULE}.MainDatabaseSettings")
def test_example_deploy_create_tables_with_specific_tables(
    _mock_settings: MagicMock,
    mock_db_cls: MagicMock,
) -> None:
    mock_db = mock_db_cls.return_value
    deploy = ExampleDeploy()
    fake_table = MagicMock()

    with patch(f"{_DEPLOY_MODULE}.Base") as mock_base:
        deploy.create_tables(tables=[fake_table])
        mock_db.ensure_schema.assert_called_once_with(EXAMPLE_SCHEMA)
        mock_base.metadata.create_all.assert_called_once_with(mock_db.engine, tables=[fake_table])


@patch(f"{_DEPLOY_MODULE}.Database")
@patch(f"{_DEPLOY_MODULE}.MainDatabaseSettings")
def test_example_deploy_drop_tables_uses_translated_engine(
    _mock_settings: MagicMock,
    mock_db_cls: MagicMock,
) -> None:
    deploy = ExampleDeploy()

    with patch(f"{_DEPLOY_MODULE}.Base") as mock_base:
        deploy.drop_tables()
        mock_base.metadata.drop_all.assert_called_once_with(mock_db_cls.return_value.engine, tables=None)


@patch(f"{_DEPLOY_MODULE}.Database")
@patch(f"{_DEPLOY_MODULE}.MainDatabaseSettings")
def test_example_deploy_recreate_calls_drop_then_create(
    _mock_settings: MagicMock,
    mock_db_cls: MagicMock,
) -> None:
    mock_db = mock_db_cls.return_value
    deploy = ExampleDeploy()

    with patch(f"{_DEPLOY_MODULE}.Base") as mock_base:
        deploy.recreate_tables()
        mock_base.metadata.drop_all.assert_called_once_with(mock_db.engine, tables=None)
        mock_db.ensure_schema.assert_called_once_with(EXAMPLE_SCHEMA)
        mock_base.metadata.create_all.assert_called_once_with(mock_db.engine, tables=None)


@patch(f"{_DEPLOY_MODULE}.Database")
@patch(f"{_DEPLOY_MODULE}.MainDatabaseSettings")
def test_example_deploy_populate_inherits_from_deploy(
    _mock_settings: MagicMock,
    mock_db_cls: MagicMock,
) -> None:
    mock_db = mock_db_cls.return_value
    mock_session = MagicMock()
    mock_db.create_session.return_value.__enter__ = MagicMock(return_value=mock_session)
    mock_db.create_session.return_value.__exit__ = MagicMock(return_value=False)

    deploy = ExampleDeploy()
    assert deploy.populate_tables() is True


# ---------------------------------------------------------------------------
# Database schema_translate_map tests
# ---------------------------------------------------------------------------


def _noop_init(self: Database, *_args: Any, **_kwargs: Any) -> None:
    pass


def test_database_stores_schema_translate_map() -> None:
    mock_settings = MagicMock()
    translate_map = {None: "dc_example", "scraping": "dc_example"}

    with patch.object(Database, "__init__", _noop_init):
        database = Database(mock_settings)

    database._schema_translate_map = translate_map  # noqa: SLF001  # pyright: ignore[reportPrivateUsage, reportAttributeAccessIssue]
    assert database._schema_translate_map == translate_map  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]


@patch(f"{_DB_MODULE}.create_engine")
@patch(f"{_DB_MODULE}.database_classes")
def test_database_applies_schema_translate_map_to_engine(
    mock_db_classes: MagicMock,
    mock_create_engine: MagicMock,
) -> None:
    mock_settings = MagicMock()
    mock_settings.database_type = MagicMock()
    mock_engine = mock_create_engine.return_value
    translate_map: dict[str | None, str | None] = {None: "dc_example"}

    database = Database(mock_settings, schema_translate_map=translate_map)

    mock_engine.execution_options.assert_called_once_with(schema_translate_map=translate_map)
    assert database.engine == mock_engine.execution_options.return_value


@patch(f"{_DB_MODULE}.create_engine")
@patch(f"{_DB_MODULE}.database_classes")
def test_database_without_schema_translate_map_keeps_original_engine(
    mock_db_classes: MagicMock,
    mock_create_engine: MagicMock,
) -> None:
    mock_settings = MagicMock()
    mock_settings.database_type = MagicMock()
    mock_engine = mock_create_engine.return_value

    database = Database(mock_settings)

    mock_engine.execution_options.assert_not_called()
    assert database.engine == mock_engine


@patch(f"{_DB_MODULE}.MainDatabaseSettings")
@patch(f"{_DB_MODULE}.create_engine")
@patch(f"{_DB_MODULE}.database_classes")
def test_database_propagates_schema_translate_map_to_system_db(
    mock_db_classes: MagicMock,
    mock_create_engine: MagicMock,
    mock_main_settings: MagicMock,
) -> None:
    mock_settings = MagicMock()
    mock_settings.database_type = MagicMock()
    translate_map: dict[str | None, str | None] = {None: "dc_example"}

    database = Database(mock_settings, schema_translate_map=translate_map)
    system_db = database._get_system_db()  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]

    assert system_db._schema_translate_map == translate_map  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]


# ---------------------------------------------------------------------------
# FunWatchRegistry.set_system_db tests
# ---------------------------------------------------------------------------


def test_fun_watch_registry_set_system_db_overrides_lazy_instance() -> None:
    FunWatchRegistry.reset()
    registry = FunWatchRegistry.instance()
    mock_database = MagicMock(spec=Database)

    registry.set_system_db(mock_database)

    assert registry._system_db is mock_database  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
    FunWatchRegistry.reset()


def test_fun_watch_registry_set_system_db_is_thread_safe() -> None:
    FunWatchRegistry.reset()
    registry = FunWatchRegistry.instance()
    mock_database = MagicMock(spec=Database)
    errors: list[Exception] = []

    def set_db() -> None:
        try:
            registry.set_system_db(mock_database)
        except Exception as exception:
            errors.append(exception)

    threads = [threading.Thread(target=set_db) for _ in range(10)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert not errors
    assert registry._system_db is mock_database  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
    FunWatchRegistry.reset()
