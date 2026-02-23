from unittest.mock import MagicMock, patch

from data_collector.tables.deploy import Deploy

_DEPLOY_MODULE = "data_collector.tables.deploy"


@patch(f"{_DEPLOY_MODULE}.Database")
@patch(f"{_DEPLOY_MODULE}.MainDatabaseSettings")
def test_populate_tables_returns_true_on_success(
    _mock_settings: MagicMock,
    mock_db_cls: MagicMock,
) -> None:
    mock_db = mock_db_cls.return_value
    mock_session = MagicMock()
    mock_db.create_session.return_value.__enter__ = MagicMock(return_value=mock_session)
    mock_db.create_session.return_value.__exit__ = MagicMock(return_value=False)

    deploy = Deploy()
    assert deploy.populate_tables() is True


@patch(f"{_DEPLOY_MODULE}.Database")
@patch(f"{_DEPLOY_MODULE}.MainDatabaseSettings")
def test_populate_tables_returns_false_on_error(
    _mock_settings: MagicMock,
    mock_db_cls: MagicMock,
) -> None:
    mock_db = mock_db_cls.return_value
    mock_session = MagicMock()
    mock_db.create_session.return_value.__enter__ = MagicMock(return_value=mock_session)
    mock_db.create_session.return_value.__exit__ = MagicMock(return_value=False)
    mock_db.merge.side_effect = RuntimeError("DB connection failed")

    deploy = Deploy()
    assert deploy.populate_tables() is False
