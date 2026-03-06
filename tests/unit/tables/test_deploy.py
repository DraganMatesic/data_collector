from unittest.mock import MagicMock, patch

import pytest

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


# ---------------------------------------------------------------------------
# Splunk provisioning tests
# ---------------------------------------------------------------------------


@pytest.fixture()
def deploy() -> Deploy:
    with patch(f"{_DEPLOY_MODULE}.Database"), patch(f"{_DEPLOY_MODULE}.MainDatabaseSettings"):
        return Deploy()


@patch(f"{_DEPLOY_MODULE}.SplunkAdminSettings")
@patch(f"{_DEPLOY_MODULE}.LogSettings")
def test_setup_splunk_returns_false_when_credentials_missing(
    mock_log_settings: MagicMock,
    mock_admin: MagicMock,
    deploy: Deploy,
) -> None:
    mock_admin.return_value.admin_user = None
    mock_admin.return_value.admin_password = None
    mock_log_settings.return_value.splunk_index = "data_collector"
    mock_log_settings.return_value.splunk_sourcetype = "data_collector:structured"

    assert deploy.setup_splunk() is False


@patch(f"{_DEPLOY_MODULE}.http_requests")
@patch(f"{_DEPLOY_MODULE}.SplunkAdminSettings")
@patch(f"{_DEPLOY_MODULE}.LogSettings")
def test_setup_splunk_creates_index_and_sourcetype(
    mock_log_settings: MagicMock,
    mock_admin: MagicMock,
    mock_http: MagicMock,
    deploy: Deploy,
) -> None:
    mock_admin.return_value.admin_user = "admin"
    mock_admin.return_value.admin_password = "pass"
    mock_admin.return_value.mgmt_url = "https://localhost:8089"
    mock_admin.return_value.verify_tls = False

    mock_log_settings.return_value.splunk_index = "data_collector"
    mock_log_settings.return_value.splunk_sourcetype = "data_collector:structured"

    get_404 = MagicMock(status_code=404)
    post_ok = MagicMock(status_code=200)
    post_ok.raise_for_status = MagicMock()

    mock_http.get.return_value = get_404
    mock_http.post.return_value = post_ok
    mock_http.RequestException = Exception

    assert deploy.setup_splunk() is True

    mock_http.get.assert_any_call(
        "https://localhost:8089/services/data/indexes/data_collector",
        auth=("admin", "pass"),
        verify=False,
        params={"output_mode": "json"},
        timeout=Deploy._SPLUNK_TIMEOUT,
    )
    mock_http.post.assert_any_call(
        "https://localhost:8089/services/data/indexes",
        auth=("admin", "pass"),
        verify=False,
        data={"name": "data_collector", "output_mode": "json"},
        timeout=Deploy._SPLUNK_TIMEOUT,
    )
    mock_http.post.assert_any_call(
        "https://localhost:8089/servicesNS/nobody/search/configs/conf-props",
        auth=("admin", "pass"),
        verify=False,
        data={"name": "data_collector:structured", "output_mode": "json"},
        timeout=Deploy._SPLUNK_TIMEOUT,
    )


@patch(f"{_DEPLOY_MODULE}.http_requests")
@patch(f"{_DEPLOY_MODULE}.SplunkAdminSettings")
@patch(f"{_DEPLOY_MODULE}.LogSettings")
def test_setup_splunk_skips_existing_index_and_sourcetype(
    mock_log_settings: MagicMock,
    mock_admin: MagicMock,
    mock_http: MagicMock,
    deploy: Deploy,
) -> None:
    mock_admin.return_value.admin_user = "admin"
    mock_admin.return_value.admin_password = "pass"
    mock_admin.return_value.mgmt_url = "https://localhost:8089"
    mock_admin.return_value.verify_tls = False

    mock_log_settings.return_value.splunk_index = "data_collector"
    mock_log_settings.return_value.splunk_sourcetype = "data_collector:structured"

    get_ok = MagicMock(status_code=200)
    mock_http.get.return_value = get_ok
    mock_http.RequestException = Exception

    assert deploy.setup_splunk() is True
    mock_http.post.assert_not_called()


@patch(f"{_DEPLOY_MODULE}.http_requests")
@patch(f"{_DEPLOY_MODULE}.SplunkAdminSettings")
@patch(f"{_DEPLOY_MODULE}.LogSettings")
def test_clean_splunk_posts_to_clean_endpoint(
    mock_log_settings: MagicMock,
    mock_admin: MagicMock,
    mock_http: MagicMock,
    deploy: Deploy,
) -> None:
    mock_admin.return_value.admin_user = "admin"
    mock_admin.return_value.admin_password = "pass"
    mock_admin.return_value.mgmt_url = "https://localhost:8089"
    mock_admin.return_value.verify_tls = False

    mock_log_settings.return_value.splunk_index = "data_collector"

    delete_ok = MagicMock(status_code=200)
    delete_ok.raise_for_status = MagicMock()
    mock_http.delete.return_value = delete_ok

    get_404 = MagicMock(status_code=404)
    mock_http.get.return_value = get_404

    post_ok = MagicMock(status_code=200)
    post_ok.raise_for_status = MagicMock()
    mock_http.post.return_value = post_ok
    mock_http.RequestException = Exception

    assert deploy.clean_splunk() is True

    mock_http.delete.assert_called_once_with(
        "https://localhost:8089/services/data/indexes/data_collector",
        auth=("admin", "pass"),
        verify=False,
        params={"output_mode": "json"},
        timeout=Deploy._SPLUNK_DELETE_TIMEOUT,
    )
    mock_http.post.assert_called_once_with(
        "https://localhost:8089/services/data/indexes",
        auth=("admin", "pass"),
        verify=False,
        data={"name": "data_collector", "output_mode": "json"},
        timeout=Deploy._SPLUNK_TIMEOUT,
    )


@patch(f"{_DEPLOY_MODULE}.SplunkAdminSettings")
@patch(f"{_DEPLOY_MODULE}.LogSettings")
def test_clean_splunk_returns_false_when_credentials_missing(
    mock_log_settings: MagicMock,
    mock_admin: MagicMock,
    deploy: Deploy,
) -> None:
    mock_admin.return_value.admin_user = None
    mock_admin.return_value.admin_password = None
    mock_log_settings.return_value.splunk_index = "data_collector"

    assert deploy.clean_splunk() is False


@patch(f"{_DEPLOY_MODULE}.http_requests")
@patch(f"{_DEPLOY_MODULE}.SplunkAdminSettings")
@patch(f"{_DEPLOY_MODULE}.LogSettings")
def test_clean_splunk_returns_false_on_request_error(
    mock_log_settings: MagicMock,
    mock_admin: MagicMock,
    mock_http: MagicMock,
    deploy: Deploy,
) -> None:
    mock_admin.return_value.admin_user = "admin"
    mock_admin.return_value.admin_password = "pass"
    mock_admin.return_value.mgmt_url = "https://localhost:8089"
    mock_admin.return_value.verify_tls = False

    mock_log_settings.return_value.splunk_index = "data_collector"

    mock_http.RequestException = Exception
    mock_http.delete.side_effect = Exception("Connection refused")

    assert deploy.clean_splunk() is False


@pytest.mark.parametrize("status_code", [401, 403])
@patch(f"{_DEPLOY_MODULE}.http_requests")
@patch(f"{_DEPLOY_MODULE}.SplunkAdminSettings")
@patch(f"{_DEPLOY_MODULE}.LogSettings")
def test_setup_splunk_returns_false_on_auth_failure(
    mock_log_settings: MagicMock,
    mock_admin: MagicMock,
    mock_http: MagicMock,
    deploy: Deploy,
    status_code: int,
) -> None:
    mock_admin.return_value.admin_user = "admin"
    mock_admin.return_value.admin_password = "pass"
    mock_admin.return_value.mgmt_url = "https://localhost:8089"
    mock_admin.return_value.verify_tls = False

    mock_log_settings.return_value.splunk_index = "data_collector"
    mock_log_settings.return_value.splunk_sourcetype = "data_collector:structured"

    mock_http.get.return_value = MagicMock(status_code=status_code)
    mock_http.RequestException = Exception

    assert deploy.setup_splunk() is False


@pytest.mark.parametrize("status_code", [401, 403])
@patch(f"{_DEPLOY_MODULE}.http_requests")
@patch(f"{_DEPLOY_MODULE}.SplunkAdminSettings")
@patch(f"{_DEPLOY_MODULE}.LogSettings")
def test_clean_splunk_returns_false_on_auth_failure(
    mock_log_settings: MagicMock,
    mock_admin: MagicMock,
    mock_http: MagicMock,
    deploy: Deploy,
    status_code: int,
) -> None:
    mock_admin.return_value.admin_user = "admin"
    mock_admin.return_value.admin_password = "pass"
    mock_admin.return_value.mgmt_url = "https://localhost:8089"
    mock_admin.return_value.verify_tls = False

    mock_log_settings.return_value.splunk_index = "data_collector"

    mock_http.delete.return_value = MagicMock(status_code=status_code)
    mock_http.RequestException = Exception

    assert deploy.clean_splunk() is False
