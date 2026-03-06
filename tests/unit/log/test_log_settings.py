from __future__ import annotations

import pytest

from data_collector.settings.main import LogSettings, SplunkAdminSettings

SPLUNK_ENV_VARS = (
    "DC_LOG_SPLUNK_ENABLED",
    "DC_LOG_SPLUNK_URL",
    "DC_LOG_SPLUNK_TOKEN",
    "DC_LOG_SPLUNK_VERIFY_TLS",
    "DC_LOG_SPLUNK_CA_BUNDLE",
    "DC_LOG_SPLUNK_INDEX",
    "DC_LOG_SPLUNK_SOURCETYPE",
    "DC_LOG_ERROR_FILE",
)


def _clear_splunk_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for env_var in SPLUNK_ENV_VARS:
        monkeypatch.delenv(env_var, raising=False)


def test_log_settings_reads_dc_log_splunk_environment_variables(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_splunk_env(monkeypatch)
    monkeypatch.setenv("DC_LOG_SPLUNK_ENABLED", "true")
    monkeypatch.setenv("DC_LOG_SPLUNK_URL", "https://splunk.local:8088/services/collector")
    monkeypatch.setenv("DC_LOG_SPLUNK_TOKEN", "token-value")
    monkeypatch.setenv("DC_LOG_SPLUNK_VERIFY_TLS", "false")
    monkeypatch.setenv("DC_LOG_SPLUNK_CA_BUNDLE", "C:/certs/splunk-ca.pem")

    settings = LogSettings()

    assert settings.log_to_splunk is True
    assert settings.splunk_hec_url == "https://splunk.local:8088/services/collector"
    assert settings.splunk_token == "token-value"
    assert settings.splunk_verify_tls is False
    assert settings.splunk_ca_bundle == "C:/certs/splunk-ca.pem"
    assert settings.splunk_index == "default"
    assert settings.splunk_sourcetype == "data_collector:structured"

    _clear_splunk_env(monkeypatch)


def test_log_settings_reads_splunk_index_and_sourcetype_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_splunk_env(monkeypatch)
    monkeypatch.setenv("DC_LOG_SPLUNK_INDEX", "my_index")
    monkeypatch.setenv("DC_LOG_SPLUNK_SOURCETYPE", "my_sourcetype")

    settings = LogSettings()

    assert settings.splunk_index == "my_index"
    assert settings.splunk_sourcetype == "my_sourcetype"
    _clear_splunk_env(monkeypatch)


def test_log_settings_keeps_ca_bundle_optional(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_splunk_env(monkeypatch)
    monkeypatch.setenv("DC_LOG_SPLUNK_ENABLED", "true")
    monkeypatch.setenv("DC_LOG_SPLUNK_URL", "https://splunk.local:8088/services/collector")
    monkeypatch.setenv("DC_LOG_SPLUNK_TOKEN", "token-value")
    monkeypatch.setenv("DC_LOG_SPLUNK_VERIFY_TLS", "false")

    settings = LogSettings()

    assert settings.log_to_splunk is True
    assert settings.splunk_ca_bundle is None

    _clear_splunk_env(monkeypatch)


def test_log_settings_allows_field_name_initialization() -> None:
    settings = LogSettings(
        log_to_splunk=True,
        splunk_hec_url="https://splunk.local:8088/services/collector",
        splunk_token="token-value",
        splunk_verify_tls=False,
        splunk_ca_bundle=None,
    )

    assert settings.log_to_splunk is True
    assert settings.splunk_hec_url == "https://splunk.local:8088/services/collector"
    assert settings.splunk_token == "token-value"
    assert settings.splunk_verify_tls is False
    assert settings.splunk_ca_bundle is None


def test_log_settings_reads_log_error_file_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_splunk_env(monkeypatch)
    monkeypatch.setenv("DC_LOG_ERROR_FILE", "collector-error.log")

    settings = LogSettings()

    assert settings.log_error_file == "collector-error.log"
    _clear_splunk_env(monkeypatch)


def test_log_settings_defaults_log_error_file_when_env_is_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_splunk_env(monkeypatch)
    monkeypatch.setenv("DC_LOG_ERROR_FILE", "")

    settings = LogSettings()

    assert settings.log_error_file == "error.log"
    _clear_splunk_env(monkeypatch)


SPLUNK_ADMIN_ENV_VARS = (
    "DC_SPLUNK_MGMT_URL",
    "DC_SPLUNK_ADMIN_USER",
    "DC_SPLUNK_ADMIN_PASSWORD",
    "DC_SPLUNK_MGMT_VERIFY_TLS",
)


def _clear_admin_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for env_var in SPLUNK_ADMIN_ENV_VARS:
        monkeypatch.delenv(env_var, raising=False)


def test_splunk_admin_settings_reads_env_variables(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_admin_env(monkeypatch)
    monkeypatch.setenv("DC_SPLUNK_MGMT_URL", "https://splunk.local:8089")
    monkeypatch.setenv("DC_SPLUNK_ADMIN_USER", "admin")
    monkeypatch.setenv("DC_SPLUNK_ADMIN_PASSWORD", "secret")
    monkeypatch.setenv("DC_SPLUNK_MGMT_VERIFY_TLS", "true")

    settings = SplunkAdminSettings()

    assert settings.mgmt_url == "https://splunk.local:8089"
    assert settings.admin_user == "admin"
    assert settings.admin_password == "secret"
    assert settings.verify_tls is True
    _clear_admin_env(monkeypatch)


def test_splunk_admin_settings_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_admin_env(monkeypatch)

    settings = SplunkAdminSettings()

    assert settings.mgmt_url == "https://127.0.0.1:8089"
    assert settings.admin_user is None
    assert settings.admin_password is None
    assert settings.verify_tls is False
    _clear_admin_env(monkeypatch)


def test_splunk_admin_settings_allows_field_name_initialization() -> None:
    settings = SplunkAdminSettings(
        mgmt_url="https://splunk.local:8089",
        admin_user="admin",
        admin_password="secret",
        verify_tls=True,
    )

    assert settings.mgmt_url == "https://splunk.local:8089"
    assert settings.admin_user == "admin"
    assert settings.admin_password == "secret"
    assert settings.verify_tls is True
