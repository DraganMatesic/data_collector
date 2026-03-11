"""Tests for ProxySettings zone-parameterized proxy credentials."""

import pytest
from pydantic import ValidationError

from data_collector.proxy.models import ProxyData
from data_collector.settings.proxy import ProxySettings

_ZONE = "SCRAPING"
_ENV_PREFIX = f"DC_PROXY_{{}}_{_ZONE}"


def _set_required_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Set all required DC_PROXY_*_SCRAPING env vars."""
    monkeypatch.setenv(f"DC_PROXY_HOST_{_ZONE}", "brd.superproxy.io")
    monkeypatch.setenv(f"DC_PROXY_PORT_{_ZONE}", "22225")
    monkeypatch.setenv(f"DC_PROXY_USERNAME_{_ZONE}", "brd-customer-xxx-zone-residential_hr")
    monkeypatch.setenv(f"DC_PROXY_PASSWORD_{_ZONE}", "test-password")


class TestFromZone:
    """Tests for ProxySettings.from_zone() factory."""

    def test_reads_required_env_vars(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _set_required_env(monkeypatch)
        settings = ProxySettings.from_zone("scraping")

        assert settings.host == "brd.superproxy.io"
        assert settings.port == 22225
        assert settings.username == "brd-customer-xxx-zone-residential_hr"
        assert settings.password == "test-password"

    def test_case_insensitive_zone(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _set_required_env(monkeypatch)
        settings = ProxySettings.from_zone("Scraping")

        assert settings.host == "brd.superproxy.io"

    def test_optional_country_defaults_none(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _set_required_env(monkeypatch)
        settings = ProxySettings.from_zone("scraping")

        assert settings.country is None

    def test_optional_country_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _set_required_env(monkeypatch)
        monkeypatch.setenv(f"DC_PROXY_COUNTRY_{_ZONE}", "hr")
        settings = ProxySettings.from_zone("scraping")

        assert settings.country == "hr"

    def test_optional_protocol_defaults_http(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _set_required_env(monkeypatch)
        settings = ProxySettings.from_zone("scraping")

        assert settings.protocol == "http"

    def test_optional_protocol_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _set_required_env(monkeypatch)
        monkeypatch.setenv(f"DC_PROXY_PROTOCOL_{_ZONE}", "socks5")
        settings = ProxySettings.from_zone("scraping")

        assert settings.protocol == "socks5"

    def test_missing_required_raises_validation_error(self) -> None:
        with pytest.raises(ValidationError):
            ProxySettings.from_zone("nonexistent_zone")

    def test_different_zones_read_different_env_vars(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _set_required_env(monkeypatch)
        monkeypatch.setenv("DC_PROXY_HOST_SUDREG", "other-proxy.io")
        monkeypatch.setenv("DC_PROXY_PORT_SUDREG", "33333")
        monkeypatch.setenv("DC_PROXY_USERNAME_SUDREG", "brd-customer-xxx-zone-sudreg")
        monkeypatch.setenv("DC_PROXY_PASSWORD_SUDREG", "other-password")

        scraping = ProxySettings.from_zone("scraping")
        sudreg = ProxySettings.from_zone("sudreg")

        assert scraping.host == "brd.superproxy.io"
        assert sudreg.host == "other-proxy.io"
        assert scraping.port == 22225
        assert sudreg.port == 33333
        assert scraping.username != sudreg.username


class TestDirectConstruction:
    """Tests for direct keyword construction (populate_by_name)."""

    def test_all_fields(self) -> None:
        settings = ProxySettings(
            host="brd.superproxy.io",
            port=22225,
            username="brd-customer-xxx-zone-test",
            password="secret",
            country="hr",
            protocol="socks5",
        )

        assert settings.host == "brd.superproxy.io"
        assert settings.port == 22225
        assert settings.username == "brd-customer-xxx-zone-test"
        assert settings.password == "secret"
        assert settings.country == "hr"
        assert settings.protocol == "socks5"

    def test_defaults_applied(self) -> None:
        settings = ProxySettings(
            host="brd.superproxy.io",
            port=22225,
            username="test-user",
            password="test-pass",
        )

        assert settings.country is None
        assert settings.protocol == "http"


class TestToProxyData:
    """Tests for ProxySettings.to_proxy_data() conversion."""

    def test_all_fields_mapped(self) -> None:
        settings = ProxySettings(
            host="brd.superproxy.io",
            port=22225,
            username="brd-customer-xxx-zone-residential_hr",
            password="secret",
            country="hr",
            protocol="socks5",
        )
        proxy_data = settings.to_proxy_data()

        assert isinstance(proxy_data, ProxyData)
        assert proxy_data.host == "brd.superproxy.io"
        assert proxy_data.port == 22225
        assert proxy_data.username == "brd-customer-xxx-zone-residential_hr"
        assert proxy_data.password == "secret"
        assert proxy_data.country == "hr"
        assert proxy_data.protocol == "socks5"

    def test_country_none_propagated(self) -> None:
        settings = ProxySettings(
            host="brd.superproxy.io",
            port=22225,
            username="test-user",
            password="test-pass",
        )
        proxy_data = settings.to_proxy_data()

        assert proxy_data.country is None
        assert proxy_data.protocol == "http"
