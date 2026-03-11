"""Tests for proxy data models, exceptions, and domain utilities."""

import pytest

from data_collector.proxy.models import Proxy, ProxyAcquisitionTimeout, ProxyData, extract_root_domain


class TestProxyData:
    """Tests for ProxyData frozen dataclass."""

    def test_basic_construction(self) -> None:
        proxy_data = ProxyData(
            host="zproxy.lum-superproxy.io",
            port=22225,
            username="lum-customer-xxx",
            password="secret",
        )
        assert proxy_data.host == "zproxy.lum-superproxy.io"
        assert proxy_data.port == 22225
        assert proxy_data.username == "lum-customer-xxx"
        assert proxy_data.password == "secret"
        assert proxy_data.country is None
        assert proxy_data.protocol == "http"

    def test_with_country_and_protocol(self) -> None:
        proxy_data = ProxyData(
            host="proxy.example.com",
            port=8080,
            username="user",
            password="pass",
            country="hr",
            protocol="socks5",
        )
        assert proxy_data.country == "hr"
        assert proxy_data.protocol == "socks5"

    def test_frozen(self) -> None:
        proxy_data = ProxyData(host="h", port=1, username="u", password="p")
        with pytest.raises(AttributeError):
            proxy_data.host = "other"  # type: ignore[misc]


class TestProxy:
    """Tests for Proxy frozen dataclass."""

    def test_basic_construction(self) -> None:
        proxy = Proxy(
            url="http://user:pass@host:8080",
            ip_address="1.2.3.4",
            session_id="abc123",
            target_domain="sub.gov.de",
        )
        assert proxy.url == "http://user:pass@host:8080"
        assert proxy.ip_address == "1.2.3.4"
        assert proxy.session_id == "abc123"
        assert proxy.target_domain == "sub.gov.de"

    def test_frozen(self) -> None:
        proxy = Proxy(url="u", ip_address="1.2.3.4", session_id="s", target_domain="d")
        with pytest.raises(AttributeError):
            proxy.url = "other"  # type: ignore[misc]


class TestProxyAcquisitionTimeout:
    """Tests for ProxyAcquisitionTimeout exception."""

    def test_is_exception(self) -> None:
        error = ProxyAcquisitionTimeout("timeout after 120s")
        assert isinstance(error, Exception)
        assert str(error) == "timeout after 120s"

    def test_can_be_raised_and_caught(self) -> None:
        with pytest.raises(ProxyAcquisitionTimeout, match="no proxy found"):
            raise ProxyAcquisitionTimeout("no proxy found")


class TestExtractRootDomain:
    """Tests for extract_root_domain utility."""

    def test_simple_domain(self) -> None:
        assert extract_root_domain("example.com") == "example.com"

    def test_subdomain(self) -> None:
        assert extract_root_domain("sub.example.com") == "example.com"

    def test_deep_subdomain(self) -> None:
        assert extract_root_domain("a.b.c.example.com") == "example.com"

    def test_country_tld(self) -> None:
        assert extract_root_domain("gov.de") == "gov.de"

    def test_subdomain_of_country_tld(self) -> None:
        assert extract_root_domain("sub.gov.de") == "gov.de"

    def test_co_uk(self) -> None:
        assert extract_root_domain("api.example.co.uk") == "example.co.uk"

    def test_com_au(self) -> None:
        assert extract_root_domain("shop.example.com.au") == "example.com.au"

    def test_invalid_domain_raises(self) -> None:
        with pytest.raises(ValueError, match="Cannot extract root domain"):
            extract_root_domain("")
