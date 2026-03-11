"""Tests for proxy provider interface and implementations."""

from unittest.mock import patch

from data_collector.proxy.models import ProxyData
from data_collector.proxy.provider import BrightDataProvider


class TestBrightDataProvider:
    """Tests for BrightDataProvider URL construction."""

    def test_url_without_country(self) -> None:
        proxy_data = ProxyData(
            host="zproxy.lum-superproxy.io",
            port=22225,
            username="lum-customer-xxx",
            password="secret",
        )
        provider = BrightDataProvider(proxy_data)
        url = provider.build_proxy_url("abc123")
        assert url == "http://lum-customer-xxx-session-abc123:secret@zproxy.lum-superproxy.io:22225"

    def test_url_with_country(self) -> None:
        proxy_data = ProxyData(
            host="zproxy.lum-superproxy.io",
            port=22225,
            username="lum-customer-xxx",
            password="secret",
            country="hr",
        )
        provider = BrightDataProvider(proxy_data)
        url = provider.build_proxy_url("def456")
        assert url == "http://lum-customer-xxx-country-hr-session-def456:secret@zproxy.lum-superproxy.io:22225"

    def test_url_with_socks5_protocol(self) -> None:
        proxy_data = ProxyData(
            host="proxy.example.com",
            port=1080,
            username="user",
            password="pass",
            protocol="socks5",
        )
        provider = BrightDataProvider(proxy_data)
        url = provider.build_proxy_url("xyz789")
        assert url.startswith("socks5://")

    def test_proxy_data_stored(self) -> None:
        proxy_data = ProxyData(host="h", port=1, username="u", password="p")
        provider = BrightDataProvider(proxy_data)
        assert provider.proxy_data is proxy_data

    def test_is_healthy_delegates_to_verify_ip(self) -> None:
        proxy_data = ProxyData(host="h", port=1, username="u", password="p")
        provider = BrightDataProvider(proxy_data)
        with patch("data_collector.proxy.provider.verify_ip", return_value="1.2.3.4") as mock_verify:
            result = provider.is_healthy()
        assert result is True
        mock_verify.assert_called_once()

    def test_is_healthy_returns_false_on_failure(self) -> None:
        proxy_data = ProxyData(host="h", port=1, username="u", password="p")
        provider = BrightDataProvider(proxy_data)
        with patch("data_collector.proxy.provider.verify_ip", return_value=None):
            result = provider.is_healthy()
        assert result is False
