"""Tests for proxy judge IP verification."""

from unittest.mock import MagicMock, patch

from data_collector.proxy.judges import (
    _MAX_JUDGE_RESPONSE_LENGTH,  # pyright: ignore[reportPrivateUsage]
    PROXY_JUDGES,
    extract_ip,
    verify_ip,
)


class TestExtractIp:
    """Tests for extract_ip function."""

    def test_ipv4_from_json_httpbin(self) -> None:
        content = b'{"origin": "203.0.113.42"}'
        assert extract_ip(content) == "203.0.113.42"

    def test_ipv4_from_json_ipify(self) -> None:
        content = b'{"ip": "198.51.100.1"}'
        assert extract_ip(content) == "198.51.100.1"

    def test_ipv4_from_plain_text(self) -> None:
        content = b"198.51.100.1\n"
        assert extract_ip(content) == "198.51.100.1"

    def test_ipv4_from_plain_text_no_newline(self) -> None:
        content = b"10.0.0.1"
        assert extract_ip(content) == "10.0.0.1"

    def test_ipv6_simple(self) -> None:
        content = b"2001:0db8:85a3:0000:0000:8a2e:0370:7334"
        assert extract_ip(content) == "2001:0db8:85a3:0000:0000:8a2e:0370:7334"

    def test_ipv6_from_json(self) -> None:
        content = b'{"origin": "2001:db8::1"}'
        assert extract_ip(content) == "2001:db8::1"

    def test_no_ip_returns_none(self) -> None:
        content = b"no ip address here"
        assert extract_ip(content) is None

    def test_empty_content(self) -> None:
        assert extract_ip(b"") is None

    def test_ipv4_preferred_over_ipv6(self) -> None:
        content = b'{"ipv4": "1.2.3.4", "ipv6": "2001:db8::1"}'
        assert extract_ip(content) == "1.2.3.4"


class TestVerifyIp:
    """Tests for verify_ip function."""

    def test_returns_ip_on_success(self) -> None:
        mock_response = MagicMock()
        mock_response.content = b'{"origin": "1.2.3.4"}'

        with patch("data_collector.proxy.judges.Request") as mock_request_cls:
            mock_request = MagicMock()
            mock_request.get.return_value = mock_response
            mock_request_cls.return_value = mock_request

            result = verify_ip("http://proxy:8080", ["https://judge1.example.com/ip"])

        assert result == "1.2.3.4"
        mock_request.set_proxy.assert_called_once_with("http://proxy:8080")

    def test_skips_oversized_response(self) -> None:
        mock_response = MagicMock()
        mock_response.content = b"x" * (_MAX_JUDGE_RESPONSE_LENGTH + 1)

        with patch("data_collector.proxy.judges.Request") as mock_request_cls:
            mock_request = MagicMock()
            mock_request.get.return_value = mock_response
            mock_request_cls.return_value = mock_request

            result = verify_ip("http://proxy:8080", ["https://judge1.example.com/ip"])

        assert result is None

    def test_failover_to_second_judge(self) -> None:
        failed_response = MagicMock()
        failed_response.content = b"<html>error page</html>" * 50

        success_response = MagicMock()
        success_response.content = b'{"ip": "5.6.7.8"}'

        with patch("data_collector.proxy.judges.Request") as mock_request_cls:
            mock_request = MagicMock()
            mock_request.get.side_effect = [failed_response, success_response]
            mock_request_cls.return_value = mock_request

            result = verify_ip("http://proxy:8080", ["https://judge1.com/ip", "https://judge2.com/ip"])

        assert result == "5.6.7.8"

    def test_returns_none_when_all_judges_fail(self) -> None:
        with patch("data_collector.proxy.judges.Request") as mock_request_cls:
            mock_request = MagicMock()
            mock_request.get.side_effect = ConnectionError("timeout")
            mock_request_cls.return_value = mock_request

            result = verify_ip("http://proxy:8080", ["https://judge1.com/ip"])

        assert result is None

    def test_returns_none_when_response_is_none(self) -> None:
        with patch("data_collector.proxy.judges.Request") as mock_request_cls:
            mock_request = MagicMock()
            mock_request.get.return_value = None
            mock_request_cls.return_value = mock_request

            result = verify_ip("http://proxy:8080", ["https://judge1.com/ip"])

        assert result is None

    def test_judges_list_has_entries(self) -> None:
        assert len(PROXY_JUDGES) >= 3
