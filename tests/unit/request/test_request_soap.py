import logging
from unittest.mock import MagicMock, patch

from zeep.exceptions import Fault

from data_collector.utilities.request import Request


# ---------------------------------------------------------------------------
# create_soap_client
# ---------------------------------------------------------------------------

_TRANSPORT_PATH = "data_collector.utilities.request.Transport"


def test_create_soap_client_success() -> None:
    with patch("zeep.Client") as mock_client, patch(_TRANSPORT_PATH) as mock_transport:
        mock_client.return_value = MagicMock()
        req = Request(timeout=5, retries=0)
        client = req.create_soap_client("https://example.com/service?wsdl")
        assert client is not None
        mock_client.assert_called_once()
        # Verify session is passed to Transport
        assert mock_transport.call_args.kwargs.get("session") is not None


def test_create_soap_client_applies_session_config() -> None:
    with patch("zeep.Client") as mock_client, patch(_TRANSPORT_PATH) as mock_transport:
        mock_client.return_value = MagicMock()
        req = Request(timeout=10, retries=0)
        req.set_headers({"X-Api-Key": "secret"})
        req.set_auth("user", "pass")
        req.set_proxy("http://proxy:8080")
        req.create_soap_client("https://example.com/service?wsdl")

        session = mock_transport.call_args.kwargs["session"]
        assert session.headers["X-Api-Key"] == "secret"
        assert session.auth == ("user", "pass")
        assert session.proxies == {"http": "http://proxy:8080", "https": "http://proxy:8080"}


# ---------------------------------------------------------------------------
# soap_call
# ---------------------------------------------------------------------------

def test_soap_call_success() -> None:
    mock_method = MagicMock(return_value={"name": "Test Corp"})
    req = Request(timeout=5, retries=0)
    result = req.soap_call(mock_method, registration_number="12345678")
    assert result == {"name": "Test Corp"}
    mock_method.assert_called_once_with(registration_number="12345678")


def test_soap_call_fault() -> None:
    mock_method = MagicMock(side_effect=Fault("Invalid OIB"))
    req = Request(timeout=5, retries=0)
    result = req.soap_call(mock_method, oib="12345")
    assert result is None
    assert req.request_err == 1


def test_soap_call_raise_faults() -> None:
    mock_method = MagicMock(side_effect=Fault("Invalid OIB"))
    req = Request(timeout=5, retries=0)
    try:
        req.soap_call(mock_method, raise_faults=True, oib="12345")
        assert False, "Expected Fault"
    except Fault:
        pass
    assert req.request_err == 1


def test_soap_call_timeout() -> None:
    mock_method = MagicMock(side_effect=Exception("Connection timeout"))
    req = Request(timeout=5, retries=0)
    result = req.soap_call(mock_method, id="123")
    assert result is None
    assert req.timeout_err == 1


def test_soap_call_should_abort_after_fault() -> None:
    mock_method = MagicMock(side_effect=Fault("Server error"))
    req = Request(timeout=5, retries=0)
    req.soap_call(mock_method, id="123")
    logger = logging.getLogger("test")
    assert req.should_abort(logger) is True


def test_soap_call_records_request_count() -> None:
    mock_method = MagicMock(return_value="result")
    req = Request(timeout=5, retries=0)
    req.soap_call(mock_method)
    assert req.request_count == 1
