from unittest.mock import patch

import httpx
import pytest
import respx

from data_collector.utilities.request import Request

# ---------------------------------------------------------------------------
# Async GET / POST
# ---------------------------------------------------------------------------

@respx.mock
@pytest.mark.asyncio
async def test_async_get_success() -> None:
    respx.get("https://example.com/page").mock(return_value=httpx.Response(200, text="OK"))
    req = Request(timeout=5, retries=0)
    resp = await req.async_get("https://example.com/page")
    assert resp is not None
    assert resp.status_code == 200


@respx.mock
@pytest.mark.asyncio
async def test_async_post_success() -> None:
    respx.post("https://example.com/api").mock(return_value=httpx.Response(200, json={"ok": True}))
    req = Request(timeout=5, retries=0)
    resp = await req.async_post("https://example.com/api", json={"q": "test"})
    assert resp is not None
    assert resp.status_code == 200


# ---------------------------------------------------------------------------
# Async retry
# ---------------------------------------------------------------------------

@respx.mock
@pytest.mark.asyncio
async def test_async_retry_on_503() -> None:
    route = respx.get("https://example.com/page")
    route.side_effect = [
        httpx.Response(503),
        httpx.Response(200, text="OK"),
    ]
    with patch("asyncio.sleep", return_value=None):
        req = Request(timeout=5, retries=2)
        resp = await req.async_get("https://example.com/page")
    assert resp is not None
    assert resp.status_code == 200
    assert route.call_count == 2


# ---------------------------------------------------------------------------
# Async error handling
# ---------------------------------------------------------------------------

@respx.mock
@pytest.mark.asyncio
async def test_async_timeout_error() -> None:
    respx.get("https://example.com/page").mock(side_effect=httpx.ReadTimeout("timeout"))
    with patch("asyncio.sleep", return_value=None):
        req = Request(timeout=5, retries=0)
        resp = await req.async_get("https://example.com/page")
    assert resp is None
    assert req.timeout_err == 1


@respx.mock
@pytest.mark.asyncio
async def test_async_has_errors() -> None:
    respx.get("https://example.com/page").mock(side_effect=httpx.ReadTimeout("timeout"))
    with patch("asyncio.sleep", return_value=None):
        req = Request(timeout=5, retries=0)
        await req.async_get("https://example.com/page")
    assert req.has_errors() is True


@respx.mock
@pytest.mark.asyncio
async def test_async_no_retry_on_401() -> None:
    route = respx.get("https://example.com/page").mock(return_value=httpx.Response(401))
    req = Request(timeout=5, retries=3)
    resp = await req.async_get("https://example.com/page")
    assert resp is not None
    assert resp.status_code == 401
    assert route.call_count == 1
