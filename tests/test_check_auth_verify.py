"""Tests for check_coros_auth's optional server-side token verification (issue #49).

Covers:
  - default (verify_with_server=False) stays a local-only check, no probe
  - verify_with_server=True maps accept / reject / inconclusive outcomes
"""
import asyncio

import httpx
import pytest

from coros_mcp import coros_api
from coros_mcp.server import check_coros_auth


def _stored():
    return coros_api.StoredAuth(
        access_token="tok", user_id="u1", region="eu",
        timestamp=0, mobile_access_token=None, mobile_login_payload=None,
    )


@pytest.fixture(autouse=True)
def _fresh_token(monkeypatch):
    # Report the local token as fresh so the branch under test is reached.
    monkeypatch.setattr(coros_api, "get_stored_auth", lambda: _stored())


def test_default_does_not_probe_server(monkeypatch):
    called = False

    async def _boom(_auth):
        nonlocal called
        called = True

    monkeypatch.setattr(coros_api, "verify_web_token", _boom)
    result = asyncio.run(check_coros_auth())
    assert "server_verified" not in result
    assert called is False


def test_verify_accepted(monkeypatch):
    async def _ok(_auth):
        return None

    monkeypatch.setattr(coros_api, "verify_web_token", _ok)
    result = asyncio.run(check_coros_auth(verify_with_server=True))
    assert result["server_verified"] is True


def test_verify_rejected_on_auth_error(monkeypatch):
    async def _rejected(_auth):
        raise coros_api.CorosAPIError("1019", "Access token is invalid")

    monkeypatch.setattr(coros_api, "verify_web_token", _rejected)
    result = asyncio.run(check_coros_auth(verify_with_server=True))
    assert result["server_verified"] is False
    assert "invalid" in result["server_message"].lower()


def test_verify_rejected_on_http_401(monkeypatch):
    async def _401(_auth):
        raise httpx.HTTPStatusError(
            "unauthorized",
            request=httpx.Request("GET", "https://x"),
            response=httpx.Response(401),
        )

    monkeypatch.setattr(coros_api, "verify_web_token", _401)
    result = asyncio.run(check_coros_auth(verify_with_server=True))
    assert result["server_verified"] is False


def test_verify_inconclusive_on_network_error(monkeypatch):
    async def _netfail(_auth):
        raise httpx.ConnectError("boom")

    monkeypatch.setattr(coros_api, "verify_web_token", _netfail)
    result = asyncio.run(check_coros_auth(verify_with_server=True))
    # A transport failure must NOT be reported as an invalid token.
    assert result["server_verified"] is None
    assert "inconclusive" in result["server_message"]
