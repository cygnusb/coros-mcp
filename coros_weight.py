#!/usr/bin/env python3
"""Fetch current body weight (kg) via the official COROS MCP server (queryUserInfo).

Self-contained: no third-party OAuth dependency.
- Uses OAuth 2.0 Dynamic Client Registration (RFC 7591) to register a public client on the fly
- PKCE (S256) authorization-code flow
- Calls the queryUserInfo tool on mcpcn.coros.com/mcp

Token is persisted at ~/.hermes/coros_mcp_token.json. access_token is valid ~30 days;
refresh_token is auto-renewed (COROS returns HTTP 500 intermittently, retried internally).

Usage:
    from coros_weight import get_weight_kg
    kg = asyncio.run(get_weight_kg())   # -> 69.4 (float) or None
"""
import sys, os, json, asyncio, httpx
from pathlib import Path
from datetime import datetime, timezone
import base64, hashlib, secrets

TOKEN_FILE = Path.home() / ".hermes" / "coros_mcp_token.json"
ISSUER = "https://mcpcn.coros.com"
MCP_ENDPOINT = f"{ISSUER}/mcp"
REDIRECT_URI = "http://localhost:8765/callback"
SCOPES = "openid mcp.tools offline_access"


def _b64url(b: bytes) -> str:
    return base64.urlsafe_b64encode(b).rstrip(b"=").decode()


def _gen_pkce():
    verifier = _b64url(secrets.token_bytes(32))
    challenge = _b64url(hashlib.sha256(verifier.encode()).digest())
    return verifier, challenge


def _gen_state():
    return _b64url(secrets.token_bytes(16))


async def _register_client(c: httpx.AsyncClient):
    """Dynamic Client Registration (RFC 7591)."""
    r = await c.post(f"{ISSUER}/connect/register", json={
        "client_name": "hermes-coros-weight",
        "redirect_uris": [REDIRECT_URI],
        "grant_types": ["authorization_code"],
        "response_types": ["code"],
        "token_endpoint_auth_method": "none",
        "scope": SCOPES,
    }, timeout=30)
    r.raise_for_status()
    d = r.json()
    return d.get("client_id"), d.get("client_secret")


async def _exchange_code(c, client_id, client_secret, code, verifier):
    data = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": REDIRECT_URI,
        "client_id": client_id,
        "code_verifier": verifier,
    }
    if client_secret:
        data["client_secret"] = client_secret
    r = await c.post(f"{ISSUER}/oauth2/token", data=data, timeout=30)
    r.raise_for_status()
    return r.json()


async def _refresh(client_id, client_secret, refresh_token):
    data = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": client_id,
    }
    if client_secret:
        data["client_secret"] = client_secret
    last: Exception | None = None
    for _ in range(3):
        try:
            async with httpx.AsyncClient(timeout=30) as cli:
                r = await cli.post(f"{ISSUER}/oauth2/token", data=data, timeout=30)
            r.raise_for_status()
            return r.json()
        except Exception as e:  # noqa: BLE001
            last = e
            await asyncio.sleep(2)
    if last is not None:
        raise last
    raise RuntimeError("refresh failed (unknown error)")


def _save(blob):
    TOKEN_FILE.parent.mkdir(parents=True, exist_ok=True)
    TOKEN_FILE.write_text(json.dumps(blob, indent=2))


def _load():
    return json.loads(TOKEN_FILE.read_text()) if TOKEN_FILE.exists() else None


async def _ensure_token(blob) -> dict:
    now = datetime.now(timezone.utc).timestamp()
    if blob.get("access_token") and now < blob.get("expires_at", 0) - 300:
        return blob
    refreshed = await _refresh(blob["client_id"], blob.get("client_secret"), blob["refresh_token"])
    refreshed["client_id"] = blob["client_id"]
    refreshed["client_secret"] = blob.get("client_secret")
    refreshed["expires_at"] = now + (refreshed.get("expires_in") or 2592000)
    _save(refreshed)
    return refreshed


async def get_weight_kg() -> float | None:
    """Return current body weight in kg, or None on failure.

    First use requires an authorization flow (call prepare_auth() to get the
    authorization URL; after the user approves in the browser, pass the callback
    URL to complete_auth()). Once authorized, the persisted token is used and
    auto-renewed.
    """
    blob = _load()
    if not blob:
        print("warning: not authorized yet - call prepare_auth() to get the auth URL")
        return None
    blob = await _ensure_token(blob)
    token = blob["access_token"]
    h = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream",
        "MCP-Protocol-Version": "2025-06-18",
    }
    async with httpx.AsyncClient(timeout=30) as c:
        r = await c.post(MCP_ENDPOINT, json={"jsonrpc": "2.0", "id": 1,
            "method": "initialize", "params": {"protocolVersion": "2025-06-18",
            "capabilities": {}, "clientInfo": {"name": "hermes", "version": "1.0"}}}, headers=h)
        sid = r.headers.get("mcp-session-id")
        if sid:
            h["Mcp-Session-Id"] = sid
        r2 = await c.post(MCP_ENDPOINT, json={"jsonrpc": "2.0", "id": 2,
            "method": "tools/call", "params": {"name": "queryUserInfo", "arguments": {}}}, headers=h)
        d = r2.json()
        if "result" not in d:
            return None
        import re
        txt = d["result"]["content"][0]["text"]
        m = re.search(r"Weight:\s*([\d.]+)\s*kg", txt)
        return float(m.group(1)) if m else None


async def prepare_auth():
    """Return dict(url, state, verifier, client_id, client_secret).
    The user opens `url` in a browser, approves, then passes the callback URL to complete_auth()."""
    async with httpx.AsyncClient(timeout=30) as c:
        client_id, client_secret = await _register_client(c)
        verifier, challenge = _gen_pkce()
        state = _gen_state()
        url = (f"{ISSUER}/oauth2/authorize?response_type=code&client_id={client_id}"
               f"&redirect_uri={REDIRECT_URI}&scope={SCOPES}&state={state}"
               f"&code_challenge={challenge}&code_challenge_method=S256")
        return {"url": url, "state": state, "verifier": verifier,
                "client_id": client_id, "client_secret": client_secret}


async def complete_auth(code: str, meta: dict):
    """Exchange the authorization callback code for a token and persist it. `meta` comes from prepare_auth()."""
    async with httpx.AsyncClient(timeout=30) as c:
        tok = await _exchange_code(c, meta["client_id"], meta.get("client_secret"), code, meta["verifier"])
    tok["client_id"] = meta["client_id"]
    tok["client_secret"] = meta.get("client_secret")
    tok["expires_at"] = datetime.now(timezone.utc).timestamp() + (tok.get("expires_in") or 2592000)
    _save(tok)
    return tok


if __name__ == "__main__":
    async def _main():
        w = await get_weight_kg()
        print(f"current weight: {w} kg" if w else "fetch failed")
    asyncio.run(_main())
