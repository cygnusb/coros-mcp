#!/usr/bin/env python3
"""COROS 官方 MCP 封装：拉取当前体重（queryUserInfo）。

复用 cmoron/coros-cli 的 OAuth（Dynamic Client Registration + PKCE）。
token 持久化在 ~/.hermes/coros_mcp_token.json，access_token 约30天有效，
refresh_token 自动续（COROS 偶发500，脚本内重试）。
"""
import sys, os, json, asyncio, httpx
from pathlib import Path
from datetime import datetime, timezone

PROBE = "/tmp/coros_probe/coros-cli"
if PROBE not in sys.path:
    sys.path.insert(0, PROBE)

from coros_cli.mcp.metadata import metadata_for_region
from coros_cli.mcp.oauth import exchange_code, refresh_access_token
from coros_cli.mcp.pkce import generate_code_verifier, generate_state, code_challenge
from coros_cli.mcp.models import RegisteredClient, TokenResponse

TOKEN_FILE = Path.home() / ".hermes" / "coros_mcp_token.json"
REDIRECT_URI = "http://localhost:8765/callback"


def _save(blob):
    TOKEN_FILE.parent.mkdir(parents=True, exist_ok=True)
    TOKEN_FILE.write_text(json.dumps(blob, indent=2))


def _load():
    if TOKEN_FILE.exists():
        return json.loads(TOKEN_FILE.read_text())
    return None


async def _ensure_token(blob) -> dict:
    """确保有有效 access_token，必要时 refresh（带重试）。"""
    now = datetime.now(timezone.utc).timestamp()
    expires_at = blob.get("expires_at", 0)
    if blob.get("access_token") and now < expires_at - 300:
        return blob
    # 需要 refresh
    meta = metadata_for_region(blob["region"])
    client = RegisteredClient(client_id=blob["client_id"], client_secret=blob.get("client_secret"))
    last_err = None
    for _ in range(3):
        try:
            async with httpx.AsyncClient(timeout=30) as c:
                t = await refresh_access_token(c, meta, client, blob["refresh_token"])
            blob["access_token"] = t.access_token
            if t.refresh_token:
                blob["refresh_token"] = t.refresh_token
            blob["expires_at"] = now + (t.expires_in or 2592000)
            _save(blob)
            return blob
        except Exception as e:
            last_err = e
            await asyncio.sleep(2)
    raise RuntimeError(f"refresh 失败: {last_err}")


async def get_weight_kg() -> float | None:
    """返回当前体重(kg)，失败返回 None。"""
    blob = _load()
    if not blob:
        return None
    blob = await _ensure_token(blob)
    meta = metadata_for_region(blob["region"])
    url = meta.mcp_endpoint
    token = blob["access_token"]
    h = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream",
        "MCP-Protocol-Version": "2025-06-18",
    }
    async with httpx.AsyncClient(timeout=30) as c:
        r = await c.post(url, json={"jsonrpc": "2.0", "id": 1, "method": "initialize",
                                    "params": {"protocolVersion": "2025-06-18", "capabilities": {},
                                               "clientInfo": {"name": "hermes", "version": "1.0"}}}, headers=h)
        sid = r.headers.get("mcp-session-id")
        if sid:
            h["Mcp-Session-Id"] = sid
        r2 = await c.post(url, json={"jsonrpc": "2.0", "id": 2, "method": "tools/call",
                                     "params": {"name": "queryUserInfo", "arguments": {}}}, headers=h)
        d = r2.json()
        if "result" not in d:
            return None
        txt = d["result"]["content"][0]["text"]
        # 解析 "Weight: 69.4 kg"
        import re
        m = re.search(r"Weight:\s*([\d.]+)\s*kg", txt)
        return float(m.group(1)) if m else None


if __name__ == "__main__":
    async def _main():
        w = await get_weight_kg()
        print(f"当前体重: {w} kg = {w*2:.1f} 斤" if w else "获取失败")
    asyncio.run(_main())
