"""Tests for the review fixes applied after the v0.3 release.

Covers:
  1. _compact_activity: adjacent-only lap dedup preserves non-adjacent repeats
  2. get_help: stays in sync with the registered MCP tool set (no drift)
  3. get_stored_auth: env-token precedence vs. refreshable credentials
  4. _env_int: malformed env value falls back instead of crashing at import
  5. add_planned_workout: does not mutate the caller's program dict
"""

import asyncio

import pytest

from coros_mcp import coros_api
from coros_mcp.server import _compact_activity, get_help, mcp

# ---------------------------------------------------------------------------
# 1. Adjacent-only lap dedup
# ---------------------------------------------------------------------------

class TestLapDedupAdjacentOnly:
    def test_collapses_adjacent_climbing_copy(self):
        items = [{"avgHr": 140, "distance": 500}]
        data = {
            "lapList": [
                {"type": 2, "lapItemList": [dict(i) for i in items]},
                {"type": 3, "lapItemList": [dict(i) for i in items]},  # back-to-back copy
                {"type": 4, "lapItemList": [{"avgHr": 160, "distance": 600}]},
            ]
        }
        out = _compact_activity(data)
        assert [lap["type"] for lap in out["lapList"]] == [2, 4]

    def test_preserves_non_adjacent_identical_laps(self):
        """Two identical recovery intervals separated by work laps must survive."""
        recovery = [{"avgHr": 110, "avgPower": 120}]
        work = [{"avgHr": 175, "avgPower": 300}]
        data = {
            "lapList": [
                {"lapIndex": 1, "lapItemList": [dict(i) for i in recovery]},
                {"lapIndex": 2, "lapItemList": [dict(i) for i in work]},
                {"lapIndex": 3, "lapItemList": [dict(i) for i in recovery]},  # same items, not adjacent
            ]
        }
        out = _compact_activity(data)
        assert [lap["lapIndex"] for lap in out["lapList"]] == [1, 2, 3]

    def test_empty_item_lists_are_never_deduped(self):
        data = {
            "lapList": [
                {"lapIndex": 1, "lapItemList": [{"avgPower": 0}]},  # compacts to []
                {"lapIndex": 2, "lapItemList": [{"note": ""}]},     # compacts to []
            ]
        }
        out = _compact_activity(data)
        assert [lap["lapIndex"] for lap in out["lapList"]] == [1, 2]

    def test_collapses_run_of_more_than_two_copies(self):
        items = [{"avgHr": 140}]
        data = {
            "lapList": [
                {"type": 2, "lapItemList": [dict(i) for i in items]},
                {"type": 3, "lapItemList": [dict(i) for i in items]},
                {"type": 5, "lapItemList": [dict(i) for i in items]},
            ]
        }
        out = _compact_activity(data)
        assert len(out["lapList"]) == 1
        assert out["lapList"][0]["type"] == 2


# ---------------------------------------------------------------------------
# 2. get_help stays in sync with the registry
# ---------------------------------------------------------------------------

def test_get_help_matches_registered_tools():
    registered = {t.name for t in asyncio.run(mcp.list_tools())}
    listed = {entry["name"] for entry in asyncio.run(get_help())["tools"]}
    assert listed == registered, (
        "get_help is out of sync with the registered MCP tools. "
        f"Missing from get_help: {registered - listed}. "
        f"Listed but not registered: {listed - registered}."
    )


# ---------------------------------------------------------------------------
# 3. get_stored_auth env-token precedence
# ---------------------------------------------------------------------------

class TestGetStoredAuthPrecedence:
    def _stored(self, token="stored-tok"):
        return coros_api.StoredAuth(
            access_token=token, user_id="u1", region="eu",
            timestamp=0, mobile_access_token=None, mobile_login_payload=None,
        )

    def test_env_token_wins_when_no_credentials(self, monkeypatch):
        monkeypatch.setenv("COROS_ACCESS_TOKEN", "env-tok")
        monkeypatch.delenv("COROS_EMAIL", raising=False)
        monkeypatch.delenv("COROS_PASSWORD", raising=False)
        monkeypatch.setattr(coros_api, "_load_auth", lambda: self._stored())
        monkeypatch.setattr(coros_api, "_is_token_valid", lambda a: True)
        auth = coros_api.get_stored_auth()
        assert auth is not None and auth.access_token == "env-tok"

    def test_valid_stored_token_wins_over_env_when_credentials_present(self, monkeypatch):
        monkeypatch.setenv("COROS_ACCESS_TOKEN", "stale-env-tok")
        monkeypatch.setenv("COROS_EMAIL", "a@b.c")
        monkeypatch.setenv("COROS_PASSWORD", "pw")
        monkeypatch.setattr(coros_api, "_load_auth", lambda: self._stored("fresh-stored"))
        monkeypatch.setattr(coros_api, "_is_token_valid", lambda a: True)
        auth = coros_api.get_stored_auth()
        # The freshly-minted stored token must take effect, not the stale env one.
        assert auth is not None and auth.access_token == "fresh-stored"

    def test_env_token_seeds_when_credentials_present_but_no_valid_stored(self, monkeypatch):
        monkeypatch.setenv("COROS_ACCESS_TOKEN", "env-tok")
        monkeypatch.setenv("COROS_EMAIL", "a@b.c")
        monkeypatch.setenv("COROS_PASSWORD", "pw")
        monkeypatch.setattr(coros_api, "_load_auth", lambda: None)
        monkeypatch.setattr(coros_api, "_is_token_valid", lambda a: False)
        auth = coros_api.get_stored_auth()
        assert auth is not None and auth.access_token == "env-tok"

    def test_returns_none_when_no_env_token_and_no_valid_stored(self, monkeypatch):
        monkeypatch.delenv("COROS_ACCESS_TOKEN", raising=False)
        monkeypatch.setenv("COROS_EMAIL", "a@b.c")
        monkeypatch.setenv("COROS_PASSWORD", "pw")
        monkeypatch.setattr(coros_api, "_load_auth", lambda: None)
        monkeypatch.setattr(coros_api, "_is_token_valid", lambda a: False)
        assert coros_api.get_stored_auth() is None


# ---------------------------------------------------------------------------
# 4. _env_int robustness
# ---------------------------------------------------------------------------

class TestEnvInt:
    def test_valid_value(self, monkeypatch):
        from coros_mcp.cache import sync
        monkeypatch.setenv("COROS_STABLE_DAYS", "5")
        assert sync._env_int("COROS_STABLE_DAYS", 2) == 5

    def test_missing_uses_default(self, monkeypatch):
        from coros_mcp.cache import sync
        monkeypatch.delenv("COROS_STABLE_DAYS", raising=False)
        assert sync._env_int("COROS_STABLE_DAYS", 2) == 2

    def test_malformed_falls_back_without_raising(self, monkeypatch):
        from coros_mcp.cache import sync
        monkeypatch.setenv("COROS_STABLE_DAYS", "zwei")
        assert sync._env_int("COROS_STABLE_DAYS", 2) == 2


# ---------------------------------------------------------------------------
# 5. add_planned_workout does not mutate the caller's program
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_add_planned_workout_does_not_mutate_program(monkeypatch):
    captured = {}

    async def fake_post(auth, payload):
        captured["payload"] = payload

    monkeypatch.setattr(coros_api, "_post_schedule_update", fake_post)

    entity = {"idInPlan": 7, "happenDay": "20260613"}
    program = {"name": "Run"}  # intentionally lacks idInPlan
    auth = coros_api.StoredAuth(
        access_token="t", user_id="u", region="eu", timestamp=0,
        mobile_access_token=None, mobile_login_payload=None,
    )

    await coros_api.add_planned_workout(auth, entity, program)

    # Caller's dict is untouched...
    assert "idInPlan" not in program
    # ...while the sent payload carries the resolved idInPlan.
    assert captured["payload"]["programs"][0]["idInPlan"] == 7
