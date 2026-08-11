"""Tests for agent-hardening toolset gating (COROS_MCP_TOOLSET /
COROS_MCP_HIDE_AUTH_TOOLS) and the stricter parameter schemas.

The server module is reloaded per scenario because gating happens at import
time (tool registration). A fixture restores the default full toolset
afterwards so other test modules see the unmodified module.
"""
import asyncio
import importlib

import pytest

import coros_mcp.server as server_mod

WRITE_TOOLS = {
    "save_workout_template",
    "save_strength_workout_template",
    "delete_workout_template",
    "schedule_workout",
    "schedule_strength_workout",
    "schedule_workout_template",
    "add_planned_workout",
    "update_scheduled_workout",
    "remove_scheduled_workout",
}
ADVANCED_TOOLS = {
    "list_training_plans_raw",
    "list_planned_activities_raw",
    "calculate_workout_program",
}
AUTH_TOOLS = {"authenticate_coros", "authenticate_coros_mobile"}
READONLY_TOOLS = {
    "get_help",
    "check_coros_auth",
    "get_daily_metrics",
    "get_sleep_data",
    "list_activities",
    "get_activity_detail",
    "list_workout_templates",
    "list_training_plans",
    "list_planned_activities",
    "list_exercises",
    "sync_coros_data",
    "get_cache_status",
}
ALL_TOOLS = READONLY_TOOLS | WRITE_TOOLS | ADVANCED_TOOLS | AUTH_TOOLS


def _registered_tools(mod):
    return {t.name for t in asyncio.run(mod.mcp.list_tools())}


@pytest.fixture
def reload_server(monkeypatch):
    def _reload(toolset=None, hide_auth=None):
        if toolset is None:
            monkeypatch.delenv("COROS_MCP_TOOLSET", raising=False)
        else:
            monkeypatch.setenv("COROS_MCP_TOOLSET", toolset)
        if hide_auth is None:
            monkeypatch.delenv("COROS_MCP_HIDE_AUTH_TOOLS", raising=False)
        else:
            monkeypatch.setenv("COROS_MCP_HIDE_AUTH_TOOLS", hide_auth)
        return importlib.reload(server_mod)

    yield _reload
    # Restore the default (full) toolset for subsequent test modules.
    monkeypatch.delenv("COROS_MCP_TOOLSET", raising=False)
    monkeypatch.delenv("COROS_MCP_HIDE_AUTH_TOOLS", raising=False)
    importlib.reload(server_mod)


def test_default_registers_all_tools(reload_server):
    mod = reload_server()
    assert _registered_tools(mod) == ALL_TOOLS


def test_readonly_hides_write_advanced_and_auth_tools(reload_server):
    mod = reload_server(toolset="readonly")
    assert _registered_tools(mod) == READONLY_TOOLS


def test_hide_auth_tools_only(reload_server):
    mod = reload_server(hide_auth="1")
    assert _registered_tools(mod) == ALL_TOOLS - AUTH_TOOLS


def test_invalid_toolset_fails_fast(reload_server):
    with pytest.raises(ValueError, match="COROS_MCP_TOOLSET"):
        reload_server(toolset="readonyl")
    # Leave the module importable again (reload with valid env happens in
    # fixture teardown, but the failed reload leaves a broken module object
    # behind — restore immediately so later asserts in THIS test could run).


def test_hidden_tools_stay_plain_callables(reload_server):
    mod = reload_server(toolset="readonly")
    # Not registered over MCP, but still importable/callable for tests.
    assert callable(mod.save_workout_template)
    assert callable(mod.authenticate_coros)


def test_get_help_lists_only_enabled_tools(reload_server):
    mod = reload_server(toolset="readonly")
    out = asyncio.run(mod.get_help())
    assert out["toolset"] == "readonly"
    assert {t["name"] for t in out["tools"]} == READONLY_TOOLS

    mod = reload_server()
    out = asyncio.run(mod.get_help())
    assert out["toolset"] == "full"
    assert {t["name"] for t in out["tools"]} == ALL_TOOLS


def test_write_tools_carry_destructive_annotations(reload_server):
    mod = reload_server()
    tools = {t.name: t for t in asyncio.run(mod.mcp.list_tools())}
    assert tools["get_daily_metrics"].annotations.readOnlyHint is True
    assert tools["delete_workout_template"].annotations.destructiveHint is True
    assert tools["remove_scheduled_workout"].annotations.destructiveHint is True
    assert tools["schedule_workout"].annotations.readOnlyHint is False
    assert tools["schedule_workout"].annotations.destructiveHint is False


def test_schema_constraints_present(reload_server):
    mod = reload_server()
    tools = {t.name: t for t in asyncio.run(mod.mcp.list_tools())}
    weeks = tools["get_daily_metrics"].parameters["properties"]["weeks"]
    assert weeks["minimum"] == 1 and weeks["maximum"] == 52
    start_day = tools["list_activities"].parameters["properties"]["start_day"]
    assert start_day["pattern"] == r"^\d{8}$"
    region = tools["authenticate_coros"].parameters["properties"]["region"]
    assert set(region["enum"]) == {"eu", "us"}
    sync_day = tools["sync_coros_data"].parameters["properties"]["start_day"]
    assert sync_day["pattern"] == r"^(\d{8})?$"


def test_mcp_layer_rejects_malformed_date(reload_server):
    """An ISO date like 2025-03-16 must be rejected before any API call."""
    from fastmcp import Client
    from fastmcp.exceptions import ToolError

    mod = reload_server()

    async def _call():
        async with Client(mod.mcp) as client:
            await client.call_tool(
                "list_activities",
                {"start_day": "2025-03-16", "end_day": "20250320"},
            )

    with pytest.raises(ToolError):
        asyncio.run(_call())
