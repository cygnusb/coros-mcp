"""Tests for the optional program-level `overview` (the workout's own description).

`overview` on the four workout tools is the program-level field the COROS app
shows as the workout's description, not the per-exercise sid_ key. These pin:
the payload is unchanged when it is omitted; the text arrives verbatim, newlines
kept; each tool passes it through to the API layer; and the stripped schedule
listing returns it as-is.
"""

import pytest

from coros_mcp import coros_api, server
from coros_mcp.coros_api import (
    _build_strength_program_payload,
    _build_workout_program_payload,
    _strip_schedule,
)

TEXT = "Aim: easy aerobic\n+ 6 strides\n+ mobility 10'"

STEPS = [{"name": "Run", "duration_minutes": 30, "intensity_low": 120, "intensity_high": 140}]
EXERCISE = {
    "origin_id": "0",
    "name": "T0000",
    "overview": "sid_strength_test",
    "target_type": 3,
    "target_value": 10,
    "rest_seconds": 60,
}


def _stored():
    return coros_api.StoredAuth(
        access_token="tok", user_id="u1", region="eu",
        timestamp=0, mobile_access_token=None, mobile_login_payload=None,
    )


# ---------------------------------------------------------------------------
# Payload builders
# ---------------------------------------------------------------------------

def test_cycling_without_overview_has_no_key():
    # Today's cycling payload carries no overview key; omitting it must not add one.
    assert "overview" not in _build_workout_program_payload("w", STEPS, sport_type=2)


def test_cycling_with_overview():
    payload = _build_workout_program_payload("w", STEPS, sport_type=2, overview=TEXT)
    assert payload["overview"] == TEXT


def test_running_without_overview_stays_empty():
    assert _build_workout_program_payload("w", STEPS, sport_type=100)["overview"] == ""


def test_running_with_overview_keeps_newlines():
    payload = _build_workout_program_payload("w", STEPS, sport_type=100, overview=TEXT)
    assert payload["overview"] == TEXT
    assert payload["overview"].count("\n") == 2


def test_strength_without_overview_stays_empty():
    assert _build_strength_program_payload("s", [EXERCISE], {})["overview"] == ""


def test_strength_overview_is_program_level_only():
    payload = _build_strength_program_payload("s", [EXERCISE], {}, overview=TEXT)
    assert payload["overview"] == TEXT
    # The per-exercise sid_ key is untouched.
    assert payload["exercises"][0]["overview"] == "sid_strength_test"


# ---------------------------------------------------------------------------
# API layer: the scheduled program carries the text
# ---------------------------------------------------------------------------

@pytest.fixture
def captured_schedule(monkeypatch):
    seen = {}

    async def fake_post(auth, program, happen_day, sort_no):
        seen["program"] = program
        return {"ok": True}

    async def fake_catalog(auth):
        return {}

    monkeypatch.setattr(coros_api, "_post_schedule_inline", fake_post)
    monkeypatch.setattr(coros_api, "_load_strength_catalog", fake_catalog)
    return seen


async def test_schedule_workout_sends_overview(captured_schedule):
    await coros_api.schedule_workout(_stored(), "w", STEPS, "20261124", sport_type=100, overview=TEXT)
    assert captured_schedule["program"]["overview"] == TEXT


async def test_schedule_strength_workout_sends_overview(captured_schedule):
    await coros_api.schedule_strength_workout(_stored(), "s", [EXERCISE], "20261124", overview=TEXT)
    assert captured_schedule["program"]["overview"] == TEXT


# ---------------------------------------------------------------------------
# MCP tools: each passes overview through to the API layer (and in the right slot)
# ---------------------------------------------------------------------------

@pytest.fixture
def tool_calls(monkeypatch):
    calls = {}

    def recorder(name, result):
        async def fake(auth, *args, **kwargs):
            calls[name] = (args, kwargs)
            return result
        return fake

    response = {"plan_id": "p", "id_in_plan": "i", "plan_program_id": "pp",
                "entity_id": "e", "enrichment_ok": True}
    monkeypatch.setattr(coros_api, "get_stored_auth", lambda: _stored())
    monkeypatch.setattr(coros_api, "save_workout_template", recorder("save_workout_template", "id1"))
    monkeypatch.setattr(coros_api, "schedule_workout", recorder("schedule_workout", response))
    monkeypatch.setattr(coros_api, "save_strength_workout_template",
                        recorder("save_strength_workout_template", "id2"))
    monkeypatch.setattr(coros_api, "schedule_strength_workout",
                        recorder("schedule_strength_workout", response))
    return calls


def _overview_arg(call):
    args, kwargs = call
    return kwargs.get("overview", args[-1] if args else None)


async def test_tool_save_workout_template_passes_overview(tool_calls):
    result = await server.save_workout_template("w", STEPS, overview=TEXT)
    assert "error" not in result
    assert _overview_arg(tool_calls["save_workout_template"]) == TEXT


async def test_tool_schedule_workout_passes_overview(tool_calls):
    result = await server.schedule_workout("w", STEPS, "20261124", sort_no=2, overview=TEXT)
    assert "error" not in result
    args, _ = tool_calls["schedule_workout"]
    assert _overview_arg(tool_calls["schedule_workout"]) == TEXT
    assert 2 in args  # sort_no still reaches the API in its own slot


async def test_tool_schedule_strength_workout_passes_overview(tool_calls):
    result = await server.schedule_strength_workout("s", [EXERCISE], "20261124", overview=TEXT)
    assert "error" not in result
    assert _overview_arg(tool_calls["schedule_strength_workout"]) == TEXT


async def test_tool_save_strength_workout_template_passes_overview(tool_calls):
    result = await server.save_strength_workout_template("s", [EXERCISE], overview=TEXT)
    assert "error" not in result
    assert _overview_arg(tool_calls["save_strength_workout_template"]) == TEXT


async def test_tools_default_to_empty_overview(tool_calls):
    await server.schedule_workout("w", STEPS, "20261124")
    assert _overview_arg(tool_calls["schedule_workout"]) == ""


# ---------------------------------------------------------------------------
# list_planned_activities (stripped): program overview returned verbatim
# ---------------------------------------------------------------------------

def test_stripped_schedule_keeps_program_overview():
    data = {"programs": [{"name": "w", "overview": TEXT,
                          "exercises": [{"overview": "sid_strength_squats"}]}]}
    program = _strip_schedule(data)["programs"][0]
    assert program["overview"] == TEXT
    # Per-exercise sid_ keys are still made readable, as before.
    assert program["exercises"][0]["overview"] == "Squats"
