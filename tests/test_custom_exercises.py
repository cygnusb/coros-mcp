"""Tests for custom strength exercise management (create / update / delete).

HTTP is mocked with httpx.MockTransport: each test installs a handler that
records the requests coros_api sends and answers like the Training Hub API
(see docs/custom-exercise-api.md for the live-tested request shapes).
"""

import asyncio
import json

import httpx
import pytest

from coros_mcp import coros_api
from coros_mcp.coros_api import (
    EXERCISE_EQUIPMENT,
    EXERCISE_MUSCLES,
    EXERCISE_PARTS,
    _apply_custom_exercise_changes,
    _build_custom_exercise_payload,
    _build_strength_program_payload,
    resolve_exercise_codes,
)

US_BASE = "https://teamapi.coros.com"
EU_BASE = "https://teameuapi.coros.com"


def _auth(region="us"):
    return coros_api.StoredAuth(access_token="tok", user_id="u1", region=region, timestamp=0)


BUILTIN = {
    "id": "425827615547506688", "userId": 0, "name": "T1001",
    "overview": "sid_strength_kwando_kickboxing", "part": [0], "muscle": [14, 7],
    "muscleRelevance": [1], "equipment": [1], "sportType": 4, "exerciseType": 2,
    "targetType": 3, "targetValue": 15, "restType": 1, "restValue": 30, "animationId": 46,
}
BUILTIN_WITH_ACCESS = {**BUILTIN, "id": "425827615547506689", "access": 0}
CUSTOM = {
    "id": "100000000000000001", "userId": 400000000000000001, "name": "Monster Walk",
    "overview": "", "part": [5], "muscle": [14, 10], "muscleRelevance": [7, 15],
    "equipment": [9], "sportType": 4, "exerciseType": 2, "access": 1,
    "targetType": 3, "targetValue": 15, "restType": 1, "restValue": 30, "animationId": 0,
}
CUSTOM_DESCRIBED = {**CUSTOM, "id": "100000000000000003", "name": "Copenhagen Plank",
                    "overview": "Top leg on the bench, hold the hips level."}
# Custom exercises created in the app may lack the equipment / muscleRelevance keys.
CUSTOM_SPARSE = {
    "id": "100000000000000002", "userId": 400000000000000001, "name": "Reverse Snow Angels",
    "overview": "", "part": [3], "muscle": [11], "sportType": 4, "exerciseType": 2,
    "targetType": 3, "targetValue": 15, "restType": 1, "restValue": 30,
}


class _FakeCoros:
    """MockTransport handler: serves the exercise catalogue and records writes."""

    def __init__(self, catalogue=None, write_result="0000", write_data=None):
        self.catalogue = [BUILTIN, CUSTOM, CUSTOM_SPARSE] if catalogue is None else catalogue
        self.write_result = write_result
        self.write_data = write_data
        self.requests: list[httpx.Request] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if request.url.path == "/training/exercise/query":
            return httpx.Response(200, json={"result": "0000", "data": self.catalogue})
        if self.write_result != "0000":
            return httpx.Response(200, json={"result": self.write_result, "message": "nope"})
        return httpx.Response(200, json={"result": "0000", "data": self.write_data})

    def writes(self):
        return [r for r in self.requests if r.method == "POST"]


@pytest.fixture
def fake_coros(monkeypatch):
    fake = _FakeCoros()
    real_client = httpx.AsyncClient

    def _client(**kwargs):
        return real_client(transport=httpx.MockTransport(fake), **kwargs)

    monkeypatch.setattr(coros_api.httpx, "AsyncClient", _client)
    return fake


@pytest.fixture(autouse=True)
def clean_catalog_cache():
    coros_api._reset_strength_catalog_cache()
    yield
    coros_api._reset_strength_catalog_cache()


# ---------------------------------------------------------------------------
# Code ↔ label mapping
# ---------------------------------------------------------------------------

def test_resolve_labels_case_insensitive_and_codes():
    assert resolve_exercise_codes(["shoulders", "Back", 4, "6"], EXERCISE_PARTS, "part") == [1, 3, 4, 6]


def test_resolve_dedupes_preserving_order():
    assert resolve_exercise_codes(["Glutes", 14, "abductor"], EXERCISE_MUSCLES, "muscle") == [14, 10]


def test_resolve_scalar_and_none():
    assert resolve_exercise_codes("Dumbbells", EXERCISE_EQUIPMENT, "equipment") == [2]
    assert resolve_exercise_codes(None, EXERCISE_EQUIPMENT, "equipment") == []


@pytest.mark.parametrize("bad", ["Legs", 7, "99", ""])
def test_resolve_unknown_value_lists_options(bad):
    with pytest.raises(ValueError, match=r"unknown part .*valid: 0=Whole Body"):
        resolve_exercise_codes([bad], EXERCISE_PARTS, "part")


def test_whole_body_is_code_zero():
    # Code 0 is falsy — it must still resolve, not be dropped.
    assert resolve_exercise_codes([0], EXERCISE_PARTS, "part") == [0]


# ---------------------------------------------------------------------------
# Create payload
# ---------------------------------------------------------------------------

def test_create_payload_matches_documented_example():
    """docs/custom-exercise-api.md: Seated DB Shoulder Press."""
    payload = _build_custom_exercise_payload(
        "Seated DB Shoulder Press", part=["Shoulders"], muscle=["Deltoids"], equipment=["Dumbbells"]
    )
    assert payload == {
        "sportType": 4, "exerciseType": 2, "name": "Seated DB Shoulder Press", "overview": "",
        "part": [1], "muscle": [1], "muscleRelevance": [], "equipment": [2],
        "access": 1, "intensityCustom": 0, "intensityMultiplier": 0, "intensityType": 1,
        "intensityValue": 0, "intensityValueExtend": 0, "restType": 1, "restValue": 30,
        "targetType": 3, "targetValue": 15,
    }


def test_create_payload_time_target():
    payload = _build_custom_exercise_payload(
        "Wall Sit", part=["Legs/Hips"], target_type=2, target_value=45, rest_seconds=60
    )
    assert (payload["targetType"], payload["targetValue"], payload["restValue"]) == (2, 45, 60)


@pytest.mark.parametrize(
    ("kwargs", "match"),
    [
        ({"name": "  ", "part": [1]}, "requires a name"),
        ({"name": "X", "part": []}, "requires at least one part"),
        ({"name": "X", "part": ["Whole Body"]}, "Whole Body exercise requires at least one muscle"),
        ({"name": "X", "part": [1], "target_type": 1}, "target_type must be"),
        ({"name": "X", "part": [1], "target_value": 0}, "target_value must be positive"),
        ({"name": "X", "part": [1], "rest_seconds": -1}, "rest_seconds must be non-negative"),
    ],
)
def test_create_payload_validation(kwargs, match):
    with pytest.raises(ValueError, match=match):
        _build_custom_exercise_payload(**kwargs)


def test_create_payload_description_goes_to_overview_as_sent():
    text = "Back against the pad.\nLower to ear height."
    payload = _build_custom_exercise_payload("X", part=["Shoulders"], description=text)
    assert payload["overview"] == text


def test_whole_body_with_muscle_is_valid():
    payload = _build_custom_exercise_payload("Burpee", part=["Whole Body"], muscle=["Quadriceps"])
    assert payload["part"] == [0]


# ---------------------------------------------------------------------------
# Update payload
# ---------------------------------------------------------------------------

def test_update_payload_changes_only_given_fields_and_drops_user_id():
    payload = _apply_custom_exercise_changes(CUSTOM, name="Banded Monster Walk", equipment=["Bands", "Others"])
    assert "userId" not in payload
    assert payload["name"] == "Banded Monster Walk"
    assert payload["equipment"] == [9, 16]
    assert payload["id"] == CUSTOM["id"]
    assert payload["muscle"] == CUSTOM["muscle"]
    assert payload["part"] == CUSTOM["part"]
    assert payload["targetValue"] == CUSTOM["targetValue"]


def test_update_payload_empty_list_clears_field():
    assert _apply_custom_exercise_changes(CUSTOM, muscle_relevance=[])["muscleRelevance"] == []


def test_update_payload_fills_missing_arrays():
    payload = _apply_custom_exercise_changes(CUSTOM_SPARSE, name="Snow Angels")
    assert payload["equipment"] == []
    assert payload["muscleRelevance"] == []


def test_update_payload_target_and_rest():
    payload = _apply_custom_exercise_changes(CUSTOM, target_type=2, target_value=40, rest_seconds=45)
    assert (payload["targetType"], payload["targetValue"], payload["restType"], payload["restValue"]) == (
        2, 40, 1, 45,
    )


def test_update_payload_description():
    assert _apply_custom_exercise_changes(CUSTOM, description="Band above the knees.")["overview"] == (
        "Band above the knees."
    )
    assert _apply_custom_exercise_changes(CUSTOM_DESCRIBED, description="")["overview"] == ""
    # Omitted → the current description is kept.
    assert _apply_custom_exercise_changes(CUSTOM_DESCRIBED, name="CP")["overview"] == CUSTOM_DESCRIBED["overview"]


def test_update_payload_refuses_builtin():
    with pytest.raises(ValueError, match="built-in"):
        _apply_custom_exercise_changes(BUILTIN, name="Mine now")


def test_update_payload_revalidates():
    with pytest.raises(ValueError, match="requires at least one part"):
        _apply_custom_exercise_changes(CUSTOM, part=[])


# ---------------------------------------------------------------------------
# HTTP: create / update / delete
# ---------------------------------------------------------------------------

async def test_create_posts_payload_and_returns_new_exercise(fake_coros):
    created = {**CUSTOM, "id": "100000000000000009", "name": "Seated DB Shoulder Press"}
    fake_coros.write_data = created
    coros_api._strength_catalog_cache = {"stale": {}}
    coros_api._strength_catalog_loaded_at = 1e12

    result = await coros_api.create_custom_exercise(
        _auth(), "Seated DB Shoulder Press", ["Shoulders"], ["Deltoids"], ["Dumbbells"]
    )

    (req,) = fake_coros.writes()
    assert str(req.url) == US_BASE + "/training/exercise/add"
    assert req.headers["accessToken"] == "tok"
    assert json.loads(req.headers["yfheader"]) == {"userId": "u1"}
    body = json.loads(req.content)
    assert body["name"] == "Seated DB Shoulder Press"
    assert (body["part"], body["muscle"], body["equipment"]) == ([1], [1], [2])
    assert result == created
    # The cached catalogue would not know the new exercise — it must be dropped.
    assert coros_api._strength_catalog_cache is None


async def test_create_sends_description(fake_coros):
    fake_coros.write_data = CUSTOM_DESCRIBED
    await coros_api.create_custom_exercise(
        _auth(), "Copenhagen Plank", ["Core"], description=CUSTOM_DESCRIBED["overview"]
    )
    body = json.loads(fake_coros.writes()[0].content)
    assert body["overview"] == CUSTOM_DESCRIBED["overview"]


async def test_create_uses_region_base_url(fake_coros):
    fake_coros.write_data = CUSTOM
    await coros_api.create_custom_exercise(_auth("eu"), "X", ["Back"])
    assert str(fake_coros.writes()[0].url) == EU_BASE + "/training/exercise/add"


async def test_create_validation_error_sends_nothing(fake_coros):
    with pytest.raises(ValueError):
        await coros_api.create_custom_exercise(_auth(), "X", ["Whole Body"])
    assert fake_coros.requests == []


async def test_create_api_error_raises(fake_coros):
    fake_coros.write_result = "5001"
    with pytest.raises(coros_api.CorosAPIError, match="exercise create"):
        await coros_api.create_custom_exercise(_auth(), "X", ["Back"])


async def test_update_fetches_current_then_posts_full_object(fake_coros):
    result = await coros_api.update_custom_exercise(_auth(), CUSTOM["id"], name="Lateral Band Walk")

    query, update = fake_coros.requests
    assert query.method == "GET" and query.url.path == "/training/exercise/query"
    assert query.url.params["sportType"] == "4"
    assert str(update.url) == US_BASE + "/training/exercise/update"
    body = json.loads(update.content)
    expected = {k: v for k, v in CUSTOM.items() if k != "userId"}
    assert body == {**expected, "name": "Lateral Band Walk"}
    assert result == {**body, "userId": CUSTOM["userId"]}


async def test_update_sends_description(fake_coros):
    await coros_api.update_custom_exercise(_auth(), CUSTOM["id"], description="Small steps, toes forward.")
    body = json.loads(fake_coros.writes()[0].content)
    assert body["overview"] == "Small steps, toes forward."
    assert body["name"] == CUSTOM["name"]


async def test_update_refuses_builtin_without_writing(fake_coros):
    with pytest.raises(ValueError, match="built-in"):
        await coros_api.update_custom_exercise(_auth(), BUILTIN["id"], name="Mine now")
    assert fake_coros.writes() == []


async def test_update_description_on_builtin_is_refused(fake_coros):
    """A built-in's overview is a sid_ i18n key — never overwrite it."""
    with pytest.raises(ValueError, match="built-in"):
        await coros_api.update_custom_exercise(_auth(), BUILTIN["id"], description="my notes")
    assert fake_coros.writes() == []


async def test_update_unknown_id(fake_coros):
    with pytest.raises(ValueError, match="not found"):
        await coros_api.update_custom_exercise(_auth(), "42", name="X")
    assert fake_coros.writes() == []


async def test_delete_posts_ids_with_user_id(fake_coros):
    deleted = await coros_api.delete_custom_exercises(_auth(), [CUSTOM["id"], CUSTOM_SPARSE["id"]])

    (req,) = fake_coros.writes()
    assert req.url.path == "/training/exercise/delete"
    assert req.url.params["userId"] == "u1"
    assert json.loads(req.content) == [CUSTOM["id"], CUSTOM_SPARSE["id"]]
    assert [e["name"] for e in deleted] == ["Monster Walk", "Reverse Snow Angels"]


async def test_delete_is_all_or_nothing_when_a_builtin_is_included(fake_coros):
    with pytest.raises(ValueError, match="built-in"):
        await coros_api.delete_custom_exercises(_auth(), [CUSTOM["id"], BUILTIN["id"]])
    assert fake_coros.writes() == []


async def test_delete_empty_list_raises(fake_coros):
    with pytest.raises(ValueError, match="must not be empty"):
        await coros_api.delete_custom_exercises(_auth(), [])
    assert fake_coros.requests == []


# ---------------------------------------------------------------------------
# Strength workouts with custom exercises
# ---------------------------------------------------------------------------

def _strength_exercise(**overrides):
    base = {"origin_id": CUSTOM["id"], "target_type": 3, "target_value": 12, "rest_seconds": 45}
    base.update(overrides)
    return base


def test_strength_payload_uses_custom_display_name_and_empty_overview():
    by_id = {CUSTOM["id"]: CUSTOM}
    ex = _build_strength_program_payload("w", [_strength_exercise()], by_id)["exercises"][0]
    assert ex["name"] == "Monster Walk"
    assert ex["overview"] == ""
    assert ex["originId"] == CUSTOM["id"]
    assert (ex["part"], ex["muscle"], ex["equipment"]) == ([5], [14, 10], [9])


def test_strength_payload_carries_custom_description():
    by_id = {CUSTOM_DESCRIBED["id"]: CUSTOM_DESCRIBED}
    ex = _build_strength_program_payload(
        "w", [_strength_exercise(origin_id=CUSTOM_DESCRIBED["id"])], by_id
    )["exercises"][0]
    assert (ex["name"], ex["overview"]) == ("Copenhagen Plank", CUSTOM_DESCRIBED["overview"])


def test_strength_payload_copies_access_from_catalogue():
    by_id = {CUSTOM["id"]: CUSTOM, BUILTIN_WITH_ACCESS["id"]: BUILTIN_WITH_ACCESS}
    exercises = _build_strength_program_payload(
        "w", [_strength_exercise(), _strength_exercise(origin_id=BUILTIN_WITH_ACCESS["id"])], by_id
    )["exercises"]
    assert [ex["access"] for ex in exercises] == [1, 0]


def test_strength_payload_catalogue_miss_omits_access():
    ex = _build_strength_program_payload("w", [_strength_exercise()], {})["exercises"][0]
    assert "access" not in ex


def test_strength_payload_builtin_fallback_from_catalogue():
    by_id = {BUILTIN["id"]: BUILTIN}
    ex = _build_strength_program_payload("w", [_strength_exercise(origin_id=BUILTIN["id"])], by_id)["exercises"][0]
    assert (ex["name"], ex["overview"]) == ("T1001", "sid_strength_kwando_kickboxing")


def test_strength_payload_explicit_name_and_overview_win():
    by_id = {CUSTOM["id"]: CUSTOM}
    ex = _build_strength_program_payload(
        "w", [_strength_exercise(name="Monster Walk", overview="")], by_id
    )["exercises"][0]
    assert (ex["name"], ex["overview"]) == ("Monster Walk", "")


def test_strength_payload_catalogue_miss_keeps_old_defaults():
    ex = _build_strength_program_payload("w", [_strength_exercise()], {})["exercises"][0]
    assert (ex["name"], ex["overview"]) == ("", "sid_strength_training")


async def test_new_custom_exercise_is_visible_to_next_strength_workout(fake_coros):
    """Create → schedule in one session: the catalogue cache must not hide
    the new exercise's name and metadata from the workout payload."""
    await coros_api._load_strength_catalog(_auth())  # warm cache without the new exercise
    new = {**CUSTOM, "id": "100000000000000009", "name": "Seated DB Shoulder Press", "part": [1]}
    fake_coros.write_data = new

    await coros_api.create_custom_exercise(_auth(), "Seated DB Shoulder Press", ["Shoulders"])
    fake_coros.catalogue = [BUILTIN, CUSTOM, new]
    by_id = await coros_api._load_strength_catalog(_auth())

    ex = _build_strength_program_payload("w", [_strength_exercise(origin_id=new["id"])], by_id)["exercises"][0]
    assert (ex["name"], ex["overview"], ex["part"]) == ("Seated DB Shoulder Press", "", [1])


# ---------------------------------------------------------------------------
# MCP tools
# ---------------------------------------------------------------------------

@pytest.fixture
def server(monkeypatch, fake_coros):
    from coros_mcp import server as server_mod

    monkeypatch.setattr(coros_api, "get_stored_auth", lambda: _auth())
    return server_mod


def test_create_tool_returns_id_and_workout_exercise(server, fake_coros):
    fake_coros.write_data = {**CUSTOM, "id": "100000000000000009", "name": "Wall Sit",
                             "targetType": 2, "targetValue": 45, "restValue": 60}
    out = asyncio.run(server.create_custom_exercise(
        name="Wall Sit", part=["Legs/Hips"], muscle=["Quadriceps"],
        target_type="time", target_value=45, rest_seconds=60,
    ))
    assert out["created"] is True
    assert out["exercise"]["exercise_id"] == "100000000000000009"
    assert out["workout_exercise"] == {
        "origin_id": "100000000000000009", "name": "Wall Sit", "overview": "",
        "target_type": 2, "target_value": 45, "rest_seconds": 60,
    }
    body = json.loads(fake_coros.writes()[0].content)
    assert (body["targetType"], body["part"], body["muscle"]) == (2, [5], [7])


def test_create_tool_description(server, fake_coros):
    fake_coros.write_data = CUSTOM_DESCRIBED
    out = asyncio.run(server.create_custom_exercise(
        name="Copenhagen Plank", part=["Core"], description=CUSTOM_DESCRIBED["overview"],
    ))
    assert json.loads(fake_coros.writes()[0].content)["overview"] == CUSTOM_DESCRIBED["overview"]
    assert out["exercise"]["description"] == CUSTOM_DESCRIBED["overview"]
    # The workout entry carries the description along.
    assert out["workout_exercise"]["overview"] == CUSTOM_DESCRIBED["overview"]


def test_create_tool_reports_unknown_label(server, fake_coros):
    out = asyncio.run(server.create_custom_exercise(name="X", part=["Legs"]))
    assert out["created"] is False
    assert "unknown part 'Legs'" in out["error"]
    assert fake_coros.requests == []


def test_update_tool_summary_uses_labels(server, fake_coros):
    out = asyncio.run(server.update_custom_exercise(CUSTOM["id"], equipment=["Kettlebell"], target_type="time"))
    assert out["updated"] is True
    assert out["exercise"]["equipment"] == ["Kettlebell"]
    assert out["exercise"]["muscle"] == ["Glutes", "Abductor"]
    assert out["exercise"]["target_type"] == "time"


def test_update_tool_description(server, fake_coros):
    out = asyncio.run(server.update_custom_exercise(CUSTOM["id"], description="Small steps."))
    assert out["updated"] is True
    assert out["exercise"]["description"] == "Small steps."
    assert out["workout_exercise"]["overview"] == "Small steps."
    assert json.loads(fake_coros.writes()[0].content)["overview"] == "Small steps."


def test_update_tool_omitted_description_is_unchanged(server, fake_coros):
    fake_coros.catalogue = [BUILTIN, CUSTOM_DESCRIBED]
    out = asyncio.run(server.update_custom_exercise(CUSTOM_DESCRIBED["id"], name="CP"))
    assert out["exercise"]["description"] == CUSTOM_DESCRIBED["overview"]


def test_summary_never_reports_builtin_sid_key_as_description(server):
    assert server._exercise_summary(BUILTIN)["description"] == ""


def test_update_tool_refuses_builtin(server, fake_coros):
    out = asyncio.run(server.update_custom_exercise(BUILTIN["id"], name="Mine now"))
    assert out["updated"] is False
    assert "built-in" in out["error"]
    assert fake_coros.writes() == []


def test_delete_tool(server, fake_coros):
    out = asyncio.run(server.delete_custom_exercise([CUSTOM["id"]]))
    assert out == {"deleted": True, "exercises": [{"exercise_id": CUSTOM["id"], "name": "Monster Walk"}]}


def test_delete_tool_does_not_retry_after_non_auth_error(server, fake_coros, monkeypatch):
    """A blind retry after a lost response would re-check ids that are
    already gone and report a misleading 'not found'."""
    fake_coros.write_result = "5001"
    calls = 0

    async def _login():
        nonlocal calls
        calls += 1
        return _auth()

    monkeypatch.setattr(coros_api, "try_auto_login", _login)
    out = asyncio.run(server.delete_custom_exercise([CUSTOM["id"]]))
    assert out["deleted"] is False
    assert calls == 0
    assert len(fake_coros.writes()) == 1


def test_delete_tool_description_requires_confirmation(server):
    tools = {t.name: t for t in asyncio.run(server.mcp.list_tools())}
    assert "confirmation" in tools["delete_custom_exercise"].description
    assert tools["delete_custom_exercise"].annotations.destructiveHint is True


def test_list_exercises_custom_only(server):
    out = asyncio.run(server.list_exercises(custom_only=True))
    assert [e["name"] for e in out["exercises"]] == ["Monster Walk", "Reverse Snow Angels"]
    assert out["count"] == 2
    assert asyncio.run(server.list_exercises())["count"] == 3
