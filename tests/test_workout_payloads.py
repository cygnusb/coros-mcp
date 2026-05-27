"""Tests for the strength-workout payload builder.

These pin the reverse-engineered encoding rules in
`_build_strength_program_payload` so they survive future tweaks.
Pure JSON-shape assertions — no HTTP, no auth, no mocks.
"""

import asyncio
from unittest.mock import AsyncMock

import httpx
import pytest

import coros_api
from coros_api import (
    _build_strength_program_payload,
    _build_workout_program_payload,
    _load_strength_catalog,
    _reset_strength_catalog_cache,
)


def _exercise(**overrides):
    """Minimal exercise dict — only the keys the builder reads."""
    base = {
        "origin_id": "0",
        "name": "T0000",
        "overview": "sid_strength_test",
        "target_type": 3,
        "target_value": 10,
        "rest_seconds": 60,
    }
    base.update(overrides)
    return base


def _build(exercises=None, by_id=None, sets=1):
    if exercises is None:
        exercises = [_exercise()]
    if by_id is None:
        by_id = {}
    return _build_strength_program_payload(
        name="test workout",
        exercises=exercises,
        by_id=by_id,
        sets=sets,
    )


# ---------------------------------------------------------------------------
# Weight encoding — bodyweight / kg / lbs
# ---------------------------------------------------------------------------

def test_bodyweight_omits_both():
    payload = _build([_exercise()])
    ex = payload["exercises"][0]
    assert ex["intensityValue"] == ""
    assert ex["intensityCustom"] == 1
    assert ex["intensityDisplayUnit"] == "6"


def test_weight_kg():
    payload = _build([_exercise(weight_kg=27.9)])
    ex = payload["exercises"][0]
    assert ex["intensityValue"] == 27900
    assert ex["intensityPercent"] == 0
    assert ex["intensityDisplayUnit"] == "6"
    assert ex["intensityCustom"] == 0
    assert ex["isIntensityPercent"] is False


def test_weight_kg_zero_renders_zero_kg():
    """weight_kg=0 explicitly is NOT bodyweight."""
    payload = _build([_exercise(weight_kg=0)])
    ex = payload["exercises"][0]
    assert ex["intensityValue"] == 0
    assert ex["intensityCustom"] == 0
    assert ex["intensityDisplayUnit"] == "6"


def test_weight_lbs():
    payload = _build([_exercise(weight_lbs=45)])
    ex = payload["exercises"][0]
    # 45 * 0.45359237 * 1000 = 20411.65665 → 20412
    assert ex["intensityValue"] == 20412
    assert ex["intensityPercent"] == 45_000_000
    assert ex["intensityDisplayUnit"] == "7"
    assert ex["intensityCustom"] == 0


def test_weight_kg_and_lbs_raises():
    with pytest.raises(ValueError):
        _build([_exercise(weight_kg=10, weight_lbs=22)])


def test_negative_weight_kg_raises():
    with pytest.raises(ValueError):
        _build([_exercise(weight_kg=-1)])


def test_negative_weight_lbs_raises():
    with pytest.raises(ValueError):
        _build([_exercise(weight_lbs=-1)])


# ---------------------------------------------------------------------------
# Rest encoding — Skip rests vs MM:SS
# ---------------------------------------------------------------------------

def test_skip_rests_when_zero():
    payload = _build([_exercise(rest_seconds=0)])
    ex = payload["exercises"][0]
    assert ex["restType"] == 3
    assert ex["restValue"] == 0


def test_rest_seconds_positive():
    payload = _build([_exercise(rest_seconds=90)])
    ex = payload["exercises"][0]
    assert ex["restType"] == 1
    assert ex["restValue"] == 90


# ---------------------------------------------------------------------------
# Per-exercise sets vs circuit sets
# ---------------------------------------------------------------------------

def test_per_exercise_sets():
    payload = _build([_exercise(sets=3)], sets=1)
    assert payload["exercises"][0]["sets"] == 3


# ---------------------------------------------------------------------------
# Regression-pinned constants (commit cf2cec4, payload contract)
# ---------------------------------------------------------------------------

def test_status_one_on_every_exercise():
    """Restored 2026-05-21 (commit cf2cec4) — API may treat missing as
    disabled in the future."""
    payload = _build([
        _exercise(name="A"),
        _exercise(name="B", weight_kg=10),
        _exercise(name="C", weight_lbs=20),
    ])
    for ex in payload["exercises"]:
        assert ex["status"] == 1


def test_sport_type_4_program_and_exercise():
    payload = _build([_exercise(), _exercise()])
    assert payload["sportType"] == 4
    for ex in payload["exercises"]:
        assert ex["sportType"] == 4


def test_exercise_num_and_total_sets():
    payload = _build([_exercise(), _exercise(), _exercise()], sets=2)
    assert payload["exerciseNum"] == 3
    assert payload["totalSets"] == 2
    assert payload["sets"] == 2


def test_intensity_type_one_for_strength():
    payload = _build([_exercise(), _exercise(weight_kg=10)])
    for ex in payload["exercises"]:
        assert ex["intensityType"] == 1


# ---------------------------------------------------------------------------
# Duration math
# ---------------------------------------------------------------------------

def test_duration_per_exercise_sets():
    """1 exercise, time target 30s + 10s rest, per-ex sets=3, circuit sets=1."""
    payload = _build(
        [_exercise(target_type=2, target_value=30, rest_seconds=10, sets=3)],
        sets=1,
    )
    assert payload["duration"] == (30 + 10) * 3


def test_duration_circuit_sets():
    """1 exercise, time target 30s + 10s rest, per-ex sets=1, circuit sets=3."""
    payload = _build(
        [_exercise(target_type=2, target_value=30, rest_seconds=10)],
        sets=3,
    )
    assert payload["duration"] == (30 + 10) * 1 * 3


def test_duration_reps_target_excludes_value():
    """For target_type=3 (reps), only rest counts toward duration."""
    payload = _build(
        [_exercise(target_type=3, target_value=12, rest_seconds=60)],
        sets=1,
    )
    assert payload["duration"] == 60


# ---------------------------------------------------------------------------
# Catalog enrichment (Training Machines / Training Parts diagrams)
# ---------------------------------------------------------------------------

def test_catalog_metadata_propagates_when_present():
    by_id = {
        "T1061": {
            "id": "T1061",
            "muscle": ["quads", "glutes"],
            "muscleRelevance": [1.0, 0.8],
            "part": ["legs"],
            "equipment": [3],
            "animationId": 42,
        }
    }
    payload = _build([_exercise(origin_id="T1061")], by_id=by_id)
    ex = payload["exercises"][0]
    assert ex["muscle"] == ["quads", "glutes"]
    assert ex["muscleRelevance"] == [1.0, 0.8]
    assert ex["part"] == ["legs"]
    assert ex["equipment"] == [3]
    assert ex["animationId"] == 42


def test_catalog_miss_gives_empty_lists():
    """Resilience per commit b1c8328 — workout still creates, only
    diagram metadata is lost."""
    payload = _build([_exercise(origin_id="T9999")], by_id={})
    ex = payload["exercises"][0]
    assert ex["muscle"] == []
    assert ex["muscleRelevance"] == []
    assert ex["part"] == []
    assert ex["equipment"] == []
    assert ex["animationId"] == 0


# ---------------------------------------------------------------------------
# Empty input
# ---------------------------------------------------------------------------

def test_empty_exercises_raises():
    with pytest.raises(ValueError):
        _build(exercises=[])


# ---------------------------------------------------------------------------
# Cycling/intervals builder (_build_workout_program_payload)
# ---------------------------------------------------------------------------

def test_cycling_plain_steps_total_seconds():
    payload = _build_workout_program_payload(
        name="Z2",
        steps=[
            {"name": "Warmup", "duration_minutes": 10, "intensity_low": 150, "intensity_high": 200},
            {"name": "Main",   "duration_minutes": 30, "intensity_low": 200, "intensity_high": 240},
        ],
    )
    assert payload["estimatedTime"] == (10 + 30) * 60
    assert payload["name"] == "Z2"
    assert payload["sportType"] == 2
    assert payload["access"] == 1
    assert len(payload["exercises"]) == 2


def test_cycling_repeat_group_expands_total():
    """Repeat group: iteration_seconds * repeat is added to estimatedTime,
    and the group header + sub-steps are all emitted (1 header + N subs)."""
    payload = _build_workout_program_payload(
        name="3x10",
        steps=[
            {"name": "Warmup", "duration_minutes": 10, "intensity_low": 150, "intensity_high": 200},
            {"repeat": 3, "steps": [
                {"name": "On",  "duration_minutes": 10, "intensity_low": 265, "intensity_high": 285},
                {"name": "Off", "duration_minutes": 3,  "intensity_low": 150, "intensity_high": 175},
            ]},
        ],
    )
    # 10 + 3*(10+3) = 49 min
    assert payload["estimatedTime"] == (10 + 3 * (10 + 3)) * 60
    # 1 warmup + 1 group header + 2 sub-steps = 4 exercises
    assert len(payload["exercises"]) == 4


def test_cycling_repeat_group_links_subs_to_header():
    """Sub-steps reference the group header via groupId; header has isGroup=True."""
    payload = _build_workout_program_payload(
        name="2x5",
        steps=[
            {"repeat": 2, "steps": [
                {"name": "On",  "duration_minutes": 5, "intensity_low": 200, "intensity_high": 230},
                {"name": "Off", "duration_minutes": 2, "intensity_low": 150, "intensity_high": 175},
            ]},
        ],
    )
    header, sub1, sub2 = payload["exercises"]
    assert header["isGroup"] is True
    assert header["sets"] == 2
    assert sub1["isGroup"] is False
    assert sub1["groupId"] == str(header["id"])
    assert sub2["groupId"] == str(header["id"])


def test_cycling_power_legacy_aliases():
    """power_low_w / power_high_w are accepted as legacy aliases."""
    payload = _build_workout_program_payload(
        name="legacy",
        steps=[
            {"name": "Step", "duration_minutes": 5, "power_low_w": 200, "power_high_w": 240},
        ],
    )
    ex = payload["exercises"][0]
    assert ex["intensityValue"] == 200
    assert ex["intensityValueExtend"] == 240


def test_cycling_empty_steps_raises():
    with pytest.raises(ValueError):
        _build_workout_program_payload(name="empty", steps=[])


def test_cycling_sport_and_intensity_types_propagate():
    payload = _build_workout_program_payload(
        name="hr",
        steps=[{"name": "S", "duration_minutes": 5, "intensity_low": 140, "intensity_high": 160}],
        sport_type=200,
        intensity_type=2,
    )
    assert payload["sportType"] == 200
    for ex in payload["exercises"]:
        assert ex["sportType"] == 200
    # Non-group steps use the caller-provided intensity_type
    assert payload["exercises"][0]["intensityType"] == 2


# ---------------------------------------------------------------------------
# Strength catalog cache (_load_strength_catalog)
# ---------------------------------------------------------------------------

_SAMPLE_CATALOG = [
    {"id": "T1010", "muscle": ["abs"], "part": ["core"], "equipment": [1]},
    {"id": "T1052", "muscle": ["lats"], "part": ["back"], "equipment": [5]},
    {"id": "T1120", "muscle": [], "part": [], "equipment": []},
]


@pytest.fixture
def clean_catalog_cache():
    """Reset the module-level cache before and after each test."""
    _reset_strength_catalog_cache()
    yield
    _reset_strength_catalog_cache()


async def test_catalog_cache_first_call_fetches(clean_catalog_cache, monkeypatch):
    mock = AsyncMock(return_value=_SAMPLE_CATALOG)
    monkeypatch.setattr(coros_api, "fetch_exercises", mock)

    result = await _load_strength_catalog(auth=None)  # auth ignored by the mock

    assert mock.await_count == 1
    assert set(result.keys()) == {"T1010", "T1052", "T1120"}
    assert result["T1052"]["muscle"] == ["lats"]


async def test_catalog_cache_second_call_within_ttl_uses_cache(clean_catalog_cache, monkeypatch):
    mock = AsyncMock(return_value=_SAMPLE_CATALOG)
    monkeypatch.setattr(coros_api, "fetch_exercises", mock)

    await _load_strength_catalog(auth=None)
    await _load_strength_catalog(auth=None)

    assert mock.await_count == 1


async def test_catalog_cache_refetches_after_ttl(clean_catalog_cache, monkeypatch):
    mock = AsyncMock(return_value=_SAMPLE_CATALOG)
    monkeypatch.setattr(coros_api, "fetch_exercises", mock)

    # First call populates cache at t=0
    fake_now = [0.0]
    monkeypatch.setattr(coros_api.time, "monotonic", lambda: fake_now[0])
    await _load_strength_catalog(auth=None)
    assert mock.await_count == 1

    # Jump past TTL (1h) — next call must refetch
    fake_now[0] = coros_api._STRENGTH_CATALOG_TTL_SECONDS + 1
    await _load_strength_catalog(auth=None)
    assert mock.await_count == 2


async def test_catalog_cache_httperror_returns_empty_does_not_poison(clean_catalog_cache, monkeypatch):
    mock = AsyncMock(side_effect=httpx.ConnectError("boom"))
    monkeypatch.setattr(coros_api, "fetch_exercises", mock)

    result = await _load_strength_catalog(auth=None)
    assert result == {}
    assert coros_api._strength_catalog_cache is None

    # Next call retries because cache is still unset.
    mock.side_effect = None
    mock.return_value = _SAMPLE_CATALOG
    result = await _load_strength_catalog(auth=None)
    assert set(result.keys()) == {"T1010", "T1052", "T1120"}
    assert mock.await_count == 2


class _CountingLock(asyncio.Lock):
    """asyncio.Lock that publicly counts how many tasks are currently
    waiting on (or holding) acquire(). Used by the concurrent-coalesce
    test to deterministically wait until N tasks are queued — without
    poking at asyncio.Lock._waiters."""

    def __init__(self) -> None:
        super().__init__()
        self.in_flight = 0

    async def acquire(self) -> bool:
        self.in_flight += 1
        try:
            return await super().acquire()
        except BaseException:
            self.in_flight -= 1
            raise

    def release(self) -> None:
        super().release()
        self.in_flight -= 1


async def test_catalog_cache_concurrent_calls_coalesce(clean_catalog_cache, monkeypatch):
    """Five gathered calls should trigger exactly one fetch_exercises invocation.

    Uses a _CountingLock substitute so gated_fetch can deterministically
    wait until all five tasks are queued on the cache lock before the fetch
    returns — without this gate the first task could finish before peers
    arrive, hiding regressions in the in-lock re-check branch.
    """
    call_count = 0
    release_fetch = asyncio.Event()
    counting_lock = _CountingLock()
    monkeypatch.setattr(coros_api, "_strength_catalog_lock", counting_lock)

    async def gated_fetch(_auth, _sport_type):
        nonlocal call_count
        call_count += 1
        # Spin until all 5 tasks have entered _strength_catalog_lock.acquire
        # (one holds it via us, four are queued).
        while counting_lock.in_flight < 5:
            await asyncio.sleep(0)
        await release_fetch.wait()
        return _SAMPLE_CATALOG

    monkeypatch.setattr(coros_api, "fetch_exercises", gated_fetch)

    # Start five concurrent calls; they all enter _load_strength_catalog
    # and contend for the lock. The first acquires it and stalls inside
    # gated_fetch waiting on release_fetch.
    # asyncio.timeout(5) guards against regressions where _load_strength_catalog
    # stops contending on the lock (cache short-circuit, etc.) — without it
    # the spin in gated_fetch would hang pytest forever.
    async with asyncio.timeout(5):
        tasks = [asyncio.create_task(_load_strength_catalog(auth=None)) for _ in range(5)]
        # No external sync needed: gated_fetch only proceeds once in_flight == 5.
        release_fetch.set()
        results = await asyncio.gather(*tasks)

    assert call_count == 1
    for r in results:
        assert set(r.keys()) == {"T1010", "T1052", "T1120"}
