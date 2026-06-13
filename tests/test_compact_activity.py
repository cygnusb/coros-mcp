"""Tests for `_compact_activity` — the activity-detail response compactor.

Pure dict-shape assertions — no HTTP, no auth, no mocks.
"""

from coros_mcp.server import _compact_activity, _is_empty_value


def test_drops_bulky_top_level_keys():
    data = {
        "summary": {"distance": 5000},
        "userInfo": {"name": "x"},
        "userProfile": {"weight": 70},
        "deviceList": [{"id": 1}],
        "newMessageCount": 3,
        "level": 2,
        "zoneList": [{"zone": 1}],
    }
    out = _compact_activity(data)
    assert set(out) == {"summary", "zoneList"}
    # Original input is not mutated.
    assert "userInfo" in data


def test_strips_empty_and_constant_lap_item_fields():
    data = {
        "lapList": [
            {
                "lapIndex": 0,
                "lapItemList": [
                    {
                        "avgHr": 150,
                        "avgPower": 0,          # zero -> dropped
                        "note": "",             # empty string -> dropped
                        "splits": [],           # empty list -> dropped
                        "isRest": False,        # False -> dropped
                        "missing": None,        # None -> dropped
                        "sportType": 100,       # constant field -> dropped
                        "lapType": 1,           # constant field -> dropped
                        "distance": 1000,
                    }
                ],
            }
        ]
    }
    out = _compact_activity(data)
    item = out["lapList"][0]["lapItemList"][0]
    assert item == {"avgHr": 150, "distance": 1000}
    # Lap-level fields outside lapItemList survive untouched.
    assert out["lapList"][0]["lapIndex"] == 0


def test_keeps_truthy_booleans_and_nonzero_values():
    data = {
        "lapList": [
            {"lapItemList": [{"flagged": True, "cadence": 90, "grade": 0.0}]}
        ]
    }
    item = _compact_activity(data)["lapList"][0]["lapItemList"][0]
    assert item == {"flagged": True, "cadence": 90}  # grade 0.0 dropped


def test_deduplicates_identical_lap_item_lists():
    lap_items = [{"avgHr": 140, "distance": 500}]
    data = {
        "lapList": [
            {"type": 2, "lapItemList": [dict(i) for i in lap_items]},
            {"type": 3, "lapItemList": [dict(i) for i in lap_items]},  # copy
            {"type": 4, "lapItemList": [{"avgHr": 160, "distance": 600}]},
        ]
    }
    out = _compact_activity(data)
    assert len(out["lapList"]) == 2
    assert out["lapList"][0]["type"] == 2
    assert out["lapList"][1]["type"] == 4


def test_handles_missing_lap_list():
    data = {"summary": {"distance": 1}}
    assert _compact_activity(data) == {"summary": {"distance": 1}}


def test_is_empty_value():
    for empty in (None, False, 0, 0.0, "", []):
        assert _is_empty_value(empty)
    for kept in (True, 1, 0.1, "x", [0], {"a": 1}):
        assert not _is_empty_value(kept)
