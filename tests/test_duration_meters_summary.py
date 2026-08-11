"""Tests for distance-based step support in _summarize_steps and the new
mixed-duration warning (review feedback on PR #52).

Covers:
  1. _summarize_steps: time-only steps (existing behavior preserved)
  2. _summarize_steps: distance-only steps, including repeat groups
  3. _summarize_steps: steps_count unaffected by the new return value
  4. _has_mixed_durations: pure time / pure distance / actually mixed
  5. _attach_mixed_duration_warning: adds, no-ops, and appends-not-overwrites
"""

from coros_mcp.server import (
    _MIXED_DURATION_WARNING,
    _attach_mixed_duration_warning,
    _has_mixed_durations,
    _summarize_steps,
)

# ---------------------------------------------------------------------------
# 1. Time-only steps - existing behavior preserved
# ---------------------------------------------------------------------------


def test_time_only_steps():
    steps = [
        {"name": "Warmup", "duration_minutes": 10},
        {"name": "Main", "duration_minutes": 30},
    ]
    total_minutes, distance_meters_total, steps_count = _summarize_steps(steps)
    assert total_minutes == 40
    assert distance_meters_total == 0
    assert steps_count == 2


def test_time_only_repeat_group():
    steps = [
        {
            "repeat": 4,
            "steps": [
                {"name": "On", "duration_minutes": 5},
                {"name": "Off", "duration_minutes": 2},
            ],
        }
    ]
    total_minutes, distance_meters_total, steps_count = _summarize_steps(steps)
    assert total_minutes == 28  # (5 + 2) * 4
    assert distance_meters_total == 0
    assert steps_count == 3  # 1 header + 2 subs, not multiplied by repeat


# ---------------------------------------------------------------------------
# 2. Distance-only steps
# ---------------------------------------------------------------------------


def test_distance_only_steps():
    steps = [
        {"name": "Warmup", "duration_meters": 1000},
        {"name": "Main", "duration_meters": 5000},
    ]
    total_minutes, distance_meters_total, steps_count = _summarize_steps(steps)
    assert total_minutes == 0
    assert distance_meters_total == 6000
    assert steps_count == 2


def test_distance_repeat_group_applies_multiplier():
    steps = [
        {
            "repeat": 3,
            "steps": [
                {"name": "On", "duration_meters": 400},
                {"name": "Off", "duration_meters": 200},
            ],
        }
    ]
    total_minutes, distance_meters_total, steps_count = _summarize_steps(steps)
    assert total_minutes == 0
    assert distance_meters_total == 1800  # (400 + 200) * 3
    assert steps_count == 3


# ---------------------------------------------------------------------------
# 3. Mixed time + distance
# ---------------------------------------------------------------------------


def test_mixed_steps_both_totals_populated():
    steps = [
        {"name": "Warmup", "duration_minutes": 10},
        {"name": "Interval", "duration_meters": 1000},
    ]
    total_minutes, distance_meters_total, steps_count = _summarize_steps(steps)
    assert total_minutes == 10
    assert distance_meters_total == 1000
    assert steps_count == 2


# ---------------------------------------------------------------------------
# 4. _has_mixed_durations
# ---------------------------------------------------------------------------


def test_has_mixed_durations_false_when_time_only():
    steps = [{"duration_minutes": 10}, {"duration_minutes": 5}]
    assert _has_mixed_durations(steps) is False


def test_has_mixed_durations_false_when_distance_only():
    steps = [{"duration_meters": 1000}, {"duration_meters": 2000}]
    assert _has_mixed_durations(steps) is False


def test_has_mixed_durations_true_when_mixed():
    steps = [{"duration_minutes": 10}, {"duration_meters": 1000}]
    assert _has_mixed_durations(steps) is True


def test_has_mixed_durations_checks_inside_repeat_groups():
    steps = [
        {
            "repeat": 2,
            "steps": [
                {"duration_minutes": 5},
                {"duration_meters": 400},
            ],
        }
    ]
    assert _has_mixed_durations(steps) is True


# ---------------------------------------------------------------------------
# 5. _attach_mixed_duration_warning
# ---------------------------------------------------------------------------


def test_attach_mixed_duration_warning_adds_when_mixed():
    steps = [{"duration_minutes": 10}, {"duration_meters": 1000}]
    result = _attach_mixed_duration_warning({"scheduled": True}, steps)
    assert result["warning"] == _MIXED_DURATION_WARNING


def test_attach_mixed_duration_warning_noop_when_not_mixed():
    steps = [{"duration_minutes": 10}, {"duration_minutes": 5}]
    result = _attach_mixed_duration_warning({"scheduled": True}, steps)
    assert "warning" not in result


def test_attach_mixed_duration_warning_appends_not_overwrites():
    steps = [{"duration_minutes": 10}, {"duration_meters": 1000}]
    result = _attach_mixed_duration_warning({"scheduled": True, "warning": "enrichment failed"}, steps)
    assert result["warning"] == "enrichment failed " + _MIXED_DURATION_WARNING
