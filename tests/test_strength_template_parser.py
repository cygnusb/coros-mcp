"""Tests for the strength read path (GH #60).

`_parse_workout` only ever knew the endurance targetType vocabulary and sent
everything else to `duration_seconds`, so a 3x12 squat read back out of the
library as "12 seconds" and 27.9 kg read back as `intensity_low: 27900`.
Strength now has its own parser; `fetch_workout_templates` dispatches on
sportType=4.

The fixtures below are the WIRE shapes of a real strength template built in
the Coros app and read back from /training/program/list on 2026-09-09: a 4x12
decline dumbbell bench press at 31 kg, a 4x6 greatest stretch at bodyweight,
the same press entered in pounds, a 3x60s burpee with an effort target, and
4x10 bicycle crunches with rests skipped.

That capture corrected three guesses taken from the write side:

  - a real lbs exercise comes back with `intensityPercent: 0` -- only
    templates this server wrote carry the typed pounds there, so the pound
    value has to be recovered from the kg equivalent in `intensityValue`;
  - `intensityCustom: 1` is the bodyweight marker in the app's data too, not
    just this server's. An exercise left at the app's default reads
    `intensityValue: 0, intensityCustom: 0, intensityDisplayUnit: 0` and is a
    real **0.0 kg**, not bodyweight -- verified by flipping one exercise from
    that default to Bodyweight in the app and diffing the re-read, where
    `intensityCustom` 0 -> 1 was the entire change;
  - `intensityDisplayUnit: 0` carrying a non-zero value is not a weight at
    all. It is the app's effort target on a 1-10 RPE scale (the burpee shows
    "Target 5, Moderate" for `intensityValue: 5`), stored unscaled.

The round-trip test is the load-bearing one for the write side: every value
the write side encodes, the read side must decode back to what was typed.
Note what it does NOT assert -- that the two speak the same NAMES. They do on
the load axis (weight_kg/weight_lbs/rest_seconds/sets/origin_id) and do not on
the target axis, where the write side takes target_type/target_value and the
read side returns reps/duration_seconds. That gap is deliberate and left to a
follow-up; see the GH #60 PR description.

Pure dict-shape assertions -- no HTTP, no auth, no mocks.
"""

import pytest

from coros_mcp.coros_api import (
    _build_strength_program_payload,
    _parse_strength_workout,
    _parse_workout,
)


def _exercise(**overrides):
    """Minimal WIRE-shaped strength exercise -- what the API hands back.

    Modelled on the 2026-09-09 capture, including that the API returns
    `intensityDisplayUnit` as an int (6) where the write side sends a
    string ("6").
    """
    base = {
        "name": "T1061",
        "originId": "1061",
        "overview": "sid_strength_squats",
        "targetType": 3,
        "targetValue": 12,
        "sets": 3,
        "restType": 1,
        "restValue": 60,
        "intensityValue": 27900,
        "intensityCustom": 0,
        "intensityDisplayUnit": 6,
        "intensityPercent": 0,
    }
    base.update(overrides)
    return base


def _item(*exercises, **overrides):
    base = {
        "id": 900,
        "name": "strength_decode_ref",
        "sportType": 4,
        "exerciseNum": len(exercises),
        "duration": 600,
        "sets": 1,
        "exercises": list(exercises),
    }
    base.update(overrides)
    return base


def _parse_one(**overrides):
    return _parse_strength_workout(_item(_exercise(**overrides)))["exercises"][0]


# ---------------------------------------------------------------------------
# targetType: reps vs seconds vs unknown
# ---------------------------------------------------------------------------

def test_reps_are_reps_not_seconds():
    """The bug in GH #60: targetType=3 is reps in the strength namespace, and
    the endurance parser reported it as duration_seconds -- so a 3x12 squat
    read back as "3 sets of 12 seconds"."""
    ex = _parse_one(targetType=3, targetValue=12)
    assert ex["reps"] == 12
    assert "duration_seconds" not in ex
    assert ex["sets"] == 3


def test_time_based_strength_exercise_keeps_duration_seconds():
    """targetType=2 is seconds in BOTH namespaces (a plank, say) -- correct
    already, so it keeps its name."""
    ex = _parse_one(targetType=2, targetValue=60)
    assert ex["duration_seconds"] == 60
    assert "reps" not in ex


def test_unknown_target_type_is_surfaced_raw_not_guessed():
    """An unknown targetType must be visibly unparsed rather than silently
    labelled seconds -- that catch-all guess is what produced #59 and #60."""
    ex = _parse_one(targetType=99, targetValue=7)
    assert ex["target_type_raw"] == 99
    assert ex["target_value_raw"] == 7
    assert "duration_seconds" not in ex
    assert "reps" not in ex


def test_endurance_parser_also_stops_guessing():
    """Same fix on the endurance side: only targetType=2 is seconds there."""
    parsed = _parse_workout({"sportType": 1, "exercises": [
        {"name": "???", "targetType": 42, "targetValue": 500},
    ]})["exercises"][0]
    assert parsed["target_type_raw"] == 42
    assert parsed["target_value_raw"] == 500
    assert "duration_seconds" not in parsed


# ---------------------------------------------------------------------------
# Weight -- kg / lbs / bodyweight / explicit zero
# ---------------------------------------------------------------------------

def test_kg_weight_is_unscaled():
    """intensityValue is kg x 1000 (27900 -> 27.9), the value that used to
    surface raw as intensity_low."""
    ex = _parse_one(intensityValue=27900, intensityDisplayUnit="6", intensityCustom=0)
    assert ex["weight_kg"] == 27.9
    assert "weight_lbs" not in ex
    assert "bodyweight" not in ex


def test_lbs_from_a_real_app_created_exercise():
    """The exact wire shape of the pounds exercise in the 2026-09-09 capture:
    displayUnit 7 (int), the kg equivalent in intensityValue, and
    **intensityPercent: 0**.

    The first draft of this parser read pounds out of intensityPercent, which
    is only populated on templates this server wrote -- against real app data
    it returned 0 lbs. The pound value has to be recovered from the kg
    equivalent (19051 -> 19.051 kg -> 42 lbs), and rounded, or the athlete
    gets 42.0002.
    """
    ex = _parse_one(
        intensityValue=19051,
        intensityPercent=0,
        intensityDisplayUnit=7,
        intensityCustom=0,
    )
    assert ex["weight_lbs"] == 42
    assert "weight_kg" not in ex


def test_lbs_from_a_template_this_server_wrote():
    """Our own writer does populate intensityPercent (lbs x 1e6) and sends
    displayUnit as a string -- both shapes must decode."""
    ex = _parse_one(
        intensityValue=20411,
        intensityPercent=45_000_000,
        intensityDisplayUnit="7",
        intensityCustom=0,
    )
    assert ex["weight_lbs"] == 45
    assert "weight_kg" not in ex


def test_bodyweight_app_shape():
    """App-created bodyweight: intensityCustom=1 with a ZERO value (this
    server writes "" instead) and the unit left at 6.

    The marker is the only thing separating this from a 0.0 kg exercise --
    see the test below, which is the same row before it was switched to
    Bodyweight in the app.
    """
    ex = _parse_one(intensityValue=0, intensityCustom=1, intensityDisplayUnit=6)
    assert ex["bodyweight"] is True
    assert "weight_kg" not in ex


def test_app_default_zero_weight_is_not_bodyweight():
    """An untouched exercise reads `intensityValue: 0, intensityCustom: 0,
    intensityDisplayUnit: 0` and the app renders it "0.0 kg".

    Treating unit 0 as "no unit, therefore bodyweight" was wrong, and wrong
    in the direction that matters: reporting bodyweight here and rewriting
    the template would drop a real (if zero) weight, because an omitted
    weight is exactly how the write side spells bodyweight.
    """
    ex = _parse_one(intensityValue=0, intensityCustom=0, intensityDisplayUnit=0)
    assert ex["weight_kg"] == 0
    assert "bodyweight" not in ex


def test_effort_target_is_not_a_weight():
    """displayUnit 0 with a value is the app's effort target on its 1-10 RPE
    scale, stored UNSCALED -- the timed burpee in the capture shows
    "Target 5, Moderate" for intensityValue=5.

    Read as a weight it becomes 0.005 kg, which is what this parser did
    before the app was consulted.
    """
    ex = _parse_one(targetType=2, targetValue=60,
                    intensityValue=5, intensityCustom=0, intensityDisplayUnit=0)
    assert ex["effort_target"] == 5
    assert "weight_kg" not in ex
    assert ex["duration_seconds"] == 60


def test_null_weight_reads_as_bodyweight():
    """The "" this server writes for bodyweight comes back from the API as
    None -- observed 2026-09-09 when a written template was read back. A null
    weight is no weight, which is what bodyweight means to the write side.
    """
    ex = _parse_one(intensityValue=None, intensityCustom=1, intensityDisplayUnit=6)
    assert ex["bodyweight"] is True
    assert "weight_kg" not in ex


def test_effort_target_outside_the_apps_scale_is_preserved():
    """The server accepts an effort target of 99 (the app's scale is 1-10)
    and hands it back unchanged -- confirmed by POSTing one live. The parser
    reports what is stored rather than clamping it to a range the server
    itself does not enforce; a value no app screen can produce is exactly
    what a reader needs to see.
    """
    assert _parse_one(intensityValue=99, intensityCustom=0,
                      intensityDisplayUnit=0)["effort_target"] == 99


def test_unknown_display_unit_is_surfaced_raw():
    """Same principle as an unknown targetType: show it, don't scale it."""
    ex = _parse_one(intensityValue=1234, intensityCustom=0, intensityDisplayUnit=9)
    assert ex["intensity_value_raw"] == 1234
    assert ex["intensity_display_unit_raw"] == 9
    assert "weight_kg" not in ex


def test_bodyweight_marker_written_by_this_server():
    """intensityCustom=1 with an empty intensityValue is what save_strength_
    workout_template emits for bodyweight."""
    ex = _parse_one(intensityValue="", intensityCustom=1, intensityDisplayUnit="6")
    assert ex["bodyweight"] is True
    assert "weight_kg" not in ex


def test_explicit_zero_kg_is_not_bodyweight():
    """weight_kg=0 renders as "0.00 kg" in the app and is deliberately
    distinct from Bodyweight on the write side. A falsiness check here would
    collapse the two, so the parser keys off intensityCustom."""
    ex = _parse_one(intensityValue=0, intensityCustom=0, intensityDisplayUnit="6")
    assert ex["weight_kg"] == 0
    assert "bodyweight" not in ex


def test_strength_exercises_carry_no_endurance_intensity_fields():
    """intensity_low/intensity_high mean nothing in the strength namespace --
    they would carry a raw wire weight. Omitting them is more honest."""
    ex = _parse_one()
    assert "intensity_low" not in ex
    assert "intensity_high" not in ex


# ---------------------------------------------------------------------------
# Rest
# ---------------------------------------------------------------------------

def test_rest_seconds_from_rest_type_1():
    assert _parse_one(restType=1, restValue=90)["rest_seconds"] == 90


def test_skip_rests_reads_back_as_zero():
    """restType=3 is the app's "Skip rests" and carries no value -- the write
    side emits it for rest_seconds=0, so it must read back as 0.

    Confirmed in the 2026-09-09 capture by the bicycle-crunches row, saved
    with rests skipped: restType 3, restValue 0. Note the app's smallest
    *real* rest is 3 seconds -- below that it only offers Skip rests -- so a
    written rest_seconds of 1 or 2 has no representation in the app.
    """
    ex = _parse_one(restType=3, restValue=0)
    assert ex["rest_seconds"] == 0


def test_unknown_rest_type_is_surfaced_raw():
    ex = _parse_one(restType=7, restValue=5)
    assert ex["rest_type_raw"] == 7
    assert "rest_seconds" not in ex


# ---------------------------------------------------------------------------
# Header and program-level fields
# ---------------------------------------------------------------------------

def test_header_fields_shared_with_endurance():
    parsed = _parse_strength_workout(_item(_exercise()))
    assert parsed["id"] == "900"
    assert parsed["name"] == "strength_decode_ref"
    assert parsed["sport_type"] == 4
    assert parsed["sport_name"] == "Strength"
    assert parsed["exercise_count"] == 1


def test_circuit_rounds_and_total_duration_are_reported():
    """Program-level `sets` repeats the whole exercise list (circuit rounds);
    endurance templates have no equivalent."""
    parsed = _parse_strength_workout(_item(_exercise(), sets=3, duration=1800))
    assert parsed["sets"] == 3
    assert parsed["total_duration_seconds"] == 1800


def test_null_program_sets_normalizes_to_one_round():
    """App-created templates send `sets: null` -- they express repetition per
    exercise instead (the capture had sets=null with 4 sets on each of its
    three exercises). Reporting None as the circuit count would be worse than
    saying "one round"."""
    parsed = _parse_strength_workout(_item(_exercise(), sets=None))
    assert parsed["sets"] == 1


# ---------------------------------------------------------------------------
# Endurance repeat-group headers (also from the 2026-09-09 capture)
# ---------------------------------------------------------------------------

def test_app_created_repeat_group_header_is_not_a_step():
    """A repeat-group header in an app-created endurance template is
    targetType=0/targetValue=0 with isGroup=True and the repeat count in
    `sets` -- the shape found in the "Pyramide" template.

    The old catch-all reported it as a phantom `duration_seconds: 0` step;
    the honest fallback surfaced it as an unknown type on the first live read
    after that change, which is how it was found at all.
    """
    parsed = _parse_workout({"sportType": 1, "exercises": [
        {"name": "", "targetType": 0, "targetValue": 0, "sets": 3, "isGroup": True, "groupId": "0"},
        {"name": "T3001", "targetType": 5, "targetValue": 40000, "sets": 1,
         "isGroup": False, "groupId": "434164070839664642"},
    ]})["exercises"]

    header, sub_step = parsed
    assert header["is_group"] is True
    assert header["repeat"] == 3
    assert "duration_seconds" not in header
    assert "target_type_raw" not in header
    # Sub-steps stay flat but carry their parent's id, so the grouping is at
    # least reconstructable by the caller.
    assert sub_step["distance_meters"] == 400
    assert sub_step["group_id"] == "434164070839664642"
    assert "group_id" not in header


# ---------------------------------------------------------------------------
# Round trip -- the load-bearing test
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("weight,expected", [
    ({}, {"bodyweight": True}),
    ({"weight_kg": 27.9}, {"weight_kg": 27.9}),
    ({"weight_kg": 0}, {"weight_kg": 0}),
    ({"weight_lbs": 45}, {"weight_lbs": 45}),
])
def test_write_read_round_trip_speaks_one_vocabulary(weight, expected):
    """Every value save_strength_workout encodes, list_workout_templates must
    decode back to what was typed -- 27.9 kg in, 27.9 kg out, not 27900.

    The `written` dict below deliberately uses target_type/target_value while
    the assertions read `reps`: that mismatch is the one axis where read and
    write do NOT share a name yet (see the module docstring). The VALUE
    survives, which is what makes a mapped read-modify-write possible; the
    names not matching is what makes it manual.

    origin_id/overview matter here specifically: the write side REQUIRES
    origin_id, so a parsed template that omitted it could not be rewritten at
    all.
    """
    written = {
        "origin_id": "1061",
        "name": "T1061",
        "overview": "sid_strength_squats",
        "target_type": 3,
        "target_value": 12,
        "sets": 3,
        "rest_seconds": 90,
        **weight,
    }
    payload = _build_strength_program_payload(
        name="round trip", exercises=[written], by_id={},
    )
    read = _parse_strength_workout({"id": 1, **payload})["exercises"][0]

    assert read["origin_id"] == written["origin_id"]
    assert read["name"] == written["name"]
    assert read["overview"] == written["overview"]
    assert read["reps"] == written["target_value"]
    assert read["sets"] == written["sets"]
    assert read["rest_seconds"] == written["rest_seconds"]
    for key, value in expected.items():
        assert read[key] == value


# ---------------------------------------------------------------------------
# Dispatch happens per exercise, not per program
# ---------------------------------------------------------------------------


def test_strength_exercise_inside_an_endurance_program_is_parsed_as_strength():
    """Both builders emit `hybridTotalSets`, so the wire format admits mixed
    programs, and the strength builder stamps `sportType: 4` on every exercise.

    Dispatching on the program sport alone would send a hybrid template's
    strength steps through the endurance parser -- reporting 12 reps as 12
    seconds and 27.9 kg as `intensity_low: 27900`, which is GH #60 again one
    sport further along.
    """
    parsed = _parse_workout({"sportType": 1, "exercises": [
        {"name": "Easy run", "targetType": 2, "targetValue": 600, "intensityValue": 140},
        {"name": "T1061", "sportType": 4, "originId": "1061", "overview": "sid_strength_squats",
         "targetType": 3, "targetValue": 12, "sets": 3, "restType": 1, "restValue": 60,
         "intensityValue": 27900, "intensityDisplayUnit": 6, "intensityCustom": 0},
    ]})["exercises"]

    assert parsed[0]["duration_seconds"] == 600      # endurance step, unchanged
    assert parsed[1]["reps"] == 12                   # strength step, not seconds
    assert parsed[1]["weight_kg"] == 27.9            # not intensity_low: 27900
    assert "intensity_low" not in parsed[1]


def test_exercise_without_its_own_sport_type_falls_back_to_the_program():
    """App-created templates need not stamp sportType per exercise, so the
    program sport stays the fallback."""
    parsed = _parse_strength_workout({"sportType": 4, "exercises": [
        {"name": "T1061", "originId": "1061", "targetType": 3, "targetValue": 10, "sets": 2},
    ]})["exercises"][0]
    assert parsed["reps"] == 10


# ---------------------------------------------------------------------------
# Wire types: what comes back is not what the write side sends
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("wire_value", [27900, "27900"])
def test_weight_survives_a_string_intensity_value(wire_value):
    """`intensityDisplayUnit` goes out as a string ("6") and comes back an int,
    so the numeric fields are not type-stable either. `_unscale` raises
    TypeError on a string -- coerce before dividing rather than trusting it.
    """
    ex = _parse_one(intensityValue=wire_value, intensityDisplayUnit="6", intensityCustom=0)
    assert ex["weight_kg"] == 27.9


def test_pounds_survive_a_string_intensity_percent():
    ex = _parse_one(
        intensityValue=19051, intensityDisplayUnit="7",
        intensityPercent="42000000", intensityCustom=0,
    )
    assert ex["weight_lbs"] == 42


def test_unparseable_intensity_value_is_raw_not_bodyweight():
    """A value that merely fails to parse is UNKNOWN, not bodyweight.

    Only the two confirmed spellings of "no weight" -- the app's
    intensityCustom=1 and this server's "" -- mean bodyweight. Collapsing
    anything non-numeric into bodyweight would be the same guess this issue
    exists to remove, and would silently rewrite a load on a read-modify-write.
    """
    ex = _parse_one(intensityValue="junk", intensityDisplayUnit="6", intensityCustom=0)
    assert ex["intensity_value_raw"] == "junk"
    assert "bodyweight" not in ex
    assert "weight_kg" not in ex
