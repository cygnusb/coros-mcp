"""testRhr (app resting HR) must survive parsing and cache round-trip.

The Coros /analyse/dayDetail/query response carries two resting-HR fields:
- rhr:     daily aggregate shown in the web dashboard / Training Hub
- testRhr: measured resting HR shown in the Coros app
"""

from coros_mcp.coros_api import _parse_daily_record
from coros_mcp.models import DailyRecord


def test_parse_daily_record_keeps_both_rhr_values():
    rec = _parse_daily_record({"happenDay": 20260813, "rhr": 56, "testRhr": 49})
    assert rec.rhr == 56
    assert rec.test_rhr == 49


def test_parse_daily_record_test_rhr_absent_is_none():
    rec = _parse_daily_record({"happenDay": 20260812, "rhr": 46})
    assert rec.rhr == 46
    assert rec.test_rhr is None


def test_test_rhr_survives_json_round_trip():
    rec = DailyRecord(date="20260813", rhr=56, test_rhr=49)
    restored = DailyRecord.model_validate_json(rec.model_dump_json())
    assert restored.test_rhr == 49
