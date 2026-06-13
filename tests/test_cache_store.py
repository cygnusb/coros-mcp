"""Direct SQLite round-trip tests for coros_mcp/cache/store.py.

These exercise the real persistence layer against a temporary on-disk DB
(CACHE_DB is monkeypatched per test), rather than mocking the store away.

Covered:
  * upsert/get round trips for daily, sleep, and activity records
  * INSERT OR REPLACE upsert semantics (latest write wins)
  * inclusive range-query boundaries and result ordering
  * MIN/MAX date helpers on empty and populated tables
  * _activity_start_day timestamp parsing (10-/13-digit, encoded strings,
    timezone date-boundary crossing, and unparseable input)
  * upsert_activities skipping rows with unparseable start_time
  * cache_status counts and coverage
  * init_db idempotency
"""

from datetime import UTC, datetime, timedelta, timezone

import pytest

from coros_mcp.cache import store
from coros_mcp.models import ActivitySummary, DailyRecord, SleepPhases, SleepRecord

TZ_PLUS8 = timezone(timedelta(hours=8))


@pytest.fixture
def temp_db(tmp_path, monkeypatch):
    """Point store.CACHE_DB at a fresh temp file and create the schema."""
    db = tmp_path / "cache.db"
    monkeypatch.setattr(store, "CACHE_DB", db)
    store.init_db()
    return db


# ---------------------------------------------------------------------------
# Builders
# ---------------------------------------------------------------------------

def daily(date: str, **kw) -> DailyRecord:
    return DailyRecord(date=date, **kw)


def sleep(date: str, **kw) -> SleepRecord:
    return SleepRecord(date=date, **kw)


def activity(activity_id: str, start_time: str | None, **kw) -> ActivitySummary:
    return ActivitySummary(activity_id=activity_id, start_time=start_time, **kw)


def secs(dt: datetime) -> str:
    """UTC datetime → 10-digit unix-seconds string."""
    return str(int(dt.timestamp()))


def millis(dt: datetime) -> str:
    """UTC datetime → 13-digit unix-millis string."""
    return str(int(dt.timestamp() * 1000))


# ---------------------------------------------------------------------------
# init_db
# ---------------------------------------------------------------------------

class TestInitDb:
    def test_idempotent(self, temp_db):
        # Second call must not raise (CREATE TABLE IF NOT EXISTS).
        store.init_db()
        store.init_db()
        assert store.cache_status()["daily_records"]["count"] == 0

    def test_creates_parent_directory(self, tmp_path, monkeypatch):
        nested = tmp_path / "a" / "b" / "cache.db"
        monkeypatch.setattr(store, "CACHE_DB", nested)
        store.init_db()
        assert nested.exists()


# ---------------------------------------------------------------------------
# Daily records
# ---------------------------------------------------------------------------

class TestDailyRecords:
    def test_round_trip_preserves_fields(self, temp_db):
        rec = daily("20260101", rhr=48, training_load=320, vo2max=55, stamina_level=3.2)
        store.upsert_daily_records([rec])
        got = store.get_daily_records("20260101", "20260101")
        assert len(got) == 1
        assert got[0] == rec  # full pydantic equality, all fields survive JSON round trip

    def test_empty_input_is_noop(self, temp_db):
        store.upsert_daily_records([])
        assert store.get_daily_records("00000000", "99999999") == []

    def test_range_is_inclusive_on_both_ends(self, temp_db):
        store.upsert_daily_records([daily(d) for d in ("20260101", "20260102", "20260103")])
        got = store.get_daily_records("20260101", "20260103")
        assert [r.date for r in got] == ["20260101", "20260102", "20260103"]

    def test_range_excludes_outside_dates(self, temp_db):
        store.upsert_daily_records([daily(d) for d in ("20251231", "20260101", "20260104")])
        got = store.get_daily_records("20260101", "20260103")
        assert [r.date for r in got] == ["20260101"]

    def test_results_ordered_ascending(self, temp_db):
        store.upsert_daily_records([daily(d) for d in ("20260103", "20260101", "20260102")])
        got = store.get_daily_records("20260101", "20260103")
        assert [r.date for r in got] == ["20260101", "20260102", "20260103"]

    def test_upsert_replaces_existing_date(self, temp_db):
        store.upsert_daily_records([daily("20260101", rhr=50)])
        store.upsert_daily_records([daily("20260101", rhr=42)])
        got = store.get_daily_records("20260101", "20260101")
        assert len(got) == 1  # not duplicated
        assert got[0].rhr == 42  # latest write wins

    def test_min_max_empty(self, temp_db):
        assert store.get_min_daily_date() is None
        assert store.get_max_daily_date() is None

    def test_min_max_populated(self, temp_db):
        store.upsert_daily_records([daily(d) for d in ("20260105", "20260101", "20260110")])
        assert store.get_min_daily_date() == "20260101"
        assert store.get_max_daily_date() == "20260110"


# ---------------------------------------------------------------------------
# Sleep records
# ---------------------------------------------------------------------------

class TestSleepRecords:
    def test_round_trip_with_nested_phases(self, temp_db):
        rec = sleep(
            "20260101",
            total_duration_minutes=455,
            phases=SleepPhases(deep_minutes=90, light_minutes=250, rem_minutes=100, awake_minutes=15),
            avg_hr=52,
            quality_score=88,
        )
        store.upsert_sleep_records([rec])
        got = store.get_sleep_records("20260101", "20260101")
        assert got == [rec]
        assert got[0].phases.deep_minutes == 90

    def test_range_and_order(self, temp_db):
        store.upsert_sleep_records([sleep(d) for d in ("20260103", "20260101", "20260102")])
        got = store.get_sleep_records("20260101", "20260102")
        assert [r.date for r in got] == ["20260101", "20260102"]

    def test_upsert_replaces(self, temp_db):
        store.upsert_sleep_records([sleep("20260101", quality_score=10)])
        store.upsert_sleep_records([sleep("20260101", quality_score=99)])
        got = store.get_sleep_records("20260101", "20260101")
        assert len(got) == 1
        assert got[0].quality_score == 99

    def test_min_max(self, temp_db):
        assert store.get_min_sleep_date() is None
        store.upsert_sleep_records([sleep(d) for d in ("20260102", "20260108")])
        assert store.get_min_sleep_date() == "20260102"
        assert store.get_max_sleep_date() == "20260108"


# ---------------------------------------------------------------------------
# _activity_start_day — timestamp/date parsing
# ---------------------------------------------------------------------------

class TestActivityStartDay:
    def test_seconds_with_fixed_tz(self, monkeypatch):
        monkeypatch.setattr(store, "_LOCAL_TZ", TZ_PLUS8)
        # 2026-01-01 02:00 UTC → 10:00 on 2026-01-01 in +8
        ts = secs(datetime(2026, 1, 1, 2, 0, tzinfo=UTC))
        assert store._activity_start_day(activity("a", ts)) == "20260101"

    def test_seconds_crosses_date_boundary_with_tz(self, monkeypatch):
        monkeypatch.setattr(store, "_LOCAL_TZ", TZ_PLUS8)
        # 2026-01-01 20:00 UTC → 04:00 on 2026-01-02 in +8
        ts = secs(datetime(2026, 1, 1, 20, 0, tzinfo=UTC))
        assert store._activity_start_day(activity("a", ts)) == "20260102"

    def test_milliseconds_13_digits(self, monkeypatch):
        monkeypatch.setattr(store, "_LOCAL_TZ", TZ_PLUS8)
        ts = millis(datetime(2026, 1, 1, 2, 0, tzinfo=UTC))
        assert len(ts) == 13
        assert store._activity_start_day(activity("a", ts)) == "20260101"

    def test_seconds_falls_back_to_system_tz_when_unset(self, monkeypatch):
        monkeypatch.setattr(store, "_LOCAL_TZ", None)
        dt = datetime(2026, 6, 13, 12, 0, tzinfo=UTC)
        ts = secs(dt)
        expected = datetime.fromtimestamp(int(ts)).strftime("%Y%m%d")
        assert store._activity_start_day(activity("a", ts)) == expected

    def test_encoded_yyyymmdd_string(self):
        # 8 numeric digits: not 10/13 → treated as already-encoded date prefix.
        assert store._activity_start_day(activity("a", "20260101")) == "20260101"

    def test_encoded_yyyymmddhhmmss_string(self):
        # 14 numeric digits → first 8 used as the date.
        assert store._activity_start_day(activity("a", "20260101153000")) == "20260101"

    def test_empty_start_time_returns_blank(self):
        assert store._activity_start_day(activity("a", None)) == ""
        assert store._activity_start_day(activity("a", "")) == ""

    def test_non_numeric_junk_returns_blank(self):
        assert store._activity_start_day(activity("a", "not-a-date")) == ""


# ---------------------------------------------------------------------------
# Activities — upsert / get
# ---------------------------------------------------------------------------

class TestActivities:
    def _acts_on(self, *dates):
        """Activities whose start_time is noon-UTC on the given YYYYMMDD dates."""
        out = []
        for i, d in enumerate(dates):
            dt = datetime.strptime(d, "%Y%m%d").replace(hour=12, tzinfo=UTC)
            out.append(activity(f"act{i}", secs(dt), name=f"Run {i}", sport_type=100))
        return out

    @pytest.fixture(autouse=True)
    def _utc_tz(self, monkeypatch):
        # Pin tz so noon-UTC stays on the same calendar day for indexing.
        monkeypatch.setattr(store, "_LOCAL_TZ", UTC)

    def test_round_trip(self, temp_db):
        acts = self._acts_on("20260101")
        store.upsert_activities(acts)
        got = store.get_activities("20260101", "20260101")
        assert got == acts

    def test_get_ordered_descending_by_day(self, temp_db):
        store.upsert_activities(self._acts_on("20260101", "20260103", "20260102"))
        got = store.get_activities("20260101", "20260103")
        # store.get_activities orders by start_day DESC (newest first)
        days = [store._activity_start_day(a) for a in got]
        assert days == ["20260103", "20260102", "20260101"]

    def test_range_filtering(self, temp_db):
        store.upsert_activities(self._acts_on("20251231", "20260101", "20260105"))
        got = store.get_activities("20260101", "20260102")
        assert [a.activity_id for a in got] == ["act1"]

    def test_upsert_replaces_by_activity_id(self, temp_db):
        dt = datetime(2026, 1, 1, 12, tzinfo=UTC)
        store.upsert_activities([activity("same", secs(dt), name="old")])
        store.upsert_activities([activity("same", secs(dt), name="new")])
        got = store.get_activities("20260101", "20260101")
        assert len(got) == 1
        assert got[0].name == "new"

    def test_skips_unparseable_start_time(self, temp_db, caplog):
        good = self._acts_on("20260101")[0]
        bad = activity("bad", "garbage")
        store.upsert_activities([good, bad])
        all_acts = store.get_activities("00000000", "99999999")
        assert {a.activity_id for a in all_acts} == {"act0"}  # 'bad' was skipped
        assert "Skipping activity bad" in caplog.text

    def test_all_unparseable_is_noop(self, temp_db):
        store.upsert_activities([activity("x", None), activity("y", "")])
        assert store.get_activities("00000000", "99999999") == []

    def test_min_max_activity_date(self, temp_db):
        assert store.get_min_activity_date() is None
        store.upsert_activities(self._acts_on("20260105", "20260101", "20260110"))
        assert store.get_min_activity_date() == "20260101"
        assert store.get_max_activity_date() == "20260110"


# ---------------------------------------------------------------------------
# cache_status
# ---------------------------------------------------------------------------

class TestCacheStatus:
    def test_empty_db(self, tmp_path, monkeypatch):
        db = tmp_path / "cache.db"
        monkeypatch.setattr(store, "CACHE_DB", db)
        # cache_status calls init_db() itself — no prior schema needed.
        status = store.cache_status()
        for key in ("daily_records", "sleep_records", "activities"):
            assert status[key] == {"count": 0, "from": None, "to": None}
        assert status["db_path"] == str(db)

    def test_populated_counts_and_coverage(self, temp_db, monkeypatch):
        monkeypatch.setattr(store, "_LOCAL_TZ", UTC)  # deterministic activity indexing
        store.upsert_daily_records([daily(d) for d in ("20260101", "20260102")])
        store.upsert_sleep_records([sleep("20260101")])
        dt = datetime(2026, 1, 3, 12, tzinfo=UTC)
        store.upsert_activities([activity("a", secs(dt))])

        status = store.cache_status()
        assert status["daily_records"] == {"count": 2, "from": "20260101", "to": "20260102"}
        assert status["sleep_records"] == {"count": 1, "from": "20260101", "to": "20260101"}
        assert status["activities"]["count"] == 1
        assert status["activities"]["from"] == "20260103"
