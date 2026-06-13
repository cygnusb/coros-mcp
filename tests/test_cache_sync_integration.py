"""Integration tests for the cached fetch wrappers in coros_mcp/cache/sync.py.

Unlike test_cache_sync.py (which unit-tests _resolve_fetch_range / sync_all in
isolation with everything mocked), these drive the *real* SQLite store through
fetch_*_cached() while mocking only the network layer (coros_api.*). This proves
the cache actually short-circuits the API, fetches only uncached ranges, chunks
long ranges, paginates activities, and persists results.

_today() is pinned to 20260613 so the STABLE_AFTER_DAYS=2 cutoff is 20260611;
test ranges sit safely in the historical (stable) zone unless stated otherwise.
asyncio.sleep is neutralised so the inter-chunk/inter-page pacing doesn't slow
the suite.
"""

from datetime import UTC, datetime

import pytest

from coros_mcp.cache import store, sync
from coros_mcp.models import ActivitySummary, DailyRecord, SleepRecord, StoredAuth

TODAY = "20260613"
AUTH = StoredAuth(access_token="t", user_id="u", region="eu", timestamp=0)


@pytest.fixture
def wired(tmp_path, monkeypatch):
    """Real temp DB + pinned today + no-op sleep. Returns the db path."""
    db = tmp_path / "cache.db"
    monkeypatch.setattr(store, "CACHE_DB", db)
    monkeypatch.setattr(store, "_LOCAL_TZ", UTC)
    monkeypatch.setattr(sync, "_today", lambda: TODAY)

    async def _no_sleep(*_a, **_k):
        return None

    monkeypatch.setattr(sync.asyncio, "sleep", _no_sleep)
    store.init_db()
    return db


def daily_range(start: str, end: str) -> list[DailyRecord]:
    """One DailyRecord per day in [start, end]."""
    out, cur = [], start
    while cur <= end:
        out.append(DailyRecord(date=cur, rhr=50))
        cur = sync._date_add(cur, 1)
    return out


def act_on(activity_id: str, day: str) -> ActivitySummary:
    dt = datetime.strptime(day, "%Y%m%d").replace(hour=12, tzinfo=UTC)
    return ActivitySummary(activity_id=activity_id, start_time=str(int(dt.timestamp())))


# ---------------------------------------------------------------------------
# fetch_daily_records_cached
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
class TestFetchDailyCached:
    async def test_cold_cache_fetches_and_persists(self, wired, monkeypatch):
        calls = []

        async def fake(auth, s, e):
            calls.append((s, e))
            return daily_range(s, e)

        monkeypatch.setattr(sync.coros_api, "fetch_daily_records", fake)
        got = await sync.fetch_daily_records_cached(AUTH, "20260101", "20260110")

        assert [r.date for r in got] == [sync._date_add("20260101", i) for i in range(10)]
        assert calls == [("20260101", "20260110")]
        # Persisted: a direct store read sees the same rows.
        assert len(store.get_daily_records("20260101", "20260110")) == 10

    async def test_second_call_fully_covered_skips_api(self, wired, monkeypatch):
        calls = []

        async def fake(auth, s, e):
            calls.append((s, e))
            return daily_range(s, e)

        monkeypatch.setattr(sync.coros_api, "fetch_daily_records", fake)
        await sync.fetch_daily_records_cached(AUTH, "20260101", "20260110")
        await sync.fetch_daily_records_cached(AUTH, "20260103", "20260108")

        assert len(calls) == 1  # second request served entirely from cache

    async def test_tail_gap_fetches_only_uncached_tail(self, wired, monkeypatch):
        calls = []

        async def fake(auth, s, e):
            calls.append((s, e))
            return daily_range(s, e)

        monkeypatch.setattr(sync.coros_api, "fetch_daily_records", fake)
        await sync.fetch_daily_records_cached(AUTH, "20260101", "20260110")
        calls.clear()
        await sync.fetch_daily_records_cached(AUTH, "20260101", "20260115")

        # Only the uncached tail (11th onward) is fetched, not the whole range.
        assert len(calls) == 1
        fetch_from, fetch_to = calls[0]
        assert fetch_from == "20260111"
        assert fetch_to == "20260115"

    async def test_long_range_is_chunked(self, wired, monkeypatch):
        calls = []

        async def fake(auth, s, e):
            calls.append((s, e))
            return daily_range(s, e)

        monkeypatch.setattr(sync.coros_api, "fetch_daily_records", fake)
        # 200 days > API_CHUNK_DAYS (84) → expect 3 chunks (84 + 84 + 32).
        end = sync._date_add("20260101", 199)
        await sync.fetch_daily_records_cached(AUTH, "20260101", end)

        assert len(calls) == 3
        # Chunks must be contiguous and non-overlapping.
        assert calls[0][0] == "20260101"
        for prev, nxt in zip(calls, calls[1:], strict=False):
            assert sync._date_add(prev[1], 1) == nxt[0]
        assert calls[-1][1] == end
        # Every day landed in the cache exactly once.
        assert len(store.get_daily_records("20260101", end)) == 200

    async def test_empty_api_result_returns_empty_without_crash(self, wired, monkeypatch):
        async def fake(auth, s, e):
            return []

        monkeypatch.setattr(sync.coros_api, "fetch_daily_records", fake)
        got = await sync.fetch_daily_records_cached(AUTH, "20260101", "20260110")
        assert got == []
        assert store.get_max_daily_date() is None


# ---------------------------------------------------------------------------
# fetch_sleep_cached
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
class TestFetchSleepCached:
    async def test_cold_then_cached(self, wired, monkeypatch):
        calls = []

        async def fake(auth, s, e):
            calls.append((s, e))
            return [SleepRecord(date=s)]

        monkeypatch.setattr(sync.coros_api, "fetch_sleep", fake)
        await sync.fetch_sleep_cached(AUTH, "20260101", "20260101")
        got = await sync.fetch_sleep_cached(AUTH, "20260101", "20260101")

        assert len(calls) == 1
        assert [r.date for r in got] == ["20260101"]


# ---------------------------------------------------------------------------
# fetch_activities_cached — pagination + page exhaustion
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
class TestFetchActivitiesCached:
    async def test_exhausts_all_api_pages(self, wired, monkeypatch):
        # 250 activities across the range, API serves 100 per page → 3 pages.
        days = [sync._date_add("20260101", i) for i in range(250)]
        all_acts = [act_on(f"a{i}", d) for i, d in enumerate(days)]
        seen_pages = []

        async def fake(auth, s, e, page, size):
            seen_pages.append(page)
            start = (page - 1) * size
            return all_acts[start:start + size], len(all_acts)

        monkeypatch.setattr(sync.coros_api, "fetch_activities", fake)
        end = days[-1]
        _, total = await sync.fetch_activities_cached(AUTH, "20260101", end, page=1, size=30)

        assert seen_pages == [1, 2, 3]  # all pages pulled
        assert total == 250  # full set cached
        assert len(store.get_activities("20260101", end)) == 250

    async def test_local_pagination_slices_cached_results(self, wired, monkeypatch):
        days = [sync._date_add("20260101", i) for i in range(50)]
        all_acts = [act_on(f"a{i}", d) for i, d in enumerate(days)]

        async def fake(auth, s, e, page, size):
            start = (page - 1) * size
            return all_acts[start:start + size], len(all_acts)

        monkeypatch.setattr(sync.coros_api, "fetch_activities", fake)
        end = days[-1]

        page1, total = await sync.fetch_activities_cached(AUTH, "20260101", end, page=1, size=20)
        page2, _ = await sync.fetch_activities_cached(AUTH, "20260101", end, page=2, size=20)
        page3, _ = await sync.fetch_activities_cached(AUTH, "20260101", end, page=3, size=20)

        assert total == 50
        assert (len(page1), len(page2), len(page3)) == (20, 20, 10)
        # Pages are disjoint and ordered (store returns start_day DESC).
        ids = [a.activity_id for a in page1 + page2 + page3]
        assert len(set(ids)) == 50

    async def test_second_call_in_stable_range_skips_api(self, wired, monkeypatch):
        days = [sync._date_add("20260101", i) for i in range(10)]
        all_acts = [act_on(f"a{i}", d) for i, d in enumerate(days)]
        call_count = 0

        async def fake(auth, s, e, page, size):
            nonlocal call_count
            call_count += 1
            return all_acts, len(all_acts)

        monkeypatch.setattr(sync.coros_api, "fetch_activities", fake)
        end = days[-1]
        await sync.fetch_activities_cached(AUTH, "20260101", end)
        before = call_count
        await sync.fetch_activities_cached(AUTH, "20260101", end)
        assert call_count == before  # fully covered, no extra API hits


# ---------------------------------------------------------------------------
# Unstable-window re-fetch (recent data always re-pulled)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
class TestUnstableWindowRefetch:
    async def test_recent_range_refetched_even_when_cached(self, wired, monkeypatch):
        # cutoff = TODAY - 2 = 20260611. A range touching today is "unstable"
        # and must be re-fetched on every call to pick up delayed syncs.
        calls = []

        async def fake(auth, s, e):
            calls.append((s, e))
            return daily_range(s, e)

        monkeypatch.setattr(sync.coros_api, "fetch_daily_records", fake)
        await sync.fetch_daily_records_cached(AUTH, "20260610", TODAY)
        calls.clear()
        await sync.fetch_daily_records_cached(AUTH, "20260610", TODAY)

        assert calls, "recent (unstable) range should be re-fetched, not served from cache"
        # Re-fetch starts no earlier than the stable cutoff (20260611).
        assert calls[0][0] >= "20260611"


# ---------------------------------------------------------------------------
# sync_all — full backfill: error resilience + progress reporting
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
class TestSyncAll:
    async def test_continues_and_records_errors_when_one_type_fails(self, wired, monkeypatch):
        async def daily_boom(auth, s, e):
            raise RuntimeError("daily endpoint down")

        async def sleep_ok(auth, s, e):
            return [SleepRecord(date=s)]

        async def acts_ok(auth, s, e, page, size):
            return [act_on("a0", s)], 1

        monkeypatch.setattr(sync.coros_api, "fetch_daily_records", daily_boom)
        monkeypatch.setattr(sync.coros_api, "fetch_sleep", sleep_ok)
        monkeypatch.setattr(sync.coros_api, "fetch_activities", acts_ok)

        stats = await sync.sync_all(AUTH, start_day="20260101", end_day="20260105")

        # daily failed but sleep + activities still ran and persisted.
        assert stats["daily"] == 0
        assert any("daily" in err for err in stats["errors"])
        assert stats["sleep"] >= 1
        assert stats["activities"] >= 1
        assert store.get_max_sleep_date() == "20260101"
        assert "cache" in stats  # final cache_status() attached

    async def test_all_types_fail_records_three_errors_and_completes(self, wired, monkeypatch):
        async def boom_daily(auth, s, e):
            raise RuntimeError("daily down")

        async def boom_sleep(auth, s, e):
            raise RuntimeError("sleep down")

        async def boom_acts(auth, s, e, page, size):
            raise RuntimeError("activities down")

        monkeypatch.setattr(sync.coros_api, "fetch_daily_records", boom_daily)
        monkeypatch.setattr(sync.coros_api, "fetch_sleep", boom_sleep)
        monkeypatch.setattr(sync.coros_api, "fetch_activities", boom_acts)

        stats = await sync.sync_all(AUTH, "20260101", "20260105")

        # Every type's error is captured, sync still returns cleanly with zero counts.
        assert stats["daily"] == 0 and stats["sleep"] == 0 and stats["activities"] == 0
        assert any("daily" in e for e in stats["errors"])
        assert any("sleep" in e for e in stats["errors"])
        assert any("activities" in e for e in stats["errors"])

    async def test_progress_callback_receives_messages(self, wired, monkeypatch):
        async def ok_daily(auth, s, e):
            return []

        async def ok_sleep(auth, s, e):
            return []

        async def ok_acts(auth, s, e, page, size):
            return [], 0

        monkeypatch.setattr(sync.coros_api, "fetch_daily_records", ok_daily)
        monkeypatch.setattr(sync.coros_api, "fetch_sleep", ok_sleep)
        monkeypatch.setattr(sync.coros_api, "fetch_activities", ok_acts)

        messages = []

        async def on_progress(msg):
            messages.append(msg)

        await sync.sync_all(AUTH, "20260101", "20260105", on_progress=on_progress)

        assert messages, "on_progress should have been called"
        assert messages[-1] == "done"
        assert any("syncing" in m for m in messages)

    async def test_happy_path_persists_all_three_types(self, wired, monkeypatch):
        async def ok_daily(auth, s, e):
            return daily_range(s, e)

        async def ok_sleep(auth, s, e):
            return [SleepRecord(date=s)]

        async def ok_acts(auth, s, e, page, size):
            return [act_on("a0", s)], 1

        monkeypatch.setattr(sync.coros_api, "fetch_daily_records", ok_daily)
        monkeypatch.setattr(sync.coros_api, "fetch_sleep", ok_sleep)
        monkeypatch.setattr(sync.coros_api, "fetch_activities", ok_acts)

        stats = await sync.sync_all(AUTH, "20260101", "20260105")

        assert stats["errors"] == []
        assert stats["daily"] == 5  # one record per day, single chunk
        assert stats["sleep"] == 1
        assert stats["activities"] == 1
        assert len(store.get_daily_records("20260101", "20260105")) == 5
