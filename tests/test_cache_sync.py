"""Tests for PR#14 review fixes in cache/sync.py.

Covers:
  1. _resolve_fetch_range: correct detection of historical/tail/both/covered gaps
  2. sync_all: cache continuity enforcement (stop extended to existing_max)
  3. STABLE_AFTER_DAYS: configurable via COROS_STABLE_DAYS env var
  4. cli.py: redundant asyncio import removed (static check)
"""

import importlib
import os
import sys
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# Fixed reference dates (deterministic, independent of real today)
TODAY = "20260414"
CUTOFF = "20260412"   # TODAY - 2 days (STABLE_AFTER_DAYS=2)

MIN_C = "20260301"    # existing cache lower bound
MAX_C = "20260414"    # existing cache upper bound (today)


def resolve(min_cached, max_cached, start_day, end_day, cutoff=CUTOFF):
    from cache.sync import _resolve_fetch_range
    return _resolve_fetch_range(min_cached, max_cached, start_day, end_day, cutoff)


# ---------------------------------------------------------------------------
# 1. _resolve_fetch_range — Issue 1 + 2
# ---------------------------------------------------------------------------

class TestResolveFetchRange:

    # --- empty cache ---

    def test_empty_cache_returns_full_range(self):
        """Empty cache: fetch exactly what was requested."""
        result = resolve(None, None, "20240101", "20240630")
        assert result == ("20240101", "20240630")

    # --- fully covered ---

    def test_fully_covered_returns_none(self):
        """Cache covers [20260301, 20260414], request [20260305, 20260410] is fully inside
        and end_day is below the unstable cutoff — no API call needed."""
        result = resolve(MIN_C, MAX_C, "20260305", "20260410", cutoff=CUTOFF)
        assert result is None

    def test_issue1_historical_range_never_synced(self):
        """Issue 1 regression: cache up to today, request a range never synced.

        Old logic: max_cached("20260414") >= end_day("20240630") → skipped → silent empty.
        New logic: start_day < min_cached → historical gap detected → fetch triggered.
        """
        result = resolve(MIN_C, MAX_C, "20240101", "20240630")
        assert result is not None
        fetch_from, fetch_to = result
        assert fetch_from == "20240101"
        # bridge_end must reach at least min_cached - 1 = "20260228"
        assert fetch_to >= "20260228"

    # --- historical gap ---

    def test_historical_gap_bridges_to_min_cached(self):
        """Request ends before min_cached: fetch bridges up to min_cached-1."""
        result = resolve("20260301", "20260414", "20250101", "20250630")
        assert result is not None
        fetch_from, fetch_to = result
        assert fetch_from == "20250101"
        assert fetch_to == "20260228"   # min_cached("20260301") - 1 day

    def test_historical_gap_end_overlaps_min_cached(self):
        """Request end_day >= min_cached: end already overlaps existing data,
        no extra bridging needed beyond end_day."""
        result = resolve("20260301", "20260414", "20250101", "20260315")
        assert result is not None
        fetch_from, fetch_to = result
        assert fetch_from == "20250101"
        assert fetch_to == "20260315"   # end_day wins over bridge_end

    # --- tail gap ---

    def test_tail_gap_end_beyond_max_cached(self):
        """Request end_day > max_cached: only the uncached tail is fetched."""
        result = resolve("20260301", "20260410", "20260305", "20260420")
        assert result is not None
        fetch_from, fetch_to = result
        assert fetch_to == "20260420"
        # fetch_from must be >= max_cached+1 (incremental logic)
        assert fetch_from >= "20260411"

    def test_tail_gap_end_within_unstable_window(self):
        """end_day falls within STABLE_AFTER_DAYS window: re-fetch the unstable tail
        even though end_day <= max_cached."""
        # end_day="20260413" >= cutoff="20260412" → tail_gap=True
        result = resolve("20260301", "20260414", "20260305", "20260413")
        assert result is not None
        fetch_from, fetch_to = result
        assert fetch_to == "20260413"

    # --- both gaps ---

    def test_both_gaps_fetches_full_range(self):
        """start_day < min_cached AND end_day > max_cached: fetch the entire request."""
        result = resolve("20260301", "20260410", "20250101", "20260420")
        assert result == ("20250101", "20260420")


# ---------------------------------------------------------------------------
# 2. sync_all — cache continuity enforcement
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
class TestSyncAllContinuity:

    async def _run_sync_all(self, end_day, max_daily, max_sleep, max_activity):
        """Run sync_all with mocked store and API, return the stop value used."""
        calls = []

        async def fake_fetch_daily(auth, start, end):
            calls.append(("daily", start, end))
            return []

        async def fake_fetch_sleep(auth, start, end):
            calls.append(("sleep", start, end))
            return []

        async def fake_fetch_activities(auth, start, end, page, size):
            return [], 0

        with patch("cache.sync.get_max_daily_date", return_value=max_daily), \
             patch("cache.sync.get_max_sleep_date", return_value=max_sleep), \
             patch("cache.sync.get_max_activity_date", return_value=max_activity), \
             patch("cache.sync.get_min_daily_date", return_value=None), \
             patch("cache.sync.get_min_sleep_date", return_value=None), \
             patch("cache.sync.get_min_activity_date", return_value=None), \
             patch("cache.sync.init_db"), \
             patch("cache.sync.upsert_daily_records"), \
             patch("cache.sync.upsert_sleep_records"), \
             patch("cache.sync.upsert_activities"), \
             patch("cache.sync.get_activities", return_value=[]), \
             patch("cache.sync.cache_status", return_value={}), \
             patch("coros_api.fetch_daily_records", side_effect=fake_fetch_daily), \
             patch("coros_api.fetch_sleep", side_effect=fake_fetch_sleep), \
             patch("coros_api.fetch_activities", return_value=([], 0)):

            from cache.sync import sync_all
            stats = await sync_all(
                auth=MagicMock(),
                start_day="20260301",
                end_day=end_day,
            )

        return calls

    async def test_stop_extended_when_end_day_before_existing_max(self):
        """If end_day < existing max_cached, stop must be extended to existing_max
        to prevent a mid-range gap."""
        # existing_max across all types = "20260414" (today)
        # user requests end_day = "20260331" (before existing_max)
        calls = await self._run_sync_all(
            end_day="20260331",
            max_daily="20260414",
            max_sleep="20260414",
            max_activity="20260414",
        )
        # At least one chunk must reach beyond 20260331
        daily_calls = [c for c in calls if c[0] == "daily"]
        last_end = max(c[2] for c in daily_calls)
        assert last_end >= "20260414", (
            f"stop was not extended: last chunk ended at {last_end}"
        )

    async def test_stop_not_extended_when_no_existing_cache(self):
        """If cache is empty, stop stays at the requested end_day."""
        calls = await self._run_sync_all(
            end_day="20260331",
            max_daily=None,
            max_sleep=None,
            max_activity=None,
        )
        daily_calls = [c for c in calls if c[0] == "daily"]
        last_end = max(c[2] for c in daily_calls)
        assert last_end == "20260331"

    async def test_stop_not_extended_when_end_day_beyond_existing_max(self):
        """If end_day is already >= existing_max, stop stays at end_day."""
        calls = await self._run_sync_all(
            end_day="20260420",
            max_daily="20260414",
            max_sleep="20260414",
            max_activity="20260414",
        )
        daily_calls = [c for c in calls if c[0] == "daily"]
        last_end = max(c[2] for c in daily_calls)
        assert last_end == "20260420"


# ---------------------------------------------------------------------------
# 3. STABLE_AFTER_DAYS — env var configurability
# ---------------------------------------------------------------------------

class TestStableAfterDaysEnvVar:

    def _reload_module_with_env(self, value=None):
        """Reload cache.sync with a given COROS_STABLE_DAYS env value."""
        env = os.environ.copy()
        if value is None:
            env.pop("COROS_STABLE_DAYS", None)
        else:
            env["COROS_STABLE_DAYS"] = value

        with patch.dict(os.environ, env, clear=True):
            import cache.sync as sync_mod
            importlib.reload(sync_mod)
            return sync_mod.STABLE_AFTER_DAYS

    def test_default_is_two(self):
        assert self._reload_module_with_env(None) == 2

    def test_env_var_overrides_default(self):
        assert self._reload_module_with_env("5") == 5

    def test_env_var_zero_disables_refetch(self):
        assert self._reload_module_with_env("0") == 0


# ---------------------------------------------------------------------------
# 4. cli.py — redundant asyncio import removed (static check)
# ---------------------------------------------------------------------------

class TestCliRedundantImport:

    def test_no_local_asyncio_import_in_cmd_sync(self):
        """cmd_sync must not contain a local `import asyncio` — it is already
        imported at module level."""
        import inspect
        import cli
        src = inspect.getsource(cli.cmd_sync)
        assert "import asyncio" not in src, (
            "Redundant `import asyncio` still present inside cmd_sync"
        )
