"""Cached fetch functions and full-sync logic.

Each fetch_*_cached() function:
  1. Checks what's already in the local SQLite cache.
  2. Only hits the Coros API for dates not yet cached (the "tail").
     Data within the last STABLE_AFTER_DAYS days is always re-fetched
     to pick up same-day activities and delayed watch→phone syncs.
  3. Writes new records back to the cache before returning.

sync_all() does a full historical backfill in 12-week chunks.
"""

import asyncio
import logging
import os
from datetime import datetime, timedelta
from typing import Callable, Coroutine, Optional

logger = logging.getLogger(__name__)

import coros_api
from cache.utils import LOCAL_TZ
from cache.store import (
    cache_status,
    get_activities,
    get_daily_records,
    get_max_activity_date,
    get_max_daily_date,
    get_max_sleep_date,
    get_min_activity_date,
    get_min_daily_date,
    get_min_sleep_date,
    get_sleep_records,
    init_db,
    upsert_activities,
    upsert_daily_records,
    upsert_sleep_records,
)
from models import ActivitySummary, DailyRecord, SleepRecord, StoredAuth


# ---------------------------------------------------------------------------
# Date helpers
# ---------------------------------------------------------------------------

def _date_add(day: str, days: int) -> str:
    dt = datetime.strptime(day, "%Y%m%d") + timedelta(days=days)
    return dt.strftime("%Y%m%d")


def _today() -> str:
    """Return today's date as YYYYMMDD in the configured local timezone."""
    if LOCAL_TZ is not None:
        return datetime.now(tz=LOCAL_TZ).strftime("%Y%m%d")
    return datetime.now().strftime("%Y%m%d")


# Data older than this many days is considered stable (immutable).
# Recent data is always re-fetched to capture same-day activities and
# delayed watch→phone syncs (HRV, sleep scores can arrive hours later).
# Override with COROS_STABLE_DAYS env var (e.g. set to 0 to disable re-fetch,
# or higher if your watch takes longer to sync to the phone).
STABLE_AFTER_DAYS = int(os.getenv("COROS_STABLE_DAYS", "2"))


def _fetch_start(max_cached: Optional[str], requested_start: str) -> str:
    """First date we need to fetch from the API.

    For historical data (older than STABLE_AFTER_DAYS), only fetch the
    uncached tail. For recent data, always re-fetch from the stable
    cutoff so that delayed syncs and same-day additions are picked up.
    """
    if max_cached is None:
        return requested_start
    cutoff = _date_add(_today(), -STABLE_AFTER_DAYS)
    # If max_cached is within the unstable window, re-fetch from cutoff.
    # Otherwise only fetch the uncached tail.
    stable_start = _date_add(max_cached, 1) if max_cached < cutoff else cutoff
    return max(stable_start, requested_start)


# ---------------------------------------------------------------------------
# Cached fetch wrappers
# ---------------------------------------------------------------------------

def _resolve_fetch_range(
    min_cached: Optional[str],
    max_cached: Optional[str],
    start_day: str,
    end_day: str,
    cutoff: str,
) -> Optional[tuple[str, str]]:
    """Determine the (fetch_from, fetch_to) range needed to satisfy [start_day, end_day].

    Returns None when the cache fully covers the requested range and no API
    call is needed.  Otherwise returns the tightest range that both satisfies
    the request and keeps the cache contiguous:

    - Empty cache            → fetch the entire requested range.
    - Historical gap         → start_day precedes min_cached; bridge rightward
                               to min_cached-1 so the new data joins the existing
                               cache without leaving a gap in the middle.
    - Tail gap / recent data → end_day exceeds max_cached, or falls within the
                               unstable window; fetch only the uncached tail.
    - Both gaps              → start_day < min_cached AND end_day > max_cached;
                               fetch the whole requested range.
    - Fully covered          → [min_cached, max_cached] contains [start_day,
                               end_day] and end_day is outside the unstable
                               window; no fetch required.
    """
    if max_cached is None:
        return (start_day, end_day)

    historical_gap = min_cached is not None and start_day < min_cached
    tail_gap = max_cached < end_day or end_day >= cutoff

    if historical_gap and tail_gap:
        return (start_day, end_day)

    if historical_gap:
        # Fetch from start_day and bridge up to the existing cache boundary so
        # the cache stays contiguous.  If end_day already reaches or overlaps
        # min_cached, no bridging needed beyond end_day.
        bridge_end = max(end_day, _date_add(min_cached, -1))
        if bridge_end > end_day:
            logger.warning(
                "Cache contiguity bridge extended fetch range: requested %s→%s, "
                "but fetching %s→%s to close gap to existing cache (min_cached=%s). "
                "This may fetch significantly more data than requested.",
                start_day, end_day, start_day, bridge_end, min_cached,
            )
        return (start_day, bridge_end)

    if tail_gap:
        return (_fetch_start(max_cached, start_day), end_day)

    return None  # fully covered


async def fetch_daily_records_cached(
    auth: StoredAuth, start_day: str, end_day: str
) -> list[DailyRecord]:
    """Return daily metrics for [start_day, end_day], fetching only what is not yet cached."""
    init_db()
    cutoff = _date_add(_today(), -STABLE_AFTER_DAYS)
    fetch_range = _resolve_fetch_range(
        get_min_daily_date(), get_max_daily_date(), start_day, end_day, cutoff
    )
    if fetch_range:
        new = await coros_api.fetch_daily_records(auth, *fetch_range)
        if new:
            upsert_daily_records(new)
    return get_daily_records(start_day, end_day)


async def fetch_sleep_cached(
    auth: StoredAuth, start_day: str, end_day: str
) -> list[SleepRecord]:
    """Return sleep records for [start_day, end_day], fetching only what is not yet cached."""
    init_db()
    cutoff = _date_add(_today(), -STABLE_AFTER_DAYS)
    fetch_range = _resolve_fetch_range(
        get_min_sleep_date(), get_max_sleep_date(), start_day, end_day, cutoff
    )
    if fetch_range:
        new = await coros_api.fetch_sleep(auth, *fetch_range)
        if new:
            upsert_sleep_records(new)
    return get_sleep_records(start_day, end_day)


async def fetch_activities_cached(
    auth: StoredAuth,
    start_day: str,
    end_day: str,
    page: int = 1,
    size: int = 30,
) -> tuple[list[ActivitySummary], int]:
    """Return activities for [start_day, end_day], fetching only what is not yet cached."""
    init_db()
    cutoff = _date_add(_today(), -STABLE_AFTER_DAYS)
    fetch_range = _resolve_fetch_range(
        get_min_activity_date(), get_max_activity_date(), start_day, end_day, cutoff
    )
    if fetch_range:
        await _fetch_all_activity_pages(auth, *fetch_range)
    cached = get_activities(start_day, end_day)
    start_idx = (page - 1) * size
    return cached[start_idx: start_idx + size], len(cached)


async def _fetch_all_activity_pages(
    auth: StoredAuth, start_day: str, end_day: str
) -> None:
    """Exhaust all pages for a date range and store results."""
    page_num = 1
    while True:
        acts, total = await coros_api.fetch_activities(
            auth, start_day, end_day, page=page_num, size=100
        )
        if acts:
            upsert_activities(acts)
        if not acts or page_num * 100 >= total:
            break
        page_num += 1
        await asyncio.sleep(0.3)


# ---------------------------------------------------------------------------
# Full historical sync
# ---------------------------------------------------------------------------

async def sync_all(
    auth: StoredAuth,
    start_day: str,
    end_day: Optional[str] = None,
    on_progress: Optional[Callable[[str], Coroutine]] = None,
) -> dict:
    """
    Backfill all data from start_day to end_day (default: today) in 12-week chunks.

    Overwrites any existing cache entries for the covered period so that
    corrected data from the Coros API always wins.

    Parameters
    ----------
    auth       : valid StoredAuth
    start_day  : YYYYMMDD — start of range (e.g. "20230101")
    end_day    : YYYYMMDD — end of range, defaults to today
    on_progress: optional async callable(msg: str) for progress updates

    Returns dict with sync statistics and final cache status.
    """
    init_db()
    today = _today()
    chunk_days = 12 * 7  # 12 weeks — within the API's 24-week dayDetail limit

    stop = end_day if end_day else today

    # Enforce cache continuity: if an explicit end_day falls before the current
    # cache frontier, extend stop to cover the gap, preventing a mid-range hole
    # between the newly synced range and the existing cached data.
    existing_max = max(
        (d for d in [
            get_max_daily_date(),
            get_max_sleep_date(),
            get_max_activity_date(),
        ] if d is not None),
        default=None,
    )
    if existing_max and existing_max > stop:
        stop = existing_max
    stats: dict = {"daily": 0, "sleep": 0, "activities": 0, "errors": []}

    async def _progress(msg: str) -> None:
        if on_progress:
            await on_progress(msg)

    cursor = start_day
    while cursor <= stop:
        chunk_end = min(_date_add(cursor, chunk_days - 1), stop)
        await _progress(f"syncing {cursor} → {chunk_end}")

        # --- daily records ---
        try:
            records = await coros_api.fetch_daily_records(auth, cursor, chunk_end)
            if records:
                upsert_daily_records(records)
                stats["daily"] += len(records)
        except Exception as exc:
            stats["errors"].append(f"daily {cursor}–{chunk_end}: {exc}")
        await asyncio.sleep(0.5)

        # --- sleep records ---
        try:
            sleeps = await coros_api.fetch_sleep(auth, cursor, chunk_end)
            if sleeps:
                upsert_sleep_records(sleeps)
                stats["sleep"] += len(sleeps)
        except Exception as exc:
            stats["errors"].append(f"sleep {cursor}–{chunk_end}: {exc}")
        await asyncio.sleep(0.5)

        # --- activities (all pages) ---
        try:
            await _fetch_all_activity_pages(auth, cursor, chunk_end)
            chunk_acts = get_activities(cursor, chunk_end)
            stats["activities"] += len(chunk_acts)
        except Exception as exc:
            stats["errors"].append(f"activities {cursor}–{chunk_end}: {exc}")
        await asyncio.sleep(0.5)

        cursor = _date_add(chunk_end, 1)

    await _progress("done")
    stats["cache"] = cache_status()
    return stats
