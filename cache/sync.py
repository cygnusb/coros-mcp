"""Cached fetch functions and full-sync logic.

Each fetch_*_cached() function:
  1. Checks what's already in the local SQLite cache.
  2. Only hits the Coros API for dates not yet cached (the "tail").
  3. Writes new records back to the cache before returning.

sync_all() does a full historical backfill in 12-week chunks.
"""

import asyncio
from datetime import datetime, timedelta
from typing import Callable, Coroutine, Optional

import coros_api
from cache.store import (
    cache_status,
    get_activities,
    get_daily_records,
    get_max_activity_date,
    get_max_daily_date,
    get_max_sleep_date,
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
    return datetime.now().strftime("%Y%m%d")


def _fetch_start(max_cached: Optional[str], requested_start: str) -> str:
    """First date we need to fetch from the API."""
    if max_cached is None:
        return requested_start
    day_after = _date_add(max_cached, 1)
    # Never go earlier than what was requested (cache may already cover that)
    return max(day_after, requested_start)


# ---------------------------------------------------------------------------
# Cached fetch wrappers
# ---------------------------------------------------------------------------

async def fetch_daily_records_cached(
    auth: StoredAuth, start_day: str, end_day: str
) -> list[DailyRecord]:
    """Return daily metrics for [start_day, end_day], fetching only the uncached tail."""
    init_db()
    max_cached = get_max_daily_date()

    if max_cached is None or max_cached < end_day:
        fetch_from = _fetch_start(max_cached, start_day)
        new = await coros_api.fetch_daily_records(auth, fetch_from, end_day)
        if new:
            upsert_daily_records(new)

    return get_daily_records(start_day, end_day)


async def fetch_sleep_cached(
    auth: StoredAuth, start_day: str, end_day: str
) -> list[SleepRecord]:
    """Return sleep records for [start_day, end_day], fetching only the uncached tail."""
    init_db()
    max_cached = get_max_sleep_date()

    if max_cached is None or max_cached < end_day:
        fetch_from = _fetch_start(max_cached, start_day)
        new = await coros_api.fetch_sleep(auth, fetch_from, end_day)
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
    """Return activities for [start_day, end_day], fetching only the uncached tail."""
    init_db()
    max_cached = get_max_activity_date()

    if max_cached is None or max_cached < end_day:
        fetch_from = _fetch_start(max_cached, start_day)
        await _fetch_all_activity_pages(auth, fetch_from, end_day)

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
