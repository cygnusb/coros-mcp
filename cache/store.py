"""SQLite-backed local store for Coros data.

DB location: ~/.config/coros-mcp/cache.db
Three data tables (daily_records, sleep_records, activities) plus a
sync_meta table that tracks the latest synced date per data type.
"""

import os
import sqlite3
import time
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

# Local timezone used to compute start_day (the calendar date visible to the user).
# Set COROS_TIMEZONE to your UTC offset in hours (e.g. "8" for CST, "-5" for EST).
# Defaults to the system local timezone when unset.
# This only affects the start_day index column; start_time/end_time are always stored
# as UTC Unix seconds (unchanged from the Coros API response).
_tz_offset = os.getenv("COROS_TIMEZONE")
_LOCAL_TZ: timezone | None = (
    timezone(timedelta(hours=int(_tz_offset))) if _tz_offset is not None else None
)

from models import ActivitySummary, DailyRecord, SleepRecord

CACHE_DB = Path.home() / ".config" / "coros-mcp" / "cache.db"

_CREATE_SQL = """
    CREATE TABLE IF NOT EXISTS daily_records (
        date TEXT PRIMARY KEY,
        data TEXT NOT NULL,
        synced_at INTEGER NOT NULL
    );
    CREATE TABLE IF NOT EXISTS sleep_records (
        date TEXT PRIMARY KEY,
        data TEXT NOT NULL,
        synced_at INTEGER NOT NULL
    );
    CREATE TABLE IF NOT EXISTS activities (
        activity_id TEXT PRIMARY KEY,
        start_day   TEXT NOT NULL,
        data        TEXT NOT NULL,
        synced_at   INTEGER NOT NULL
    );
    CREATE INDEX IF NOT EXISTS activities_start_day
        ON activities(start_day);
"""


@contextmanager
def _conn():
    CACHE_DB.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(CACHE_DB)
    con.row_factory = sqlite3.Row
    try:
        yield con
        con.commit()
    finally:
        con.close()


def init_db() -> None:
    """Create tables and indexes if they don't exist yet."""
    with _conn() as con:
        con.executescript(_CREATE_SQL)


# ---------------------------------------------------------------------------
# Daily records
# ---------------------------------------------------------------------------

def upsert_daily_records(records: list[DailyRecord]) -> None:
    now = int(time.time())
    with _conn() as con:
        con.executemany(
            "INSERT OR REPLACE INTO daily_records (date, data, synced_at) VALUES (?, ?, ?)",
            [(r.date, r.model_dump_json(), now) for r in records],
        )


def get_daily_records(start_day: str, end_day: str) -> list[DailyRecord]:
    with _conn() as con:
        rows = con.execute(
            "SELECT data FROM daily_records WHERE date >= ? AND date <= ? ORDER BY date",
            (start_day, end_day),
        ).fetchall()
    return [DailyRecord.model_validate_json(r["data"]) for r in rows]


def get_max_daily_date() -> Optional[str]:
    with _conn() as con:
        row = con.execute("SELECT MAX(date) AS d FROM daily_records").fetchone()
    return row["d"] if row and row["d"] else None


def get_min_daily_date() -> Optional[str]:
    with _conn() as con:
        row = con.execute("SELECT MIN(date) AS d FROM daily_records").fetchone()
    return row["d"] if row and row["d"] else None


# ---------------------------------------------------------------------------
# Sleep records
# ---------------------------------------------------------------------------

def upsert_sleep_records(records: list[SleepRecord]) -> None:
    now = int(time.time())
    with _conn() as con:
        con.executemany(
            "INSERT OR REPLACE INTO sleep_records (date, data, synced_at) VALUES (?, ?, ?)",
            [(r.date, r.model_dump_json(), now) for r in records],
        )


def get_sleep_records(start_day: str, end_day: str) -> list[SleepRecord]:
    with _conn() as con:
        rows = con.execute(
            "SELECT data FROM sleep_records WHERE date >= ? AND date <= ? ORDER BY date",
            (start_day, end_day),
        ).fetchall()
    return [SleepRecord.model_validate_json(r["data"]) for r in rows]


def get_max_sleep_date() -> Optional[str]:
    with _conn() as con:
        row = con.execute("SELECT MAX(date) AS d FROM sleep_records").fetchone()
    return row["d"] if row and row["d"] else None


def get_min_sleep_date() -> Optional[str]:
    with _conn() as con:
        row = con.execute("SELECT MIN(date) AS d FROM sleep_records").fetchone()
    return row["d"] if row and row["d"] else None


# ---------------------------------------------------------------------------
# Activities
# ---------------------------------------------------------------------------

def _activity_start_day(a: ActivitySummary) -> str:
    """Return YYYYMMDD local date for DB indexing.

    start_time is a UTC Unix seconds value (as returned by the Coros API).
    The local date is computed using COROS_TIMEZONE (UTC offset in hours) when set,
    otherwise falls back to the system local timezone via datetime.fromtimestamp().
    This date is used only for range queries — it is the calendar date as seen
    by the user, not the UTC date.
    """
    if not a.start_time:
        return ""
    s = a.start_time
    # UTC Unix timestamp (10 digits = seconds, 13 digits = milliseconds)
    if s.isdigit():
        if len(s) == 13:  # milliseconds
            ts = int(s) / 1000
        elif len(s) == 10:  # seconds
            ts = int(s)
        else:
            ts = None
        if ts is not None:
            if _LOCAL_TZ is not None:
                return datetime.fromtimestamp(ts, tz=_LOCAL_TZ).strftime("%Y%m%d")
            else:
                return datetime.fromtimestamp(ts).strftime("%Y%m%d")
    # YYYYMMDDHHMMSS or YYYYMMDD already encoded as string
    if len(s) >= 8 and s[:8].isdigit():
        return s[:8]
    return ""


def fmt_local_time(unix_secs: str | None) -> str | None:
    """Convert a UTC Unix seconds string to a local datetime string for display.

    Uses COROS_TIMEZONE (UTC offset in hours) when set, otherwise falls back
    to the system local timezone.  Returns None for missing/non-numeric values.

    Example: "1742079723" -> "2025-03-16 07:02:03" (on a UTC+8 system)
    """
    if not unix_secs or not str(unix_secs).isdigit():
        return unix_secs
    ts = int(unix_secs)
    if _LOCAL_TZ is not None:
        return datetime.fromtimestamp(ts, tz=_LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S")
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")


def upsert_activities(activities: list[ActivitySummary]) -> None:
    now = int(time.time())
    with _conn() as con:
        con.executemany(
            "INSERT OR REPLACE INTO activities (activity_id, start_day, data, synced_at) VALUES (?, ?, ?, ?)",
            [(a.activity_id, _activity_start_day(a), a.model_dump_json(), now) for a in activities],
        )


def get_activities(start_day: str, end_day: str) -> list[ActivitySummary]:
    with _conn() as con:
        rows = con.execute(
            "SELECT data FROM activities WHERE start_day >= ? AND start_day <= ? ORDER BY start_day DESC",
            (start_day, end_day),
        ).fetchall()
    return [ActivitySummary.model_validate_json(r["data"]) for r in rows]


def get_max_activity_date() -> Optional[str]:
    with _conn() as con:
        row = con.execute("SELECT MAX(start_day) AS d FROM activities").fetchone()
    return row["d"] if row and row["d"] else None


def get_min_activity_date() -> Optional[str]:
    with _conn() as con:
        row = con.execute("SELECT MIN(start_day) AS d FROM activities").fetchone()
    return row["d"] if row and row["d"] else None


# ---------------------------------------------------------------------------
# Cache status
# ---------------------------------------------------------------------------

def cache_status() -> dict:
    """Return record counts and date coverage for each data type."""
    init_db()
    with _conn() as con:
        def _stats(table: str, date_col: str = "date") -> dict:
            n = con.execute(f"SELECT COUNT(*) AS n FROM {table}").fetchone()["n"]
            lo = con.execute(f"SELECT MIN({date_col}) AS d FROM {table}").fetchone()["d"]
            hi = con.execute(f"SELECT MAX({date_col}) AS d FROM {table}").fetchone()["d"]
            return {"count": n, "from": lo, "to": hi}

        return {
            "daily_records": _stats("daily_records"),
            "sleep_records": _stats("sleep_records"),
            "activities":    _stats("activities", "start_day"),
            "db_path": str(CACHE_DB),
        }
