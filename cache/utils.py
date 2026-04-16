"""Shared utilities for the cache package."""

import os
from datetime import datetime, timedelta, timezone

# Local timezone for display formatting.
# Set COROS_TIMEZONE to your UTC offset in hours (e.g. "8" for CST, "-5" for EST).
# Defaults to the system local timezone when unset.
_tz_offset = os.getenv("COROS_TIMEZONE")
LOCAL_TZ: timezone | None = (
    timezone(timedelta(hours=int(_tz_offset))) if _tz_offset is not None else None
)


def fmt_local_time(unix_secs: str | None) -> str | None:
    """Convert a UTC Unix seconds string to a local datetime string for display.

    Uses COROS_TIMEZONE (UTC offset in hours) when set, otherwise falls back
    to the system local timezone.  Returns None for missing/non-numeric values.

    Example: "1742079723" -> "2025-03-16 07:02:03" (on a UTC+8 system)
    """
    if not unix_secs or not str(unix_secs).isdigit():
        return unix_secs
    ts = int(unix_secs)
    if LOCAL_TZ is not None:
        return datetime.fromtimestamp(ts, tz=LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S")
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
