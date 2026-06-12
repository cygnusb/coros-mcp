"""Shared utilities for the cache package."""

import os
import re
from datetime import datetime, timedelta, timezone


def _parse_tz_offset(value: str) -> timezone:
    """Parse a COROS_TIMEZONE string into a timezone object.

    Accepted formats (all may be prefixed with + or -):
      ISO-style:   "+05:30", "-05:30", "5:30"
      Float hours: "5.5", "-5.75"
      Integer:     "8", "-5"

    Raises ValueError for unrecognised input.
    """
    value = value.strip()
    # ISO-style ±HH:MM or H:MM
    m = re.fullmatch(r"([+-]?\d{1,2}):(\d{2})", value)
    if m:
        sign = -1 if value.startswith("-") else 1
        hours = int(m.group(1).lstrip("+-"))
        minutes = int(m.group(2))
        return timezone(sign * timedelta(hours=hours, minutes=minutes))
    # Float or integer hours
    return timezone(timedelta(hours=float(value)))


# Local timezone for display formatting.
# Set COROS_TIMEZONE to your UTC offset in hours (e.g. "8", "-5", "5.5", "+05:30").
# Defaults to the system local timezone when unset.
_tz_offset = os.getenv("COROS_TIMEZONE")
LOCAL_TZ: timezone | None = _parse_tz_offset(_tz_offset) if _tz_offset is not None else None


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
