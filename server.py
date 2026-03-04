"""
Coros MCP Server — Sleep & HRV data via the unofficial Coros Training Hub API.

Usage:
    python server.py

MCP config (Claude Code):
    claude mcp add coros \\
      -e COROS_EMAIL=you@example.com \\
      -e COROS_PASSWORD=yourpass \\
      -e COROS_REGION=eu \\
      -- python /path/to/coros-mcp/server.py
"""

import os
from datetime import datetime, timedelta

from dotenv import load_dotenv
from fastmcp import FastMCP

import coros_api

load_dotenv()

mcp = FastMCP("coros-mcp")


# ---------------------------------------------------------------------------
# Tool: authenticate_coros
# ---------------------------------------------------------------------------

@mcp.tool()
async def authenticate_coros(
    email: str,
    password: str,
    region: str = "eu",
) -> dict:
    """
    Authenticate with the Coros Training Hub API and store the access token.

    Parameters
    ----------
    email : str
        Coros account email address.
    password : str
        Coros account password (plain text — hashed with MD5 before sending).
    region : str
        "eu" (default) or "us".  EU users must use "eu" — tokens are
        region-bound (EU tokens only work on teameuapi.coros.com).

    Returns
    -------
    dict with keys: authenticated, user_id, region, message
    """
    try:
        auth = await coros_api.login(email, password, region)
        return {
            "authenticated": True,
            "user_id": auth.user_id,
            "region": auth.region,
            "message": f"Token stored at {coros_api.AUTH_FILE}",
        }
    except Exception as exc:
        return {
            "authenticated": False,
            "error": str(exc),
        }


# ---------------------------------------------------------------------------
# Tool: check_coros_auth
# ---------------------------------------------------------------------------

@mcp.tool()
async def check_coros_auth() -> dict:
    """
    Check whether a valid Coros access token is stored locally.

    Returns
    -------
    dict with keys: authenticated, user_id, region, expires_in_hours (approx)
    """
    auth = coros_api.get_stored_auth()
    if auth is None:
        return {
            "authenticated": False,
            "message": "No valid token found. Call authenticate_coros first.",
        }

    import time
    age_ms = int(time.time() * 1000) - auth.timestamp
    remaining_ms = coros_api.TOKEN_TTL_MS - age_ms
    remaining_hours = round(remaining_ms / 3_600_000, 1)

    return {
        "authenticated": True,
        "user_id": auth.user_id,
        "region": auth.region,
        "expires_in_hours": remaining_hours,
    }


# ---------------------------------------------------------------------------
# Tool: get_hrv_data
# ---------------------------------------------------------------------------

@mcp.tool()
async def get_hrv_data() -> dict:
    """
    Retrieve nightly HRV (RMSSD) data from Coros for the last ~7 days.

    Data comes from the dashboard endpoint — no date range parameter available.
    Returns whatever the API provides (typically the last 7 nights).

    Returns
    -------
    dict with keys: records (list of HRV records), count
    Each record contains:
      - date: YYYYMMDD
      - avg_sleep_hrv: average nightly RMSSD in ms
      - baseline: rolling baseline RMSSD
      - standard_deviation: RMSSD standard deviation
      - interval_list: percentile band boundaries
    """
    auth = coros_api.get_stored_auth()
    if auth is None:
        return {
            "error": "Not authenticated. Call authenticate_coros first.",
            "records": [],
        }

    try:
        records = await coros_api.fetch_hrv(auth)
        return {
            "records": [r.model_dump() for r in records],
            "count": len(records),
        }
    except Exception as exc:
        return {"error": str(exc), "records": []}


# ---------------------------------------------------------------------------
# Tool: get_sleep_data
# ---------------------------------------------------------------------------

@mcp.tool()
async def get_sleep_data(date: str, days: int = 1) -> dict:
    """
    Retrieve sleep data from Coros for one or more days.

    WARNING: Sleep phase data (deep/light/REM/awake) is NOT yet available
    through the Training Hub web API.  This tool is a placeholder — it will
    return an error until the correct mobile-app endpoint is discovered.

    Parameters
    ----------
    date : str
        Start date in YYYYMMDD format (e.g. "20240315").
    days : int
        Number of days to fetch (1–30). Default: 1.

    Returns
    -------
    dict with keys: records (list of sleep records), count
    """
    auth = coros_api.get_stored_auth()
    if auth is None:
        return {
            "error": "Not authenticated. Call authenticate_coros first.",
            "records": [],
        }

    days = max(1, min(days, 30))
    start_dt = datetime.strptime(date, "%Y%m%d")
    end_dt = start_dt + timedelta(days=days - 1)
    end_day = end_dt.strftime("%Y%m%d")

    try:
        records = await coros_api.fetch_sleep(auth, date, end_day)
        return {
            "records": [r.model_dump() for r in records],
            "count": len(records),
            "date_range": f"{date} – {end_day}",
        }
    except Exception as exc:
        return {
            "error": str(exc),
            "hint": "Sleep phase data is not available through the Training Hub "
                    "web API. The endpoint needs to be discovered from the Coros "
                    "mobile app API.",
            "records": [],
        }


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    mcp.run()


if __name__ == "__main__":
    main()
