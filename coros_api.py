"""
Coros Training Hub API client.

Auth mechanism: MD5-hashed password + accessToken header.
HRV data comes from /dashboard/query (last 7 days of nightly RMSSD).
Sleep phase data is NOT available through the Training Hub web API.
"""

import hashlib
import json
import time
from pathlib import Path
from typing import Optional

import httpx

from models import HRVRecord, SleepPhases, SleepRecord, StoredAuth

# ---------------------------------------------------------------------------
# Endpoint constants
# ---------------------------------------------------------------------------

ENDPOINTS = {
    "login": "/account/login",
    "dashboard": "/dashboard/query",   # contains sleepHrvData (last 7 days)
    "sleep": "/sleep/query",           # NOT available on Training Hub API
}

# Login works on teamapi.coros.com but tokens are only valid on the
# region-specific API host.  Always use the regional URL for all calls.
BASE_URLS = {
    "eu": "https://teameuapi.coros.com",
    "us": "https://teamapi.coros.com",
}

AUTH_FILE = Path.home() / ".config" / "coros-mcp" / "auth.json"
TOKEN_TTL_MS = 24 * 60 * 60 * 1000  # 24 hours in milliseconds


# ---------------------------------------------------------------------------
# Token storage
# ---------------------------------------------------------------------------

def _save_auth(auth: StoredAuth) -> None:
    AUTH_FILE.parent.mkdir(parents=True, exist_ok=True)
    AUTH_FILE.write_text(auth.model_dump_json())
    AUTH_FILE.chmod(0o600)


def _load_auth() -> Optional[StoredAuth]:
    if not AUTH_FILE.exists():
        return None
    try:
        data = json.loads(AUTH_FILE.read_text())
        return StoredAuth(**data)
    except Exception:
        return None


def _is_token_valid(auth: StoredAuth) -> bool:
    now_ms = int(time.time() * 1000)
    return (now_ms - auth.timestamp) < TOKEN_TTL_MS


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

def _md5(value: str) -> str:
    return hashlib.md5(value.encode()).hexdigest()


def _base_url(region: str) -> str:
    return BASE_URLS.get(region, BASE_URLS["eu"])


async def login(email: str, password: str, region: str = "eu") -> StoredAuth:
    """Authenticate against Coros API and persist the token."""
    url = _base_url(region) + ENDPOINTS["login"]
    payload = {
        "account": email,
        "accountType": 2,
        "pwd": _md5(password),
    }
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(url, json=payload, headers={"Content-Type": "application/json"})
        resp.raise_for_status()
        body = resp.json()

    if body.get("result") != "0000":
        raise ValueError(f"Coros login failed: {body.get('message', 'unknown error')}")

    data = body.get("data", {})
    auth = StoredAuth(
        access_token=data["accessToken"],
        user_id=data["userId"],
        region=region,
        timestamp=int(time.time() * 1000),
    )
    _save_auth(auth)
    return auth


def get_stored_auth() -> Optional[StoredAuth]:
    """Return stored auth if it exists and is not expired."""
    auth = _load_auth()
    if auth and _is_token_valid(auth):
        return auth
    return None


# ---------------------------------------------------------------------------
# API headers
# ---------------------------------------------------------------------------

def _auth_headers(auth: StoredAuth) -> dict:
    return {
        "Content-Type": "application/json",
        "accessToken": auth.access_token,
        "yfheader": json.dumps({"userId": auth.user_id}),
    }


# ---------------------------------------------------------------------------
# HRV data  (confirmed: /dashboard/query → data.summaryInfo.sleepHrvData)
# ---------------------------------------------------------------------------

async def fetch_hrv(auth: StoredAuth) -> list[HRVRecord]:
    """
    Fetch nightly HRV data from the Coros dashboard endpoint.

    Returns the last ~7 days of data (whatever the API provides).
    There is no date-range parameter — the dashboard always returns recent data.
    """
    url = _base_url(auth.region) + ENDPOINTS["dashboard"]
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.get(url, headers=_auth_headers(auth))
        resp.raise_for_status()
        body = resp.json()

    if body.get("result") != "0000":
        raise ValueError(f"Coros dashboard API error: {body.get('message', 'unknown error')}")

    hrv_data = body.get("data", {}).get("summaryInfo", {}).get("sleepHrvData", {})
    records: list[HRVRecord] = []

    for item in hrv_data.get("sleepHrvList", []):
        records.append(HRVRecord(
            date=str(item.get("happenDay", "")),
            avg_sleep_hrv=item.get("avgSleepHrv"),
            baseline=item.get("sleepHrvBase"),
            standard_deviation=item.get("sleepHrvSd"),
            interval_list=item.get("sleepHrvIntervalList"),
        ))

    # Also include today's summary if available and not already in the list
    today_day = hrv_data.get("happenDay")
    if today_day and not any(r.date == str(today_day) for r in records):
        records.append(HRVRecord(
            date=str(today_day),
            avg_sleep_hrv=hrv_data.get("avgSleepHrv"),
            baseline=hrv_data.get("sleepHrvBase"),
            standard_deviation=hrv_data.get("sleepHrvSd"),
            interval_list=hrv_data.get("sleepHrvAllIntervalList"),
        ))

    return sorted(records, key=lambda r: r.date)


# ---------------------------------------------------------------------------
# Sleep data  (NOT available through Training Hub web API)
# ---------------------------------------------------------------------------

async def fetch_sleep(auth: StoredAuth, start_day: str, end_day: str) -> list[SleepRecord]:
    """
    Fetch sleep data for a date range.

    WARNING: Sleep phase data (deep/light/REM/awake) is NOT available through
    the Coros Training Hub web API.  This endpoint is a placeholder for when
    the correct mobile-app API endpoint is discovered.
    """
    url = _base_url(auth.region) + ENDPOINTS["sleep"]
    params = {
        "userId": auth.user_id,
        "startDay": start_day,
        "endDay": end_day,
    }
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.get(url, params=params, headers=_auth_headers(auth))
        resp.raise_for_status()
        body = resp.json()

    if body.get("result") != "0000":
        raise ValueError(f"Coros sleep API error: {body.get('message', 'unknown error')}")

    records: list[SleepRecord] = []
    for item in body.get("data", {}).get("list", []):
        phases = SleepPhases(
            deep_minutes=item.get("deepSleepMinutes"),
            light_minutes=item.get("lightSleepMinutes"),
            rem_minutes=item.get("remSleepMinutes"),
            awake_minutes=item.get("awakeSleepMinutes"),
        )
        records.append(SleepRecord(
            date=str(item.get("date", "")),
            total_duration_minutes=item.get("totalSleepMinutes"),
            phases=phases,
            sleep_start=item.get("sleepStartTime"),
            sleep_end=item.get("sleepEndTime"),
            quality_score=item.get("sleepScore"),
        ))
    return records
