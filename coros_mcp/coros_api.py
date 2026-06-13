"""
Coros Training Hub API client.

Auth mechanism: MD5-hashed password + accessToken header.
HRV data comes from /dashboard/query (last 7 days of nightly RMSSD).
Sleep phase data comes from the mobile API (/coros/data/statistic/daily on apieu.coros.com).
"""

import asyncio
import contextlib
import hashlib
import json
import logging
import os
import random
import time

import httpx

from coros_mcp.auth.storage import get_token, store_token
from coros_mcp.models import (
    ActivitySummary,
    DailyRecord,
    HRVRecord,
    SleepPhases,
    SleepRecord,
    StoredAuth,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Endpoint constants
# ---------------------------------------------------------------------------

USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36"  # noqa: E501

MOBILE_LOGIN_ENDPOINT = "/coros/user/login"

# AES key hardcoded in libencrypt-lib.so (reverse-engineered from Coros APK)
_MOBILE_AES_IV = b"weloop3_2015_03#"

ENDPOINTS = {
    "login": "/account/login",
    "dashboard": "/dashboard/query",        # contains sleepHrvData (last 7 days)
    "analyse": "/analyse/query",            # summary + t7dayList (28 days, has VO2max/fitness)
    "analyse_detail": "/analyse/dayDetail/query",  # daily metrics with date range (up to 24 weeks)
    "sleep": "/coros/data/statistic/daily",  # mobile API (apieu.coros.com)
    "activity_list": "/activity/query",
    "activity_detail": "/activity/detail/query",
    "sport_types": "/activity/fit/getImportSportList",
    "workout_list": "/training/program/query",  # POST — list/fetch workout programs
    "plan_list": "/training/plan/query",         # POST — list training plans
    "workout_add": "/training/program/add",     # POST — create new structured workout
    "workout_calculate": "/training/program/calculate",  # POST — recalc distance/time/load/chart
    "workout_delete": "/training/program/delete",  # POST — delete workout(s), body: ["id1", ...]
    "schedule_sum": "/training/schedule/querysum",  # GET — planned calendar aggregates
    "schedule": "/training/schedule/query",         # GET — planned calendar detail
    "schedule_update": "/training/schedule/update", # POST — add workout to calendar
    "exercises": "/training/exercise/query",        # GET — exercise catalogue by sport type
}

# Login works on teamapi.coros.com but tokens are only valid on the
# region-specific API host.  Always use the regional URL for all calls.
BASE_URLS = {
    "eu": "https://teameuapi.coros.com",
    "us": "https://teamapi.coros.com",
    "asia": "https://teamcnapi.coros.com",
    "cn": "https://teamcnapi.coros.com",
}

# Mobile app API — used for sleep data (different host from Training Hub web API)
MOBILE_BASE_URLS = {
    "eu": "https://apieu.coros.com",
    "us": "https://api.coros.com",
    "asia": "https://apicn.coros.com",
    "cn": "https://apicn.coros.com",
}

TOKEN_TTL_MS = 24 * 60 * 60 * 1000  # 24 hours in milliseconds


class CorosAPIError(ValueError):
    """Coros API returned a non-success result code.

    Carries the raw result code so callers can distinguish auth failures
    (retryable after re-login) from other API errors.
    """

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


def _check_response(body: dict, context: str) -> None:
    """Raise CorosAPIError if the Coros API response indicates an error."""
    if body.get("result") != "0000":
        raise CorosAPIError(
            str(body.get("result")),
            f"Coros {context} error: {body.get('message', 'unknown error')} "
            f"(result={body.get('result')})",
        )


# ---------------------------------------------------------------------------
# Token storage  (keyring → encrypted file, managed by auth.storage)
# ---------------------------------------------------------------------------

def _save_auth(auth: StoredAuth) -> None:
    store_token(auth.model_dump_json())


def _load_auth() -> StoredAuth | None:
    result = get_token()
    if not result.success or not result.token:
        return None
    try:
        data = json.loads(result.token)
        return StoredAuth(**data)
    except Exception:
        logger.debug("Failed to parse stored auth blob", exc_info=True)
        return None


def _is_token_valid(auth: StoredAuth) -> bool:
    now_ms = int(time.time() * 1000)
    return (now_ms - auth.timestamp) < TOKEN_TTL_MS


# ---------------------------------------------------------------------------
# Mobile API encryption  (AES-128-CBC, key reverse-engineered from APK)
# ---------------------------------------------------------------------------

def _mobile_encrypt(plaintext: str, app_key: str) -> str:
    """
    Encrypt a string for the Coros mobile login API.

    Scheme reverse-engineered from libencrypt-lib.so in the Coros Android APK:
      1. XOR plaintext bytes with appKey bytes cyclically
      2. PKCS7-pad the XOR'd result to a 16-byte boundary
      3. AES-128-CBC encrypt: key = appKey bytes, IV = 'weloop3_2015_03#'
      4. Base64-encode the ciphertext
    """
    import base64

    from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

    key = app_key.encode("ascii")
    data = plaintext.encode("utf-8")
    xored = bytes(b ^ key[i % len(key)] for i, b in enumerate(data))
    pad_len = 16 - (len(xored) % 16)
    padded = xored + bytes([pad_len] * pad_len)
    encryptor = Cipher(algorithms.AES(key), modes.CBC(_MOBILE_AES_IV)).encryptor()
    ciphertext = encryptor.update(padded) + encryptor.finalize()
    return base64.b64encode(ciphertext).decode("ascii")


async def _mobile_login(email: str, password: str, region: str = "eu") -> tuple[str, dict]:
    """
    Authenticate against the Coros mobile API with encrypted credentials.

    Returns (access_token, login_payload_for_replay).
    The login_payload can be replayed to refresh the token without re-entering credentials.
    """
    mobile_base = MOBILE_BASE_URLS.get(region, MOBILE_BASE_URLS["eu"])
    url = mobile_base + MOBILE_LOGIN_ENDPOINT
    app_key = str(random.randint(1_000_000_000_000_000, 9_999_999_999_999_999))
    payload = {
        "account": _mobile_encrypt(email, app_key) + "\n",
        "accountType": 2,
        "appKey": app_key,
        "clientType": 1,
        "hasHrCalibrated": 0,
        "kbValidity": 0,
        "pwd": _mobile_encrypt(_md5(password), app_key) + "\n",
        "region": "310|Europe/Berlin|US",
        "skipValidation": False,
    }
    yfheader = json.dumps({
        "appVersion": 1125917087236096,
        "clientType": 1,
        "language": "en-US",
        "mobileName": "sdk_gphone64_arm64,google,Google",
        "releaseType": 1,
        "systemVersion": "13",
        "timezone": 4,
        "versionCode": "404080400",
    }, separators=(",", ":"))
    headers = {
        "content-type": "application/json",
        "accept-encoding": "gzip",
        "user-agent": "okhttp/4.12.0",
        "request-time": str(int(time.time() * 1000)),
        "yfheader": yfheader,
    }
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(url, json=payload, headers=headers)
        resp.raise_for_status()
        body = resp.json()

    _check_response(body, "mobile login")

    token = body.get("data", {}).get("accessToken")
    if not token:
        raise ValueError("No accessToken in Coros mobile login response")

    return token, payload


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

def _md5(value: str) -> str:
    return hashlib.md5(value.encode()).hexdigest()


def _base_url(region: str) -> str:
    return BASE_URLS.get(region, BASE_URLS["eu"])


async def login(email: str, password: str, region: str = "eu", *, skip_mobile: bool = True) -> StoredAuth:
    """Authenticate against Coros API and persist the token."""
    pwd_hash = _md5(password)
    login_payload = {
        "account": email,
        "accountType": 2,
        "pwd": pwd_hash,
    }
    json_headers = {"Content-Type": "application/json", "User-Agent": USER_AGENT}

    async with httpx.AsyncClient(timeout=30) as client:
        # Training Hub token (teameuapi.coros.com)
        resp = await client.post(
            _base_url(region) + ENDPOINTS["login"],
            json=login_payload,
            headers=json_headers,
        )
        resp.raise_for_status()
        body = resp.json()

        _check_response(body, "login")

        data = body.get("data", {})

    # Mobile API token (apieu.coros.com) — needed for sleep data
    # Uses AES-encrypted credentials (key reverse-engineered from libencrypt-lib.so)
    mobile_token = None
    mobile_payload = None
    if not skip_mobile:
        try:
            mobile_token, mobile_payload = await _mobile_login(email, password, region)
        except Exception:
            logger.debug("Mobile login failed during combined login", exc_info=True)

    auth = StoredAuth(
        access_token=data["accessToken"],
        user_id=data["userId"],
        region=region,
        timestamp=int(time.time() * 1000),
        mobile_access_token=mobile_token,
        mobile_login_payload=mobile_payload,
    )
    _save_auth(auth)
    return auth


async def login_mobile(email: str, password: str, region: str = "eu") -> StoredAuth:
    """Authenticate against the Coros mobile API only and persist the token.

    If an existing StoredAuth exists, updates the mobile fields and the
    region (a mobile token is only valid on its regional host, and a region
    switch invalidates the old web token anyway). Otherwise creates a
    minimal StoredAuth with only mobile credentials.
    """
    mobile_token, mobile_payload = await _mobile_login(email, password, region)

    existing = _load_auth()
    if existing:
        if existing.region != region:
            logger.warning(
                "Mobile login region %r differs from stored region %r — "
                "updating stored region; re-run web auth if web calls fail.",
                region, existing.region,
            )
        existing = existing.model_copy(update={
            "region": region,
            "mobile_access_token": mobile_token,
            "mobile_login_payload": mobile_payload,
        })
        _save_auth(existing)
        return existing

    auth = StoredAuth(
        access_token="",
        user_id="",
        region=region,
        timestamp=int(time.time() * 1000),
        mobile_access_token=mobile_token,
        mobile_login_payload=mobile_payload,
    )
    _save_auth(auth)
    return auth


def get_stored_auth() -> StoredAuth | None:
    """Return stored auth if it exists and is not expired.

    When COROS_ACCESS_TOKEN env var is set, it replaces only the web access
    token (for MCP server use cases where keyring is not accessible in the
    subprocess). Stored user_id, region, and mobile token/payload are kept
    so sleep data still works alongside an env-provided web token.
    """
    # Prefer explicit env var token when provided
    access_token = os.environ.get("COROS_ACCESS_TOKEN")
    if access_token:
        stored = _load_auth()
        region = os.environ.get("COROS_REGION") or (stored.region if stored else "eu")
        # Timestamp is set to now so the TTL check always passes — env-var
        # tokens are assumed to be externally managed and always valid.
        return StoredAuth(
            access_token=access_token,
            user_id=stored.user_id if stored else "env",
            region=region,
            timestamp=int(time.time() * 1000),
            mobile_access_token=stored.mobile_access_token if stored else None,
            mobile_login_payload=stored.mobile_login_payload if stored else None,
        )
    # Fall back to stored auth
    auth = _load_auth()
    if auth and _is_token_valid(auth):
        return auth
    return None


def get_env_credentials() -> tuple[str, str, str] | None:
    """Return (email, password, region) from env vars, or None if not fully set."""
    email = os.environ.get("COROS_EMAIL")
    password = os.environ.get("COROS_PASSWORD")
    region = os.environ.get("COROS_REGION", "eu")
    if email and password:
        return email, password, region
    return None


async def try_auto_login() -> StoredAuth | None:
    """Attempt login using COROS_EMAIL/PASSWORD env vars. Returns None on failure.

    Always skips mobile login — the mobile token is obtained lazily on the first
    call to fetch_sleep(), so the Coros mobile app session is never disrupted by
    routine web-token refreshes.
    """
    creds = get_env_credentials()
    if creds is None:
        return None
    email, password, region = creds
    try:
        return await login(email, password, region)  # skip_mobile=True by default
    except Exception:
        logger.debug("Auto-login from env credentials failed", exc_info=True)
        return None


# ---------------------------------------------------------------------------
# API headers
# ---------------------------------------------------------------------------

def _auth_headers(auth: StoredAuth) -> dict:
    return {
        "Content-Type": "application/json",
        "User-Agent": USER_AGENT,
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

    _check_response(body, "dashboard")

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
# Daily analysis data  (/analyse/dayDetail/query — up to 24 weeks)
# ---------------------------------------------------------------------------

def _parse_daily_record(item: dict) -> DailyRecord:
    """Parse a single day record from either endpoint."""
    return DailyRecord(
        date=str(item.get("happenDay", "")),
        avg_sleep_hrv=item.get("avgSleepHrv"),
        baseline=item.get("sleepHrvBase"),
        interval_list=item.get("sleepHrvIntervalList"),
        rhr=item.get("rhr"),
        training_load=item.get("trainingLoad"),
        training_load_ratio=item.get("trainingLoadRatio"),
        tired_rate=item.get("tiredRateNew"),
        ati=item.get("ati"),
        cti=item.get("cti"),
        performance=item.get("performance"),
        distance=item.get("distance"),
        duration=item.get("duration"),
        vo2max=item.get("vo2max"),
        lthr=item.get("lthr"),
        ltsp=item.get("ltsp"),
        stamina_level=item.get("staminaLevel"),
        stamina_level_7d=item.get("staminaLevel7d"),
    )


async def fetch_daily_records(
    auth: StoredAuth, start_day: str, end_day: str
) -> list[DailyRecord]:
    """
    Fetch daily metrics (HRV, RHR, training load, VO2max, etc.) for a date range.

    Merges data from two endpoints:
    - /analyse/dayDetail/query: supports up to ~24 weeks (no VO2max/fitness)
    - /analyse/query: last ~28 days with VO2max, LTHR, stamina (merged in)
    """
    headers = _auth_headers(auth)
    base = _base_url(auth.region)

    async with httpx.AsyncClient(timeout=30) as client:
        detail_resp, analyse_resp = await asyncio.gather(
            client.get(
                base + ENDPOINTS["analyse_detail"],
                params={"startDay": start_day, "endDay": end_day},
                headers=headers,
            ),
            client.get(
                base + ENDPOINTS["analyse"],
                headers=headers,
            ),
        )
    detail_resp.raise_for_status()
    detail_body = detail_resp.json()
    analyse_resp.raise_for_status()
    analyse_body = analyse_resp.json()

    _check_response(detail_body, "analyse")

    # Build records from dayDetail (long range)
    records_by_date: dict[str, DailyRecord] = {}
    for item in detail_body.get("data", {}).get("dayList", []):
        rec = _parse_daily_record(item)
        records_by_date[rec.date] = rec

    # Merge VO2max/fitness fields from t7dayList (last ~28 days)
    if analyse_body.get("result") == "0000":
        for item in analyse_body.get("data", {}).get("t7dayList", []):
            date = str(item.get("happenDay", ""))
            if date in records_by_date:
                rec = records_by_date[date]
                if (v := item.get("vo2max")) is not None:
                    rec.vo2max = v
                if (v := item.get("lthr")) is not None:
                    rec.lthr = v
                if (v := item.get("ltsp")) is not None:
                    rec.ltsp = v
                if (v := item.get("staminaLevel")) is not None:
                    rec.stamina_level = v
                if (v := item.get("staminaLevel7d")) is not None:
                    rec.stamina_level_7d = v

    return sorted(records_by_date.values(), key=lambda r: r.date)


# ---------------------------------------------------------------------------
# Activity data
# ---------------------------------------------------------------------------

SPORT_NAMES: dict[int, str] = {
    100: "Running", 102: "Trail Running", 103: "Track Running", 104: "Hiking",
    200: "Road Bike", 201: "Indoor Cycling", 203: "Gravel Bike", 204: "MTB",
    400: "Cardio", 402: "Strength", 403: "Yoga",
    900: "Walking", 9807: "Bike Commute",
}


def _parse_activity(item: dict) -> ActivitySummary:
    sport_type = item.get("sportType")
    # The Coros API field "calorie" is in physical calories (cal), NOT kilocalories (kcal).
    # A typical 60-minute run returns ~600 000 cal, which equals 600 kcal.
    # This is counterintuitive because consumer fitness apps and nutrition labels
    # always display energy in kcal (sometimes written as "Calories" with a capital C).
    # We store the raw value as-is; callers must divide by 1000 to get kcal.
    cal_raw = item.get("calorie")
    return ActivitySummary(
        activity_id=str(item.get("labelId", "")),
        name=item.get("name") or item.get("remark"),
        sport_type=sport_type,
        sport_name=SPORT_NAMES.get(sport_type, f"Sport {sport_type}") if sport_type else None,
        start_time=str(item["startTime"]) if item.get("startTime") else None,
        end_time=str(item["endTime"]) if item.get("endTime") else None,
        duration_seconds=item.get("totalTime"),
        distance_meters=item.get("distance") if item.get("distance") is not None else item.get("totalDistance"),
        avg_hr=item.get("avgHr"),
        max_hr=item.get("maxHr"),
        calories=cal_raw,
        training_load=item.get("trainingLoad"),
        avg_power=item.get("avgPower"),
        normalized_power=item.get("np"),
        elevation_gain=(
            item.get("ascent")
            if item.get("ascent") is not None
            else (item.get("totalAscent") if item.get("totalAscent") is not None else item.get("elevationGain"))
        ),
        elevation_loss=item.get("descent") if item.get("descent") is not None else item.get("totalDescent"),  # noqa: E501
    )


async def fetch_activities(
    auth: StoredAuth,
    start_day: str,
    end_day: str,
    page: int = 1,
    size: int = 30,
    mode_list: list[int] | None = None,
) -> tuple[list[ActivitySummary], int]:
    """
    Fetch activity list for a date range.
    Returns (activities, total_count).
    """
    params: dict = {
        "startDay": start_day,
        "endDay": end_day,
        "pageNumber": page,
        "size": size,
    }
    if mode_list:
        params["modeList"] = ",".join(str(m) for m in mode_list)

    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.get(
            _base_url(auth.region) + ENDPOINTS["activity_list"],
            params=params,
            headers=_auth_headers(auth),
        )
        resp.raise_for_status()
        body = resp.json()

    _check_response(body, "activity list")

    data = body.get("data", {})
    items = data.get("dataList", data.get("list", []))
    total = data.get("totalCount") or data.get("count") or len(items)
    return [_parse_activity(i) for i in items], total


async def fetch_activity_detail(auth: StoredAuth, activity_id: str, sport_type: int = 0) -> dict:
    """
    Fetch full activity detail including laps, HR zones, and metrics.
    Returns raw API data dict.
    Requires sport_type (e.g. 200=Road Bike, 201=Indoor Cycling, 100=Running).
    """
    headers = {k: v for k, v in _auth_headers(auth).items() if k != "Content-Type"}
    url = _base_url(auth.region) + ENDPOINTS["activity_detail"]
    form_data = {"labelId": activity_id, "userId": auth.user_id, "sportType": str(sport_type)}

    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(url, data=form_data, headers=headers)
        resp.raise_for_status()
        body = resp.json()

    _check_response(body, "activity detail")

    data = body.get("data", {})
    # Strip large time-series arrays that bloat the response
    for key in ("graphList", "frequencyList", "gpsLightDuration"):
        data.pop(key, None)
    return data


# ---------------------------------------------------------------------------
# Workout programs  (/training/program/query + /training/program/add)
# ---------------------------------------------------------------------------

# sportType=2 = Indoor Cycling (indoor trainer); intensityType=6 = power in watts
# targetType=2 = time-based (seconds); exerciseType=2 = cycling block
# IntensityType values: 1=weight, 2=HR, 3=pace, 4=speed, 5=none, 6=power, 7=cadence

# Note: the workout API uses sportType=1 for Running; the activity API uses
# 100 (and 102 Trail, 103 Track). _build_workout_program_payload maps the
# activity-side run IDs → 1 on the way out. The old 100: "Running" entry here
# never round-tripped, because the API only ever speaks sportType=1 for runs.
# Keyed by WORKOUT-namespace (wire) sport IDs — the sportType the workout API
# stores and returns, not the activity-namespace IDs callers pass in. All run
# flavors (activity 100/102/103) are collapsed to wire 1 on write, so a run
# fetched back always reads as wire 1 here. Only consult this with wire IDs;
# map activity IDs through _RUNNING_ACTIVITY_SPORT_TYPES first.
WORKOUT_SPORT_NAMES: dict[int, str] = {
    1: "Running",
    2: "Indoor Cycling",
    4: "Strength",
    200: "Road Bike",
    201: "Indoor Cycling (alt)",
}

# Activity-namespace sport IDs that are run flavors. The workout API has no
# separate trail/track/treadmill workout type — they all collapse to the
# single Running wire ID (sportType=1) and carry the same metadata block.
_RUNNING_ACTIVITY_SPORT_TYPES = frozenset({100, 102, 103})

# Cycling sport IDs that pass through to the wire unchanged (no namespace
# remap, no running metadata block): 2 Indoor Cycling, 200 Road Bike,
# 201 Indoor Cycling (alt).
_CYCLING_SPORT_TYPES = frozenset({2, 200, 201})

# Every sport_type _build_workout_program_payload accepts. Anything else is
# rejected rather than emitted as-is: an unknown ID would otherwise produce a
# cycling-shaped payload with a bogus wire sportType that fails silently on
# the COROS side. (Strength uses a separate builder and is not listed here.)
_KNOWN_SPORT_TYPES = _RUNNING_ACTIVITY_SPORT_TYPES | _CYCLING_SPORT_TYPES


def _parse_workout(item: dict) -> dict:
    exercises = []
    for ex in item.get("exercises", []):
        exercises.append({
            "name": ex.get("name"),
            "duration_seconds": ex.get("targetValue"),
            "intensity_low": ex.get("intensityValue"),
            "intensity_high": ex.get("intensityValueExtend"),
            "sets": ex.get("sets", 1),
        })
    # sportType from the workout API is always a wire ID (runs come back as 1,
    # never 100/102/103), so the wire-keyed lookup below is correct here.
    sport = item.get("sportType")
    return {
        "id": str(item.get("id", "")),
        "name": item.get("name"),
        "sport_type": sport,
        "sport_name": WORKOUT_SPORT_NAMES.get(sport, f"Sport {sport}") if sport is not None else None,
        "estimated_time_seconds": item.get("estimatedTime"),
        "exercise_count": item.get("exerciseNum", len(exercises)),
        "exercises": exercises,
    }


async def fetch_workout_templates(auth: StoredAuth) -> list[dict]:
    """List all reusable workout templates in the user's library."""
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            _base_url(auth.region) + ENDPOINTS["workout_list"],
            json={},
            headers=_auth_headers(auth),
        )
        resp.raise_for_status()
        body = resp.json()

    _check_response(body, "workout list")

    return [_parse_workout(w) for w in body.get("data", [])]


def _parse_training_plan(item: dict) -> dict:
    programs = item.get("programs", [])
    entities = item.get("entities", [])
    return {
        "id": str(item.get("id", "")),
        "name": item.get("name"),
        "overview": item.get("overview"),
        "status": item.get("status"),
        "execute_status": item.get("executeStatus"),
        "start_day": item.get("startDay"),
        "end_day": item.get("endDay"),
        "total_day": item.get("totalDay"),
        "min_weeks": item.get("minWeeks"),
        "max_weeks": item.get("maxWeeks"),
        "program_count": len(programs),
        "entity_count": len(entities),
    }


async def _fetch_training_plans_data(
    auth: StoredAuth, status_list: list[int] | None = None
) -> list[dict]:
    """Shared POST for /training/plan/query. Returns the raw plan list."""
    payload = {"statusList": status_list or [1, 2]}
    params = {"teamId": "", "userId": ""}
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            _base_url(auth.region) + ENDPOINTS["plan_list"],
            params=params,
            json=payload,
            headers=_auth_headers(auth),
        )
        resp.raise_for_status()
        body = resp.json()

    _check_response(body, "training plan list")
    return body.get("data", [])


async def fetch_training_plans(
    auth: StoredAuth,
    status_list: list[int] | None = None,
) -> list[dict]:
    """List user training plans (summarized). Defaults to statusList [1, 2]."""
    return [_parse_training_plan(p) for p in await _fetch_training_plans_data(auth, status_list)]


async def fetch_training_plans_raw(
    auth: StoredAuth,
    status_list: list[int] | None = None,
) -> list[dict]:
    """List user training plans without stripping API fields."""
    return await _fetch_training_plans_data(auth, status_list)


def _build_workout_program_payload(
    name: str,
    steps: list[dict],
    sport_type: int = 2,
    intensity_type: int | None = None,
) -> dict:
    """Sync builder for the cycling/intervals/running program dict.

    steps: list of dicts — either plain steps or repeat groups (see
    save_workout_template docstring).

    sport_type uses the activity namespace (the IDs list_activities
    returns), not the workout-API wire IDs:

      - 2   = Indoor Cycling (default)
      - 200 = Road Bike, 201 = Indoor Cycling (alt)
      - 100 = Running, 102 = Trail Running, 103 = Track Running
        (all mapped to the workout-side wire ID sportType=1 and given the
        metadata block COROS requires for runs)

    Passing the wire ID 1 directly is rejected — callers must use the
    activity-side run IDs so the running metadata block is always applied.
    Cycling (the default) is unchanged.

    intensity_type=None resolves per sport: runs default to HR (2), the
    natural running target; cycling/everything else defaults to power (6).
    Pass an explicit value to override.
    """
    if not steps:
        raise ValueError("workout requires at least one step")
    exercises: list[dict] = []
    top_index = 0  # counts top-level positions for sortNo
    total_seconds = 0
    ex_id = 0  # sequential exercise IDs (API uses these to link groups)

    # Workout API uses a single Running wire ID (sportType=1); the activity
    # API splits runs into 100 (Running), 102 (Trail), 103 (Track). Accept
    # the activity-side IDs (what list_activities returns) and map them onto
    # the wire ID. Reject the wire ID itself so a caller can't slip past the
    # running metadata block and reproduce the app-crash / strength-render bug.
    if sport_type == 1:
        raise ValueError(
            "Pass sport_type=100 for running (activity-namespace ID); 1 is the "
            "internal workout-API ID and must not be passed directly."
        )
    if sport_type not in _KNOWN_SPORT_TYPES:
        raise ValueError(
            f"Unknown sport_type={sport_type}. Supported: "
            "100/102/103 (Running/Trail/Track), 2 (Indoor Cycling), "
            "200 (Road Bike), 201 (Indoor Cycling alt)."
        )
    is_running = sport_type in _RUNNING_ACTIVITY_SPORT_TYPES
    wire_sport_type = 1 if is_running else sport_type

    # Resolve the per-sport default intensity: runs default to HR (2), the
    # natural running target; cycling keeps power (6). An explicit value wins.
    if intensity_type is None:
        intensity_type = 2 if is_running else 6

    for step in steps:
        if "repeat" in step:
            # --- Repeat group ---
            top_index += 1
            ex_id += 1
            group_sort = 16777216 * top_index
            group_id = ex_id

            sub_steps = step["steps"]
            iteration_seconds = sum(
                int(s["duration_minutes"] * 60) for s in sub_steps
            )
            total_seconds += iteration_seconds * step["repeat"]

            exercises.append({
                "id": group_id,
                "name": "Group",
                "exerciseType": 0,
                "sportType": wire_sport_type,
                "intensityType": 0,
                "intensityValue": 0,
                "targetType": 2,
                "targetValue": iteration_seconds,
                "sets": step["repeat"],
                "sortNo": group_sort,
                "restType": 3,
                "restValue": 0,
                "groupId": "0",
                "isGroup": True,
                "originId": "0",
            })

            for j, sub in enumerate(sub_steps):
                ex_id += 1
                sub_duration = int(sub["duration_minutes"] * 60)
                exercises.append({
                    "id": ex_id,
                    "name": sub["name"],
                    "exerciseType": 2,
                    "sportType": wire_sport_type,
                    "intensityType": intensity_type,
                    "intensityValue": sub.get("intensity_low", sub.get("power_low_w", 0)),
                    "intensityValueExtend": sub.get("intensity_high", sub.get("power_high_w", 0)),
                    "targetType": 2,
                    "targetValue": sub_duration,
                    "sets": 1,
                    "sortNo": group_sort + 65536 * (j + 1),
                    "restType": 3,
                    "restValue": 0,
                    "groupId": str(group_id),
                    "isGroup": False,
                    "originId": "0",
                })
        else:
            # --- Plain step ---
            top_index += 1
            ex_id += 1
            duration_s = int(step["duration_minutes"] * 60)
            total_seconds += duration_s
            exercises.append({
                "id": ex_id,
                "name": step["name"],
                "exerciseType": 2,
                "sportType": wire_sport_type,
                "intensityType": intensity_type,
                "intensityValue": step.get("intensity_low", step.get("power_low_w", 0)),
                "intensityValueExtend": step.get("intensity_high", step.get("power_high_w", 0)),
                "targetType": 2,
                "targetValue": duration_s,
                "sets": 1,
                "sortNo": 16777216 * top_index,
                "restType": 3,
                "restValue": 0,
                "groupId": "0",
                "isGroup": False,
                "originId": "0",
            })

    payload = {
        "name": name,
        "sportType": wire_sport_type,
        "estimatedTime": total_seconds,
        "access": 1,
        "exercises": exercises,
    }

    # Running programs need the same metadata block strength programs
    # carry. Without it the COROS app fails to parse the entry or renders
    # it as strength on the watch.
    if is_running:
        # exerciseType markers (1=warmup, 3=cooldown) attach to the FIRST and
        # LAST top-level steps, and only when those steps are plain. A repeat
        # group is structural (never warmup/cooldown) and its sub-steps are
        # always main work. Gating on the first/last top-level *items* — not on
        # a count of plain steps — keeps the markers correct for shapes that mix
        # a single plain step with a group: "[warmup, intervals]" still tags the
        # warmup, "[intervals, cooldown]" still tags the cooldown. Everything
        # else (interior plain steps, single-step workouts) stays main (the
        # exerciseType=2 written at construction).
        top_level_plain = [
            e for e in exercises
            if not e.get("isGroup") and e.get("groupId", "0") == "0"
        ]
        if len(steps) > 1 and top_level_plain:
            if "repeat" not in steps[0]:
                top_level_plain[0]["exerciseType"] = 1   # warmup
            if "repeat" not in steps[-1]:
                top_level_plain[-1]["exerciseType"] = 3  # cooldown
        # Per-step run metadata applies to every non-group step — top-level
        # plain steps AND repeat sub-steps — so interval blocks render too.
        # The group container carries none. hrType=2 marks HR-based targets.
        for ex in exercises:
            if ex.get("isGroup"):
                continue
            # Repeat sub-steps (groupId != "0") are always main work. Set it
            # explicitly here so running classification owns it, rather than
            # silently inheriting the exerciseType=2 the construction path
            # happens to write — a cycling-path refactor must not break this.
            if ex.get("groupId", "0") != "0":
                ex["exerciseType"] = 2
            ex.setdefault("exerciseKind", 0)
            ex.setdefault("gradeSystem", 0)
            ex["hrType"] = 2 if intensity_type == 2 else 0
            ex.setdefault("intensityMultiplier", 0)
            ex.setdefault("intensityPercent", 0)
            ex.setdefault("intensityPercentExtend", 0)
            ex.setdefault("onsightGradeOffset", 0)
            ex.setdefault("overview", "")
            ex.setdefault("packageTime", 0)
            ex.setdefault("sourceId", "0")
            ex.setdefault("subType", 0)
            ex.setdefault("targetDisplayUnit", 0)
        # exerciseNum / totalSets count real exercise steps only. A repeat
        # group adds a structural container row (isGroup=True) to `exercises`
        # that is glue, not a step — counting it inflates these by one per
        # group. Flat workouts have no containers, so this matches len() there.
        real_step_count = sum(1 for e in exercises if not e.get("isGroup"))
        payload.update({
            "duration": total_seconds,
            "exerciseNum": real_step_count,
            "gradeSystemVersion": 0,
            "hybridTotalSets": 0,
            "overview": "",
            "poolLength": 0,
            "poolLengthId": 0,
            "poolLengthUnit": 0,
            "referExercise": {
                "gradeSystem": 0,
                "hrType": 3 if intensity_type == 2 else 0,
                "intensityType": 0,
                "valueType": 1,
            },
            "sourceUrl": "",
            # subType=65535 marks a structured workout (shared with strength).
            "subType": 65535,
            "totalSets": real_step_count,
            "trainingLoad": 0,
            "type": 0,
            "videoCoverUrl": "",
            "videoUrl": "",
        })

    return payload


async def save_workout_template(
    auth: StoredAuth,
    name: str,
    steps: list[dict],
    sport_type: int = 2,
    intensity_type: int | None = None,
) -> str:
    """
    Save a reusable cycling/intervals workout template to the Coros library.

    steps: list of dicts — either plain steps or repeat groups.

    Plain step:
      - name: str — step label (e.g. "10:00 Warm-up")
      - duration_minutes: float — step duration in minutes
      - intensity_low: int — lower intensity target (watts, BPM, etc. per intensity_type)
      - intensity_high: int — upper intensity target (0 = open-ended)

    Repeat group:
      - repeat: int — number of repetitions
      - steps: list[dict] — sub-steps (same format as plain steps)

    Returns the new workout ID.
    """
    payload = _build_workout_program_payload(name, steps, sport_type, intensity_type)

    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            _base_url(auth.region) + ENDPOINTS["workout_add"],
            json=payload,
            headers=_auth_headers(auth),
        )
        resp.raise_for_status()
        body = resp.json()

    _check_response(body, "workout create")

    return str(body.get("data", ""))


async def delete_workout_template(auth: StoredAuth, workout_id: str) -> None:
    """Delete a saved workout template by ID."""
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            _base_url(auth.region) + ENDPOINTS["workout_delete"],
            json=[workout_id],
            headers=_auth_headers(auth),
        )
        resp.raise_for_status()
        body = resp.json()

    _check_response(body, "workout delete")


# ---------------------------------------------------------------------------
# Planned activities (training schedule calendar)
# ---------------------------------------------------------------------------

async def _fetch_schedule_data(
    client: httpx.AsyncClient,
    auth: StoredAuth,
    start_day: str,
    end_day: str,
) -> dict:
    """Shared GET for /training/schedule/query. Returns the raw 'data' dict
    (no stripping). Takes a caller-provided client so internal flows can
    reuse a connection across multiple round-trips."""
    params: dict[str, str | int] = {
        "startDate": start_day,
        "endDate": end_day,
        "supportRestExercise": 1,
    }
    resp = await client.get(
        _base_url(auth.region) + ENDPOINTS["schedule"],
        params=params,
        headers=_auth_headers(auth),
    )
    resp.raise_for_status()
    body = resp.json()
    _check_response(body, "schedule")
    return body.get("data") or {}


async def fetch_schedule(
    auth: StoredAuth, start_day: str, end_day: str
) -> dict:
    """
    Fetch planned activities from the Coros training calendar.

    start_day / end_day: YYYYMMDD strings.
    Returns the stripped schedule dict.
    """
    async with httpx.AsyncClient(timeout=30) as client:
        data = await _fetch_schedule_data(client, auth, start_day, end_day)
    return _strip_schedule(data)


async def fetch_schedule_raw(
    auth: StoredAuth, start_day: str, end_day: str
) -> dict:
    """
    Fetch planned activities without stripping fields.

    Raw schedule payloads are needed when updating an existing planned workout:
    /training/schedule/update expects the full entity/program objects, including
    planId, planProgramId, idInPlan, exerciseBarChart, and version fields.
    """
    async with httpx.AsyncClient(timeout=30) as client:
        return await _fetch_schedule_data(client, auth, start_day, end_day)


async def calculate_workout_program(auth: StoredAuth, program: dict) -> dict:
    """
    Recalculate a workout program after edits.

    Mirrors the Training Hub /training/program/calculate request captured from
    the web app. The response updates derived fields such as duration,
    estimatedDistance, estimatedValue/trainingLoad, and exerciseBarChart.
    """
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            _base_url(auth.region) + ENDPOINTS["workout_calculate"],
            json=program,
            headers=_auth_headers(auth),
        )
        resp.raise_for_status()
        body = resp.json()

    _check_response(body, "workout calculate")

    data = body.get("data")
    return data if data is not None else body


def apply_workout_calculation(program: dict, calculation: dict) -> dict:
    """
    Return a copy of program with calculate() derived fields applied.

    The calculate endpoint returns plan* fields rather than the original program
    field names. Training Hub writes those values back onto the program before
    sending /training/schedule/update.
    """
    updated = dict(program)

    if (value := calculation.get("exerciseBarChart")) is not None:
        updated["exerciseBarChart"] = value

    if (value := calculation.get("planDuration")) is not None:
        updated["duration"] = value
        updated["estimatedTime"] = value

    if (value := calculation.get("planTrainingLoad")) is not None:
        updated["trainingLoad"] = value
        updated["estimatedValue"] = value

    if (value := calculation.get("planElevGain")) is not None:
        updated["elevGain"] = value

    if (value := calculation.get("planDistance")) is not None:
        updated["distance"] = value
        with contextlib.suppress(TypeError, ValueError):
            updated["estimatedDistance"] = int(float(value))

    if (value := calculation.get("planSets")) is not None and "sets" in updated:
        updated["sets"] = value
    if (value := calculation.get("planHybridTotalSets")) is not None and "totalSets" in updated:
        updated["totalSets"] = value

    return updated


_EXERCISE_DROP = frozenset({
    "videoInfos", "videoUrl", "videoUrlArrStr", "coverUrlArrStr",
    "thumbnailUrl", "sourceUrl", "animationId",
    "access", "deleted", "defaultOrder", "status", "createTimestamp",
    "userId", "muscle", "muscleRelevance", "part", "equipment",
    "sortNo", "originId", "isDefaultAdd", "intensityCustom",
    "intensityDisplayUnit", "isIntensityPercent",
})

_PROGRAM_DROP = frozenset({
    "exerciseBarChart", "headPic", "profile", "sex", "star", "nickname",
    "essence", "originEssence", "access", "authorId", "deleted", "pbVersion",
    "version", "status", "createTimestamp", "thirdPartyId",
    "isTargetTypeConsistent", "pitch", "simple", "unit",
    "distanceDisplayUnit", "elevGain", "estimatedDistance", "estimatedTime",
    "estimatedType", "strengthType", "targetType", "targetValue",
    "planId", "planIdIndex", "userId",
})

_ENTITY_DROP = frozenset({
    "exerciseBarChart", "completeRate", "score", "standardRate",
    "dayNo", "operateUserId", "thirdParty", "thirdPartyId",
    "sortNo", "sortNoInSchedule", "userId", "planId", "planIdIndex",
})

_TOP_DROP = frozenset({
    "sportDatasInPlan", "sportDatasNotInPlan", "likeTpIds", "starTimestamp",
    "score", "sourceUrl", "inSchedule", "pauseInApp", "access", "authorId",
    "category", "pbVersion", "version", "thirdPartyId", "maxIdInPlan",
    "maxPlanProgramId", "weekStages", "subPlans", "userInfos",
    "type", "unit", "totalDay", "status", "startDay", "createTime",
    "updateTimestamp", "userId",
})


def _drop_keys(d: dict, keys: frozenset) -> dict:
    return {k: v for k, v in d.items() if k not in keys}


def _readable_overview(overview: str) -> str:
    """Convert 'sid_strength_squats' → 'Squats', 'sid_run_warm_up_dist' → 'Run warm up dist'."""
    for prefix in ("sid_strength_", "sid_run_", "sid_"):
        if overview.startswith(prefix):
            overview = overview[len(prefix):]
            break
    return overview.replace("_", " ").capitalize()


def _strip_exercise(ex: dict) -> dict:
    out = _drop_keys(ex, _EXERCISE_DROP)
    if "overview" in out:
        out["overview"] = _readable_overview(out["overview"])
    return out


def _strip_program(prog: dict) -> dict:
    out = _drop_keys(prog, _PROGRAM_DROP)
    if "exercises" in out:
        out["exercises"] = [_strip_exercise(e) for e in out["exercises"]]
    return out


def _strip_schedule(data: dict) -> dict:
    out = _drop_keys(data, _TOP_DROP)
    if "entities" in out:
        out["entities"] = [_drop_keys(e, _ENTITY_DROP) for e in out["entities"]]
    if "programs" in out:
        out["programs"] = [_strip_program(p) for p in out["programs"]]
    return out


# 1 lb = 0.45359237 kg (exact, NIST).
_LB_TO_KG = 0.45359237


# Module-level cache for the strength-exercise catalog. The MCP server is
# long-lived and the Coros catalog is effectively static within a session.
# 1h TTL balances "session never refetches" with "long-running server
# eventually picks up catalog additions if they ever happen".
# Cache is process-global (not region/auth-scoped) — catalog IDs are global
# across Coros regions, and the MCP server is single-user in practice.
_STRENGTH_CATALOG_TTL_SECONDS = 3600
_strength_catalog_cache: dict | None = None
_strength_catalog_loaded_at: float = 0.0
_strength_catalog_lock = asyncio.Lock()


def _reset_strength_catalog_cache() -> None:
    """Test-only helper: clear the module-level strength-catalog cache so
    the next call to _load_strength_catalog refetches. Not part of the
    public API — production code has no reason to invalidate the cache
    (process restart is the supported way to pick up catalog changes)."""
    global _strength_catalog_cache, _strength_catalog_loaded_at
    _strength_catalog_cache = None
    _strength_catalog_loaded_at = 0.0


def _catalog_is_fresh(now: float) -> bool:
    return (
        _strength_catalog_cache is not None
        and now - _strength_catalog_loaded_at < _STRENGTH_CATALOG_TTL_SECONDS
    )


async def _load_strength_catalog(auth: StoredAuth) -> dict:
    """Fetch the strength-exercise catalog and index by id, memoized at
    module scope with a TTL. Returns {} on transient network failure —
    callers treat empty as a resilient miss (workout still creates, only
    diagram metadata is lost).

    Auth and API-level errors (ValueError from _check_response) propagate
    so the user learns about a broken token instead of silently getting a
    workout without metadata.
    """
    global _strength_catalog_cache, _strength_catalog_loaded_at
    if _catalog_is_fresh(time.monotonic()):
        return _strength_catalog_cache  # type: ignore[return-value]

    async with _strength_catalog_lock:
        # Re-check inside the lock — another coroutine may have populated
        # the cache while we were waiting.
        if _catalog_is_fresh(time.monotonic()):
            return _strength_catalog_cache  # type: ignore[return-value]

        try:
            catalog = await fetch_exercises(auth, 4)
        except httpx.HTTPError:
            # Don't cache failures — leave cache unset so a later call retries.
            return {}
        _strength_catalog_cache = {str(e.get("id")): e for e in catalog}
        _strength_catalog_loaded_at = time.monotonic()
        return _strength_catalog_cache


def _build_strength_program_payload(
    name: str,
    exercises: list[dict],
    by_id: dict,
    sets: int = 1,
) -> dict:
    """Sync builder for the strength program dict — the JSON body that
    /training/program/add accepts and that schedule/update accepts inline.

    by_id is the catalog lookup ({id: catalog_entry}) used to populate per-
    exercise muscle/part/equipment metadata and animationId (video guidance).
    Pass {} to skip catalog enrichment.

    Validation (raises ValueError):
      - empty exercises
      - both weight_kg and weight_lbs set on the same exercise
      - negative weight
    """
    if not exercises:
        raise ValueError("strength workout requires at least one exercise")

    sets = max(1, sets)

    built = []
    total_duration = 0
    for ex in exercises:
        target_value = ex["target_value"]

        # Rest encoding: rest_seconds=0 → restType=3 ("Skip rests"),
        # rest_seconds>0 → restType=1 ("Rest MM:SS"). Verified against
        # app-created workouts.
        rest = int(ex.get("rest_seconds", 60))
        if rest <= 0:
            rest_type, rest_value = 3, 0
        else:
            rest_type, rest_value = 1, rest

        ex_sets = max(1, int(ex.get("sets", 1)))

        # Weight encoding (reverse-engineered 2026-05-20 from iOS-app payloads):
        #   Bodyweight (both weight_kg and weight_lbs omitted):
        #       intensityValue   = ""   (empty string, NOT 0)
        #       intensityCustom  = 1
        #       Renders as "Bodyweight".
        #   Weighted kg:
        #       intensityValue   = round(kg × 1000), intensityPercent = 0
        #       intensityDisplayUnit = "6", intensityCustom = 0
        #   Weighted lbs:
        #       intensityValue   = round(lbs × 0.45359237 × 1000)
        #       intensityPercent = round(lbs × 1_000_000)
        #       intensityDisplayUnit = "7", intensityCustom = 0
        #   weight_kg=0 explicitly → renders "0.00 kg" (intensityValue=0,
        #   intensityCustom=0). Distinct from bodyweight.
        #
        # round() (not int()) because float multiplications can land just
        # below the integer boundary (e.g. 27.9 * 1000 → 27899.999...).
        weight_kg = ex.get("weight_kg")
        weight_lbs = ex.get("weight_lbs")
        if weight_kg is not None and weight_lbs is not None:
            raise ValueError(
                "exercise specifies both weight_kg and weight_lbs — pick one"
            )
        if weight_lbs is not None:
            weight_lbs = float(weight_lbs)
            if weight_lbs < 0:
                raise ValueError(
                    f"weight_lbs must be non-negative, got {weight_lbs}"
                )
            intensity_value: int | str = round(weight_lbs * _LB_TO_KG * 1000)
            intensity_percent = round(weight_lbs * 1_000_000)
            display_unit = "7"
            intensity_custom = 0
        elif weight_kg is not None:
            weight_kg = float(weight_kg)
            if weight_kg < 0:
                raise ValueError(
                    f"weight_kg must be non-negative, got {weight_kg}"
                )
            intensity_value = round(weight_kg * 1000)
            intensity_percent = 0
            display_unit = "6"
            intensity_custom = 0
        else:
            # Bodyweight — empty string is the iOS-app marker.
            intensity_value = ""
            intensity_percent = 0
            display_unit = "6"
            intensity_custom = 1

        total_duration += ((target_value if ex["target_type"] == 2 else 0) + rest) * ex_sets

        cat = by_id.get(str(ex["origin_id"]), {})
        muscle = cat.get("muscle") or []
        muscle_relevance = cat.get("muscleRelevance") or []
        part = cat.get("part") or []
        equipment = cat.get("equipment") or []
        animation_id = cat.get("animationId", 0)

        built.append({
            "animationId": animation_id,
            "exerciseKind": 0,
            "exerciseType": 2,
            "gradeSystem": 0,
            "groupId": "0",
            "hrType": 0,
            "intensityCustom": intensity_custom,
            "intensityDisplayUnit": display_unit,
            "intensityMultiplier": 0,
            "intensityPercent": intensity_percent,
            "intensityPercentExtend": 0,
            "intensityType": 1,
            "intensityValue": intensity_value,
            "intensityValueExtend": 0,
            "isDefaultAdd": 0,
            "isGroup": False,
            "isIntensityPercent": False,
            "muscle": muscle,
            "muscleRelevance": muscle_relevance,
            "name": ex.get("name", ""),
            "onsightGradeOffset": 0,
            "originId": ex["origin_id"],
            "overview": ex.get("overview", "sid_strength_training"),
            "part": part,
            "equipment": equipment,
            "packageTime": 0,
            "restType": rest_type,
            "restValue": rest_value,
            "sets": ex_sets,
            "sourceId": "0",
            "sportType": 4,
            "status": 1,
            "subType": 0,
            "targetDisplayUnit": 0,
            "targetType": ex["target_type"],
            "targetValue": target_value,
        })

    total_duration *= sets
    payload = {
        "duration": total_duration,
        "exerciseNum": len(exercises),
        "exercises": built,
        "gradeSystemVersion": 0,
        "hybridTotalSets": 0,
        "name": name,
        "overview": "",
        # pool* fields are pool-swim metadata, irrelevant for strength
        # (sportType=4). The Coros app sets them to 0 on strength workouts.
        "poolLength": 0,
        "poolLengthId": 0,
        "poolLengthUnit": 0,
        "referExercise": {"gradeSystem": 0, "hrType": 0, "intensityType": 0, "valueType": 1},
        "sets": sets,
        "sourceUrl": "",
        "sportType": 4,
        "subType": 65535,
        "totalSets": sets,
        "trainingLoad": 0,
        "type": 0,
        "videoCoverUrl": "",
        "videoUrl": "",
    }
    return payload


async def save_strength_workout_template(
    auth: StoredAuth,
    name: str,
    exercises: list[dict],
    sets: int = 1,
) -> str:
    """
    Save a reusable strength workout template to the Coros library.

    exercises: list of dicts with keys:
      - origin_id: str  — exercise catalogue ID (from list_exercises)
      - name: str       — T-code name (e.g. "T1061")
      - overview: str   — sid_ key (e.g. "sid_strength_squats")
      - target_type: int — 2=time (seconds), 3=reps
      - target_value: int — seconds or reps
      - rest_seconds: int — rest after this exercise. 0 → "Skip rests".
      - weight_kg: float (optional) — prescribed weight in kg.
      - weight_lbs: float (optional) — prescribed weight in pounds.
        Mutually exclusive with weight_kg; pick one.
        Omitting BOTH renders as "Bodyweight" in the app.
        Explicit weight_kg=0 renders as "0.00 kg" — different from omitting.
        For dumbbell exercises, by convention this is the per-hand weight.
        The Coros app does not render ranges — single values only.

    sets: number of circuit repetitions.

    Returns the new workout ID.
    """
    by_id = await _load_strength_catalog(auth)
    payload = _build_strength_program_payload(name, exercises, by_id, sets)

    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            _base_url(auth.region) + ENDPOINTS["workout_add"],
            json=payload,
            headers=_auth_headers(auth),
        )
        resp.raise_for_status()
        body = resp.json()

    _check_response(body, "strength workout create")

    return str(body.get("data", ""))


async def _fetch_raw_workout(auth: StoredAuth, workout_id: str) -> dict | None:
    """Return the raw workout object for a given ID from the workout list.
    Returns None only when the list call succeeds but the ID is absent —
    API-level errors raise via _check_response so callers don't confuse
    'API broke' with 'not in library'."""
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            _base_url(auth.region) + ENDPOINTS["workout_list"],
            json={},
            headers=_auth_headers(auth),
        )
        resp.raise_for_status()
        body = resp.json()
    _check_response(body, "workout list")
    for w in body.get("data", []):
        if str(w.get("id", "")) == str(workout_id):
            return w
    return None


async def _post_schedule_inline(
    auth: StoredAuth,
    program: dict,
    happen_day: str,
    sort_no: int = 1,
) -> dict:
    """Resolve next idInPlan + POST /training/schedule/update with the program
    embedded inline, then GET the schedule again to surface server-assigned
    identifiers. Returns a 5-key dict: plan_id, id_in_plan, plan_program_id,
    entity_id (all strings) and enrichment_ok (bool). On enrichment failure
    the schedule POST has already succeeded — only id_in_plan is populated,
    the other three string IDs are empty and enrichment_ok is False so the
    caller can surface a warning instead of piping empty IDs straight into
    remove_scheduled_workout.

    NOTE: idInPlan is resolved as maxIdInPlan + 1 from the pre-POST schedule
    GET. This is racy under concurrent calls for the same happen_day —
    pre-existing behavior, acceptable for single-user MCP. Do not call this
    in parallel for the same date.
    """
    async with httpx.AsyncClient(timeout=30) as client:
        pre_data = await _fetch_schedule_data(client, auth, happen_day, happen_day)
        try:
            id_in_plan = int(pre_data.get("maxIdInPlan", 0)) + 1
        except (TypeError, ValueError):
            id_in_plan = 1

        program_with_id = {**program, "idInPlan": id_in_plan}

        # pbVersion=2 + versionObjects status=1 reverse-engineered from iOS;
        # status=3 is the delete marker (see remove_scheduled_workout).
        payload = {
            "entities": [{
                "happenDay": happen_day,
                "idInPlan": id_in_plan,
                "sortNoInSchedule": sort_no,
            }],
            "programs": [program_with_id],
            "versionObjects": [{"id": id_in_plan, "status": 1}],
            "pbVersion": 2,
        }

        resp = await client.post(
            _base_url(auth.region) + ENDPOINTS["schedule_update"],
            json=payload,
            headers=_auth_headers(auth),
        )
        resp.raise_for_status()
        body = resp.json()
        _check_response(body, "schedule update")

        # schedule/update's response omits the identifiers that
        # remove_scheduled_workout requires; re-fetch and locate our entry
        # by client-computed idInPlan (unique within a plan). Best-effort:
        # POST already succeeded, lookup failure must not propagate as a
        # schedule failure.
        result = {
            "plan_id": "",
            "id_in_plan": str(id_in_plan),
            "plan_program_id": "",
            "entity_id": "",
            "enrichment_ok": False,
        }
        try:
            post_data = await _fetch_schedule_data(client, auth, happen_day, happen_day)
            for entity in post_data.get("entities") or []:
                if str(entity.get("idInPlan", "")) == str(id_in_plan):
                    result["plan_id"] = str(post_data.get("id", ""))
                    result["id_in_plan"] = str(entity.get("idInPlan", id_in_plan))
                    result["plan_program_id"] = str(entity.get("planProgramId", ""))
                    result["entity_id"] = str(entity.get("id", ""))
                    result["enrichment_ok"] = bool(
                        result["plan_id"]
                        and result["plan_program_id"]
                        and result["entity_id"]
                    )
                    break
        except (httpx.HTTPError, ValueError):
            pass

    return result


async def schedule_workout_template(
    auth: StoredAuth,
    workout_id: str,
    happen_day: str,
    sort_no: int = 1,
) -> dict:
    """
    Add an existing library workout template to the Coros training calendar.

    happen_day: YYYYMMDD string.
    sort_no: order within the day (1 = first workout).

    Returns the server response 'data' dict (shape depends on Coros API).
    """
    program = await _fetch_raw_workout(auth, workout_id)
    if program is None:
        raise ValueError(f"Workout {workout_id} not found in library.")
    return await _post_schedule_inline(auth, program, happen_day, sort_no)


async def schedule_workout(
    auth: StoredAuth,
    name: str,
    steps: list[dict],
    happen_day: str,
    sport_type: int = 2,
    intensity_type: int | None = None,
    sort_no: int = 1,
) -> dict:
    """
    Build + schedule a one-off cycling/intervals workout for happen_day.
    Does NOT persist a library entry — the program is embedded inline
    in the schedule POST.

    steps: same shape as save_workout_template (plain steps or repeat groups).

    Returns the server response 'data' dict (shape depends on Coros API).
    """
    program = _build_workout_program_payload(name, steps, sport_type, intensity_type)
    return await _post_schedule_inline(auth, program, happen_day, sort_no)


async def schedule_strength_workout(
    auth: StoredAuth,
    name: str,
    exercises: list[dict],
    happen_day: str,
    sets: int = 1,
    sort_no: int = 1,
) -> dict:
    """
    Build + schedule a one-off strength workout for happen_day. Does NOT
    persist a library entry — the program is embedded inline in the
    schedule POST.

    exercises: same shape as save_strength_workout_template. Empty list raises.

    Returns the server response 'data' dict (shape depends on Coros API).
    """
    by_id = await _load_strength_catalog(auth)
    program = _build_strength_program_payload(name, exercises, by_id, sets)
    return await _post_schedule_inline(auth, program, happen_day, sort_no)


async def remove_scheduled_workout(
    auth: StoredAuth,
    plan_id: str,
    id_in_plan: str,
    plan_program_id: str | None = None,
) -> None:
    """
    Remove a scheduled workout from the Coros training calendar.

    plan_id: top-level plan ID (the 'id' field from list_planned_activities).
    id_in_plan: entity's idInPlan value.
    plan_program_id: entity's planProgramId (defaults to id_in_plan if omitted).
    """
    payload = {
        "versionObjects": [{
            "id": id_in_plan,
            "planProgramId": plan_program_id or id_in_plan,
            "planId": plan_id,
            "status": 3,  # 3 = delete
        }],
        "pbVersion": 2,
    }
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            _base_url(auth.region) + ENDPOINTS["schedule_update"],
            json=payload,
            headers=_auth_headers(auth),
        )
        resp.raise_for_status()
        body = resp.json()

    _check_response(body, "schedule delete")


async def _post_schedule_update(auth: StoredAuth, payload: dict) -> None:
    """POST a versioned payload to /training/schedule/update."""
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            _base_url(auth.region) + ENDPOINTS["schedule_update"],
            json=payload,
            headers=_auth_headers(auth),
        )
        resp.raise_for_status()
        body = resp.json()

    _check_response(body, "schedule update")


async def add_planned_workout(
    auth: StoredAuth,
    entity: dict,
    program: dict,
    version_object: dict | None = None,
) -> None:
    """
    Add a planned workout to the training calendar from a raw entity/program.

    This mirrors the Training Hub schedule/update payload used when adding an
    inline workout that is not first fetched from the workout library.
    """
    id_in_plan = entity.get("idInPlan") or program.get("idInPlan")
    if id_in_plan is None:
        raise ValueError("entity/program must include idInPlan for schedule add")

    program.setdefault("idInPlan", id_in_plan)
    payload = {
        "entities": [entity],
        "programs": [program],
        "versionObjects": [version_object or {"id": id_in_plan, "status": 1}],
        "pbVersion": 2,
    }
    await _post_schedule_update(auth, payload)


async def update_scheduled_workout(
    auth: StoredAuth,
    entity: dict,
    program: dict,
    version_object: dict | None = None,
) -> None:
    """
    Update an existing planned workout on the training calendar.

    This uses the same /training/schedule/update endpoint as scheduling and
    deletion, but sends versionObjects.status=2. The entity/program should come
    from fetch_schedule_raw(), with any intended edits applied. If the program
    content changes, call calculate_workout_program() first and pass the
    calculated program here.
    """
    id_in_plan = str(entity.get("idInPlan") or program.get("idInPlan") or "")
    plan_id = str(entity.get("planId") or program.get("planId") or "")
    plan_program_id = str(entity.get("planProgramId") or program.get("planProgramId") or "")
    if not id_in_plan or not plan_id:
        raise ValueError("entity/program must include idInPlan and planId for schedule update")

    payload = {
        "entities": [entity],
        "programs": [program],
        "versionObjects": [
            version_object or {
                "id": id_in_plan,
                "status": 2,
                "planProgramId": plan_program_id,
                "planId": plan_id,
            }
        ],
        "pbVersion": 2,
    }
    await _post_schedule_update(auth, payload)


async def fetch_exercises(auth: StoredAuth, sport_type: int) -> list[dict]:
    """
    Fetch the exercise catalogue for a given sport type.

    Used to look up strength/conditioning exercises (e.g. sport_type=4 for
    strength) that appear in planned workouts but have no inline detail.
    Returns the raw list of exercise definitions.
    """
    params: dict[str, str | int] = {"userId": auth.user_id, "sportType": sport_type}
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.get(
            _base_url(auth.region) + ENDPOINTS["exercises"],
            params=params,
            headers=_auth_headers(auth),
        )
        resp.raise_for_status()
        body = resp.json()

    _check_response(body, "exercise list")

    return body.get("data", []) or []


# ---------------------------------------------------------------------------
# Mobile token auto-refresh
# ---------------------------------------------------------------------------

async def _refresh_mobile_token(auth: StoredAuth) -> bool:
    """
    Refresh the mobile API token by replaying the stored login payload.

    The stored payload contains AES-encrypted credentials generated during
    coros-mcp auth.  The server accepts replay of the same encrypted payload
    — no nonce or anti-replay protection.

    Returns True and updates auth.mobile_access_token in-place on success.
    """
    if not auth.mobile_login_payload:
        return False

    mobile_base = MOBILE_BASE_URLS.get(auth.region, MOBILE_BASE_URLS["eu"])
    url = mobile_base + MOBILE_LOGIN_ENDPOINT
    headers: dict[str, str] = {
        "content-type": "application/json",
        "accept-encoding": "gzip",
        "user-agent": "okhttp/4.12.0",
        "request-time": str(int(time.time() * 1000)),
    }
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(url, json=auth.mobile_login_payload, headers=headers)
            resp.raise_for_status()
            body = resp.json()

        if body.get("result") != "0000":
            return False

        token = body.get("data", {}).get("accessToken")
        if not token:
            return False

        auth.mobile_access_token = token
        _save_auth(auth)
        return True
    except Exception:
        logger.debug("Mobile token refresh via replay payload failed", exc_info=True)
        return False


# ---------------------------------------------------------------------------
# Mobile token — lazy acquisition and refresh
# ---------------------------------------------------------------------------

async def _ensure_mobile_token(auth: StoredAuth) -> bool:
    """Ensure auth has a valid mobile access token, acquiring one on-demand if needed.

    Resolution order:
    1. Token already present — nothing to do.
    2. Replay payload stored — try refresh (re-sends the encrypted login payload).
    3. Env credentials available — perform a fresh mobile login.

    Mobile login is deferred until the first call to fetch_sleep() so that
    normal web-token refreshes never disrupt the Coros mobile app session.
    """
    if auth.mobile_access_token:
        return True

    # Try refreshing via the stored encrypted payload (avoids re-entering creds)
    if auth.mobile_login_payload and await _refresh_mobile_token(auth):
        return True

    # Fall back to a fresh mobile login using env credentials
    creds = get_env_credentials()
    if creds is None:
        return False
    email, password, region = creds
    try:
        mobile_token, mobile_payload = await _mobile_login(email, password, region)
        auth.mobile_access_token = mobile_token
        auth.mobile_login_payload = mobile_payload
        _save_auth(auth)
        return True
    except Exception:
        logger.debug("Fresh mobile login from env credentials failed", exc_info=True)
        return False


# ---------------------------------------------------------------------------
# Sleep data  (mobile API: apieu.coros.com/coros/data/statistic/daily)
# ---------------------------------------------------------------------------

async def fetch_sleep(auth: StoredAuth, start_day: str, end_day: str) -> list[SleepRecord]:
    """
    Fetch sleep stage data for a date range from the Coros mobile API.

    Uses POST /coros/data/statistic/daily on apieu.coros.com (not the Training
    Hub web API).  Returns per-night records with deep/light/REM/awake minutes
    and sleep heart rate.

    start_day / end_day: YYYYMMDD strings.
    """
    if not await _ensure_mobile_token(auth):
        raise ValueError(
            "No mobile API token available. Set COROS_EMAIL and COROS_PASSWORD in .env "
            "for automatic acquisition, or run: coros-mcp auth-mobile"
        )

    mobile_base = MOBILE_BASE_URLS.get(auth.region, MOBILE_BASE_URLS["eu"])
    url = mobile_base + ENDPOINTS["sleep"]
    sleep_payload = {
        "allDeviceSleep": 1,
        "dataType": [5],
        "dataVersion": 0,
        "startTime": int(start_day),
        "endTime": int(end_day),
        "statisticType": 1,
    }

    async def _do_request(token: str) -> dict:
        async with httpx.AsyncClient(timeout=30) as client:
            # Token is sent both as query param and header because the mobile
            # app does the same — untested whether the header alone suffices.
            # Note: the query param means the token appears in URLs (and thus
            # in any intermediate proxy logs).
            resp = await client.post(
                url,
                params={"accessToken": token},
                json=sleep_payload,
                headers={"Content-Type": "application/json", "accesstoken": token},
            )
            resp.raise_for_status()
            return resp.json()

    token = auth.mobile_access_token
    assert token is not None  # guaranteed by _ensure_mobile_token above
    body = await _do_request(token)

    if body.get("result") == "1019" and await _refresh_mobile_token(auth):  # token expired — auto-refresh once
        body = await _do_request(auth.mobile_access_token or token)

    _check_response(body, "sleep")

    records: list[SleepRecord] = []
    for item in body.get("data", {}).get("statisticData", {}).get("dayDataList", []):
        sd = item.get("sleepData", {})
        quality = item.get("performance")
        records.append(SleepRecord(
            date=str(item.get("happenDay", "")),
            total_duration_minutes=sd.get("totalSleepTime"),
            phases=SleepPhases(
                deep_minutes=sd.get("deepTime"),
                light_minutes=sd.get("lightTime"),
                rem_minutes=sd.get("eyeTime"),
                awake_minutes=sd.get("wakeTime"),
                nap_minutes=sd.get("shortSleepTime") or None,
            ),
            avg_hr=sd.get("avgHeartRate"),
            min_hr=sd.get("minHeartRate"),
            max_hr=sd.get("maxHeartRate"),
            quality_score=quality if quality != -1 else None,
        ))
    return sorted(records, key=lambda r: r.date)
