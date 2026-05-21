"""
Coros MCP Server — Sleep, HRV, and training data via the unofficial Coros API.

Usage:
    python server.py

MCP config (Claude Code):
    claude mcp add coros \\
      -e COROS_EMAIL=you@example.com \\
      -e COROS_PASSWORD=yourpass \\
      -e COROS_REGION=eu \\
      -- python /path/to/coros-mcp/server.py

Alternatively, create a .env file in the project directory with the same
variables. If COROS_EMAIL and COROS_PASSWORD are set (via env or .env), the
server authenticates automatically on the first request and re-authenticates
transparently whenever the stored token is expired or rejected.
"""

import time
from datetime import datetime, timedelta

from dotenv import load_dotenv
from fastmcp import FastMCP

import coros_api
from cache.store import cache_status, init_db
from cache.sync import (
    fetch_activities_cached,
    fetch_daily_records_cached,
    fetch_sleep_cached,
)
from cache.sync import (
    sync_all as _sync_all,
)
from cache.utils import LOCAL_TZ, fmt_local_time
from coros_api import TOKEN_TTL_MS

load_dotenv()
init_db()

mcp = FastMCP("coros-mcp")

_NOT_AUTHENTICATED = "Not authenticated. Set COROS_EMAIL and COROS_PASSWORD in .env or call authenticate_coros."  # noqa: E501


async def _get_auth():
    """Return stored auth, auto-logging in from env vars if the token is missing/expired."""
    auth = coros_api.get_stored_auth()
    if auth is None:
        auth = await coros_api.try_auto_login()
    return auth


async def _run_with_auth(fn, auth, *args, **kwargs):
    """Call fn(auth, …). On exception, re-login from env vars and retry once."""
    try:
        return await fn(auth, *args, **kwargs)
    except Exception:
        new_auth = await coros_api.try_auto_login()
        if new_auth is None:
            raise
        return await fn(new_auth, *args, **kwargs)


_ENRICHMENT_WARNING = (
    "Schedule POST succeeded but enrichment GET could not resolve "
    "plan_id/plan_program_id/entity_id. remove_scheduled_workout will not "
    "work with this response — call list_planned_activities for the day to "
    "look up the missing identifiers."
)


def _attach_enrichment_warning(result: dict, response: dict) -> dict:
    """Add a top-level `warning` key to `result` if the inline-schedule
    enrichment GET could not populate the server-assigned identifiers.
    Default to False when the key is absent so a missing flag surfaces
    as a warning (safer than silent omission)."""
    if not response.get("enrichment_ok", False):
        result["warning"] = _ENRICHMENT_WARNING
    return result


def _summarize_steps(steps: list[dict]) -> tuple[float, int]:
    """Return (total_minutes, steps_count) for a workout step list."""
    total_minutes = 0.0
    steps_count = 0
    for s in steps:
        if "repeat" in s:
            sub_mins = sum(sub["duration_minutes"] for sub in s["steps"])
            total_minutes += sub_mins * s["repeat"]
            steps_count += 1 + len(s["steps"])
        else:
            total_minutes += s["duration_minutes"]
            steps_count += 1
    return total_minutes, steps_count


# ---------------------------------------------------------------------------
# Tool: get_help
# ---------------------------------------------------------------------------

@mcp.tool()
async def get_help() -> dict:
    """List all available Coros MCP tools with a short description of each."""
    return {
        "tools": [
            {"name": "get_help", "description": "List all available tools (this tool)"},
            {"name": "authenticate_coros", "description": "Log in with email/password; stores web API token (required before all data tools)"},  # noqa: E501
            {"name": "authenticate_coros_mobile", "description": "Add mobile token for sleep stage data (deep/light/REM/awake)"},  # noqa: E501
            {"name": "check_coros_auth", "description": "Show current auth status, region, and token expiry"},
            {"name": "get_daily_metrics", "description": "Fetch daily training metrics: HRV, sleep hours, steps, stress, resting HR, VO2max, fitness score"},  # noqa: E501
            {"name": "get_sleep_data", "description": "Fetch nightly sleep records with duration and quality score (mobile auth required for stage breakdown)"},  # noqa: E501
            {"name": "list_activities", "description": "List recorded activities (runs, rides, swims, etc.) with summaries"},  # noqa: E501
            {"name": "get_activity_detail", "description": "Get full detail for one activity by label_id"},
            {"name": "list_workout_templates", "description": "List reusable workout templates saved in the Coros library"},  # noqa: E501
            {"name": "save_workout_template", "description": "Save a reusable cycling/intervals workout template to the library"},  # noqa: E501
            {"name": "save_strength_workout_template", "description": "Save a reusable strength workout template to the library"},  # noqa: E501
            {"name": "delete_workout_template", "description": "Delete a saved workout template by workout_id"},
            {"name": "list_planned_activities", "description": "List workouts scheduled on the training calendar"},
            {"name": "schedule_workout", "description": "Schedule a one-off cycling/intervals workout for a date (no library entry)"},  # noqa: E501
            {"name": "schedule_strength_workout", "description": "Schedule a one-off strength workout for a date (no library entry)"},  # noqa: E501
            {"name": "schedule_workout_template", "description": "Schedule an existing library workout template on a specific date"},  # noqa: E501
            {"name": "remove_scheduled_workout", "description": "Remove a workout from the training calendar"},
            {"name": "list_exercises", "description": "List available strength exercises (used when building strength workouts)"},  # noqa: E501
            {"name": "sync_coros_data", "description": "Backfill local cache from the Coros API for a date range"},
            {"name": "get_cache_status", "description": "Show local cache coverage: date ranges stored for each data type"},  # noqa: E501
        ]
    }


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
        auth = await coros_api.login(email, password, region, skip_mobile=True)
        return {
            "authenticated": True,
            "user_id": auth.user_id,
            "region": auth.region,
            "message": "Token stored securely (keyring or encrypted file)",
        }
    except Exception as exc:
        return {
            "authenticated": False,
            "error": str(exc),
        }


# ---------------------------------------------------------------------------
# Tool: authenticate_coros_mobile
# ---------------------------------------------------------------------------

@mcp.tool()
async def authenticate_coros_mobile(
    email: str,
    password: str,
    region: str = "eu",
) -> dict:
    """
    Authenticate with the Coros mobile API only and store the mobile token.

    This is needed for sleep data (deep/light/REM/awake phases) which is
    only available through the mobile API (apieu.coros.com), not the
    Training Hub web API.

    Parameters
    ----------
    email : str
        Coros account email address.
    password : str
        Coros account password (plain text — encrypted before sending).
    region : str
        "eu" (default) or "us".

    Returns
    -------
    dict with keys: authenticated, region, message
    """
    try:
        auth = await coros_api.login_mobile(email, password, region)
        return {
            "authenticated": True,
            "user_id": auth.user_id or "(web auth required for user_id)",
            "region": auth.region,
            "message": "Mobile token stored. Sleep data is now available.",
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
    Check whether valid Coros access tokens are stored locally.

    Returns
    -------
    dict with keys: authenticated, user_id, region, expires_in_hours,
    mobile_authenticated, mobile_token_status
    """
    auth = coros_api.get_stored_auth()
    if auth is None:
        return {
            "authenticated": False,
            "mobile_authenticated": False,
            "message": "No valid token found. Call authenticate_coros first.",
        }

    age_ms = int(time.time() * 1000) - auth.timestamp
    remaining_ms = TOKEN_TTL_MS - age_ms
    remaining_hours = round(remaining_ms / 3_600_000, 1)

    has_mobile = bool(auth.mobile_access_token)
    if has_mobile:
        mobile_status = "present (refresh via stored payload)"
    elif auth.mobile_login_payload:
        mobile_status = "expired (can auto-refresh)"
    else:
        mobile_status = "missing (run auth or auth-mobile)"

    return {
        "authenticated": bool(auth.access_token),
        "user_id": auth.user_id,
        "region": auth.region,
        "expires_in_hours": remaining_hours,
        "mobile_authenticated": has_mobile,
        "mobile_token_status": mobile_status,
    }


# ---------------------------------------------------------------------------
# Tool: get_daily_metrics
# ---------------------------------------------------------------------------

@mcp.tool()
async def get_daily_metrics(weeks: int = 4) -> dict:
    """
    Retrieve nightly HRV and daily metrics from Coros for a configurable
    time range (up to 52 weeks).

    Historical data is served from the local SQLite cache (fast); only the
    uncached tail is fetched from the Coros API. The underlying API endpoint
    supports up to 24 weeks per call, but the cache layer handles longer
    ranges transparently by reading stored records directly.

    Parameters
    ----------
    weeks : int
        Number of weeks to fetch (1–52). Default: 4.

    Returns
    -------
    dict with keys: records (list of daily records), count, date_range
    Each record contains:
      - date: YYYYMMDD local date (per COROS_TIMEZONE, defaults to system timezone)
      - avg_sleep_hrv: average nightly RMSSD in ms
      - baseline: rolling baseline RMSSD
      - rhr: resting heart rate (bpm)
      - training_load: daily training load
      - training_load_ratio: acute/chronic training load ratio
      - tired_rate: fatigue rate
      - ati: acute training index
      - cti: chronic training index
      - distance: daily distance in meters
      - duration: daily duration in seconds
      - vo2max: VO2 Max (only available for last ~28 days)
      - lthr: lactate threshold heart rate (bpm)
      - ltsp: lactate threshold pace (s/km)
      - stamina_level: base fitness level
      - stamina_level_7d: 7-day fitness trend
    """
    auth = await _get_auth()
    if auth is None:
        return {
            "error": _NOT_AUTHENTICATED,
            "records": [],
        }

    weeks = max(1, min(weeks, 52))
    end_dt = datetime.now(tz=LOCAL_TZ) if LOCAL_TZ is not None else datetime.now()
    start_dt = end_dt - timedelta(weeks=weeks)
    start_day = start_dt.strftime("%Y%m%d")
    end_day = end_dt.strftime("%Y%m%d")

    try:
        records = await _run_with_auth(fetch_daily_records_cached, auth, start_day, end_day)
        return {
            "records": [r.model_dump() for r in records],
            "count": len(records),
            "date_range": f"{start_day} – {end_day}",
        }
    except Exception as exc:
        return {"error": str(exc), "records": []}


# ---------------------------------------------------------------------------
# Tool: get_sleep_data
# ---------------------------------------------------------------------------

@mcp.tool()
async def get_sleep_data(weeks: int = 4) -> dict:
    """
    Fetch nightly sleep data from Coros for a configurable time range.

    Returns per-night sleep stage breakdown (deep, light, REM, awake) and
    sleep heart rate for each night.  Data comes from the Coros mobile API
    (apieu.coros.com) which is separate from the Training Hub web API.

    Parameters
    ----------
    weeks : int
        Number of weeks to fetch (1–52). Default: 4.

    Returns
    -------
    dict with keys: records (list of nightly records), count, date_range
    Each record contains:
      - date: YYYYMMDD local date (the morning date — sleep started the night before;
              per COROS_TIMEZONE, defaults to system timezone)
      - total_duration_minutes: total sleep in minutes
      - phases.deep_minutes: deep sleep
      - phases.light_minutes: light sleep
      - phases.rem_minutes: REM sleep
      - phases.awake_minutes: time awake during the night
      - phases.nap_minutes: daytime nap time (if any)
      - avg_hr: average heart rate during sleep
      - min_hr: minimum heart rate during sleep
      - max_hr: maximum heart rate during sleep
      - quality_score: sleep quality score (null if not computed)
    """
    auth = await _get_auth()
    if auth is None:
        return {"error": _NOT_AUTHENTICATED, "records": []}

    weeks = max(1, min(weeks, 52))
    end_dt = datetime.now(tz=LOCAL_TZ) if LOCAL_TZ is not None else datetime.now()
    start_dt = end_dt - timedelta(weeks=weeks)
    start_day = start_dt.strftime("%Y%m%d")
    end_day = end_dt.strftime("%Y%m%d")

    try:
        records = await _run_with_auth(fetch_sleep_cached, auth, start_day, end_day)
        return {
            "records": [r.model_dump() for r in records],
            "count": len(records),
            "date_range": f"{start_day} – {end_day}",
        }
    except Exception as exc:
        return {"error": str(exc), "records": []}


# ---------------------------------------------------------------------------
# Tool: list_activities
# ---------------------------------------------------------------------------

@mcp.tool()
async def list_activities(
    start_day: str,
    end_day: str,
    page: int = 1,
    size: int = 30,
) -> dict:
    """
    List Coros activities for a date range.

    Parameters
    ----------
    start_day : str
        Start date in YYYYMMDD format — local calendar date (per COROS_TIMEZONE,
        defaults to system timezone). Example: "20250316" for March 16 in your timezone.
    end_day : str
        End date in YYYYMMDD format — local calendar date (same convention as start_day).
    page : int
        Page number (default 1).
    size : int
        Results per page (default 30, max 100).

    Returns
    -------
    dict with keys: activities (list), total_count, page
    Each activity contains: activity_id, name, sport_type, sport_name,
    start_time (local datetime string "YYYY-MM-DD HH:MM:SS", per COROS_TIMEZONE),
    end_time (same format), duration_seconds, distance_meters, avg_hr, max_hr,
    calories (in cal — divide by 1000 to get kcal), training_load, avg_power,
    normalized_power, elevation_gain.
    """
    auth = await _get_auth()
    if auth is None:
        return {"error": _NOT_AUTHENTICATED, "activities": []}
    try:
        activities, total = await _run_with_auth(fetch_activities_cached, auth, start_day, end_day, page, size)
        result = []
        for a in activities:
            d = a.model_dump()
            d["start_time"] = fmt_local_time(a.start_time)
            d["end_time"] = fmt_local_time(a.end_time)
            result.append(d)
        return {
            "activities": result,
            "total_count": total,
            "page": page,
        }
    except Exception as exc:
        return {"error": str(exc), "activities": []}


# ---------------------------------------------------------------------------
# Tool: get_activity_detail
# ---------------------------------------------------------------------------

@mcp.tool()
async def get_activity_detail(activity_id: str, sport_type: int = 0) -> dict:
    """
    Fetch full detail for a single Coros activity.

    Parameters
    ----------
    activity_id : str
        The activity ID (labelId) from list_activities.
    sport_type : int
        Sport type ID from list_activities (e.g. 200=Road Bike, 201=Indoor Cycling,
        100=Running). Required for the API call to succeed.

    Returns
    -------
    dict with full activity data including laps, HR zones, power metrics,
    elevation, and all available sport-specific fields.
    """
    auth = await _get_auth()
    if auth is None:
        return {"error": _NOT_AUTHENTICATED}
    try:
        return await _run_with_auth(coros_api.fetch_activity_detail, auth, activity_id, sport_type)
    except Exception as exc:
        return {"error": str(exc)}


# ---------------------------------------------------------------------------
# Tool: list_workout_templates
# ---------------------------------------------------------------------------

@mcp.tool()
async def list_workout_templates() -> dict:
    """
    List reusable workout templates saved in the Coros library.

    These are templates created by save_workout_template /
    save_strength_workout_template — schedulable later via
    schedule_workout_template. One-off workouts scheduled with
    schedule_workout / schedule_strength_workout do NOT appear here.

    Returns
    -------
    dict with keys: workouts (list), count
    Each entry contains: id, name, sport_type, sport_name,
    estimated_time_seconds, exercise_count, exercises (list of steps with
    name, duration_seconds, intensity_low, intensity_high, sets)
    """
    auth = await _get_auth()
    if auth is None:
        return {"error": _NOT_AUTHENTICATED, "workouts": []}
    try:
        workouts = await _run_with_auth(coros_api.fetch_workout_templates, auth)
        return {"workouts": workouts, "count": len(workouts)}
    except Exception as exc:
        return {"error": str(exc), "workouts": []}


# ---------------------------------------------------------------------------
# Tool: save_workout_template
# ---------------------------------------------------------------------------

@mcp.tool()
async def save_workout_template(
    name: str,
    steps: list[dict],
    sport_type: int = 2,
    intensity_type: int = 6,
) -> dict:
    """
    Save a REUSABLE cycling/intervals workout TEMPLATE to the Coros library.

    ⚠️ This persists to the library indefinitely. Use ONLY when the user
    explicitly asks to "save as a template", "create a workout in my
    library", "add to my workout list", or otherwise indicates they want
    a reusable template.

    For a ONE-OFF workout for a specific date — the common case — use
    schedule_workout instead. That tool builds the workout inline and
    leaves no library residue.

    If the user's intent is unclear, ASK THEM:
      "Do you want this saved as a reusable template in your library, or
       just scheduled as a one-off for [date]?"
    Don't guess.

    The saved template appears in the Coros app under Workouts and can be
    synced to the watch for guided execution.

    Parameters
    ----------
    name : str
        Workout name (e.g. "Z2 Erholung 60min").
    steps : list[dict]
        List of workout steps. Each step is either a plain step or a repeat group.

        Plain step:
        - name (str): step label, e.g. "10:00 Einfahren"
        - duration_minutes (float): step duration in minutes
        - intensity_low (int): lower intensity target (watts, BPM, etc. depending on intensity_type)
        - intensity_high (int): upper intensity target (0 = open-ended)
        Note: power_low_w / power_high_w are accepted as legacy aliases for intensity_low / intensity_high.

        Repeat group (for intervals):
        - repeat (int): number of repetitions
        - steps (list[dict]): sub-steps (same format as plain steps)

        Example:
        [
            {"name": "Warm-up", "duration_minutes": 10, "intensity_low": 148, "intensity_high": 192},
            {"repeat": 3, "steps": [
                {"name": "Sweetspot", "duration_minutes": 10, "intensity_low": 265, "intensity_high": 285},
                {"name": "Recovery", "duration_minutes": 3, "intensity_low": 150, "intensity_high": 175},
            ]},
            {"name": "Cool-down", "duration_minutes": 10, "intensity_low": 100, "intensity_high": 165},
        ]

    sport_type : int
        Sport type ID. Default 2 = Indoor Cycling (Rollen).
        Use 200 for Road Bike (outdoor), 201 for Indoor Cycling (alt).
    intensity_type : int
        Intensity type ID. Default 6 = power in watts.
        Other IntensityType values: 1=weight, 2=HR, 3=pace, 4=speed, 5=none, 6=power, 7=cadence

    Returns
    -------
    dict with keys: workout_id, name, total_minutes, steps_count, message
    """
    auth = await _get_auth()
    if auth is None:
        return {"error": _NOT_AUTHENTICATED}
    try:
        workout_id = await _run_with_auth(
            coros_api.save_workout_template, auth, name, steps, sport_type, intensity_type
        )
        total_minutes, steps_count = _summarize_steps(steps)
        return {
            "workout_id": workout_id,
            "name": name,
            "total_minutes": total_minutes,
            "steps_count": steps_count,
            "message": "Workout created. Open Coros app → Workouts to sync to watch.",
        }
    except Exception as exc:
        return {"error": str(exc)}


# ---------------------------------------------------------------------------
# Tool: delete_workout_template
# ---------------------------------------------------------------------------

@mcp.tool()
async def delete_workout_template(
    workout_id: str,
) -> dict:
    """
    Delete a saved workout TEMPLATE from the Coros library.

    Parameters
    ----------
    workout_id : str
        The workout ID to delete (from list_workout_templates).

    Returns
    -------
    dict with keys: deleted, workout_id, message
    """
    auth = await _get_auth()
    if auth is None:
        return {"error": _NOT_AUTHENTICATED}
    try:
        await _run_with_auth(coros_api.delete_workout_template, auth, workout_id)
        return {
            "deleted": True,
            "workout_id": workout_id,
            "message": "Workout template deleted.",
        }
    except Exception as exc:
        return {"error": str(exc)}


# ---------------------------------------------------------------------------
# Tool: list_planned_activities
# ---------------------------------------------------------------------------

@mcp.tool()
async def list_planned_activities(
    start_day: str,
    end_day: str,
) -> dict:
    """
    List planned (scheduled) activities from the Coros training calendar.

    Parameters
    ----------
    start_day : str
        Start date in YYYYMMDD format.
    end_day : str
        End date in YYYYMMDD format.

    Returns
    -------
    dict with keys: schedule (stripped schedule dict with entities and programs
    sub-lists), count (number of scheduled entities), date_range
    """
    auth = await _get_auth()
    if auth is None:
        return {"error": _NOT_AUTHENTICATED, "schedule": {}}
    try:
        items = await _run_with_auth(coros_api.fetch_schedule, auth, start_day, end_day)
        return {
            "schedule": items,
            "count": len(items.get("entities", [])),
            "date_range": f"{start_day} – {end_day}",
        }
    except Exception as exc:
        return {"error": str(exc), "schedule": {}}


# ---------------------------------------------------------------------------
# Tool: schedule_workout_template
# ---------------------------------------------------------------------------

@mcp.tool()
async def schedule_workout_template(
    workout_id: str,
    happen_day: str,
    sort_no: int = 1,
) -> dict:
    """
    Add an existing library workout TEMPLATE to the training calendar.

    Use this only when scheduling a previously-saved template by ID. For a
    one-off workout that doesn't need to live in the library, use the
    inline tools instead: schedule_workout (cycling/intervals) or
    schedule_strength_workout (strength).

    Parameters
    ----------
    workout_id : str
        ID of the workout template to schedule (from list_workout_templates,
        save_workout_template, or save_strength_workout_template).
    happen_day : str
        Date in YYYYMMDD format.
    sort_no : int
        Order within the day if multiple workouts are scheduled (default 1).

    Returns
    -------
    dict with keys: scheduled, workout_id, happen_day, response, and
    optionally 'warning' if enrichment lookup failed.

    The 'response' dict contains the server-assigned identifiers needed to
    later remove this calendar entry: plan_id, id_in_plan, plan_program_id,
    entity_id, plus enrichment_ok. When enrichment_ok is True, pipe the
    response into remove_scheduled_workout directly. When False, a top-level
    'warning' key is set — the schedule POST succeeded but plan_id /
    plan_program_id / entity_id are empty strings, so look them up via
    list_planned_activities before calling remove_scheduled_workout.
    """
    auth = await _get_auth()
    if auth is None:
        return {"error": _NOT_AUTHENTICATED}
    try:
        response = await _run_with_auth(
            coros_api.schedule_workout_template, auth, workout_id, happen_day, sort_no
        )
        return _attach_enrichment_warning(
            {
                "scheduled": True,
                "workout_id": workout_id,
                "happen_day": happen_day,
                "response": response,
            },
            response,
        )
    except Exception as exc:
        return {"error": str(exc), "scheduled": False}


# ---------------------------------------------------------------------------
# Tool: schedule_workout (inline, one-off cycling/intervals)
# ---------------------------------------------------------------------------

@mcp.tool()
async def schedule_workout(
    name: str,
    steps: list[dict],
    happen_day: str,
    sport_type: int = 2,
    intensity_type: int = 6,
    sort_no: int = 1,
) -> dict:
    """
    Schedule a ONE-OFF cycling/intervals workout for a specific date.

    This is the COMMON case. Use this whenever the user wants a workout
    on a specific date and doesn't explicitly ask for a reusable template.
    Does NOT save to the Coros library — leaves no template behind.

    For a REUSABLE library template instead, use save_workout_template
    (which saves it for re-scheduling later via schedule_workout_template).

    If the user's intent is unclear, ASK THEM:
      "Do you want this saved as a reusable template in your library, or
       just scheduled as a one-off for [date]?"
    Don't guess.

    Parameters
    ----------
    name : str
        Workout name as it should appear on the calendar.
    steps : list[dict]
        Same shape as save_workout_template: plain steps or repeat groups.
    happen_day : str
        Date in YYYYMMDD format.
    sport_type : int
        Sport type ID (default 2 = Indoor Cycling).
    intensity_type : int
        Intensity type ID (default 6 = power in watts).
    sort_no : int
        Order within the day (default 1).

    Returns
    -------
    dict with keys: scheduled, name, happen_day, total_minutes, steps_count,
    response, and optionally 'warning' if enrichment lookup failed.

    The 'response' dict contains the server-assigned identifiers needed to
    later remove this calendar entry: plan_id, id_in_plan, plan_program_id,
    entity_id, plus enrichment_ok. When enrichment_ok is True, pipe the
    response into remove_scheduled_workout directly. When False, a top-level
    'warning' key is set — the schedule POST succeeded but plan_id /
    plan_program_id / entity_id are empty strings, so look them up via
    list_planned_activities before calling remove_scheduled_workout.
    """
    auth = await _get_auth()
    if auth is None:
        return {"error": _NOT_AUTHENTICATED}
    try:
        response = await _run_with_auth(
            coros_api.schedule_workout,
            auth,
            name,
            steps,
            happen_day,
            sport_type,
            intensity_type,
            sort_no,
        )
        total_minutes, steps_count = _summarize_steps(steps)
        return _attach_enrichment_warning(
            {
                "scheduled": True,
                "name": name,
                "happen_day": happen_day,
                "total_minutes": total_minutes,
                "steps_count": steps_count,
                "response": response,
            },
            response,
        )
    except Exception as exc:
        return {"error": str(exc), "scheduled": False}


# ---------------------------------------------------------------------------
# Tool: schedule_strength_workout (inline, one-off strength)
# ---------------------------------------------------------------------------

@mcp.tool()
async def schedule_strength_workout(
    name: str,
    exercises: list[dict],
    happen_day: str,
    sets: int = 1,
    sort_no: int = 1,
) -> dict:
    """
    Schedule a ONE-OFF strength workout for a specific date.

    This is the COMMON case. Use this whenever the user wants a strength
    workout on a specific date and doesn't explicitly ask for a reusable
    template. Does NOT save to the Coros library — leaves no template
    behind.

    For a REUSABLE library template instead, use save_strength_workout_template
    (which saves it for re-scheduling later via schedule_workout_template).

    If the user's intent is unclear, ASK THEM:
      "Do you want this saved as a reusable template in your library, or
       just scheduled as a one-off for [date]?"
    Don't guess.

    Parameters
    ----------
    name : str
        Workout name as it should appear on the calendar.
    exercises : list[dict]
        Same shape as save_strength_workout_template (origin_id, name, overview,
        target_type, target_value, rest_seconds, optional weight_kg or
        weight_lbs, optional per-exercise sets).
    happen_day : str
        Date in YYYYMMDD format.
    sets : int
        Number of full-circuit repetitions (default 1).
    sort_no : int
        Order within the day (default 1).

    Returns
    -------
    dict with keys: scheduled, name, happen_day, sets, exercise_count,
    response, and optionally 'warning' if enrichment lookup failed.

    The 'response' dict contains the server-assigned identifiers needed to
    later remove this calendar entry: plan_id, id_in_plan, plan_program_id,
    entity_id, plus enrichment_ok. When enrichment_ok is True, pipe the
    response into remove_scheduled_workout directly. When False, a top-level
    'warning' key is set — the schedule POST succeeded but plan_id /
    plan_program_id / entity_id are empty strings, so look them up via
    list_planned_activities before calling remove_scheduled_workout.
    """
    auth = await _get_auth()
    if auth is None:
        return {"error": _NOT_AUTHENTICATED}
    try:
        response = await _run_with_auth(
            coros_api.schedule_strength_workout,
            auth,
            name,
            exercises,
            happen_day,
            sets,
            sort_no,
        )
        return _attach_enrichment_warning(
            {
                "scheduled": True,
                "name": name,
                "happen_day": happen_day,
                "sets": sets,
                "exercise_count": len(exercises),
                "response": response,
            },
            response,
        )
    except Exception as exc:
        return {"error": str(exc), "scheduled": False}


# ---------------------------------------------------------------------------
# Tool: remove_scheduled_workout
# ---------------------------------------------------------------------------

@mcp.tool()
async def remove_scheduled_workout(
    plan_id: str,
    id_in_plan: str,
    plan_program_id: str = "",
) -> dict:
    """
    Remove a scheduled workout from the Coros training calendar.

    Parameters
    ----------
    plan_id : str
        Top-level plan ID — the 'id' field returned by list_planned_activities.
    id_in_plan : str
        The entity's idInPlan value from list_planned_activities.
    plan_program_id : str
        The entity's planProgramId (leave empty to use id_in_plan).

    Returns
    -------
    dict with keys: removed, plan_id, id_in_plan
    """
    auth = await _get_auth()
    if auth is None:
        return {"error": _NOT_AUTHENTICATED}
    try:
        await _run_with_auth(
            coros_api.remove_scheduled_workout, auth, plan_id, id_in_plan, plan_program_id or None
        )
        return {"removed": True, "plan_id": plan_id, "id_in_plan": id_in_plan}
    except Exception as exc:
        return {"error": str(exc), "removed": False}


# ---------------------------------------------------------------------------
# Tool: save_strength_workout_template
# ---------------------------------------------------------------------------

@mcp.tool()
async def save_strength_workout_template(
    name: str,
    exercises: list[dict],
    sets: int = 1,
) -> dict:
    """
    Save a REUSABLE strength workout TEMPLATE to the Coros library.

    ⚠️ This persists to the library indefinitely. Use ONLY when the user
    explicitly asks to "save as a template", "create a workout in my
    library", "add to my workout list".

    For a ONE-OFF workout for a specific date — the common case — use
    schedule_strength_workout instead. That tool builds the workout inline
    and leaves no library residue.

    If the user's intent is unclear, ASK THEM:
      "Do you want this saved as a reusable template in your library, or
       just scheduled as a one-off for [date]?"
    Don't guess.

    Parameters
    ----------
    name : str
        Workout name.
    exercises : list of dicts, each with:
        - origin_id (str): exercise catalogue ID from list_exercises
        - name (str): T-code name (e.g. "T1061")
        - overview (str): sid_ key (e.g. "sid_strength_squats")
        - target_type (int): 2=time in seconds, 3=reps
        - target_value (int): number of seconds or reps
        - rest_seconds (int): rest after this exercise (default 60).
          Use 0 to render as "Skip rests" in the Coros app.
        - sets (int, optional): number of consecutive sets of this exercise
          (default 1). Use this to get "3 sets of face pull in a row" instead
          of having to duplicate the exercise entry 3 times.
        - weight_kg (float, optional): prescribed weight in kg.
        - weight_lbs (float, optional): prescribed weight in pounds.
          Mutually exclusive with weight_kg — set at most one.
          The Coros app supports mixing kg/lbs per exercise within the same
          workout; this lbs exercise will display as lbs regardless of other
          exercises' units.
          Omitting BOTH fields renders as "Bodyweight" in the app
          (intensityValue is sent as an empty string, intensityCustom=1).
          Explicitly setting weight_kg=0 renders as "0.00 kg" — distinct
          from "Bodyweight". For dumbbell exercises this is the per-hand
          weight by convention. The Coros app shows a single weight per
          exercise — it does not render ranges.

    Muscle / equipment metadata (Training Machines + Training Parts diagrams
    in the app) is auto-populated from the exercise catalog by origin_id —
    no caller action required.
    sets : int
        Number of full-circuit repetitions over the whole exercise list
        (default 1). Distinct from the per-exercise `sets` above.

    Returns
    -------
    dict with keys: workout_id, name, sets, exercise_count
    """
    auth = await _get_auth()
    if auth is None:
        return {"error": _NOT_AUTHENTICATED}
    try:
        workout_id = await _run_with_auth(coros_api.save_strength_workout_template, auth, name, exercises, sets)
        return {
            "workout_id": workout_id,
            "name": name,
            "sets": sets,
            "exercise_count": len(exercises),
        }
    except Exception as exc:
        return {"error": str(exc)}


# ---------------------------------------------------------------------------
# Tool: list_exercises
# ---------------------------------------------------------------------------

@mcp.tool()
async def list_exercises(sport_type: int = 4) -> dict:
    """
    List the exercise catalogue for a given sport type.

    Useful for resolving strength/conditioning exercises (sport_type=4)
    that appear in planned workouts by name and ID.

    Parameters
    ----------
    sport_type : int
        Sport type ID. Default 4 = Strength.

    Returns
    -------
    dict with keys: exercises (list), count, sport_type
    """
    auth = await _get_auth()
    if auth is None:
        return {"error": _NOT_AUTHENTICATED, "exercises": []}
    try:
        items = await _run_with_auth(coros_api.fetch_exercises, auth, sport_type)
        return {"exercises": items, "count": len(items), "sport_type": sport_type}
    except Exception as exc:
        return {"error": str(exc), "exercises": []}


# ---------------------------------------------------------------------------
# Tool: sync_coros_data
# ---------------------------------------------------------------------------

@mcp.tool()
async def sync_coros_data(start_day: str = "", end_day: str = "") -> dict:
    """
    Sync Coros data for a date range into the local SQLite cache.

    After the first full sync, subsequent calls to get_daily_metrics,
    get_sleep_data, and list_activities will serve historical data from
    cache and only fetch the incremental tail from the API.

    For large date ranges (> 6 months), call this tool in segments to
    avoid timeout (e.g. one segment per year). For the initial full
    historical backfill, use the CLI instead:
        coros-mcp sync --from 20230101

    Parameters
    ----------
    start_day : str
        Start of sync range in YYYYMMDD format — local calendar date
        (per COROS_TIMEZONE, defaults to system timezone).
        Defaults to two years ago if omitted.
    end_day : str
        End of sync range in YYYYMMDD format — local calendar date
        (same convention as start_day). Defaults to today if omitted.

    Returns
    -------
    dict with keys: daily (records synced), sleep (records synced),
    activities (records synced), errors (list), cache (coverage summary)
    """
    auth = await _get_auth()
    if auth is None:
        return {"error": "Not authenticated. Set COROS_EMAIL and COROS_PASSWORD or call authenticate_coros."}

    if not start_day:
        start_day = (datetime.now() - timedelta(days=730)).strftime("%Y%m%d")
    if not end_day:
        end_day = datetime.now().strftime("%Y%m%d")

    try:
        return await _sync_all(auth, start_day, end_day=end_day)
    except Exception as exc:
        return {"error": str(exc)}


# ---------------------------------------------------------------------------
# Tool: get_cache_status
# ---------------------------------------------------------------------------

@mcp.tool()
async def get_cache_status() -> dict:
    """
    Show what data is currently stored in the local cache.

    Returns
    -------
    dict with keys: daily_records, sleep_records, activities — each with:
      - count: number of cached records
      - from: earliest cached date (YYYYMMDD)
      - to: latest cached date (YYYYMMDD)
    Also includes db_path: absolute path to the SQLite file.
    """
    return cache_status()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    mcp.run()


if __name__ == "__main__":
    main()
