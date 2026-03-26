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

import os
import time
from datetime import datetime, timedelta

from dotenv import load_dotenv
from fastmcp import FastMCP

import coros_api
from coros_api import TOKEN_TTL_MS

load_dotenv()

mcp = FastMCP("coros-mcp")


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
    time range (up to 24 weeks).

    Uses the /analyse/dayDetail/query endpoint which returns daily records
    including HRV, resting heart rate, training load, and fatigue rate.

    Parameters
    ----------
    weeks : int
        Number of weeks to fetch (1–24). Default: 4.

    Returns
    -------
    dict with keys: records (list of daily records), count, date_range
    Each record contains:
      - date: YYYYMMDD
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
            "error": "Not authenticated. Set COROS_EMAIL and COROS_PASSWORD in .env or call authenticate_coros.",
            "records": [],
        }

    weeks = max(1, min(weeks, 24))
    end_dt = datetime.now()
    start_dt = end_dt - timedelta(weeks=weeks)
    start_day = start_dt.strftime("%Y%m%d")
    end_day = end_dt.strftime("%Y%m%d")

    try:
        records = await _run_with_auth(coros_api.fetch_daily_records, auth, start_day, end_day)
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
      - date: YYYYMMDD (the morning date — sleep started the night before)
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
        return {"error": "Not authenticated. Set COROS_EMAIL and COROS_PASSWORD in .env or call authenticate_coros.", "records": []}

    weeks = max(1, min(weeks, 52))
    end_dt = datetime.now()
    start_dt = end_dt - timedelta(weeks=weeks)
    start_day = start_dt.strftime("%Y%m%d")
    end_day = end_dt.strftime("%Y%m%d")

    try:
        records = await _run_with_auth(coros_api.fetch_sleep, auth, start_day, end_day)
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
        Start date in YYYYMMDD format.
    end_day : str
        End date in YYYYMMDD format.
    page : int
        Page number (default 1).
    size : int
        Results per page (default 30, max 100).

    Returns
    -------
    dict with keys: activities (list), total_count, page
    Each activity contains: activity_id, name, sport_type, sport_name,
    start_time, end_time, duration_seconds, distance_meters, avg_hr, max_hr,
    calories, training_load, avg_power, normalized_power, elevation_gain
    """
    auth = await _get_auth()
    if auth is None:
        return {"error": "Not authenticated. Set COROS_EMAIL and COROS_PASSWORD in .env or call authenticate_coros.", "activities": []}
    try:
        activities, total = await _run_with_auth(coros_api.fetch_activities, auth, start_day, end_day, page, size)
        return {
            "activities": [a.model_dump() for a in activities],
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
        return {"error": "Not authenticated. Set COROS_EMAIL and COROS_PASSWORD in .env or call authenticate_coros."}
    try:
        return await _run_with_auth(coros_api.fetch_activity_detail, auth, activity_id, sport_type)
    except Exception as exc:
        return {"error": str(exc)}


# ---------------------------------------------------------------------------
# Tool: list_workouts
# ---------------------------------------------------------------------------

@mcp.tool()
async def list_workouts() -> dict:
    """
    List all saved workout programs in the Coros account.

    Returns
    -------
    dict with keys: workouts (list), count
    Each workout contains: id, name, sport_type, sport_name,
    estimated_time_seconds, exercise_count, exercises (list of steps with
    name, duration_seconds, power_low_w, power_high_w)
    """
    auth = await _get_auth()
    if auth is None:
        return {"error": "Not authenticated. Set COROS_EMAIL and COROS_PASSWORD in .env or call authenticate_coros.", "workouts": []}
    try:
        workouts = await _run_with_auth(coros_api.fetch_workouts, auth)
        return {"workouts": workouts, "count": len(workouts)}
    except Exception as exc:
        return {"error": str(exc), "workouts": []}


# ---------------------------------------------------------------------------
# Tool: create_workout
# ---------------------------------------------------------------------------

@mcp.tool()
async def create_workout(
    name: str,
    steps: list[dict],
    sport_type: int = 2,
) -> dict:
    """
    Create a new structured workout in the Coros account.

    The workout appears in the Coros app under Workouts and can be synced
    to the watch for guided execution.

    Parameters
    ----------
    name : str
        Workout name (e.g. "Z2 Erholung 60min").
    steps : list[dict]
        List of workout steps. Each step is either a plain step or a repeat group.

        Plain step:
          - name (str): step label, e.g. "10:00 Einfahren"
          - duration_minutes (float): step duration in minutes
          - power_low_w (int): lower power target in watts
          - power_high_w (int): upper power target in watts

        Repeat group (for intervals):
          - repeat (int): number of repetitions
          - steps (list[dict]): sub-steps (same format as plain steps)

        Example:
          [
            {"name": "Warm-up", "duration_minutes": 10, "power_low_w": 148, "power_high_w": 192},
            {"repeat": 3, "steps": [
              {"name": "Sweetspot", "duration_minutes": 10, "power_low_w": 265, "power_high_w": 285},
              {"name": "Recovery", "duration_minutes": 3, "power_low_w": 150, "power_high_w": 175},
            ]},
            {"name": "Cool-down", "duration_minutes": 10, "power_low_w": 100, "power_high_w": 165},
          ]
    sport_type : int
        Sport type ID. Default 2 = Indoor Cycling (Rollen).
        Use 200 for Road Bike (outdoor), 201 for Indoor Cycling (alt).

    Returns
    -------
    dict with keys: workout_id, name, total_minutes, steps_count, message
    """
    auth = await _get_auth()
    if auth is None:
        return {"error": "Not authenticated. Set COROS_EMAIL and COROS_PASSWORD in .env or call authenticate_coros."}
    try:
        workout_id = await _run_with_auth(coros_api.create_workout, auth, name, steps, sport_type)
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
# Tool: delete_workout
# ---------------------------------------------------------------------------

@mcp.tool()
async def delete_workout(
    workout_id: str,
) -> dict:
    """
    Delete a workout program from the Coros account.

    Parameters
    ----------
    workout_id : str
        The workout ID to delete (from list_workouts).

    Returns
    -------
    dict with keys: deleted, workout_id, message
    """
    auth = await _get_auth()
    if auth is None:
        return {"error": "Not authenticated. Set COROS_EMAIL and COROS_PASSWORD in .env or call authenticate_coros."}
    try:
        await _run_with_auth(coros_api.delete_workout, auth, workout_id)
        return {
            "deleted": True,
            "workout_id": workout_id,
            "message": "Workout deleted.",
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
    dict with keys: activities (list of raw scheduled items), count, date_range
    """
    auth = await _get_auth()
    if auth is None:
        return {"error": "Not authenticated. Set COROS_EMAIL and COROS_PASSWORD in .env or call authenticate_coros.", "activities": []}
    try:
        items = await _run_with_auth(coros_api.fetch_schedule, auth, start_day, end_day)
        return {
            "activities": items,
            "count": len(items),
            "date_range": f"{start_day} – {end_day}",
        }
    except Exception as exc:
        return {"error": str(exc), "activities": []}


# ---------------------------------------------------------------------------
# Tool: schedule_workout
# ---------------------------------------------------------------------------

@mcp.tool()
async def schedule_workout(
    workout_id: str,
    happen_day: str,
    sort_no: int = 1,
) -> dict:
    """
    Add an existing workout from the library to the Coros training calendar.

    Parameters
    ----------
    workout_id : str
        ID of the workout to schedule (from list_workouts or create_workout).
    happen_day : str
        Date in YYYYMMDD format.
    sort_no : int
        Order within the day if multiple workouts are scheduled (default 1).

    Returns
    -------
    dict with keys: scheduled, workout_id, happen_day
    """
    auth = await _get_auth()
    if auth is None:
        return {"error": "Not authenticated. Set COROS_EMAIL and COROS_PASSWORD in .env or call authenticate_coros."}
    try:
        await _run_with_auth(coros_api.schedule_workout, auth, workout_id, happen_day, sort_no)
        return {"scheduled": True, "workout_id": workout_id, "happen_day": happen_day}
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
        return {"error": "Not authenticated. Set COROS_EMAIL and COROS_PASSWORD in .env or call authenticate_coros."}
    try:
        await _run_with_auth(
            coros_api.remove_scheduled_workout, auth, plan_id, id_in_plan, plan_program_id or None
        )
        return {"removed": True, "plan_id": plan_id, "id_in_plan": id_in_plan}
    except Exception as exc:
        return {"error": str(exc), "removed": False}


# ---------------------------------------------------------------------------
# Tool: create_strength_workout
# ---------------------------------------------------------------------------

@mcp.tool()
async def create_strength_workout(
    name: str,
    exercises: list[dict],
    sets: int = 1,
) -> dict:
    """
    Create a new structured strength workout program.

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
        - rest_seconds (int): rest after this exercise (default 60)
    sets : int
        Number of circuit repetitions (default 1).

    Returns
    -------
    dict with keys: workout_id, name, sets, exercise_count
    """
    auth = await _get_auth()
    if auth is None:
        return {"error": "Not authenticated. Set COROS_EMAIL and COROS_PASSWORD in .env or call authenticate_coros."}
    try:
        workout_id = await _run_with_auth(coros_api.create_strength_workout, auth, name, exercises, sets)
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
        return {"error": "Not authenticated. Set COROS_EMAIL and COROS_PASSWORD in .env or call authenticate_coros.", "exercises": []}
    try:
        items = await _run_with_auth(coros_api.fetch_exercises, auth, sport_type)
        return {"exercises": items, "count": len(items), "sport_type": sport_type}
    except Exception as exc:
        return {"error": str(exc), "exercises": []}


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    mcp.run()


if __name__ == "__main__":
    main()
