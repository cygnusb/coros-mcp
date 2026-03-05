# coros-mcp

A [Model Context Protocol (MCP)](https://modelcontextprotocol.io/) server that fetches sleep, HRV, and training data from the unofficial Coros API and exposes them to AI assistants like Claude.

**No API key required.** This server authenticates directly with your Coros Training Hub credentials. Your token is stored securely in your system keyring (or an encrypted local file as fallback), never transmitted anywhere except to Coros.

## What You Can Do

Ask your AI assistant questions like:

- "How much deep sleep and REM did I get last week?"
- "What was my HRV trend over the last 4 weeks?"
- "Show me my resting heart rate and training load for last week"
- "How many steps did I average per day this month?"
- "List my rides from last month"
- "Show me the details of my last long ride"
- "Create a 90-minute sweet spot workout for me"

## Features

| Tool | Description |
|------|-------------|
| `authenticate_coros` | Log in with email and password — token stored securely in keyring |
| `check_coros_auth` | Check whether a valid auth token is present |
| `get_daily_metrics` | Fetch daily metrics (HRV, resting HR, training load, VO2max, stamina, and more) for n weeks (default: 4) |
| `get_sleep_data` | Fetch nightly sleep stages (deep, light, REM, awake) and sleep HR for n weeks (default: 4) |
| `list_activities` | List activities for a date range with summary metrics |
| `get_activity_detail` | Fetch full detail for a single activity (laps, HR zones, power zones) |
| `list_workouts` | List all saved structured workout programs |
| `create_workout` | Create a new structured workout with named steps and power targets |

---

## Setup

### Option A: Auto-Setup with Claude Code

If you have [Claude Code](https://claude.ai/code), paste this prompt:

```
Set up the Coros MCP server from https://github.com/cygnusb/coros-mcp — clone it, create a venv, install it with pip install -e ., add it to my MCP config, then tell me to run 'coros-mcp auth' in my terminal to authenticate.
```

Claude will handle the installation and guide you through configuration.

### Option B: Manual Setup

#### Step 1: Install

```bash
git clone https://github.com/cygnusb/coros-mcp.git
cd coros-mcp
python3 -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -e .
```

Or with `uv`:

```bash
uv pip install -e .
```

#### Step 2: Add to Claude Code

```bash
claude mcp add coros -- python /path/to/coros-mcp/server.py
```

Or add to Claude Desktop (`~/Library/Application Support/Claude/claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "coros": {
      "command": "/path/to/coros-mcp/.venv/bin/python",
      "args": ["/path/to/coros-mcp/server.py"]
    }
  }
}
```

#### Step 3: Authenticate

Run the following command in your terminal — **outside** of any Claude session:

```bash
coros-mcp auth
```

You will be prompted for your email, password, and region (`eu`, `us`, or `asia`). Your credentials are sent directly to Coros and the token is stored securely in your system keyring (or an encrypted local file as fallback). **You only need to do this once** — the token persists across restarts.

**Other auth commands:**

```bash
coros-mcp auth-status                    # Check if authenticated
coros-mcp auth-clear                     # Remove stored token
coros-mcp extract-from-dump <dump_file>  # Enable sleep data auto-refresh (see below)
coros-mcp set-mobile-token               # Manually store a mobile token
```

---

## Tool Reference

### `authenticate_coros`

Log in with your Coros credentials. The auth token is stored securely in your system keyring (or an encrypted file as fallback).

```json
{ "email": "you@example.com", "password": "yourpassword", "region": "eu" }
```

Returns: `authenticated`, `user_id`, `region`, `message`

### `check_coros_auth`

Check whether a valid token is stored and how long ago it was issued.

```json
{}
```

Returns: `authenticated`, `user_id`, `region`, `expires_in_hours`

### `get_daily_metrics`

Fetch daily metrics for a configurable number of weeks (default: 4).

```json
{ "weeks": 4 }
```

Returns: `records` (list), `count`, `date_range`

Each record includes:

| Field | Source | Description |
|-------|--------|-------------|
| `date` | — | Date (YYYYMMDD) |
| `avg_sleep_hrv` | dayDetail | Nightly HRV (RMSSD ms) |
| `baseline` | dayDetail | HRV rolling baseline |
| `rhr` | dayDetail | Resting heart rate (bpm) |
| `training_load` | dayDetail | Daily training load |
| `training_load_ratio` | dayDetail | Acute/chronic training load ratio |
| `tired_rate` | dayDetail | Fatigue rate |
| `ati` / `cti` | dayDetail | Acute / chronic training index |
| `distance` / `duration` | dayDetail | Distance (m) / duration (s) |
| `vo2max` | analyse (merge) | VO2 Max (last ~28 days) |
| `lthr` | analyse (merge) | Lactate threshold heart rate (bpm) |
| `ltsp` | analyse (merge) | Lactate threshold pace (s/km) |
| `stamina_level` | analyse (merge) | Base fitness level |
| `stamina_level_7d` | analyse (merge) | 7-day fitness trend |

### `get_sleep_data`

Fetch nightly sleep stage data for a configurable number of weeks (default: 4).

```json
{ "weeks": 4 }
```

Returns: `records` (list), `count`, `date_range`

Each record includes:

| Field | Description |
|-------|-------------|
| `date` | Date (YYYYMMDD) — the morning after the sleep |
| `total_duration_minutes` | Total sleep in minutes |
| `phases.deep_minutes` | Deep sleep |
| `phases.light_minutes` | Light sleep |
| `phases.rem_minutes` | REM sleep |
| `phases.awake_minutes` | Time awake during the night |
| `phases.nap_minutes` | Daytime nap time (null if none) |
| `avg_hr` | Average heart rate during sleep |
| `min_hr` | Minimum heart rate during sleep |
| `max_hr` | Maximum heart rate during sleep |
| `quality_score` | Sleep quality score (null if not computed) |

> **Note:** Sleep data is fetched from the Coros mobile API (`apieu.coros.com`), which uses a separate token from the Training Hub web API. The token expires after ~1 hour but **refreshes automatically** as long as a login payload is stored. To enable this, capture a Coros app login via mitmproxy and run:
>
> ```bash
> coros-mcp extract-from-dump <your_capture.dump>
> ```
>
> This stores both the current token and the encrypted login payload needed for auto-refresh. See [docs/mobile-token.md](docs/mobile-token.md) for mitmproxy capture instructions. As a fallback, `coros-mcp set-mobile-token` lets you paste a token manually (no auto-refresh).

### `list_activities`

List activities for a date range.

```json
{ "start_day": "20260101", "end_day": "20260305", "page": 1, "size": 30 }
```

Returns: `activities` (list), `total_count`, `page`

Each activity includes: `activity_id`, `name`, `sport_type`, `sport_name`, `start_time`, `end_time`, `duration_seconds`, `distance_meters`, `avg_hr`, `max_hr`, `calories`, `training_load`, `avg_power`, `normalized_power`, `elevation_gain`

### `get_activity_detail`

Fetch full detail for a single activity. Requires the `sport_type` from `list_activities`.

```json
{ "activity_id": "469901014965714948", "sport_type": 200 }
```

Returns full activity data including laps, HR zones, power zones, and all sport-specific metrics.

> **Note:** Large time-series arrays (`graphList`, `frequencyList`, `gpsLightDuration`) are stripped from the response to keep it manageable.

### `list_workouts`

List all saved structured workout programs.

```json
{}
```

Returns: `workouts` (list), `count`

Each workout includes: `id`, `name`, `sport_type`, `sport_name`, `estimated_time_seconds`, `exercise_count`, `exercises` (list of steps with `name`, `duration_seconds`, `power_low_w`, `power_high_w`)

### `create_workout`

Create a new structured workout. Workouts appear in the Coros app and can be synced to the watch.

```json
{
  "name": "Sweet Spot 90min",
  "sport_type": 2,
  "steps": [
    {"name": "15:00 Einfahren",  "duration_minutes": 15, "power_low_w": 148, "power_high_w": 192},
    {"name": "20:00 Sweet Spot", "duration_minutes": 20, "power_low_w": 260, "power_high_w": 275},
    {"name": "5:00 Pause",       "duration_minutes":  5, "power_low_w": 100, "power_high_w": 150},
    {"name": "20:00 Sweet Spot", "duration_minutes": 20, "power_low_w": 260, "power_high_w": 275},
    {"name": "30:00 Ausfahren",  "duration_minutes": 30, "power_low_w": 100, "power_high_w": 192}
  ]
}
```

`sport_type`: `2` = Indoor Cycling (default), `200` = Road Bike

Returns: `workout_id`, `name`, `total_minutes`, `steps_count`, `message`

---

## Requirements

- Python ≥ 3.11
- A Coros account (Training Hub)

---

## Project Structure

```
coros-mcp/
├── server.py          # MCP server with tool definitions
├── coros_api.py       # Coros API client (auth, requests, parsers)
├── models.py          # Pydantic data models
├── auth/              # Token storage (keyring + encrypted file fallback)
├── pyproject.toml     # Project metadata & dependencies
├── .env.example       # Example configuration
└── docs/
    └── discover-endpoints.md  # Guide for discovering undocumented endpoints
```

## Dependencies

- [fastmcp](https://github.com/jlowin/fastmcp) — MCP framework
- [httpx](https://www.python-httpx.org/) — Async HTTP client
- [pydantic](https://docs.pydantic.dev/) — Data validation
- [python-dotenv](https://github.com/theskumar/python-dotenv) — `.env` support

## Disclaimer

This project uses the **unofficial** Coros Training Hub API. The API may change at any time without notice. Use at your own risk.
