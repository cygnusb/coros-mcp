# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

`coros-mcp` is a [Model Context Protocol (MCP)](https://modelcontextprotocol.io/) server that exposes Coros fitness data (sleep, HRV, training metrics, activities, workouts) to AI assistants. It uses the **unofficial** Coros API — no official API key required.

## Setup & Installation

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
# For development (pytest, ruff, mypy):
pip install -e ".[dev]"
```

## Running the Server

```bash
# Run via CLI entry point (recommended):
coros-mcp serve

# Or register with Claude Code:
claude mcp add coros -- /path/to/coros-mcp/.venv/bin/coros-mcp serve
```

## CLI Commands

```bash
coros-mcp auth           # Authenticate (web + mobile tokens)
coros-mcp auth-web       # Web API only (no sleep data)
coros-mcp auth-mobile    # Mobile API only (sleep data)
coros-mcp auth-status    # Check token status
coros-mcp auth-clear     # Remove stored tokens
coros-mcp sync [--from YYYYMMDD] [--to YYYYMMDD]  # Backfill data to local cache
coros-mcp cache-status   # Show local cache coverage
```

## Architecture

The project wraps two separate Coros APIs behind a unified MCP interface:

### Dual API Design
- **Training Hub web API** (`teameuapi.coros.com` / `teamapi.coros.com`): HRV, daily metrics, activities, workouts. Auth via MD5-hashed password → `accessToken` header. Token TTL: 24 hours.
- **Mobile API** (`apieu.coros.com` / `apius.coros.com`): Sleep stage data (deep/light/REM/awake). Auth via AES-128-CBC encrypted credentials (key reverse-engineered from Coros APK). Token TTL: ~1 hour, **auto-refreshes** by replaying the stored encrypted login payload.

### Token Storage (`coros_mcp/auth/`)
Priority chain for retrieval: `COROS_ACCESS_TOKEN` env var → system keyring → encrypted local file. The env var replaces only the web access token — stored user_id, region, and mobile token/payload are merged in so sleep data keeps working. On write, both keyring and encrypted file are updated (belt-and-suspenders). The entire `StoredAuth` object (web token + mobile token + mobile login payload for replay) is serialized as JSON and stored as a single credential.

### Key Files
All source modules live in the `coros_mcp/` package:

- **`coros_mcp/server.py`**: FastMCP tool definitions. Each `@mcp.tool()` function validates auth, delegates to `coros_api`, and returns a dict. This is the only file that imports from `fastmcp`. `_run_with_auth()` retries once after re-login — for non-idempotent writes (create/schedule) only on auth errors (`retry_all=False`), to avoid duplicate server-side writes.
- **`coros_mcp/coros_api.py`**: All HTTP logic. Contains two sets of endpoints (Training Hub + mobile), the AES encryption for mobile auth, auto-refresh logic, and response parsers. API errors raise `CorosAPIError` (carries the raw Coros result code). The `fetch_daily_records()` function merges two endpoints: `/analyse/dayDetail/query` (long range, no VO2max) + `/analyse/query` (last 28 days, has VO2max/fitness fields).
- **`coros_mcp/models.py`**: Pydantic v2 models: `StoredAuth`, `DailyRecord`, `SleepRecord`/`SleepPhases`, `HRVRecord`, `ActivitySummary`.
- **`coros_mcp/cli.py`**: CLI entry point registered as `coros-mcp` script. Delegates to `coros_api.login()` / `login_mobile()` and `cache.sync.sync_all()`.
- **`coros_mcp/cache/`**: SQLite-backed local data store. `store.py` — raw read/write; `sync.py` — smart fetch logic (resolve gaps, backfill, chunk), `_resolve_fetch_range()` decides what to hit the API for, `_fetch_chunked()` splits long uncached ranges into 12-week API calls; `utils.py` — timezone helpers.
- **`coros_mcp/auth/`**: Token storage abstraction. Priority chain: env var → encrypted file → keyring. `encrypted_store.py` uses AES-256-GCM with a machine-bound key (machine *binding* against off-machine leaks, not protection from local attackers — that comes from 0600 file perms); `keyring_store.py` wraps the system keyring.

### API Response Pattern
All Coros API responses return `result: "0000"` on success. Any other value indicates an error — check `message` field. Large time-series fields (`graphList`, `frequencyList`, `gpsLightDuration`) are stripped from activity detail responses to keep them manageable.

### Region Handling
Regions (`eu`, `us`) map to different base URLs for both APIs. EU tokens only work on EU endpoints — mixing regions causes auth failures.
