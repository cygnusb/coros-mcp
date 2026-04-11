"""CLI commands for Coros MCP Server."""
import asyncio
import getpass
import sys
import time

from auth.storage import clear_token, get_token, is_keyring_available
from coros_api import TOKEN_TTL_MS, get_stored_auth, login, login_mobile


def _prompt_credentials() -> tuple[str, str, str]:
    """Prompt for email, password, and region. Returns (email, password, region)."""
    email = input("Email: ").strip()
    if not email:
        print("Error: email is required.")
        sys.exit(1)

    password = getpass.getpass("Password: ")
    if not password:
        print("Error: password is required.")
        sys.exit(1)

    print()
    print("Region options: eu, us, asia")
    region = input("Region [eu]: ").strip().lower() or "eu"
    if region not in ("eu", "us", "asia"):
        print(f"Warning: unknown region '{region}', using it anyway.")
    return email, password, region


def cmd_auth() -> int:
    """Authenticate with Coros credentials and store token in keyring."""
    print("Coros MCP — Authentication")
    print()

    if is_keyring_available():
        print("Token will be stored in your system keyring.")
    else:
        print("System keyring not available — token will be stored in an encrypted local file.")
    print()

    email, password, region = _prompt_credentials()
    print()
    print("Authenticating…")
    try:
        auth = asyncio.run(login(email, password, region, skip_mobile=False))
        print(f"✓ Authenticated as user {auth.user_id} (region: {auth.region})")
        print("  Token stored securely. You only need to do this once.")
        return 0
    except Exception as e:
        print(f"✗ Authentication failed: {e}")
        return 1


def cmd_auth_web() -> int:
    """Authenticate with Coros web API only (no mobile token)."""
    print("Coros MCP — Web API Authentication")
    print()

    email, password, region = _prompt_credentials()
    print()
    print("Authenticating (web only)…")
    try:
        auth = asyncio.run(login(email, password, region, skip_mobile=True))
        print(f"✓ Web API authenticated as user {auth.user_id} (region: {auth.region})")
        print("  Mobile token skipped — sleep data will not be available.")
        return 0
    except Exception as e:
        print(f"✗ Authentication failed: {e}")
        return 1


def cmd_auth_mobile() -> int:
    """Authenticate with Coros mobile API only."""
    print("Coros MCP — Mobile API Authentication")
    print()

    email, password, region = _prompt_credentials()
    print()
    print("Authenticating (mobile only)…")
    try:
        auth = asyncio.run(login_mobile(email, password, region))
        print(f"✓ Mobile API authenticated (region: {auth.region})")
        print("  Sleep data is now available.")
        return 0
    except Exception as e:
        print(f"✗ Mobile authentication failed: {e}")
        return 1


def cmd_auth_status() -> int:
    """Check whether valid tokens are stored."""
    auth = get_stored_auth()
    if auth:
        age_ms = int(time.time() * 1000) - auth.timestamp
        remaining_hours = round((TOKEN_TTL_MS - age_ms) / 3_600_000, 1)

        # Web token status
        if auth.access_token:
            print(f"✓ Web API    — user_id: {auth.user_id}, region: {auth.region}, expires in ~{remaining_hours}h")
        else:
            print("✗ Web API    — not authenticated")

        # Mobile token status
        if auth.mobile_access_token:
            print("✓ Mobile API — token present (sleep data available)")
        elif auth.mobile_login_payload:
            print("⚠ Mobile API — token expired (can auto-refresh)")
        else:
            print("✗ Mobile API — not authenticated (run 'coros-mcp auth' or 'coros-mcp auth-mobile')")

        return 0
    else:
        result = get_token()
        if result.success:
            print("⚠ Token found but may be expired. Run 'coros-mcp auth' to re-authenticate.")
        else:
            print("✗ Not authenticated. Run 'coros-mcp auth' to log in.")
        return 1


def cmd_auth_clear() -> int:
    """Remove stored token from all backends."""
    result = clear_token()
    if result.success:
        print("✓ Token cleared.")
        return 0
    else:
        print(f"✗ {result.message}")
        return 1


def cmd_sync() -> int:
    """Full historical sync: pull all data from Coros and store locally."""
    import asyncio
    from cache.sync import sync_all
    from cache.store import cache_status

    auth = get_stored_auth()
    if auth is None:
        print("✗ Not authenticated. Run 'coros-mcp auth' first.")
        return 1

    # Parse optional --from flag
    start_day = None
    args = sys.argv[2:]
    if args and args[0] == "--from" and len(args) >= 2:
        start_day = args[1]
    elif args and args[0].startswith("--from="):
        start_day = args[0].split("=", 1)[1]

    if not start_day:
        from datetime import datetime, timedelta
        start_day = (datetime.now() - timedelta(days=730)).strftime("%Y%m%d")

    print(f"Coros MCP — Full Historical Sync (from {start_day})")
    print("This may take a few minutes for a large date range.")
    print()

    async def _run():
        async def on_progress(msg: str):
            print(f"  {msg}")

        return await sync_all(auth, start_day, on_progress=on_progress)

    try:
        stats = asyncio.run(_run())
        print()
        print(f"✓ Sync complete")
        print(f"  Daily records : {stats['daily']}")
        print(f"  Sleep records : {stats['sleep']}")
        print(f"  Activities    : {stats['activities']}")
        if stats["errors"]:
            print(f"  Errors        : {len(stats['errors'])}")
            for e in stats["errors"]:
                print(f"    - {e}")
        c = stats.get("cache", {})
        print()
        print("Cache coverage:")
        for key in ("daily_records", "sleep_records", "activities"):
            s = c.get(key, {})
            print(f"  {key:16s}: {s.get('count', 0)} records  [{s.get('from', '—')} → {s.get('to', '—')}]")
        return 0
    except Exception as e:
        print(f"✗ Sync failed: {e}")
        return 1


def cmd_cache_status() -> int:
    """Show local cache coverage."""
    from cache.store import cache_status, init_db
    init_db()
    c = cache_status()
    print(f"Cache: {c['db_path']}")
    print()
    for key in ("daily_records", "sleep_records", "activities"):
        s = c[key]
        if s["count"]:
            print(f"  {key:16s}: {s['count']:5d} records  [{s['from']} → {s['to']}]")
        else:
            print(f"  {key:16s}:     0 records  (empty — run 'coros-mcp sync')")
    return 0


def cmd_serve() -> int:
    """Start the MCP server (stdio mode)."""
    import server
    server.main()
    return 0


def cmd_help() -> int:
    print(
        """Coros MCP Server — CLI

Usage:
  coros-mcp serve                   Start the MCP server (used by Claude Code / OpenClaw)
  coros-mcp auth                    Authenticate with your Coros account (web + mobile)
  coros-mcp auth-web                Authenticate web API only (no sleep data)
  coros-mcp auth-mobile             Authenticate mobile API only (sleep data)
  coros-mcp auth-status             Check status of both tokens
  coros-mcp auth-clear              Remove stored token
  coros-mcp sync [--from YYYYMMDD]  Full historical sync to local cache (default: 2 years)
  coros-mcp cache-status            Show local cache coverage
  coros-mcp help                    Show this help message
"""
    )
    return 0


def main() -> None:
    command = sys.argv[1] if len(sys.argv) > 1 else "help"
    commands = {
        "serve": cmd_serve,
        "auth": cmd_auth,
        "auth-web": cmd_auth_web,
        "auth-mobile": cmd_auth_mobile,
        "auth-status": cmd_auth_status,
        "auth-clear": cmd_auth_clear,
        "sync": cmd_sync,
        "cache-status": cmd_cache_status,
        "help": cmd_help,
        "--help": cmd_help,
        "-h": cmd_help,
    }
    if command in commands:
        sys.exit(commands[command]())
    else:
        print(f"Unknown command: {command}")
        print("Run 'coros-mcp help' for usage.")
        sys.exit(1)


if __name__ == "__main__":
    main()
