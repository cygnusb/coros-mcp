"""CLI commands for Coros MCP Server."""
import asyncio
import getpass
import sys

from auth.storage import clear_token, get_token, is_keyring_available
from coros_api import get_stored_auth, login


def cmd_auth() -> int:
    """Authenticate with Coros credentials and store token in keyring."""
    print("Coros MCP — Authentication")
    print()

    if is_keyring_available():
        print("Token will be stored in your system keyring.")
    else:
        print("System keyring not available — token will be stored in an encrypted local file.")
    print()

    email = input("Email: ").strip()
    if not email:
        print("Error: email is required.")
        return 1

    password = getpass.getpass("Password: ")
    if not password:
        print("Error: password is required.")
        return 1

    print()
    print("Region options: eu, us, asia")
    region = input("Region [eu]: ").strip().lower() or "eu"
    if region not in ("eu", "us", "asia"):
        print(f"Warning: unknown region '{region}', using it anyway.")

    print()
    print("Authenticating…")
    try:
        auth = asyncio.run(login(email, password, region))
        print(f"✓ Authenticated as user {auth.user_id} (region: {auth.region})")
        print("  Token stored securely. You only need to do this once.")
        return 0
    except Exception as e:
        print(f"✗ Authentication failed: {e}")
        return 1


def cmd_auth_status() -> int:
    """Check whether a valid token is stored."""
    auth = get_stored_auth()
    if auth:
        print(f"✓ Authenticated — user_id: {auth.user_id}, region: {auth.region}")
        return 0
    else:
        result = get_token()
        if result.success:
            print("⚠ Token found but may be expired. Run 'coros-mcp auth' to re-authenticate.")
        else:
            print("✗ Not authenticated. Run 'coros-mcp auth' to log in.")
        return 1


def cmd_set_mobile_token() -> int:
    """Store a mobile API token (from mitmproxy capture) for sleep data access."""
    import json
    from coros_api import _load_auth, _save_auth

    auth = _load_auth()
    if auth is None:
        print("✗ Not authenticated. Run 'coros-mcp auth' first.")
        return 1

    print("Paste the mobile accessToken from your mitmproxy capture:")
    token = input("Token: ").strip()
    if not token:
        print("Error: token is required.")
        return 1

    auth.mobile_access_token = token
    _save_auth(auth)
    print("✓ Mobile token stored. Sleep data should now work.")
    return 0


def cmd_auth_clear() -> int:
    """Remove stored token from all backends."""
    result = clear_token()
    if result.success:
        print("✓ Token cleared.")
        return 0
    else:
        print(f"✗ {result.message}")
        return 1


def cmd_help() -> int:
    print(
        """Coros MCP Server — CLI

Usage:
  coros-mcp auth                Authenticate with your Coros account
  coros-mcp auth-status         Check if a valid token is stored
  coros-mcp auth-clear          Remove stored token
  coros-mcp set-mobile-token    Store a mobile API token for sleep data (from mitmproxy)
  coros-mcp help                Show this help message
"""
    )
    return 0


def main() -> None:
    command = sys.argv[1] if len(sys.argv) > 1 else "help"
    commands = {
        "auth": cmd_auth,
        "auth-status": cmd_auth_status,
        "auth-clear": cmd_auth_clear,
        "set-mobile-token": cmd_set_mobile_token,
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
