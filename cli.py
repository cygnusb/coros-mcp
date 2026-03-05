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


def cmd_extract_from_dump() -> int:
    """Extract mobile login payload and token from a mitmproxy dump file."""
    import json
    import subprocess
    import tempfile
    import os
    import textwrap
    from coros_api import _load_auth, _save_auth

    if len(sys.argv) < 3:
        print("Usage: coros-mcp extract-from-dump <dump_file>")
        print("  dump_file: path to a mitmproxy .dump file containing a Coros mobile login")
        return 1

    dump_file = sys.argv[2]
    if not os.path.exists(dump_file):
        print(f"✗ File not found: {dump_file}")
        return 1

    auth = _load_auth()
    if auth is None:
        print("✗ Not authenticated. Run 'coros-mcp auth' first.")
        return 1

    script = textwrap.dedent("""
        import json
        from mitmproxy import http

        results = []

        def response(flow: http.HTTPFlow):
            if '/coros/user/login' in flow.request.path:
                try:
                    req = json.loads(flow.request.get_content())
                    res = json.loads(flow.response.get_content())
                except Exception:
                    return
                if res.get('result') == '0000':
                    token = res.get('data', {}).get('accessToken', '')
                    yfheader = flow.request.headers.get('yfheader', '')
                    results.append({'token': token, 'payload': req, 'yfheader': yfheader})

        def done():
            if results:
                print('COROS_EXTRACT:' + json.dumps(results[-1]))
            else:
                print('COROS_EXTRACT:null')
    """)

    with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
        f.write(script)
        script_path = f.name

    try:
        result = subprocess.run(
            ['mitmdump', '-r', dump_file, '--quiet', '-s', script_path],
            capture_output=True, text=True, timeout=30
        )
        output = result.stdout + result.stderr
    finally:
        os.unlink(script_path)

    for line in output.splitlines():
        if line.startswith('COROS_EXTRACT:'):
            data_str = line[len('COROS_EXTRACT:'):]
            if data_str == 'null':
                print("✗ No successful Coros mobile login found in dump.")
                print("  Make sure the dump contains a fresh login, not just an existing session.")
                return 1
            data = json.loads(data_str)
            auth.mobile_access_token = data['token']
            auth.mobile_login_payload = data['payload']
            auth.mobile_yfheader = data['yfheader']
            _save_auth(auth)
            print(f"✓ Token extracted: {data['token'][:16]}…")
            print("✓ Login payload stored — sleep data will auto-refresh when token expires.")
            return 0

    print("✗ Could not parse mitmdump output.")
    print("  Is mitmdump installed? (brew install mitmproxy)")
    if output:
        print(f"  Output: {output[:300]}")
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


def cmd_help() -> int:
    print(
        """Coros MCP Server — CLI

Usage:
  coros-mcp auth                        Authenticate with your Coros account
  coros-mcp auth-status                 Check if a valid token is stored
  coros-mcp auth-clear                  Remove stored token
  coros-mcp set-mobile-token            Store a mobile API token manually (from mitmproxy)
  coros-mcp extract-from-dump <file>    Extract token + login payload from mitmproxy dump
                                        (enables automatic token refresh for sleep data)
  coros-mcp help                        Show this help message
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
        "extract-from-dump": cmd_extract_from_dump,
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
