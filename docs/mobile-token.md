# Getting a Mobile API Token for Sleep Data

Sleep data is served by the Coros mobile API (`apieu.coros.com`), which uses a separate
authentication token from the Training Hub web API. You need to capture this token once
using a proxy tool — after that, the server refreshes it automatically.

## What You Need

- An Android device or emulator with the Coros app installed
- [mitmproxy](https://mitmproxy.org/) installed on your Mac/Linux machine
- Both devices on the same Wi-Fi network

## Steps

### 1. Install mitmproxy

```bash
brew install mitmproxy   # macOS
# or: pip install mitmproxy
```

### 2. Start the proxy

```bash
mitmdump -s /dev/null
```

Note the IP address of your machine (e.g. `192.168.1.100`). The proxy listens on port `8080`.

### 3. Configure your Android device

1. Go to **Settings → Wi-Fi → (long-press your network) → Modify network**
2. Set **Proxy** to **Manual**
3. Enter your machine's IP as **Proxy hostname** and `8080` as **Port**
4. Save

### 4. Install the mitmproxy CA certificate on Android

1. Open the browser on your Android device and go to `http://mitm.it`
2. Tap **Android** and follow the instructions to install the certificate
3. Go to **Settings → Security → Trusted credentials → User** and verify the mitmproxy cert is listed

### 5. Capture the login

1. Open the Coros app on Android — **log out first** if already logged in
2. Log back in with your Coros credentials
3. On your Mac, you will see traffic in the mitmdump output

### 6. Extract the token

Run this to read the captured token from the live proxy output:

```bash
mitmdump '~u coros/user/login and ~s' --flow-detail 2 2>&1 | grep accessToken
```

Or save a dump file and read it afterward:

```bash
# During capture:
mitmdump -w coros_login.dump

# After capture, filter for login:
mitmdump -r coros_login.dump --mode regular@8082 '~u login' --flow-detail 3 2>&1 | grep -A5 '"accessToken"'
```

The login response contains:
```json
{
  "data": {
    "accessToken": "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
    ...
  }
}
```

### 7. Store the token and enable auto-refresh

Save the dump file during capture (recommended):

```bash
mitmdump -w coros_login.dump
```

Then extract the token and login payload in one step:

```bash
coros-mcp extract-from-dump coros_login.dump
```

This stores both the current token **and** the encrypted login payload. The server will
automatically refresh the token when it expires — no repeat capture needed.

**Alternative (no auto-refresh):** If you only have the token string, run:

```bash
coros-mcp set-mobile-token
# Paste the accessToken value when prompted
```

### 8. Clean up

Remove the proxy settings from your Android device's Wi-Fi configuration.

## Token Lifetime

The mobile token expires after approximately 1 hour. When `extract-from-dump` has been
used, the server refreshes it automatically in the background — `get_sleep_data` will
always work without any manual intervention.

If only `set-mobile-token` was used (no auto-refresh), re-run the capture and
`extract-from-dump` once to permanently enable auto-refresh.

## Security Note

The token is stored in the same secure location as your Training Hub token (system keyring
or encrypted local file). It is never written to disk in plaintext and is not committed to
source code.
