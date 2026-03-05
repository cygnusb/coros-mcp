# Getting a Mobile API Token for Sleep Data (Legacy)

> **This guide is no longer needed for new setups.**
> Since the AES encryption key was reverse-engineered from `libencrypt-lib.so` in the
> Coros APK, `coros-mcp auth` now obtains a mobile API token automatically with no
> mitmproxy capture required.
>
> Follow this guide only if automatic auth fails or you want to import a token manually.

Sleep data is served by the Coros mobile API (`apieu.coros.com`), which uses a separate
authentication token from the Training Hub web API.

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

### 7. Use the captured token

The token value from the response can be used directly with the Coros mobile API.
If you need to store it for debugging or manual testing, save it from the captured
response JSON (`data.accessToken`).

> In normal operation you don't need to do anything with this token — `coros-mcp auth`
> handles the full mobile login automatically using the AES encryption scheme described
> below.

### 8. Clean up

Remove the proxy settings from your Android device's Wi-Fi configuration.

## Token Lifetime

The mobile token expires after approximately 1 hour. `coros-mcp auth` stores the
encrypted login payload alongside the token, and the server replays it automatically
to refresh — `get_sleep_data` always works without any manual intervention.

## Security Note

The token is stored in the same secure location as your Training Hub token (system keyring
or encrypted local file). It is never written to disk in plaintext and is not committed to
source code.
