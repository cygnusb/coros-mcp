"""Encrypted file-based credential storage for Coros authentication.

Fallback for environments where system keyring is unavailable.
Uses AES-256-GCM with a machine-bound key.
"""

import base64
import contextlib
import functools
import hashlib
import os
import platform
import stat
from pathlib import Path

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from auth.keyring_store import CredentialResult

CONFIG_DIR = Path.home() / ".config" / "coros-mcp"
CREDENTIALS_FILE = CONFIG_DIR / "auth.enc"


@functools.lru_cache(maxsize=1)
def _get_machine_id() -> bytes:
    components = [
        platform.node(),
        platform.machine(),
        platform.system(),
    ]
    try:
        if platform.system() == "Darwin":
            import subprocess
            result = subprocess.run(
                ["ioreg", "-rd1", "-c", "IOPlatformExpertDevice"],
                capture_output=True, text=True, timeout=5,
            )
            for line in result.stdout.split("\n"):
                if "IOPlatformUUID" in line:
                    components.append(line.split("=")[-1].strip().strip('"'))
                    break
    except Exception:
        pass
    try:
        mid = Path("/etc/machine-id")
        if mid.exists():
            components.append(mid.read_text().strip())
    except Exception:
        pass
    return "|".join(components).encode("utf-8")


def _derive_key() -> bytes:
    salt = b"coros-mcp-v1"
    return hashlib.sha256(salt + _get_machine_id()).digest()


def _secure_dir() -> None:
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    with contextlib.suppress(OSError):
        os.chmod(CONFIG_DIR, stat.S_IRWXU)


def store_credential_encrypted(token: str) -> CredentialResult:
    if not token or not token.strip():
        return CredentialResult(success=False, message="Token cannot be empty")
    try:
        _secure_dir()
        nonce = os.urandom(12)
        ciphertext = AESGCM(_derive_key()).encrypt(nonce, token.strip().encode(), None)
        CREDENTIALS_FILE.write_bytes(base64.b64encode(nonce + ciphertext))
        with contextlib.suppress(OSError):
            os.chmod(CREDENTIALS_FILE, stat.S_IRUSR | stat.S_IWUSR)
        return CredentialResult(success=True, message="Token stored in encrypted file")
    except Exception as e:
        return CredentialResult(success=False, message=f"Encryption error: {e}")


def get_credential_encrypted() -> CredentialResult:
    try:
        data = base64.b64decode(CREDENTIALS_FILE.read_bytes())
        token = AESGCM(_derive_key()).decrypt(data[:12], data[12:], None).decode()
        return CredentialResult(success=True, message="Token retrieved", token=token)
    except FileNotFoundError:
        return CredentialResult(success=False, message="No credential file found")
    except Exception as e:
        return CredentialResult(success=False, message=f"Decryption error: {e}")


def clear_credential_encrypted() -> CredentialResult:
    try:
        CREDENTIALS_FILE.unlink(missing_ok=True)
        return CredentialResult(success=True, message="Credential file removed")
    except Exception as e:
        return CredentialResult(success=False, message=f"Error removing file: {e}")
