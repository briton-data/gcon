"""
Encryption at rest for secrets GCON must be able to READ BACK.

Most stored secrets (API keys, enroll tokens, reset tokens) are only ever
compared, so they are stored as hashes. A webhook signing secret is different:
every delivery is HMAC-signed with it, so it cannot be hashed. Stored in plain
text, a copy of the database (a backup, a leaked dump, a read-only SQL
injection) lets whoever holds it forge signed webhooks to every customer's
endpoint. Here the stored form is encrypted (Fernet: AES-128-CBC + HMAC-SHA256)
with a key that lives OUTSIDE the database.

The key is, in order:
  1. GCON_SECRETS_KEY -- a Fernet key (generate one with
     `python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"`).
     Use this when the disk is not persistent.
  2. A key file at GCON_SECRETS_KEY_PATH (default ./keys/secrets.key), created
     with owner-only permissions on first use. It sits beside the receipt-signing
     key (./keys/hmac_secret.key), which already has to persist for receipts to
     keep verifying, so this adds no new persistence requirement.

A stored value is "enc:v1:<token>". A value without that prefix is a secret
written before this existed: it is still read as-is and is re-written encrypted
the next time the control plane starts (see WebhookRepository).

Losing the key makes the encrypted secrets unreadable (open() raises
SecretDecryptionError). That is deliberate: there is no silent fallback to a
different key.
"""
from __future__ import annotations

import logging
import os
import threading
from pathlib import Path
from typing import Optional

from cryptography.fernet import Fernet, InvalidToken

logger = logging.getLogger(__name__)

PREFIX = "enc:v1:"
_lock = threading.Lock()
_cache: dict = {}


class SecretDecryptionError(RuntimeError):
    pass


def _fernet() -> Fernet:
    env_key = os.environ.get("GCON_SECRETS_KEY", "").strip()
    path = os.environ.get("GCON_SECRETS_KEY_PATH", "./keys/secrets.key")
    cache_key = env_key or str(Path(path).resolve())
    with _lock:
        fernet = _cache.get(cache_key)
        if fernet is not None:
            return fernet
        if env_key:
            try:
                fernet = Fernet(env_key.encode())
            except ValueError as e:
                raise RuntimeError(f"GCON_SECRETS_KEY is not a valid Fernet key: {e}") from e
        else:
            key_path = Path(path)
            if key_path.exists():
                fernet = Fernet(key_path.read_bytes().strip())
            else:
                key_path.parent.mkdir(parents=True, exist_ok=True)
                key = Fernet.generate_key()
                # Owner-only from the moment of creation.
                fd = os.open(str(key_path), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
                with os.fdopen(fd, "wb") as f:
                    f.write(key)
                logger.warning("Created a new secrets encryption key at %s. Keep it with your "
                               "other keys: without it stored webhook secrets cannot be read.", key_path)
                fernet = Fernet(key)
        _cache[cache_key] = fernet
        return fernet


def is_sealed(value: Optional[str]) -> bool:
    return isinstance(value, str) and value.startswith(PREFIX)


def seal(plaintext: str) -> str:
    return PREFIX + _fernet().encrypt(plaintext.encode("utf-8")).decode("ascii")


def open_(stored: Optional[str]) -> Optional[str]:
    """Plaintext for a stored value. A legacy (unprefixed) value is returned as-is."""
    if stored is None or not is_sealed(stored):
        return stored
    try:
        return _fernet().decrypt(stored[len(PREFIX):].encode("ascii")).decode("utf-8")
    except InvalidToken as e:
        raise SecretDecryptionError(
            "A stored secret could not be decrypted: the secrets key is not the one it was "
            "encrypted with (GCON_SECRETS_KEY / GCON_SECRETS_KEY_PATH)."
        ) from e
