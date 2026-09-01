"""
HmacKeyring — rotation-capable storage for the HMAC key(s) used to
sign/verify execution receipts (gcon.execution.verifier
.ExecutionVerifier). Closes the "mTLS certs and HMAC keys are static
once issued" gap for the HMAC half; see gcon.transport.tls_rotation
for the mTLS half.

File format
------------
A single JSON file at GCON_HMAC_KEY_PATH (default
./keys/hmac_secret.key -- same path a pre-rotation deployment already
used, see the migration note below):

    {
      "current_key_id": "k_...",
      "keys": {
        "k_...": {"secret": "<hex>", "created_at": "...", "retired_at": null},
        "k_...": {"secret": "<hex>", "created_at": "...", "retired_at": "..."}
      }
    }

`current_key_id` signs every new proof. Every key in `keys` --
current or retired -- can still verify a proof that names it via
key_id, so rotating never breaks verification of receipts already
issued under the old key; a retired key is only a "no longer used to
sign new things" marker, not a revocation. Real revocation (a key you
believe is compromised, not just superseded) is `revoke()`, which
actually removes the entry -- a proof naming a revoked key_id fails
verification from that point on. That's a real, if narrow, tradeoff:
an operator has to choose between "rotate" (safe, backward
compatible, old receipts keep verifying) and "revoke" (a compromised
key stops working immediately, at the cost of every receipt it ever
signed becoming unverifiable) -- the two are deliberately different
methods rather than one with a flag, so the caller can't reach for
revoke by habit.

Migration from a pre-rotation deployment
------------------------------------------
Before this, GCON_HMAC_KEY_PATH held a bare hex string, not JSON. On
load, a file that fails to parse as this keyring's JSON schema is
treated as exactly that: its raw contents become the secret of a
single key (key_id "legacy") and the file is rewritten in the new
format at the same path -- every receipt already signed under that
key keeps verifying (same secret, same key_id-less proof shape
verify_signature already falls back to), and rotation is available
from that point on.
"""

from __future__ import annotations

import json
import secrets
import uuid
from datetime import datetime, UTC
from pathlib import Path
from typing import Dict, Optional

import logging

logger = logging.getLogger(__name__)


def _new_key_id() -> str:
    return f"k_{uuid.uuid4().hex[:16]}"


class HmacKeyring:
    def __init__(self, path: Path, current_key_id: str, keys: Dict[str, dict]):
        self.path = path
        self.current_key_id = current_key_id
        self._keys = keys  # key_id -> {"secret", "created_at", "retired_at"}

    @property
    def current_secret(self) -> str:
        return self._keys[self.current_key_id]["secret"]

    def get_secret(self, key_id: str) -> Optional[str]:
        entry = self._keys.get(key_id)
        return entry["secret"] if entry else None

    def list_keys(self) -> Dict[str, dict]:
        """Metadata only -- never includes the raw secret, this is
        what an API/dashboard view of key state should return."""
        return {
            key_id: {"created_at": e["created_at"], "retired_at": e["retired_at"],
                      "current": key_id == self.current_key_id}
            for key_id, e in self._keys.items()
        }

    def rotate(self) -> str:
        """Generates a new key and makes it current. The previous
        current key is marked retired (timestamped) but kept -- it
        can still verify old receipts, see module docstring. Returns
        the new key_id."""
        now = datetime.now(UTC).isoformat()
        if self.current_key_id in self._keys:
            self._keys[self.current_key_id]["retired_at"] = now
        new_id = _new_key_id()
        self._keys[new_id] = {"secret": secrets.token_hex(32), "created_at": now, "retired_at": None}
        self.current_key_id = new_id
        self._save()
        logger.info(f"Rotated HMAC signing key -> {new_id}")
        return new_id

    def revoke(self, key_id: str) -> None:
        """Permanently removes a key -- see module docstring for why
        this is different from (and more destructive than) rotating
        away from it. Raises ValueError for the current key (rotate
        first) or an unknown key_id."""
        if key_id == self.current_key_id:
            raise ValueError(
                f"'{key_id}' is the current signing key; rotate() to a new "
                "key before revoking this one."
            )
        if key_id not in self._keys:
            raise ValueError(f"Unknown key_id '{key_id}'")
        del self._keys[key_id]
        self._save()
        logger.warning(f"Revoked HMAC key {key_id} -- receipts signed with it will no longer verify")

    def _save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {"current_key_id": self.current_key_id, "keys": self._keys}
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        tmp.write_text(json.dumps(payload, indent=2))
        tmp.replace(self.path)  # atomic on POSIX and Windows

    @classmethod
    def load_or_create(cls, path_str: str) -> "HmacKeyring":
        path = Path(path_str)
        path.parent.mkdir(parents=True, exist_ok=True)

        if path.exists():
            raw = path.read_text()
            try:
                data = json.loads(raw)
                if "current_key_id" in data and "keys" in data:
                    return cls(path, data["current_key_id"], data["keys"])
            except json.JSONDecodeError:
                pass
            # Not (valid) JSON -- the pre-rotation bare-hex-key format.
            # Migrate in place, see module docstring.
            legacy_secret = raw.strip()
            now = datetime.now(UTC).isoformat()
            keyring = cls(path, "legacy", {
                "legacy": {"secret": legacy_secret, "created_at": now, "retired_at": None}
            })
            keyring._save()
            logger.info(f"Migrated legacy HMAC key at {path} into keyring format (key_id=legacy)")
            return keyring

        now = datetime.now(UTC).isoformat()
        key_id = _new_key_id()
        keyring = cls(path, key_id, {
            key_id: {"secret": secrets.token_hex(32), "created_at": now, "retired_at": None}
        })
        keyring._save()
        logger.info(f"Generated new HMAC signing key at {path} (key_id={key_id})")
        return keyring
