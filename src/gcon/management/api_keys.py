"""
GCON API Keys — programmatic access key management.

Keys are generated with a random secret which is only ever shown
in full at creation/regeneration time; afterward only a masked form
is retrievable, matching how most platforms handle API secrets.

Persistence: every mutation is written through to the shared
`Database`, and all keys are loaded back into memory on registry
construction, so revocations/usage history survive a restart.

The raw secret is NEVER persisted. Only its SHA-256 hash (used for
authentication lookups) and a display mask are stored -- same approach
as EnrollTokenRepository. API secrets are 160 bits of randomness, so a
plain fast hash is sufficient here (unlike a human-chosen password,
there is nothing to brute-force); it also keeps per-request
authentication cheap. The full secret exists only on the APIKey object
returned by create_key()/regenerate_key() (so it can be shown once) and
is not recoverable afterwards, including after a restart.

This pass also closes the two concurrency bugs stress_test2.py found
in the pure in-memory version of this file:
  - find_by_secret() now iterates over a snapshot (`list(...)`)
    instead of a live dict view, so a concurrent create_key() can no
    longer trigger "RuntimeError: dictionary changed size during
    iteration".
  - mark_used()'s usage_count increment and is_valid()'s
    active/expired check now happen inside the registry's write lock
    (via Database.transaction), so concurrent authentications against
    the same key no longer lose increments to a read-modify-write race.
"""

import hashlib
import logging
import secrets
from datetime import datetime, UTC, timedelta
from uuid import uuid4

from ..storage.database import Database, dumps, loads

logger = logging.getLogger(__name__)


def _generate_secret():
    return f"gcon_{secrets.token_hex(20)}"


def _mask(secret):
    return f"{secret[:8]}{'*' * 24}{secret[-4:]}"


def _hash_secret(secret):
    return hashlib.sha256(secret.encode("utf-8")).hexdigest()


class APIKey:
    def __init__(self, name, owner_user_id, scopes=None, expires_in_days=90, key_id=None):
        self.key_id = key_id or f"key_{uuid4().hex[:8]}"
        self.name = name
        self.owner_user_id = owner_user_id
        self.scopes = scopes or ["Submit workflows", "View monitoring"]
        self._set_secret(_generate_secret())
        self.created_at = datetime.now(UTC)
        self.expires_at = (
            self.created_at + timedelta(days=expires_in_days)
            if expires_in_days else None
        )
        self.last_used_at = None
        self.usage_count = 0
        self.status = "Active"

    def _set_secret(self, secret):
        """Sets the in-memory secret and derives the only two things
        that are ever persisted from it (hash + display mask)."""
        self.secret = secret
        self.secret_hash = _hash_secret(secret)
        self.secret_masked = _mask(secret)

    def to_dict(self, reveal_secret=False):
        if reveal_secret and self.secret is None:
            raise ValueError(
                f"The secret for API key '{self.key_id}' is not recoverable -- "
                "only its hash is stored. Regenerate the key to get a new one."
            )
        return {
            "key_id": self.key_id,
            "name": self.name,
            "owner_user_id": self.owner_user_id,
            "scopes": self.scopes,
            "secret": self.secret if reveal_secret else self.secret_masked,
            "created_at": self.created_at.isoformat(),
            "expires_at": self.expires_at.isoformat() if self.expires_at else None,
            "last_used_at": self.last_used_at.isoformat() if self.last_used_at else None,
            "usage_count": self.usage_count,
            "status": self.status,
        }

    # --- persistence helpers -------------------------------------------------

    def _row(self):
        return (
            self.key_id, self.name, self.owner_user_id, dumps(self.scopes), "",
            self.secret_hash, self.secret_masked,
            self.created_at.isoformat(), self.expires_at.isoformat() if self.expires_at else None,
            self.last_used_at.isoformat() if self.last_used_at else None,
            self.usage_count, self.status,
        )

    @classmethod
    def _from_row(cls, row):
        key = cls(row["name"], row["owner_user_id"], loads(row["scopes_json"], default=[]),
                   expires_in_days=None, key_id=row["key_id"])
        key.secret = None
        key.secret_hash = row["secret_hash"]
        key.secret_masked = row["secret_masked"]
        key.created_at = datetime.fromisoformat(row["created_at"])
        key.expires_at = datetime.fromisoformat(row["expires_at"]) if row["expires_at"] else None
        key.last_used_at = datetime.fromisoformat(row["last_used_at"]) if row["last_used_at"] else None
        key.usage_count = row["usage_count"]
        key.status = row["status"]
        return key


class APIKeyManager:
    def __init__(self, db: Database = None):
        self.db = db or Database(":memory:")
        self.keys = {}
        # secret_hash -> APIKey, so authentication is one dict lookup
        # instead of scanning every key. Keyed by the hash, not the
        # secret: the raw secret is never held here.
        self._by_hash = {}
        self._hash_legacy_plaintext_secrets()
        for row in self.db.query("SELECT * FROM api_keys"):
            key = APIKey._from_row(row)
            self.keys[key.key_id] = key
            if key.secret_hash:
                self._by_hash[key.secret_hash] = key

    def _hash_legacy_plaintext_secrets(self):
        """
        One-time, idempotent conversion of rows written before secrets
        were hashed: hash + mask each raw secret, then blank the raw
        column. Existing keys keep working (the same raw secret hashes
        to the same value), they just stop being stored recoverably.
        Finds nothing to do on every boot after the first.

        Blanking the column alone is not enough -- SQLite leaves the
        old bytes in freed pages and in the WAL file -- so this also
        vacuums and checkpoints afterwards (Database.scrub()).
        """
        rows = self.db.query(
            "SELECT key_id, secret FROM api_keys "
            "WHERE secret_hash IS NULL AND secret != ''"
        )
        if not rows:
            return
        with self.db.transaction() as conn:
            for row in rows:
                conn.execute(
                    "UPDATE api_keys SET secret_hash = ?, secret_masked = ?, secret = '' "
                    "WHERE key_id = ? AND secret_hash IS NULL",
                    (_hash_secret(row["secret"]), _mask(row["secret"]), row["key_id"]),
                )
        try:
            self.db.scrub()
        except Exception as e:
            logger.warning(
                "Hashed %d legacy API key(s) but could not vacuum the database to "
                "remove the old plaintext from free pages: %s", len(rows), e,
            )
        logger.info("Converted %d legacy plaintext API key(s) to hashed storage.", len(rows))

    def _persist(self, key):
        with self.db.transaction() as conn:
            conn.execute(
                """INSERT INTO api_keys (key_id, name, owner_user_id, scopes_json, secret,
                       secret_hash, secret_masked,
                       created_at, expires_at, last_used_at, usage_count, status)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(key_id) DO UPDATE SET
                       name=excluded.name, owner_user_id=excluded.owner_user_id,
                       scopes_json=excluded.scopes_json, secret=excluded.secret,
                       secret_hash=excluded.secret_hash, secret_masked=excluded.secret_masked,
                       created_at=excluded.created_at, expires_at=excluded.expires_at,
                       last_used_at=excluded.last_used_at, usage_count=excluded.usage_count,
                       status=excluded.status""",
                key._row(),
            )

    def create_key(self, name, owner_user_id, scopes=None, expires_in_days=90):
        key = APIKey(name, owner_user_id, scopes, expires_in_days)
        self.keys[key.key_id] = key
        self._by_hash[key.secret_hash] = key
        self._persist(key)
        return key

    def get_key(self, key_id):
        if key_id not in self.keys:
            raise ValueError(f"API key '{key_id}' does not exist.")
        return self.keys[key_id]

    def revoke_key(self, key_id):
        with self.db.transaction():
            key = self.get_key(key_id)
            key.status = "Revoked"
            self._persist(key)
        return key

    def regenerate_key(self, key_id):
        with self.db.transaction():
            key = self.get_key(key_id)
            self._by_hash.pop(key.secret_hash, None)
            key._set_secret(_generate_secret())
            self._by_hash[key.secret_hash] = key
            key.status = "Active"
            key.created_at = datetime.now(UTC)
            key.last_used_at = None
            key.usage_count = 0
            self._persist(key)
        return key

    def list_keys(self):
        return list(self.keys.values())

    def find_by_secret(self, secret):
        """
        Look up a key by its raw secret, for use by the public API's
        authentication layer. Hashes the presented secret and does one
        dict lookup -- no scan over every key, and no raw secret is
        held anywhere to compare against. (A plain hash lookup, rather
        than a constant-time compare, is fine here: the attacker
        doesn't control the hash, so timing reveals nothing usable
        about a 160-bit secret.)

        The returned key may be revoked/expired -- callers must still
        check is_valid(), same as before.
        """
        if not secret:
            return None
        return self._by_hash.get(_hash_secret(secret))

    def is_valid(self, key):
        """
        Return True if a key is active and not expired. The auto
        "Active -> Expired" transition and its persistence happen
        under the write lock so it can't race with a concurrent
        revoke/regenerate touching the same row.
        """
        if key.status != "Active":
            return False
        if key.expires_at and datetime.now(UTC) > key.expires_at:
            with self.db.transaction():
                key.status = "Expired"
                self._persist(key)
            return False
        return True

    def mark_used(self, key):
        """
        Record key usage. Pulled out of APIKey.mark_used() and into
        the manager so the increment + persist happen atomically
        under the write lock — the previous unguarded
        `self.usage_count += 1` lost increments under concurrent
        authentication against the same key.
        """
        with self.db.transaction():
            key.last_used_at = datetime.now(UTC)
            key.usage_count += 1
            self._persist(key)
        return key
