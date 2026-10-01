"""
API keys are stored as a SHA-256 hash + display mask, never as the raw
secret (management/api_keys.py, migration 5 in storage/migrations.py).

Covers: nothing raw reaches disk (checked against the actual file
bytes, not just the column), authentication still works off the hash,
the secret is one-time-reveal only, regenerate invalidates the old
secret, and pre-existing plaintext rows are converted in place -- keys
that already exist keep working, and their old bytes are scrubbed from
the file.
"""
import glob
import hashlib
import sqlite3

import pytest

from gcon.management.api_keys import APIKeyManager
from gcon.storage.database import Database
from gcon.storage.migrations import MIGRATIONS


def _file_bytes(db_path):
    return b"".join(open(p, "rb").read() for p in glob.glob(str(db_path) + "*"))


@pytest.fixture
def db_path(tmp_path):
    return tmp_path / "gcon.db"


class TestNewKeys:
    def test_raw_secret_is_never_written_to_disk(self, db_path):
        db = Database(str(db_path))
        key = APIKeyManager(db).create_key("ci", "user-1")
        secret = key.secret
        row = db.query_one("SELECT * FROM api_keys WHERE key_id = ?", (key.key_id,))
        db.close()

        assert row["secret"] == ""
        assert row["secret_hash"] == hashlib.sha256(secret.encode()).hexdigest()
        assert secret.encode() not in _file_bytes(db_path)

    def test_authentication_works_from_the_hash(self, db_path):
        mgr = APIKeyManager(Database(str(db_path)))
        key = mgr.create_key("ci", "user-1")
        assert mgr.find_by_secret(key.secret) is key
        assert mgr.find_by_secret("gcon_" + "0" * 40) is None
        assert mgr.find_by_secret("") is None
        assert mgr.find_by_secret(None) is None

    def test_secret_is_revealed_at_creation_but_not_after_a_restart(self, db_path):
        db = Database(str(db_path))
        key = APIKeyManager(db).create_key("ci", "user-1")
        assert key.to_dict(reveal_secret=True)["secret"] == key.secret
        db.close()

        reloaded = APIKeyManager(Database(str(db_path))).get_key(key.key_id)
        assert reloaded.to_dict()["secret"] == key.to_dict()["secret"]  # mask survives
        assert "*" in reloaded.to_dict()["secret"]
        with pytest.raises(ValueError, match="not recoverable"):
            reloaded.to_dict(reveal_secret=True)

    def test_key_still_authenticates_after_restart(self, db_path):
        db = Database(str(db_path))
        key = APIKeyManager(db).create_key("ci", "user-1")
        secret = key.secret
        db.close()

        mgr = APIKeyManager(Database(str(db_path)))
        found = mgr.find_by_secret(secret)
        assert found is not None and found.key_id == key.key_id
        assert mgr.is_valid(found)

    def test_regenerate_kills_the_old_secret_and_persists_only_the_new_hash(self, db_path):
        db = Database(str(db_path))
        mgr = APIKeyManager(db)
        key = mgr.create_key("ci", "user-1")
        old_secret = key.secret
        mgr.regenerate_key(key.key_id)
        new_secret = key.secret
        db.close()

        assert old_secret != new_secret
        mgr2 = APIKeyManager(Database(str(db_path)))
        assert mgr2.find_by_secret(old_secret) is None
        assert mgr2.find_by_secret(new_secret).key_id == key.key_id
        blob = _file_bytes(db_path)
        assert old_secret.encode() not in blob
        assert new_secret.encode() not in blob

    def test_revoked_key_is_still_found_but_invalid(self, db_path):
        mgr = APIKeyManager(Database(str(db_path)))
        key = mgr.create_key("ci", "user-1")
        mgr.revoke_key(key.key_id)
        found = mgr.find_by_secret(key.secret)
        assert found is key
        assert not mgr.is_valid(found)


class TestLegacyPlaintextConversion:
    """Databases written before this change hold raw secrets. They must
    convert in place with no key ever stopping working."""

    def _make_pre_migration_5_db(self, path, legacy_rows):
        """A database exactly as it existed with migrations 1-4 applied:
        no secret_hash/secret_masked columns, raw secret indexed."""
        conn = sqlite3.connect(str(path))
        conn.execute(
            "CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY, "
            "name TEXT NOT NULL, applied_at TEXT NOT NULL)"
        )
        for m in MIGRATIONS:
            if m.version >= 5:
                continue
            for stmt in m.up_sql:
                conn.execute(stmt)
            conn.execute(
                "INSERT INTO schema_migrations VALUES (?, ?, 'x')", (m.version, m.name)
            )
        for kid, secret, status in legacy_rows:
            conn.execute(
                "INSERT INTO api_keys (key_id, name, owner_user_id, scopes_json, secret, "
                "created_at, expires_at, last_used_at, usage_count, status) "
                "VALUES (?, 'legacy', 'u1', '[]', ?, '2026-01-01T00:00:00+00:00', NULL, NULL, 7, ?)",
                (kid, secret, status),
            )
        conn.commit()
        conn.close()

    def test_existing_keys_keep_working_and_plaintext_is_scrubbed(self, db_path):
        legacy = [
            ("key_a", "gcon_" + "a1" * 20, "Active"),
            ("key_b", "gcon_" + "b2" * 20, "Active"),
            ("key_c", "gcon_" + "c3" * 20, "Revoked"),
        ]
        self._make_pre_migration_5_db(db_path, legacy)
        for _, secret, _ in legacy:
            assert secret.encode() in _file_bytes(db_path)  # premise: it really is plaintext

        db = Database(str(db_path))
        mgr = APIKeyManager(db)

        for kid, secret, status in legacy:
            key = mgr.find_by_secret(secret)
            assert key is not None and key.key_id == kid
            assert mgr.is_valid(key) == (status == "Active")
            assert key.usage_count == 7  # non-secret history untouched
        assert all(
            r["secret"] == "" and r["secret_hash"]
            for r in db.query("SELECT secret, secret_hash FROM api_keys")
        )
        db.close()

        blob = _file_bytes(db_path)
        for _, secret, _ in legacy:
            assert secret.encode() not in blob, "old plaintext survived in the file/WAL"

    def test_mask_of_a_converted_key_matches_the_old_display(self, db_path):
        secret = "gcon_" + "d4" * 20
        self._make_pre_migration_5_db(db_path, [("key_d", secret, "Active")])
        mgr = APIKeyManager(Database(str(db_path)))
        assert mgr.get_key("key_d").to_dict()["secret"] == f"{secret[:8]}{'*' * 24}{secret[-4:]}"

    def test_conversion_is_idempotent_across_restarts(self, db_path):
        secret = "gcon_" + "e5" * 20
        self._make_pre_migration_5_db(db_path, [("key_e", secret, "Active")])

        db = Database(str(db_path)); APIKeyManager(db); db.close()
        db = Database(str(db_path))
        first = db.query_one("SELECT secret_hash, secret_masked FROM api_keys")
        mgr = APIKeyManager(db)
        second = db.query_one("SELECT secret_hash, secret_masked FROM api_keys")
        assert dict(first) == dict(second)
        assert mgr.find_by_secret(secret).key_id == "key_e"
        db.close()

    def test_plaintext_index_is_dropped(self, db_path):
        self._make_pre_migration_5_db(db_path, [])
        db = Database(str(db_path))
        names = {r["name"] for r in db.query("SELECT name FROM sqlite_master WHERE type='index'")}
        assert "idx_api_keys_secret" not in names
        assert "idx_api_keys_secret_hash" in names
        db.close()
