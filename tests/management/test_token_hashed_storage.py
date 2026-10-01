"""
Session and password-reset tokens (staff and customer) are stored as
SHA-256(token), never raw (management/auth.py). The raw value exists only in
the caller's cookie or reset link.

Covers all four managers: nothing raw reaches the file (checked against the
actual file bytes, not just the column), lookup/expiry/destroy/consume still
work from the hash, visible session ids are unchanged, and rows written
before this change are converted in place -- live sessions and unexpired
reset links keep working, and the old raw values are scrubbed from the file.
"""
import glob
import hashlib
from datetime import datetime, UTC, timedelta

import pytest

from gcon.management.auth import (
    CustomerResetTokenManager,
    CustomerSessionManager,
    ResetTokenManager,
    SessionManager,
    hash_token,
)
from gcon.storage.database import Database


def _file_bytes(path):
    return b"".join(open(p, "rb").read() for p in glob.glob(str(path) + "*"))


@pytest.fixture
def db_path(tmp_path):
    return tmp_path / "gcon.db"


@pytest.fixture
def db(db_path):
    database = Database(str(db_path))
    yield database
    database.close()


def test_hash_token_is_sha256_hex():
    assert hash_token("abc") == hashlib.sha256(b"abc").hexdigest()
    assert len(hash_token("abc")) == 64


class TestStaffSessions:
    def test_raw_token_is_never_stored(self, db, db_path):
        token = SessionManager(db=db).create_session("u1")
        row = db.query_one("SELECT token FROM sessions")
        assert row["token"] == hash_token(token) != token
        assert token.encode() not in _file_bytes(db_path)

    def test_lookup_and_destroy_work_from_the_hash(self, db):
        sm = SessionManager(db=db)
        token = sm.create_session("u1")
        assert sm.get_user_id(token) == "u1"
        assert sm.get_user_id("not-a-real-token") is None
        assert sm.get_user_id(hash_token(token)) is None  # the stored hash is not itself a credential
        sm.destroy_session(token)
        assert sm.get_user_id(token) is None
        assert db.query_one("SELECT COUNT(*) AS c FROM sessions")["c"] == 0

    def test_expired_session_is_rejected_and_removed(self, db):
        sm = SessionManager(db=db, ttl_hours=1)
        token = sm.create_session("u1")
        past = (datetime.now(UTC) - timedelta(hours=2)).isoformat()
        db.execute("UPDATE sessions SET expires_at = ?", (past,))
        assert sm.get_user_id(token) is None
        assert db.query_one("SELECT COUNT(*) AS c FROM sessions")["c"] == 0

    def test_visible_session_id_is_unchanged_by_hashing(self, db):
        sm = SessionManager(db=db)
        token = sm.create_session("u1")
        expected = hashlib.sha256(token.encode()).hexdigest()[:12]  # how it was derived before
        assert [s["session_id"] for s in sm.list_active_for_user("u1")] == [expected]

    def test_survives_a_restart(self, db, db_path):
        token = SessionManager(db=db).create_session("u1")
        db.close()
        reopened = Database(str(db_path))
        try:
            assert SessionManager(db=reopened).get_user_id(token) == "u1"
        finally:
            reopened.close()

    def test_in_memory_mode_is_unchanged(self):
        sm = SessionManager()
        token = sm.create_session("u1")
        assert sm.get_user_id(token) == "u1"


class TestResetTokens:
    def test_raw_token_is_never_stored(self, db, db_path):
        token = ResetTokenManager(db).create_token("u1")
        assert db.query_one("SELECT token FROM password_reset_tokens")["token"] == hash_token(token)
        assert token.encode() not in _file_bytes(db_path)

    def test_single_use(self, db):
        rm = ResetTokenManager(db)
        token = rm.create_token("u1")
        assert rm.get_user_id(token) == "u1"
        rm.consume_token(token)
        assert rm.get_user_id(token) is None

    def test_invalidate_all_for_user(self, db):
        rm = ResetTokenManager(db)
        tokens = [rm.create_token("u1"), rm.create_token("u1"), rm.create_token("u2")]
        rm.invalidate_all_for_user("u1")
        assert [rm.get_user_id(t) for t in tokens] == [None, None, "u2"]

    def test_expired_token_is_rejected(self, db):
        rm = ResetTokenManager(db, ttl_minutes=30)
        token = rm.create_token("u1")
        db.execute(
            "UPDATE password_reset_tokens SET expires_at = ?",
            ((datetime.now(UTC) - timedelta(minutes=1)).isoformat(),),
        )
        assert rm.get_user_id(token) is None


class TestCustomerTokens:
    def test_customer_session_raw_token_is_never_stored(self, db, db_path):
        token = CustomerSessionManager(db).create_session("c1")
        assert db.query_one("SELECT token FROM customer_sessions")["token"] == hash_token(token)
        assert token.encode() not in _file_bytes(db_path)

    def test_customer_session_lifecycle(self, db):
        csm = CustomerSessionManager(db)
        token = csm.create_session("c1")
        assert csm.get_customer_user_id(token) == "c1"
        csm.destroy_session(token)
        assert csm.get_customer_user_id(token) is None
        other = csm.create_session("c2")
        csm.destroy_all_for_user("c2")
        assert csm.get_customer_user_id(other) is None

    def test_customer_session_expiry(self, db):
        csm = CustomerSessionManager(db)
        token = csm.create_session("c1")
        db.execute(
            "UPDATE customer_sessions SET expires_at = ?",
            ((datetime.now(UTC) - timedelta(hours=1)).isoformat(),),
        )
        assert csm.get_customer_user_id(token) is None
        assert db.query_one("SELECT COUNT(*) AS c FROM customer_sessions")["c"] == 0

    def test_customer_reset_token_raw_is_never_stored_and_is_single_use(self, db, db_path):
        crm = CustomerResetTokenManager(db)
        token = crm.create_token("c1")
        assert db.query_one("SELECT token FROM customer_password_reset_tokens")["token"] == hash_token(token)
        assert token.encode() not in _file_bytes(db_path)
        assert crm.get_customer_user_id(token) == "c1"
        crm.consume_token(token)
        assert crm.get_customer_user_id(token) is None

    def test_staff_and_customer_tokens_are_not_interchangeable(self, db):
        sm, csm = SessionManager(db=db), CustomerSessionManager(db)
        staff, customer = sm.create_session("u1"), csm.create_session("c1")
        assert csm.get_customer_user_id(staff) is None
        assert sm.get_user_id(customer) is None


class TestLegacyRawRowsAreConvertedInPlace:
    """Databases written before this change hold raw tokens. Live sessions and
    unexpired reset links must keep working, and the old bytes must be gone."""

    FUTURE = (datetime.now(UTC) + timedelta(hours=5)).isoformat()
    PAST = (datetime.now(UTC) - timedelta(hours=5)).isoformat()
    NOW = datetime.now(UTC).isoformat()

    def _seed_legacy(self, db):
        legacy = {
            "sessions": ("live-staff-session-" + "a" * 24, "expired-staff-session-" + "b" * 21),
            "reset": "legacy-staff-reset-token-" + "c" * 19,
            "customer": "legacy-customer-session-" + "d" * 19,
            "customer_reset": "legacy-customer-reset-" + "e" * 21,
        }
        for tok in legacy["sessions"]:
            assert len(tok) != 64
        db.execute("INSERT INTO sessions (token, user_id, created_at, expires_at) VALUES (?,?,?,?)",
                   (legacy["sessions"][0], "u1", self.NOW, self.FUTURE))
        db.execute("INSERT INTO sessions (token, user_id, created_at, expires_at) VALUES (?,?,?,?)",
                   (legacy["sessions"][1], "u1", self.NOW, self.PAST))
        db.execute("INSERT INTO password_reset_tokens (token, user_id, created_at, expires_at) VALUES (?,?,?,?)",
                   (legacy["reset"], "u1", self.NOW, self.FUTURE))
        db.execute("INSERT INTO customer_sessions (token, customer_user_id, created_at, expires_at) VALUES (?,?,?,?)",
                   (legacy["customer"], "c1", self.NOW, self.FUTURE))
        db.execute("INSERT INTO customer_password_reset_tokens (token, customer_user_id, created_at, expires_at) "
                   "VALUES (?,?,?,?)", (legacy["customer_reset"], "c1", self.NOW, self.FUTURE))
        return legacy

    def test_live_tokens_keep_working_and_expired_stay_dead(self, db):
        legacy = self._seed_legacy(db)
        sm, rm = SessionManager(db=db), ResetTokenManager(db)
        csm, crm = CustomerSessionManager(db), CustomerResetTokenManager(db)

        assert sm.get_user_id(legacy["sessions"][0]) == "u1"
        assert sm.get_user_id(legacy["sessions"][1]) is None
        assert rm.get_user_id(legacy["reset"]) == "u1"
        assert csm.get_customer_user_id(legacy["customer"]) == "c1"
        assert crm.get_customer_user_id(legacy["customer_reset"]) == "c1"

    def test_old_raw_values_are_scrubbed_from_the_file(self, db, db_path):
        legacy = self._seed_legacy(db)
        raw = [legacy["sessions"][0], legacy["sessions"][1], legacy["reset"],
               legacy["customer"], legacy["customer_reset"]]
        assert all(t.encode() in _file_bytes(db_path) for t in raw)  # premise: it really was plaintext

        SessionManager(db=db); ResetTokenManager(db)
        CustomerSessionManager(db); CustomerResetTokenManager(db)
        db.close()

        blob = _file_bytes(db_path)
        for t in raw:
            assert t.encode() not in blob, "old raw token survived in the file/WAL"

    def test_visible_session_id_matches_what_it_was_before_conversion(self, db):
        legacy = self._seed_legacy(db)
        sm = SessionManager(db=db)
        expected = hashlib.sha256(legacy["sessions"][0].encode()).hexdigest()[:12]
        assert [s["session_id"] for s in sm.list_active_for_user("u1")] == [expected]

    def test_conversion_is_idempotent(self, db):
        legacy = self._seed_legacy(db)
        SessionManager(db=db)
        first = [r["token"] for r in db.query("SELECT token FROM sessions ORDER BY token")]
        sm = SessionManager(db=db)  # a second boot must not re-hash the hashes
        second = [r["token"] for r in db.query("SELECT token FROM sessions ORDER BY token")]
        assert first == second
        assert sm.get_user_id(legacy["sessions"][0]) == "u1"

    def test_a_used_reset_token_stays_used_after_conversion(self, db):
        token = "legacy-used-reset-" + "f" * 25
        db.execute("INSERT INTO password_reset_tokens (token, user_id, created_at, expires_at, used_at) "
                   "VALUES (?,?,?,?,?)", (token, "u1", self.NOW, self.FUTURE, self.NOW))
        assert ResetTokenManager(db).get_user_id(token) is None
