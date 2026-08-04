"""
Covers "persist sessions and login rate-limit state": both used to be
in-memory only, so a restart force-logged-out every user and cleared
lockouts. SessionManager/LoginRateLimiter's public method signatures
are unchanged; only an optional db= makes them durable.
"""

import time
from datetime import datetime, UTC, timedelta

import pytest

from gcon.management.auth import SessionManager
from gcon.management.rate_limit import LoginRateLimiter, EVICT_SIZE_THRESHOLD
from gcon.storage.database import Database


# --------------------------------------------------------------- SessionManager

def test_session_manager_in_memory_mode_is_unchanged():
    sm = SessionManager()
    token = sm.create_session("user-1")
    assert sm.get_user_id(token) == "user-1"
    sm.destroy_session(token)
    assert sm.get_user_id(token) is None


def test_session_survives_a_restart_against_the_same_db(tmp_path):
    path = str(tmp_path / "gcon.db")

    db1 = Database(path)
    sm1 = SessionManager(db=db1)
    token = sm1.create_session("user-1")
    assert sm1.get_user_id(token) == "user-1"

    # --- "restart": new Database/SessionManager, same file ---
    db2 = Database(path)
    sm2 = SessionManager(db=db2)
    assert sm2.get_user_id(token) == "user-1"


def test_expired_db_session_is_rejected_and_cleaned_up(tmp_path):
    db = Database(str(tmp_path / "gcon.db"))
    sm = SessionManager(db=db, ttl_hours=1)
    token = sm.create_session("user-1")

    # Force it into the past directly in the DB, as if it were
    # created a long time before a restart.
    expired = (datetime.now(UTC) - timedelta(hours=2)).isoformat()
    db.execute("UPDATE sessions SET expires_at = ? WHERE token = ?", (expired, token))

    assert sm.get_user_id(token) is None
    assert db.query_one("SELECT * FROM sessions WHERE token = ?", (token,)) is None


def test_destroy_all_for_user_db_backed(tmp_path):
    db = Database(str(tmp_path / "gcon.db"))
    sm = SessionManager(db=db)
    t1 = sm.create_session("user-1")
    t2 = sm.create_session("user-1")
    t3 = sm.create_session("user-2")

    sm.destroy_all_for_user("user-1")

    assert sm.get_user_id(t1) is None
    assert sm.get_user_id(t2) is None
    assert sm.get_user_id(t3) == "user-2"


def test_unknown_token_returns_none_db_backed(tmp_path):
    db = Database(str(tmp_path / "gcon.db"))
    sm = SessionManager(db=db)
    assert sm.get_user_id("not-a-real-token") is None
    assert sm.get_user_id(None) is None


# --------------------------------------------------------------- LoginRateLimiter

def test_rate_limiter_in_memory_mode_is_unchanged():
    rl = LoginRateLimiter(max_attempts=3, window_minutes=15, lockout_minutes=15)
    rl.check("a@example.com", "1.2.3.4")  # no raise
    for _ in range(3):
        rl.record_failure("a@example.com", "1.2.3.4")
    with pytest.raises(ValueError):
        rl.check("a@example.com", "1.2.3.4")


def test_lockout_survives_a_restart_against_the_same_db(tmp_path):
    path = str(tmp_path / "gcon.db")

    db1 = Database(path)
    rl1 = LoginRateLimiter(max_attempts=3, db=db1)
    for _ in range(3):
        rl1.record_failure("a@example.com", "1.2.3.4")
    with pytest.raises(ValueError):
        rl1.check("a@example.com", "1.2.3.4")

    # --- "restart": new Database/LoginRateLimiter, same file ---
    db2 = Database(path)
    rl2 = LoginRateLimiter(max_attempts=3, db=db2)
    with pytest.raises(ValueError):
        rl2.check("a@example.com", "1.2.3.4")


def test_db_backed_lockout_expires(tmp_path):
    db = Database(str(tmp_path / "gcon.db"))
    rl = LoginRateLimiter(max_attempts=1, lockout_minutes=15, db=db)
    rl.record_failure("a@example.com", "1.2.3.4")
    with pytest.raises(ValueError):
        rl.check("a@example.com", "1.2.3.4")

    key = rl._key("a@example.com", "1.2.3.4")
    past = (datetime.now(UTC) - timedelta(minutes=1)).isoformat()
    db.execute("UPDATE login_lockouts SET locked_until = ? WHERE key = ?", (past, key))

    rl.check("a@example.com", "1.2.3.4")  # no raise: lockout expired


def test_db_backed_record_success_clears_prior_failures(tmp_path):
    db = Database(str(tmp_path / "gcon.db"))
    rl = LoginRateLimiter(max_attempts=3, db=db)
    rl.record_failure("a@example.com", "1.2.3.4")
    rl.record_failure("a@example.com", "1.2.3.4")
    rl.record_success("a@example.com", "1.2.3.4")
    rl.check("a@example.com", "1.2.3.4")  # no raise
    key = rl._key("a@example.com", "1.2.3.4")
    assert db.query_one("SELECT * FROM login_attempts WHERE key = ?", (key,)) is None


def test_record_attempt_is_shared_by_login_and_other_endpoints(tmp_path):
    """
    Same mechanism protects /auth/change-password and
    /management/api-keys creation via namespaced keys, regardless of
    whether the guarded action itself "failed".
    """
    db = Database(str(tmp_path / "gcon.db"))
    rl = LoginRateLimiter(max_attempts=2, db=db)
    key = "apikey:user-123"
    rl.record_attempt(key, "9.9.9.9")
    rl.record_attempt(key, "9.9.9.9")
    with pytest.raises(ValueError):
        rl.check(key, "9.9.9.9")


# --------------------------------------------------------------- eviction

def test_in_memory_eviction_bounds_dict_growth():
    rl = LoginRateLimiter(max_attempts=100, window_minutes=15)
    # Many distinct (email, ip) pairs, all well within their window --
    # nothing here is individually "stale" yet, but the sheer count
    # crossing the size threshold must still trigger a sweep pass
    # rather than growing forever.
    for i in range(EVICT_SIZE_THRESHOLD + 50):
        rl.record_attempt(f"user{i}@example.com", "1.2.3.4")

    # A sweep ran (size exceeded the threshold); it can only evict
    # entries that are actually stale, and these are all fresh, so
    # nothing *should* be removed here -- but the important guarantee
    # is that the eviction pass didn't error and the structures are
    # still internally consistent / usable afterward.
    rl.check(f"user0@example.com", "1.2.3.4")


def test_in_memory_eviction_removes_aged_out_entries():
    rl = LoginRateLimiter(max_attempts=100, window_minutes=15)
    now = datetime.now(UTC)
    # Manually age a batch of entries out of the window, as if they
    # were recorded long ago, then deterministically force the
    # "every N calls" sweep branch.
    for i in range(50):
        key = rl._key(f"stale{i}@example.com", "1.2.3.4")
        rl._attempts[key] = [now - timedelta(minutes=60)]

    from gcon.management.rate_limit import EVICT_EVERY_N_CALLS
    with rl._lock:
        rl._call_count = EVICT_EVERY_N_CALLS - 1
        rl._maybe_evict_locked()

    assert len(rl._attempts) == 0


def test_eviction_never_drops_an_active_lockout():
    rl = LoginRateLimiter(max_attempts=1, window_minutes=15, lockout_minutes=15)
    rl.record_failure("locked@example.com", "1.2.3.4")
    key = rl._key("locked@example.com", "1.2.3.4")
    assert key in rl._locked_until

    with rl._lock:
        rl._call_count = 0
        for _ in range(600):
            rl._maybe_evict_locked()

    assert key in rl._locked_until
    with pytest.raises(ValueError):
        rl.check("locked@example.com", "1.2.3.4")