"""
Brute-force / spam protection, keyed by an arbitrary string (usually
`email`, but also reused as a "changepw:<user_id>" or
"apikey:<user_id>" style namespaced key -- see web_server.py) plus a
client IP.

Independent of session/API-key auth (auth.py, api_keys.py) and the
gRPC mTLS layer (transport/tls.py) -- this guards endpoints those
don't cover: repeated password guesses against `/auth/login`, and
(reusing the same class under a different key namespace)
`/auth/change-password` and `/management/api-keys` creation.

Tracks failed/throttled attempts per (key, client_ip) pair with a
sliding window; once the threshold is hit, further attempts for that
pair are rejected with a lockout until the window ages out, without
revealing whether the underlying account/email exists (same generic
error used by ManagementLayer.login for a wrong password).

Persistence: when given a `db` (gcon.storage.database.Database, see
storage/migrations.py's `login_attempts`/`login_lockouts` tables),
state survives a process restart -- a restart no longer wipes every
active lockout. Without one, falls back to the original in-memory
dicts, which is what every existing caller that constructs
LoginRateLimiter() with no arguments continues to get.

Eviction: the in-memory dicts are otherwise unbounded -- an attacker
who fails logins under many different (email, ip) pairs (or just many
IPs) could grow `_attempts`/`_locked_until` without limit, a memory-
DoS vector. A cheap sweep (`_maybe_evict`) periodically drops entries
whose attempts have all aged out of the window and whose lockout (if
any) has expired, bounding memory to roughly the number of currently
"live" (recently active or locked-out) keys rather than every key
ever seen. The DB-backed path is bounded the same way by pruning old
`login_attempts` rows and expired `login_lockouts` rows inline on
every check()/record_attempt() call.
"""

import threading
from datetime import datetime, UTC, timedelta

MAX_ATTEMPTS = 5
WINDOW_MINUTES = 15
LOCKOUT_MINUTES = 15

# In-memory-mode eviction tuning: sweep whenever either dict grows
# past this many *distinct* keys, or every EVICT_EVERY_N_CALLS calls,
# whichever comes first -- so a slow trickle of distinct attackers
# still gets swept eventually, not just a sudden burst.
EVICT_SIZE_THRESHOLD = 10_000
EVICT_EVERY_N_CALLS = 500


class LoginRateLimiter:
    def __init__(self, max_attempts=MAX_ATTEMPTS, window_minutes=WINDOW_MINUTES,
                 lockout_minutes=LOCKOUT_MINUTES, db=None):
        self.max_attempts = max_attempts
        self.window = timedelta(minutes=window_minutes)
        self.lockout = timedelta(minutes=lockout_minutes)
        self.db = db
        self._attempts = {}  # key -> list[datetime]  (in-memory mode only)
        self._locked_until = {}  # key -> datetime      (in-memory mode only)
        self._lock = threading.Lock()
        self._call_count = 0

    @staticmethod
    def _key(email, client_ip):
        return f"{(email or '').strip().lower()}|{client_ip or 'unknown'}"

    # ------------------------------------------------------------
    # Public API (unchanged signatures)
    # ------------------------------------------------------------

    def check(self, email, client_ip=None):
        """
        Raise ValueError with a generic message if this (key, ip)
        pair is currently locked out. Call before performing the
        guarded action (verifying a password, creating an API key,
        ...).
        """
        key = self._key(email, client_ip)
        if self.db is not None:
            self._check_db(key)
        else:
            self._check_memory(key)

    def record_failure(self, email, client_ip=None):
        """Call after a failed attempt (wrong password, etc). Locks
        the pair out once `max_attempts` fall within the window.
        Alias of record_attempt -- kept as a separate name because
        "failure" is the natural vocabulary at login call sites."""
        self.record_attempt(email, client_ip)

    def record_attempt(self, email, client_ip=None):
        """
        Call to count one attempt toward the limit, regardless of
        whether the attempt itself "failed" in a business-logic
        sense -- e.g. for endpoints being throttled purely against
        request volume (API-key creation), every call should count,
        not just unsuccessful ones.
        """
        key = self._key(email, client_ip)
        if self.db is not None:
            self._record_attempt_db(key)
        else:
            self._record_attempt_memory(key)

    def record_success(self, email, client_ip=None):
        """Call after a successful attempt to clear any prior failures."""
        key = self._key(email, client_ip)
        if self.db is not None:
            self.db.execute("DELETE FROM login_attempts WHERE key = ?", (key,))
            self.db.execute("DELETE FROM login_lockouts WHERE key = ?", (key,))
        else:
            with self._lock:
                self._attempts.pop(key, None)
                self._locked_until.pop(key, None)

    # ------------------------------------------------------------
    # In-memory implementation
    # ------------------------------------------------------------

    def _check_memory(self, key):
        with self._lock:
            self._maybe_evict_locked()
            locked_until = self._locked_until.get(key)
            if locked_until and datetime.now(UTC) < locked_until:
                raise ValueError(
                    "Too many failed login attempts. Please try again later."
                )
            if locked_until:
                # Lockout expired -- clear it and start fresh.
                del self._locked_until[key]
                self._attempts.pop(key, None)

    def _record_attempt_memory(self, key):
        now = datetime.now(UTC)
        with self._lock:
            attempts = [t for t in self._attempts.get(key, []) if now - t < self.window]
            attempts.append(now)
            self._attempts[key] = attempts
            if len(attempts) >= self.max_attempts:
                self._locked_until[key] = now + self.lockout
            self._maybe_evict_locked()

    def _maybe_evict_locked(self):
        """Must be called with self._lock held. Cheap sweep bounding
        the size of _attempts/_locked_until (see module docstring)."""
        self._call_count += 1
        oversized = (
            len(self._attempts) > EVICT_SIZE_THRESHOLD
            or len(self._locked_until) > EVICT_SIZE_THRESHOLD
        )
        if not oversized and self._call_count % EVICT_EVERY_N_CALLS != 0:
            return

        now = datetime.now(UTC)
        expired_locks = [k for k, until in self._locked_until.items() if now >= until]
        for k in expired_locks:
            del self._locked_until[k]

        stale_attempt_keys = [
            k for k, attempts in self._attempts.items()
            if k not in self._locked_until
            and all(now - t >= self.window for t in attempts)
        ]
        for k in stale_attempt_keys:
            del self._attempts[k]

    # ------------------------------------------------------------
    # DB-backed implementation
    # ------------------------------------------------------------

    def _check_db(self, key):
        now = datetime.now(UTC)
        row = self.db.query_one(
            "SELECT locked_until FROM login_lockouts WHERE key = ?", (key,)
        )
        if row is not None:
            locked_until = datetime.fromisoformat(row["locked_until"])
            if now < locked_until:
                raise ValueError(
                    "Too many failed login attempts. Please try again later."
                )
            # Lockout expired -- clear it and start fresh.
            self.db.execute("DELETE FROM login_lockouts WHERE key = ?", (key,))
            self.db.execute("DELETE FROM login_attempts WHERE key = ?", (key,))

    def _record_attempt_db(self, key):
        now = datetime.now(UTC)
        cutoff = (now - self.window).isoformat()
        with self.db.transaction() as conn:
            conn.execute(
                "DELETE FROM login_attempts WHERE key = ? AND attempted_at < ?",
                (key, cutoff),
            )
            conn.execute(
                "INSERT INTO login_attempts (key, attempted_at) VALUES (?, ?)",
                (key, now.isoformat()),
            )
            count = conn.execute(
                "SELECT COUNT(*) AS c FROM login_attempts WHERE key = ?", (key,)
            ).fetchone()["c"]
            if count >= self.max_attempts:
                locked_until = (now + self.lockout).isoformat()
                conn.execute(
                    "INSERT INTO login_lockouts (key, locked_until) VALUES (?, ?) "
                    "ON CONFLICT(key) DO UPDATE SET locked_until = excluded.locked_until",
                    (key, locked_until),
                )
            # Bound table growth: also prune globally-expired lockouts
            # and orphaned attempt rows on this same write, rather than
            # only ever growing these tables.
            conn.execute(
                "DELETE FROM login_lockouts WHERE locked_until < ?", (cutoff,)
            )