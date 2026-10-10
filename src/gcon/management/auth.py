"""
GCON Authentication — password hashing and session management.

Passwords are hashed with PBKDF2-HMAC-SHA256 (stdlib `hashlib`,
no extra dependency) using a per-password random salt and a high
iteration count. Plaintext passwords are never stored or logged.

Session and password-reset tokens (staff and customer) are never
stored raw. The `token` column of the sessions / password_reset_tokens /
customer_sessions / customer_password_reset_tokens tables holds
SHA-256(token); the raw value exists only in the caller's cookie or reset
link. A leaked database, backup or query log therefore can't be replayed as
live sessions or reset links. Tokens are 256 bits of randomness, so a plain
fast hash is sufficient (nothing to brute-force) and keeps every request's
lookup cheap -- the same reasoning as API keys (management/api_keys.py).

Sessions are random opaque tokens mapped to a user id, with an
expiry. When SessionManager is given a `db` (a
gcon.storage.database.Database, see storage/migrations.py's
`sessions` table), sessions are durable and survive a process
restart. Without one, it falls back to the original in-memory-only
dict, which is what every existing caller that constructs
SessionManager() with no arguments continues to get.
"""

import hashlib
import hmac
import logging
import secrets
from datetime import datetime, UTC, timedelta

logger = logging.getLogger(__name__)

PBKDF2_ITERATIONS = 260_000
SESSION_TTL_HOURS = 24
SESSION_COOKIE_NAME = "gcon_session"
RESET_TOKEN_TTL_MINUTES = 30

# A raw token is secrets.token_urlsafe(32) -- always 43 characters. A stored
# SHA-256 hex digest is always 64. That length difference is how the
# one-time conversion below tells a legacy raw row from an already-hashed one.
_HASHED_TOKEN_LENGTH = 64


def hash_token(token):
    """What is stored in a token column for a given raw token."""
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _convert_legacy_tokens(db, table):
    """
    One-time, idempotent in-place conversion of rows written before tokens
    were hashed: replace each raw token with its hash. Live sessions and
    unexpired reset links keep working -- the raw token in a user's cookie or
    link hashes to the new stored value -- they just stop being stored
    replayably. Does nothing on every boot after the first.

    Blanking isn't enough on its own: SQLite leaves the old bytes in freed
    pages and the WAL file, so this vacuums and checkpoints afterwards
    (Database.scrub()). `table` is always one of this module's own table
    names, never user input.
    """
    rows = db.query(f"SELECT token FROM {table} WHERE length(token) != {_HASHED_TOKEN_LENGTH}")
    if not rows:
        return
    with db.transaction() as conn:
        for row in rows:
            conn.execute(
                f"UPDATE {table} SET token = ? WHERE token = ?",
                (hash_token(row["token"]), row["token"]),
            )
    try:
        db.scrub()
    except Exception as e:  # conversion itself succeeded; only the cleanup didn't
        logger.warning(
            "Hashed %d legacy token(s) in %s but could not vacuum the database to "
            "remove the old values from free pages: %s", len(rows), table, e,
        )
    logger.info("Converted %d legacy raw token(s) in %s to hashed storage.", len(rows), table)


def hash_password(password):
    """
    Hash a password for storage. Returns "iterations$salt_hex$hash_hex".
    """
    salt = secrets.token_hex(16)
    digest = hashlib.pbkdf2_hmac(
        "sha256", password.encode("utf-8"), bytes.fromhex(salt), PBKDF2_ITERATIONS
    )
    return f"{PBKDF2_ITERATIONS}${salt}${digest.hex()}"


def verify_password(password, stored_hash):
    """
    Verify a password against a stored hash, in constant time.
    """
    try:
        iterations_str, salt, expected_hex = stored_hash.split("$")
        iterations = int(iterations_str)
    except (ValueError, AttributeError):
        return False

    digest = hashlib.pbkdf2_hmac(
        "sha256", password.encode("utf-8"), bytes.fromhex(salt), iterations
    )
    return hmac.compare_digest(digest.hex(), expected_hex)


class SessionManager:
    """
    Session store mapping opaque tokens to user ids. Backed by the
    database (a `sessions` table) when `db` is given, so sessions
    survive a process restart; otherwise an in-memory dict, exactly
    as before. Public method signatures are unchanged either way.
    """

    def __init__(self, ttl_hours=SESSION_TTL_HOURS, db=None):
        self.sessions = {}
        self.ttl_hours = ttl_hours
        self.db = db
        if db is not None:
            _convert_legacy_tokens(db, "sessions")

    def create_session(self, user_id):
        token = secrets.token_urlsafe(32)
        created_at = datetime.now(UTC)
        expires_at = created_at + timedelta(hours=self.ttl_hours)

        if self.db is not None:
            self.db.execute(
                "INSERT INTO sessions (token, user_id, created_at, expires_at) "
                "VALUES (?, ?, ?, ?)",
                (hash_token(token), user_id, created_at.isoformat(), expires_at.isoformat()),
            )
        else:
            self.sessions[token] = {
                "user_id": user_id,
                "created_at": created_at,
                "expires_at": expires_at,
            }
        return token

    def get_user_id(self, token):
        """
        Return the user id for a valid, unexpired session token, or
        None if the token is missing/invalid/expired.
        """
        if not token:
            return None

        if self.db is not None:
            row = self.db.query_one(
                "SELECT user_id, expires_at FROM sessions WHERE token = ?", (hash_token(token),)
            )
            if row is None:
                return None
            if datetime.now(UTC) > datetime.fromisoformat(row["expires_at"]):
                self.db.execute("DELETE FROM sessions WHERE token = ?", (hash_token(token),))
                return None
            return row["user_id"]

        if token not in self.sessions:
            return None
        session = self.sessions[token]
        if datetime.now(UTC) > session["expires_at"]:
            del self.sessions[token]
            return None
        return session["user_id"]

    def destroy_session(self, token):
        if self.db is not None:
            self.db.execute("DELETE FROM sessions WHERE token = ?", (hash_token(token),))
        else:
            self.sessions.pop(token, None)

    def destroy_all_for_user(self, user_id):
        """
        Invalidate every session belonging to a user (e.g. on
        password change or account suspension).
        """
        if self.db is not None:
            self.db.execute("DELETE FROM sessions WHERE user_id = ?", (user_id,))
            return

        to_remove = [t for t, s in self.sessions.items() if s["user_id"] == user_id]
        for token in to_remove:
            del self.sessions[token]

    def count_active(self):
        """
        Count non-expired sessions across all users, for the
        dashboard's `active_sessions` card.
        """
        now = datetime.now(UTC)

        if self.db is not None:
            row = self.db.query_one(
                "SELECT COUNT(*) AS c FROM sessions WHERE expires_at > ?",
                (now.isoformat(),),
            )
            return row["c"] if row else 0

        return sum(1 for s in self.sessions.values() if s["expires_at"] > now)

    def active_user_ids(self):
        """Ids of every user with at least one unexpired session."""
        now = datetime.now(UTC)
        if self.db is not None:
            rows = self.db.query(
                "SELECT DISTINCT user_id FROM sessions WHERE expires_at > ?", (now.isoformat(),)
            )
            return {r["user_id"] for r in rows}
        return {s["user_id"] for s in self.sessions.values() if s["expires_at"] > now}

    def list_active_for_user(self, user_id):
        """
        Return metadata for a user's active sessions -- created_at
        and expires_at, plus a short, non-reversible session_id
        derived from the token for display/selection. The raw token
        itself is never returned to a client.
        """
        now = datetime.now(UTC)

        if self.db is not None:
            rows = self.db.query(
                "SELECT token, created_at, expires_at FROM sessions "
                "WHERE user_id = ? AND expires_at > ? ORDER BY created_at DESC",
                (user_id, now.isoformat()),
            )
            return [
                {
                    # The stored value already IS sha256(token), so this is the
                    # same id as before hashing was introduced.
                    "session_id": row["token"][:12],
                    "created_at": row["created_at"],
                    "expires_at": row["expires_at"],
                }
                for row in rows
            ]

        sessions = [
            {
                "session_id": hashlib.sha256(token.encode("utf-8")).hexdigest()[:12],
                "created_at": s["created_at"].isoformat(),
                "expires_at": s["expires_at"].isoformat(),
            }
            for token, s in self.sessions.items()
            if s["user_id"] == user_id and s["expires_at"] > now
        ]
        sessions.sort(key=lambda s: s["created_at"], reverse=True)
        return sessions


class ResetTokenManager:
    """
    Issues and consumes single-use, expiring password-reset tokens
    for the self-service "forgot password" flow. Always DB-backed
    (a reset link has to survive across separate requests, possibly
    from a different device than the one that requested it, so an
    in-memory-only store would silently break on any multi-process
    or restarted deployment).
    """

    def __init__(self, db, ttl_minutes=RESET_TOKEN_TTL_MINUTES):
        self.db = db
        self.ttl_minutes = ttl_minutes
        _convert_legacy_tokens(db, "password_reset_tokens")

    def create_token(self, user_id):
        token = secrets.token_urlsafe(32)
        created_at = datetime.now(UTC)
        expires_at = created_at + timedelta(minutes=self.ttl_minutes)
        self.db.execute(
            "INSERT INTO password_reset_tokens (token, user_id, created_at, expires_at) "
            "VALUES (?, ?, ?, ?)",
            (hash_token(token), user_id, created_at.isoformat(), expires_at.isoformat()),
        )
        return token

    def get_user_id(self, token):
        """
        Return the user id for a valid, unexpired, unused token, or
        None otherwise. Does not consume the token -- call
        consume_token() once the new password has actually been set.
        """
        if not token:
            return None
        row = self.db.query_one(
            "SELECT user_id, expires_at, used_at FROM password_reset_tokens WHERE token = ?",
            (hash_token(token),),
        )
        if row is None or row["used_at"] is not None:
            return None
        if datetime.now(UTC) > datetime.fromisoformat(row["expires_at"]):
            return None
        return row["user_id"]

    def consume_token(self, token):
        self.db.execute(
            "UPDATE password_reset_tokens SET used_at = ? WHERE token = ?",
            (datetime.now(UTC).isoformat(), hash_token(token)),
        )

    def invalidate_all_for_user(self, user_id):
        """
        Mark every outstanding reset token for a user as used, e.g.
        once their password has actually changed via any path, so an
        older unconsumed reset link can't still be redeemed.
        """
        now = datetime.now(UTC).isoformat()
        self.db.execute(
            "UPDATE password_reset_tokens SET used_at = ? "
            "WHERE user_id = ? AND used_at IS NULL",
            (now, user_id),
        )


# ---------------------------------------------------------------
# Customer-facing equivalents. Same logic as SessionManager/
# ResetTokenManager above (session TTL, PBKDF2, single-use expiring
# tokens) but pointed at customer_sessions/customer_password_reset_tokens
# -- separate tables, not shared with staff, on purpose: a customer
# session token and a staff session token must never be
# interchangeable, even though the underlying mechanism is
# identical. See storage/migrations.py's version 4 and
# management/customers.py for the account side of this.
# ---------------------------------------------------------------

CUSTOMER_SESSION_COOKIE_NAME = "gcon_customer_session"


class CustomerSessionManager:
    def __init__(self, db, ttl_hours=SESSION_TTL_HOURS):
        self.db = db
        self.ttl_hours = ttl_hours
        _convert_legacy_tokens(db, "customer_sessions")

    def create_session(self, customer_user_id):
        token = secrets.token_urlsafe(32)
        created_at = datetime.now(UTC)
        expires_at = created_at + timedelta(hours=self.ttl_hours)
        self.db.execute(
            "INSERT INTO customer_sessions (token, customer_user_id, created_at, expires_at) "
            "VALUES (?, ?, ?, ?)",
            (hash_token(token), customer_user_id, created_at.isoformat(), expires_at.isoformat()),
        )
        return token

    def get_customer_user_id(self, token):
        if not token:
            return None
        row = self.db.query_one(
            "SELECT customer_user_id, expires_at FROM customer_sessions WHERE token = ?",
            (hash_token(token),),
        )
        if row is None:
            return None
        if datetime.now(UTC) > datetime.fromisoformat(row["expires_at"]):
            self.db.execute("DELETE FROM customer_sessions WHERE token = ?", (hash_token(token),))
            return None
        return row["customer_user_id"]

    def destroy_session(self, token):
        self.db.execute("DELETE FROM customer_sessions WHERE token = ?", (hash_token(token),))

    def destroy_all_for_user(self, customer_user_id):
        self.db.execute(
            "DELETE FROM customer_sessions WHERE customer_user_id = ?", (customer_user_id,)
        )


class CustomerResetTokenManager:
    def __init__(self, db, ttl_minutes=RESET_TOKEN_TTL_MINUTES):
        self.db = db
        self.ttl_minutes = ttl_minutes
        _convert_legacy_tokens(db, "customer_password_reset_tokens")

    def create_token(self, customer_user_id):
        token = secrets.token_urlsafe(32)
        created_at = datetime.now(UTC)
        expires_at = created_at + timedelta(minutes=self.ttl_minutes)
        self.db.execute(
            "INSERT INTO customer_password_reset_tokens "
            "(token, customer_user_id, created_at, expires_at) VALUES (?, ?, ?, ?)",
            (hash_token(token), customer_user_id, created_at.isoformat(), expires_at.isoformat()),
        )
        return token

    def get_customer_user_id(self, token):
        if not token:
            return None
        row = self.db.query_one(
            "SELECT customer_user_id, expires_at, used_at FROM customer_password_reset_tokens "
            "WHERE token = ?",
            (hash_token(token),),
        )
        if row is None or row["used_at"] is not None:
            return None
        if datetime.now(UTC) > datetime.fromisoformat(row["expires_at"]):
            return None
        return row["customer_user_id"]

    def consume_token(self, token):
        self.db.execute(
            "UPDATE customer_password_reset_tokens SET used_at = ? WHERE token = ?",
            (datetime.now(UTC).isoformat(), hash_token(token)),
        )

    def invalidate_all_for_user(self, customer_user_id):
        now = datetime.now(UTC).isoformat()
        self.db.execute(
            "UPDATE customer_password_reset_tokens SET used_at = ? "
            "WHERE customer_user_id = ? AND used_at IS NULL",
            (now, customer_user_id),
        )