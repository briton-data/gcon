"""
GCON Authentication — password hashing and session management.

Passwords are hashed with PBKDF2-HMAC-SHA256 (stdlib `hashlib`,
no extra dependency) using a per-password random salt and a high
iteration count. Plaintext passwords are never stored or logged.

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
import secrets
from datetime import datetime, UTC, timedelta

PBKDF2_ITERATIONS = 260_000
SESSION_TTL_HOURS = 24
SESSION_COOKIE_NAME = "gcon_session"


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

    def create_session(self, user_id):
        token = secrets.token_urlsafe(32)
        created_at = datetime.now(UTC)
        expires_at = created_at + timedelta(hours=self.ttl_hours)

        if self.db is not None:
            self.db.execute(
                "INSERT INTO sessions (token, user_id, created_at, expires_at) "
                "VALUES (?, ?, ?, ?)",
                (token, user_id, created_at.isoformat(), expires_at.isoformat()),
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
                "SELECT user_id, expires_at FROM sessions WHERE token = ?", (token,)
            )
            if row is None:
                return None
            if datetime.now(UTC) > datetime.fromisoformat(row["expires_at"]):
                self.db.execute("DELETE FROM sessions WHERE token = ?", (token,))
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
            self.db.execute("DELETE FROM sessions WHERE token = ?", (token,))
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