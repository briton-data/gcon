"""
GCON Authentication — password hashing and session management.

Passwords are hashed with PBKDF2-HMAC-SHA256 (stdlib `hashlib`,
no extra dependency) using a per-password random salt and a high
iteration count. Plaintext passwords are never stored or logged.

Sessions are random opaque tokens held in memory, mapped to a user
id, with an expiry. Like the rest of the management layer, sessions
do not currently survive a server restart — that's the next piece
to add once persistence is in place.
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
    In-memory session store, mapping opaque tokens to user ids.
    """

    def __init__(self, ttl_hours=SESSION_TTL_HOURS):
        self.sessions = {}
        self.ttl_hours = ttl_hours

    def create_session(self, user_id):
        token = secrets.token_urlsafe(32)
        self.sessions[token] = {
            "user_id": user_id,
            "created_at": datetime.now(UTC),
            "expires_at": datetime.now(UTC) + timedelta(hours=self.ttl_hours),
        }
        return token

    def get_user_id(self, token):
        """
        Return the user id for a valid, unexpired session token, or
        None if the token is missing/invalid/expired.
        """
        if not token or token not in self.sessions:
            return None

        session = self.sessions[token]
        if datetime.now(UTC) > session["expires_at"]:
            del self.sessions[token]
            return None

        return session["user_id"]

    def destroy_session(self, token):
        self.sessions.pop(token, None)

    def destroy_all_for_user(self, user_id):
        """
        Invalidate every session belonging to a user (e.g. on
        password change or account suspension).
        """
        to_remove = [t for t, s in self.sessions.items() if s["user_id"] == user_id]
        for token in to_remove:
            del self.sessions[token]
