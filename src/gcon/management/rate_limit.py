"""
Login brute-force protection.

Independent of session/API-key auth (auth.py, api_keys.py) and the
gRPC mTLS layer (transport/tls.py) -- this guards the one endpoint
those don't cover: repeated password guesses against `/auth/login`.

Tracks failed attempts per (email, client_ip) pair in memory with a
sliding window; once the threshold is hit, further attempts for that
pair are rejected with a lockout until the window ages out, without
revealing whether the email exists (same generic error used by
ManagementLayer.login for a wrong password).

In-memory only, like SessionManager in auth.py -- state does not
survive a process restart. Fine for a single coordinator process;
would need a shared store (e.g. the control-plane DB) behind a
load-balanced multi-coordinator deployment.
"""

import threading
from datetime import datetime, UTC, timedelta

MAX_ATTEMPTS = 5
WINDOW_MINUTES = 15
LOCKOUT_MINUTES = 15


class LoginRateLimiter:
    def __init__(self, max_attempts=MAX_ATTEMPTS, window_minutes=WINDOW_MINUTES,
                 lockout_minutes=LOCKOUT_MINUTES):
        self.max_attempts = max_attempts
        self.window = timedelta(minutes=window_minutes)
        self.lockout = timedelta(minutes=lockout_minutes)
        self._attempts = {}  # key -> list[datetime]
        self._locked_until = {}  # key -> datetime
        self._lock = threading.Lock()

    @staticmethod
    def _key(email, client_ip):
        return f"{(email or '').strip().lower()}|{client_ip or 'unknown'}"

    def check(self, email, client_ip=None):
        """
        Raise ValueError with a generic message if this (email, ip)
        pair is currently locked out. Call before verifying the
        password.
        """
        key = self._key(email, client_ip)
        with self._lock:
            locked_until = self._locked_until.get(key)
            if locked_until and datetime.now(UTC) < locked_until:
                raise ValueError(
                    "Too many failed login attempts. Please try again later."
                )
            if locked_until:
                # Lockout expired -- clear it and start fresh.
                del self._locked_until[key]
                self._attempts.pop(key, None)

    def record_failure(self, email, client_ip=None):
        """Call after a failed password check. Locks the pair out
        once `max_attempts` failures fall within the window."""
        key = self._key(email, client_ip)
        now = datetime.now(UTC)
        with self._lock:
            attempts = [t for t in self._attempts.get(key, []) if now - t < self.window]
            attempts.append(now)
            self._attempts[key] = attempts
            if len(attempts) >= self.max_attempts:
                self._locked_until[key] = now + self.lockout

    def record_success(self, email, client_ip=None):
        """Call after a successful login to clear any prior failures."""
        key = self._key(email, client_ip)
        with self._lock:
            self._attempts.pop(key, None)
            self._locked_until.pop(key, None)
