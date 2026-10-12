"""
Request-rate limiting for the public API (/api/v1).

What this is for
----------------
`LoginRateLimiter` (rate_limit.py) counts *failed* sign-ins and locks an
identity out. It says nothing about how often an authenticated caller may
use the API: a customer with a valid key could submit or poll without any
bound. This module is that missing limit -- a plain "at most N requests in
any rolling 60 seconds" counter, applied once per request, after the API key
has been authenticated (see `require_scope` in gcon.api.api_v1).

How many
--------
The allowance is the organization's plan (gcon.management.plans): the limiter
is told the number on every call, so a staff change to a plan applies on the
next request. The environment variable below overrides every plan.

Who is counted
--------------
The identity is the caller's *organization*, not the individual key. A
customer can mint as many API keys as they like; a limit per key would give
every extra key a fresh allowance. Keys that belong to no organization (staff
keys) are counted per key. Requests that fail authentication never reach this
limiter, so they cannot use up anyone's allowance.

How it counts
-------------
A sliding window log: one timestamp per allowed request, per identity, kept
for `window_seconds`. A request is refused only if `limit` requests were
already allowed inside the window, and a refused request is NOT recorded, so
hammering a blocked endpoint does not extend the block -- the caller is let
back in as soon as their oldest request ages out, and `retry_after` says
exactly when that is.

Limits (deliberate, so nobody assumes more than this does)
----------------------------------------------------------
* In-memory and per process. With several coordinator processes each one
  keeps its own counts. Today only the HA leader serves traffic (see
  docs/FAILOVER.md), so this is one counter in practice.
* Restarting the coordinator resets all counts.
* It limits request *rate*. It does not limit how much work a request starts
  (job submission is bounded separately by GCON_MAX_CONCURRENT_JOBS_PER_ORG).

Configuration
-------------
GCON_API_RATE_LIMIT_PER_MINUTE   unset: each organization gets its plan's
                                 limit. N > 0: every organization gets N,
                                 whatever its plan. 0: request limiting is off
                                 for everyone. A value that is not a
                                 non-negative integer is ignored (with a
                                 warning) so the plans still apply -- a typo
                                 must not silently switch a security control
                                 off.
"""

from __future__ import annotations

import logging
import os
import threading
import time
from collections import deque
from typing import Callable, Deque, Dict, Optional, Tuple

log = logging.getLogger(__name__)

ENV_VAR = "GCON_API_RATE_LIMIT_PER_MINUTE"
WINDOW_SECONDS = 60.0

# Identities that have gone quiet are dropped every this many requests, so the
# table only ever holds identities active inside the last window.
_SWEEP_EVERY = 1024


def env_override() -> Optional[int]:
    """What the environment says about the limit: None = nothing (plans
    decide), 0 = off for everyone, N > 0 = N for everyone."""
    raw = os.environ.get(ENV_VAR)
    if raw is None or not raw.strip():
        return None
    try:
        value = int(raw.strip())
    except ValueError:
        value = -1
    if value < 0:
        log.warning("%s=%r is not a non-negative integer; ignoring it (plan limits apply).", ENV_VAR, raw)
        return None
    return value


class RequestRateLimiter:
    """At most `limit` allowed requests per identity in any `window_seconds`.

    `limit` given here is the default; `hit()` may be given a different one for
    a particular call (each organization has its own plan)."""

    def __init__(
        self,
        limit: int,
        window_seconds: float = WINDOW_SECONDS,
        clock: Callable[[], float] = time.monotonic,
    ):
        if limit <= 0:
            raise ValueError("limit must be a positive integer (use no limiter to disable).")
        if window_seconds <= 0:
            raise ValueError("window_seconds must be positive.")
        self.limit = limit
        self.window_seconds = window_seconds
        self._clock = clock
        self._lock = threading.Lock()
        self._windows: Dict[str, Deque[float]] = {}
        self._since_sweep = 0

    def hit(self, identity: str, limit: Optional[int] = None) -> Tuple[bool, float]:
        """
        Count one request for `identity` against `limit` (default: the limiter's
        own).

        Returns (True, 0.0) if it is allowed, or (False, retry_after_seconds)
        if the identity is over its limit. A refused request is not recorded.
        """
        limit = self.limit if limit is None else limit
        if limit <= 0:
            raise ValueError("limit must be a positive integer.")
        now = self._clock()
        cutoff = now - self.window_seconds
        with self._lock:
            self._since_sweep += 1
            if self._since_sweep >= _SWEEP_EVERY:
                self._sweep(cutoff)
                self._since_sweep = 0

            window = self._windows.get(identity)
            if window is None:
                window = self._windows[identity] = deque()
            while window and window[0] <= cutoff:
                window.popleft()

            if len(window) >= limit:
                # The request that frees enough room is the (len - limit)th
                # oldest: with the window exactly full that is the oldest one;
                # if the limit was just lowered it is a later one.
                freeing = window[len(window) - limit]
                return False, max(freeing + self.window_seconds - now, 0.0)
            window.append(now)
            return True, 0.0

    def _sweep(self, cutoff: float) -> None:
        """Drop identities with nothing inside the window. Caller holds the lock."""
        idle = [name for name, w in self._windows.items() if not w or w[-1] <= cutoff]
        for name in idle:
            del self._windows[name]

    def tracked_identities(self) -> int:
        """How many identities currently have a window (for tests/diagnostics)."""
        with self._lock:
            return len(self._windows)
