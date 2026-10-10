"""
Live counters for the coordinator's scheduler loop.

The loop itself (GCONCoordinator.scheduler_loop) already works; what it
never did was say what it is doing. This keeps a handful of in-memory
counters so the dashboard's Scheduler page can show real dispatch activity
and real scheduling failures instead of inferring them.

All values live in coordinator memory, so they reset when the coordinator
restarts (`since` records when counting began). Writes are single attribute
or deque operations, which are atomic in CPython, so the hot loop takes no
lock.
"""
import time
from collections import deque
from datetime import datetime, UTC
from typing import Any, Dict, Optional

# One timestamp per real dispatch; bounded so memory cannot grow.
_DISPATCH_RING = 5000


class SchedulerStats:
    def __init__(self):
        self.since = datetime.now(UTC)
        self.loop_state = "starting"        # what the loop is doing right now
        self.last_loop_monotonic: Optional[float] = None
        self.dispatched_total = 0
        self.last_dispatch_at: Optional[datetime] = None
        self._dispatch_times = deque(maxlen=_DISPATCH_RING)   # time.time() floats
        self.failed_passes_total = 0
        self.failed_passes_by_kind: Dict[str, int] = {}
        self.last_failure_at: Optional[datetime] = None
        self.last_failure_kind: Optional[str] = None
        self.last_failure_message: Optional[str] = None
        # Unexpected loop crashes the supervisor recovered from (or gave up on).
        self.restarts_total = 0
        self._restart_times = deque(maxlen=200)          # time.time() floats
        self.last_restart_at: Optional[datetime] = None
        self.last_restart_message: Optional[str] = None

    # ---- written by the scheduler loop / dispatch path -------------------
    def tick(self, state: str) -> None:
        self.loop_state = state
        self.last_loop_monotonic = time.monotonic()

    def dispatched(self) -> None:
        self.dispatched_total += 1
        self.last_dispatch_at = datetime.now(UTC)
        self._dispatch_times.append(time.time())

    def failed_pass(self, kind: str, message: str) -> None:
        """One pass in which a queued job could not be placed on any worker."""
        self.failed_passes_total += 1
        self.failed_passes_by_kind[kind] = self.failed_passes_by_kind.get(kind, 0) + 1
        self.last_failure_at = datetime.now(UTC)
        self.last_failure_kind = kind
        self.last_failure_message = (message or "")[:300]

    def restarted(self, message: str) -> None:
        """The loop died with an unexpected exception (not RuntimeError's normal
        "no node" path) and the supervisor is about to start it again."""
        self.restarts_total += 1
        self._restart_times.append(time.time())
        self.last_restart_at = datetime.now(UTC)
        self.last_restart_message = (message or "")[:300]

    def recent_restarts(self, window_seconds: float) -> int:
        cutoff = time.time() - window_seconds
        return sum(1 for t in list(self._restart_times) if t >= cutoff)

    # ---- read by the observability service --------------------------------
    def loop_age_seconds(self) -> Optional[float]:
        if self.last_loop_monotonic is None:
            return None
        return max(0.0, time.monotonic() - self.last_loop_monotonic)

    def dispatch_rate_per_minute(self, window_seconds: float) -> float:
        """Dispatches per minute over the trailing window. If counting began
        more recently than the window, the elapsed time is used so a young
        coordinator is not under-reported."""
        now = time.time()
        elapsed = (datetime.now(UTC) - self.since).total_seconds()
        span = max(1.0, min(window_seconds, elapsed))
        n = sum(1 for t in list(self._dispatch_times) if t >= now - span)
        return n * 60.0 / span

    def snapshot(self) -> Dict[str, Any]:
        def iso(d):
            return d.isoformat() if d else None
        return {
            "since": iso(self.since),
            "loop_state": self.loop_state,
            "loop_age_seconds": self.loop_age_seconds(),
            "dispatched_total": self.dispatched_total,
            "last_dispatch_at": iso(self.last_dispatch_at),
            "rate_per_minute": {
                "1m": self.dispatch_rate_per_minute(60),
                "5m": self.dispatch_rate_per_minute(300),
                "15m": self.dispatch_rate_per_minute(900),
            },
            "failed_passes_total": self.failed_passes_total,
            "failed_passes_by_kind": dict(self.failed_passes_by_kind),
            "last_failure_at": iso(self.last_failure_at),
            "last_failure_kind": self.last_failure_kind,
            "last_failure_message": self.last_failure_message,
            "restarts_total": self.restarts_total,
            "last_restart_at": iso(self.last_restart_at),
            "last_restart_message": self.last_restart_message,
        }
