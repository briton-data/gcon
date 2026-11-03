"""
Time throttle for durable writes of usage telemetry (API key last-used / request counts,
a user's request count, a customer's last-active time).

Each authenticated API request used to rewrite those rows, every one in its own fsync'd
transaction (the database runs with synchronous=FULL), so request throughput was capped by disk
syncs on one shared connection. The counters themselves stay exact in memory; only how often
they reach disk changes. The first use of a key / user after a start is always written, then at
most once per interval. A crash or restart can lose at most one interval of usage counts.

GCON_USAGE_WRITE_INTERVAL_SECONDS sets the interval (default 30). 0 writes every time, the old behaviour.
"""
import os
import threading
import time

DEFAULT_INTERVAL_SECONDS = 30.0


def interval_from_env() -> float:
    try:
        return max(0.0, float(os.environ.get("GCON_USAGE_WRITE_INTERVAL_SECONDS", DEFAULT_INTERVAL_SECONDS)))
    except ValueError:
        return DEFAULT_INTERVAL_SECONDS


class WriteThrottle:
    def __init__(self, interval=None):
        self.interval = interval_from_env() if interval is None else float(interval)
        self._last = {}
        self._lock = threading.Lock()

    def due(self, key) -> bool:
        """True (and the clock for `key` restarts) when a write for `key` should happen now."""
        if self.interval <= 0:
            return True
        now = time.monotonic()
        with self._lock:
            last = self._last.get(key)
            if last is not None and now - last < self.interval:
                return False
            self._last[key] = now
            return True

    def forget(self, key) -> None:
        with self._lock:
            self._last.pop(key, None)
