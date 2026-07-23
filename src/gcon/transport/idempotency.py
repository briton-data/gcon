"""
Idempotency helpers shared by `LocalTransport`-adjacent code and
`GrpcTransport`.

The actual deduplication lives in the persistence layer (unique
`request_message_id` on `job_attempts`, unique `receipt_hash` on
`receipts`, unique `(node_id, sequence)` on `heartbeats`, unique
`(attempt_id, stream, sequence)` on `execution_logs` -- see
`gcon.persistence.repositories`); this module just standardizes how
message IDs and monotonic sequence numbers are minted so every
producer (coordinator dispatch, agent heartbeat loop, agent log
writer) does it the same way.
"""

from __future__ import annotations

import itertools
import threading
import uuid


def new_message_id() -> str:
    """A fresh idempotency key for one logical operation (e.g. one
    job dispatch). Callers must reuse the *same* id across retries of
    the *same* logical operation for deduplication to work -- minting
    a new id on every retry defeats it."""
    return uuid.uuid4().hex


class SequenceCounter:
    """
    Thread-safe monotonic counter, starting at 1, for use as a
    per-stream idempotency sequence number (heartbeats, per-attempt
    log lines). Never resets for the lifetime of the process that
    owns it; a resumed counter after a reconnect should be seeded
    from the last value the coordinator acknowledged, not restarted
    at 1, or already-seen sequence numbers get reused and new events
    are silently treated as duplicates.
    """

    def __init__(self, start: int = 0):
        self._lock = threading.Lock()
        self._counter = itertools.count(start + 1)
        self._last = start

    def next(self) -> int:
        with self._lock:
            self._last = next(self._counter)
            return self._last

    @property
    def last(self) -> int:
        with self._lock:
            return self._last
