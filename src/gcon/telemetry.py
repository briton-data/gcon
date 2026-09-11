"""
Job-lifecycle telemetry: one queryable trace per job, structured
leveled logging in place of coordinator.py's bare print() calls, and
metrics derived from the event stream itself rather than separately
instrumented counters.

Three concerns, deliberately kept distinct rather than merged into
one mechanism:

  1. Cluster/connection-level events (node registered, disconnected,
     ...) -- ClusterEventRepository (persistence/repositories/
     cluster_events.py), unchanged by this module.
  2. Live, in-process notifications for the dashboard (POLICY_VIOLATION,
     EXECUTION_DISPUTED, ...) -- gcon.events.event_bus, unchanged.
  3. A job's full lifecycle as one trace -- THIS module. trace_id is
     minted once in GCONCoordinator.submit_job() and threaded through
     assign_job -> dispatch -> _run_job/_run_replicated_job ->
     create_receipt -> validate_proof, so "what happened to job X, in
     order" is one query (TelemetryRepository.for_trace), not
     reconstructed by grepping scattered log lines.

emit() vs the log_*() convenience methods
------------------------------------------
Not every one of coordinator.py's ~60 former print() call sites is a
lifecycle-significant EVENT worth a durable, individually-queryable
database row (e.g. "[QUEUE] Pending jobs: 3" is routine operational
chatter, not a fact about a specific job worth tracing later). So:

  * `debug()/info()/warning()/error()` -- always route through
    Python's standard `logging` module at the matching level (this is
    what actually replaces print()'s undifferentiated stdout dump: a
    real operator can now filter by level, redirect to a log
    aggregator, etc.). No database write, no ring-buffer entry, unless
    `event_type` is also given.
  * `emit()` -- the durable path: builds a full TelemetryEvent,
    appends to the bounded in-memory ring buffer (same eviction-cap
    pattern coordinator.py already uses for jobs/receipts -- see
    GCON_MAX_TELEMETRY_EVENTS_IN_MEMORY), and persists to the
    telemetry_events table via TelemetryRepository. Called explicitly
    at the handful of genuinely lifecycle-significant moments (job
    submitted, dispatch failed, verification pass/fail, replica
    disagreement, policy violation) -- these are also exactly the
    events derived_metrics() below counts.
  * `debug()/info()/warning()/error()` accept the same `event_type`/
    `job_id`/`node_id`/`trace_id`/`payload` kwargs as emit() -- when
    given, they log AND emit in one call, so a single call site (e.g.
    "dispatch failed") never has to remember to do both separately.
"""
from __future__ import annotations

import logging
import threading
import uuid
from collections import deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, UTC
from typing import Any, Deque, Dict, List, Optional

logger = logging.getLogger("gcon.coordinator")

_LEVELS = ("DEBUG", "INFO", "WARN", "ERROR")

# The specific event_types derived_metrics() below turns into named
# counters -- emit() accepts any event_type string, this is just the
# subset with a dedicated metric name in the spec this module was
# built against.
_METRIC_EVENT_TYPES = {
    "job_submitted": "jobs_submitted_total",
    "job_dispatch_failed": "jobs_dispatch_failed_total",
    "verification_pass": "verification_pass_total",
    "verification_fail": "verification_fail_total",
    "replica_disagreement": "replica_disagreement_total",
    "policy_violation": "policy_violation_total",
}


@dataclass(frozen=True)
class TelemetryEvent:
    event_id: str
    trace_id: str
    event_type: str
    timestamp: str
    level: str = "INFO"
    job_id: Optional[str] = None
    node_id: Optional[str] = None
    payload: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def new_trace_id() -> str:
    """Minted once per job at submit_job() -- see module docstring."""
    return uuid.uuid4().hex


class TelemetryCollector:
    """
    Owned by GCONCoordinator (self.telemetry), constructed once in
    __init__. `control_plane=None` is a fully supported mode (e.g.
    LocalTransport-only test setups with no real DB) -- events still
    go into the in-memory ring buffer and drive derived_metrics() for
    that process's lifetime, they just don't survive a restart.
    """

    def __init__(self, control_plane=None, max_in_memory: Optional[int] = None):
        self.control_plane = control_plane
        self._max_in_memory = max_in_memory if max_in_memory is not None else _default_max_in_memory()
        self._events: Deque[TelemetryEvent] = deque(maxlen=self._max_in_memory)
        self._lock = threading.RLock()

    # ------------------------------------------------------------
    # Durable path
    # ------------------------------------------------------------

    def emit(
        self,
        event_type: str,
        trace_id: str,
        level: str = "INFO",
        job_id: Optional[str] = None,
        node_id: Optional[str] = None,
        payload: Optional[Dict[str, Any]] = None,
    ) -> TelemetryEvent:
        level = level.upper()
        if level not in _LEVELS:
            level = "INFO"
        event = TelemetryEvent(
            event_id=uuid.uuid4().hex,
            trace_id=trace_id,
            event_type=event_type,
            timestamp=datetime.now(UTC).isoformat(),
            level=level,
            job_id=job_id,
            node_id=node_id,
            payload=payload or {},
        )
        with self._lock:
            # deque(maxlen=...) evicts the oldest entry automatically
            # once full -- same bounded-memory guarantee as
            # coordinator.py's own job/receipt eviction, just built
            # into the container itself rather than a separate
            # _evict_*_if_over_capacity() call.
            self._events.append(event)

        if self.control_plane is not None:
            try:
                self.control_plane.telemetry_events.record(
                    event_id=event.event_id,
                    trace_id=event.trace_id,
                    event_type=event.event_type,
                    level=event.level,
                    job_id=event.job_id,
                    node_id=event.node_id,
                    payload=event.payload or None,
                    created_at=event.timestamp,
                )
            except Exception as e:
                # A telemetry PERSISTENCE failure must never break the
                # job pipeline it's instrumenting -- the event still
                # made it into the in-memory ring buffer above, so
                # derived_metrics() for this process's lifetime is
                # still correct even if the DB write failed. Plain
                # logging here, deliberately NOT another emit() call
                # (that would recurse into this same except path on a
                # persistently-broken DB).
                logger.warning(f"[TELEMETRY] Failed to persist event {event.event_type!r}: {e!r}")
        return event

    # ------------------------------------------------------------
    # Leveled logging (replaces print()) -- optionally ALSO emit()
    # ------------------------------------------------------------

    def _log(self, level: str, message: str, event_type: Optional[str] = None,
              trace_id: Optional[str] = None, job_id: Optional[str] = None,
              node_id: Optional[str] = None, payload: Optional[Dict[str, Any]] = None) -> None:
        getattr(logger, level)(message)
        if event_type is not None:
            self.emit(
                event_type=event_type,
                # A log call with an event_type but no trace_id (most
                # of coordinator.py's former print() sites predate
                # trace_id threading) still gets a real, usable trace
                # of its own rather than being rejected -- one-off
                # events are a valid trace of length 1.
                trace_id=trace_id or new_trace_id(),
                level=level.upper() if level != "warning" else "WARN",
                job_id=job_id, node_id=node_id, payload=payload,
            )

    def debug(self, message: str, **kwargs) -> None:
        self._log("debug", message, **kwargs)

    def info(self, message: str, **kwargs) -> None:
        self._log("info", message, **kwargs)

    def warning(self, message: str, **kwargs) -> None:
        self._log("warning", message, **kwargs)

    def error(self, message: str, **kwargs) -> None:
        self._log("error", message, **kwargs)

    # ------------------------------------------------------------
    # Query / metrics
    # ------------------------------------------------------------

    def recent(self, limit: int = 200) -> List[Dict[str, Any]]:
        """In-memory only -- this process's events since it started,
        newest first. For durable/historical queries (including across
        a restart), use control_plane.telemetry_events directly (see
        api_v1.py's GET /telemetry/events, which does exactly that)."""
        with self._lock:
            return [e.to_dict() for e in reversed(self._events)]

    def derived_metrics(self) -> Dict[str, int]:
        """
        Counters computed from the event stream itself, not
        separately tracked -- so a metric can never drift from what
        actually happened, only from how many events are still in
        scope (in-memory ring buffer here; TelemetryRepository.
        count_by_event_type() covers the full durable history the
        same way, for a coordinator that's been running longer than
        the ring buffer retains).
        """
        counts: Dict[str, int] = {name: 0 for name in _METRIC_EVENT_TYPES.values()}
        with self._lock:
            for event in self._events:
                metric_name = _METRIC_EVENT_TYPES.get(event.event_type)
                if metric_name:
                    counts[metric_name] += 1
        return counts


def _default_max_in_memory() -> int:
    import os
    return int(os.environ.get("GCON_MAX_TELEMETRY_EVENTS_IN_MEMORY", "5000"))
