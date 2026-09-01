"""
LeaderElector — lease-based leader election for running more than one
GCON coordinator process against the same control-plane DB.

What this actually buys you
-----------------------------
Before this, GCON was a single coordinator with no leader election if
it dies -- if that process crashes, the cluster is down until someone
manually restarts it, however long that takes. With this, you can run
N coordinator processes pointed at the same control-plane DB file (or,
once the Postgres dialect gets wired -- see the coordinator scaffold
that already exists for it -- a real shared network DB, not just a
local SQLite file multiple processes on one host can open); exactly
one holds the lease and is "active" (runs the gRPC transport, accepts
worker connections, dispatches jobs), the rest are "standby" and
serve nothing but read-only dashboard/API queries against the shared
DB. If the active process dies, its lease expires and a standby
acquires it and takes over, typically within one lease TTL.

What this does NOT give you
------------------------------
* Zero-downtime failover. There's a real gap -- up to `ttl_seconds`
  worst case -- between the leader dying and a standby noticing.
  That's the honest cost of lease-based (vs. a real consensus
  protocol like Raft) election; getting it below a few seconds
  reliably needs exactly the kind of dedicated coordination service
  (etcd/ZooKeeper/Consul) this deployment doesn't have.
* In-flight job continuity across a failover. A job a since-dead
  leader had dispatched to a node is handled by the *existing*
  reconciliation path (recover_jobs / restore_from_persistence's
  in-flight-job handling), not anything new here -- the new leader
  picks it up the same way any coordinator restart already would.
* Multiple coordinators serving live gRPC/worker traffic
  simultaneously (active-active). This is active-passive: agents
  still connect to one coordinator's gRPC endpoint at a time. Making
  that endpoint itself durable across a failover (a VIP, a load
  balancer health-checking `/api/v1/health` and routing only to the
  leader, or client-side multi-address failover in the agent) is
  deployment/infra work outside GCON's own code, deliberately not
  invented here.

Usage
------
See scripts/run_coordinator.py for the wiring: construct with a
stable `holder_id` (falls back to hostname:pid), call `run_until_leader()`
to block until this process acquires the lease (so it never starts
serving before it's actually safe to), then `start()` to keep
renewing in the background. If the lease is ever lost after having
been the leader, `on_lose_leadership` fires -- run_coordinator.py's
callback exits the process rather than attempting to keep running as
a demoted "leader" (see that module for why: cleanly restarting into
standby mode is a much smaller, safer surface than trying to safely
tear down a live gRPC server and un-dispatch in-flight jobs).
"""

from __future__ import annotations

import logging
import os
import socket
import threading
import time
import uuid
from typing import Callable, Optional

from gcon.persistence.control_plane import ControlPlane

logger = logging.getLogger(__name__)

DEFAULT_LEASE_NAME = "coordinator-leader"


def default_holder_id() -> str:
    return f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"


class LeaderElector:
    def __init__(
        self,
        control_plane: ControlPlane,
        holder_id: Optional[str] = None,
        lease_name: str = DEFAULT_LEASE_NAME,
        ttl_seconds: Optional[float] = None,
        renew_interval_seconds: Optional[float] = None,
        on_become_leader: Optional[Callable[[], None]] = None,
        on_lose_leadership: Optional[Callable[[], None]] = None,
    ):
        self.control_plane = control_plane
        self.holder_id = holder_id or default_holder_id()
        self.lease_name = lease_name
        self.ttl_seconds = ttl_seconds or float(
            os.environ.get("GCON_HA_LEASE_TTL_SECONDS", "10")
        )
        # Renew comfortably inside the TTL, not at the edge of it --
        # one missed tick (GC pause, slow DB write) shouldn't cost
        # leadership. A third of the TTL gives 2-3 retries of margin.
        self.renew_interval_seconds = renew_interval_seconds or (self.ttl_seconds / 3)

        self.on_become_leader = on_become_leader
        self.on_lose_leadership = on_lose_leadership

        self._is_leader = False
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

    @property
    def is_leader(self) -> bool:
        with self._lock:
            return self._is_leader

    def try_tick(self) -> bool:
        """One acquire-or-renew attempt. Returns the resulting
        leadership state. Exposed separately from the background loop
        so tests (and run_until_leader) can drive it synchronously
        without a real sleep-based thread."""
        result = self.control_plane.leases.try_acquire_or_renew(
            self.lease_name, self.holder_id, self.ttl_seconds
        )
        now_leader = result is not None
        self._transition(now_leader)
        return now_leader

    def _transition(self, now_leader: bool) -> None:
        with self._lock:
            was_leader = self._is_leader
            self._is_leader = now_leader
        if now_leader and not was_leader:
            logger.info("'%s' acquired coordinator leadership", self.holder_id)
            if self.on_become_leader:
                self.on_become_leader()
        elif was_leader and not now_leader:
            logger.warning("'%s' lost coordinator leadership", self.holder_id)
            if self.on_lose_leadership:
                self.on_lose_leadership()

    def run_until_leader(self, poll_interval: Optional[float] = None) -> None:
        """Blocks (retrying at `poll_interval`, default
        renew_interval_seconds) until this process holds the lease.
        Intended for startup: a coordinator shouldn't start its gRPC
        transport / begin dispatching jobs until it knows it's safe
        to -- calling this first makes that ordering explicit rather
        than racing a background thread."""
        poll_interval = poll_interval or self.renew_interval_seconds
        while not self._stop_event.is_set():
            if self.try_tick():
                return
            self._stop_event.wait(poll_interval)

    def start(self) -> None:
        """Starts the background renew/re-acquire loop. Safe to call
        whether or not run_until_leader() was called first."""
        if self._thread is not None:
            return
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def stop(self, release: bool = True) -> None:
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=5)
        if release and self.is_leader:
            try:
                self.control_plane.leases.release(self.lease_name, self.holder_id)
            except Exception as e:
                logger.warning("failed to release lease on shutdown: %r", e)
            self._transition(False)

    def _loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                self.try_tick()
            except Exception as e:
                logger.warning("leader election tick failed: %r", e)
            self._stop_event.wait(self.renew_interval_seconds)
