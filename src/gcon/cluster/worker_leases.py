"""
Leases of GCON-hosted workers to customers.

GCON runs a fleet of workers. A customer on a plan that includes hosted workers asks for N of them and keeps those
exact workers until they release them, go idle, or lose them. While the lease lasts the worker is that customer's
alone: the scheduler gives it that customer's jobs and nobody else's. When the lease ends the worker is cleaned
before it can be leased again. Nothing here changes a worker's certificate: the worker never knows who holds it, the
coordinator does (see registry.set_lease).

This is NOT the coordinator leader-election lease (persistence/repositories/leases.py, cluster/leader_election.py).

Lifecycle of one lease (worker_leases.state):

    active ----release / idle / staff----> ending ----no job running----> sanitizing ----worker answers----> ended
       \\                                       \\                              \\
        \\__worker lost for too long__________\\_______________________________\\__> ended (worker HELD)

* active      the customer's jobs run on the worker.
* ending      no new jobs; the job already running (if any) is left to finish.
* sanitizing  the coordinator has told the worker to clean itself and is waiting for its answer.
* ended       over. The worker is back in the pool if it was cleaned; if the clean-up failed, timed out, or the
              worker was lost, it is HELD (fleet_nodes.held_reason) and cannot be leased until staff have had it
              cleaned. A worker is never returned to the pool on the strength of a timeout.

Everything is driven by two things: customer/staff calls (request, release) and tick(), which the coordinator calls
from its health loop. Every state change is written to the database first, then applied to the live registry, so a
restart loses nothing: a reconnecting worker is given its lease back when it registers.
"""
import logging
import os
import threading
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any, Callable, Dict, List, Optional

from gcon.cluster.registry import FLEET_MIN_WORKER_VERSION, node_connected, version_below

logger = logging.getLogger(__name__)

MAX_REQUEST = 50


class LeaseError(Exception):
    """Base class. `code` is stable and meant for API clients."""
    code = "lease_error"


class BadRequest(LeaseError):
    code = "bad_request"


class PlanDoesNotAllow(LeaseError):
    code = "plan_limit"


class NotEnoughWorkers(LeaseError):
    code = "not_enough_workers"

    def __init__(self, message: str, available: int):
        super().__init__(message)
        self.available = available


class LeaseNotFound(LeaseError):
    code = "not_found"


class NotAHostedWorker(LeaseError):
    code = "not_hosted_worker"


def _seconds_from_env(name: str, default: int) -> int:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        raise ValueError(f"{name}={raw!r} is not a whole number") from None
    if value < 1:
        raise ValueError(f"{name} must be at least 1")
    return value


class WorkerLeaseManager:
    def __init__(
        self,
        control_plane,
        registry,
        *,
        cap_resolver: Optional[Callable[[str], int]] = None,
        send_sanitize: Optional[Callable[[str, str], bool]] = None,
        publish: Optional[Callable[[str, Dict[str, Any]], None]] = None,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        idle_seconds: Optional[int] = None,
        sanitize_timeout_seconds: Optional[int] = None,
        lost_grace_seconds: Optional[int] = None,
    ):
        self.cp = control_plane
        self.registry = registry
        self.cap_resolver = cap_resolver
        self.send_sanitize = send_sanitize
        self.publish = publish
        self.clock = clock
        # How long a lease may sit with no job before it is released (GCON_LEASE_IDLE_MINUTES).
        self.idle_seconds = idle_seconds or _seconds_from_env("GCON_LEASE_IDLE_MINUTES", 60) * 60
        # How long to wait for a worker to say it is clean (GCON_LEASE_SANITIZE_TIMEOUT_SECONDS).
        self.sanitize_timeout_seconds = sanitize_timeout_seconds or _seconds_from_env(
            "GCON_LEASE_SANITIZE_TIMEOUT_SECONDS", 300)
        # How long a leased worker may be unreachable before the lease is given up (GCON_LEASE_WORKER_LOST_SECONDS).
        self.lost_grace_seconds = lost_grace_seconds or _seconds_from_env("GCON_LEASE_WORKER_LOST_SECONDS", 600)
        self._lock = threading.RLock()
        self._unseen_since: Dict[str, datetime] = {}

    # ------------------------------------------------------------------ lookups used by the registry
    def lookup(self, node_id: str):
        """(org_id, state) of the worker's open lease, or None. Read when a worker registers."""
        lease = self.cp.worker_leases.get_open_for_node(node_id)
        return (lease["org_id"], lease["state"]) if lease else None

    def available_nodes(self) -> List[str]:
        """Hosted workers a customer could be given right now, in a stable order."""
        out = []
        for node_id, info in self.registry.snapshot().items():
            if not info.get("fleet") or info.get("lease_state") is not None:
                continue
            if info["status"] != "idle" or info.get("draining") or info.get("quarantined") or info.get("outdated"):
                continue
            if not node_connected(info["node"]) or not getattr(info["node"], "sandboxed", False):
                continue
            if version_below(info.get("agent_version"), FLEET_MIN_WORKER_VERSION):
                continue
            row = self.cp.fleet_nodes.get(node_id)
            if row is None or row.get("held_reason") or row.get("sanitizing_since"):
                continue
            if self.cp.worker_leases.get_open_for_node(node_id) is not None:
                continue
            out.append(node_id)
        return sorted(out)

    # ------------------------------------------------------------------ customer / staff operations
    def request(self, org_id: str, count: int, requested_by: Optional[str]) -> List[Dict[str, Any]]:
        """Give `org_id` `count` hosted workers, all or none."""
        if isinstance(count, bool) or not isinstance(count, int) or not 1 <= count <= MAX_REQUEST:
            raise BadRequest(f"count must be a whole number from 1 to {MAX_REQUEST}")
        with self._lock:
            cap = int((self.cap_resolver(org_id) if self.cap_resolver else 0) or 0)
            if cap <= 0:
                raise PlanDoesNotAllow("Hosted workers are not part of this organization's plan.")
            held = self.cp.worker_leases.count_open_for_org(org_id)
            if held + count > cap:
                raise PlanDoesNotAllow(
                    f"This organization holds {held} hosted worker(s) and its plan allows {cap}; "
                    f"{count} more would pass the limit.")
            candidates = self.available_nodes()
            if len(candidates) < count:
                raise NotEnoughWorkers(
                    f"Only {len(candidates)} hosted worker(s) are free right now.", available=len(candidates))
            try:
                leases = self.cp.worker_leases.create_many([
                    {"node_id": n, "org_id": org_id, "requested_by": requested_by} for n in candidates[:count]
                ])
            except self.cp.db.IntegrityError:
                # Another coordinator took one of them between our look and our write.
                raise NotEnoughWorkers(
                    "A worker was taken by another request while this one was being made. Try again.",
                    available=len(self.available_nodes())) from None
            for lease in leases:
                self.registry.set_lease(lease["node_id"], org_id, "active")
                self._emit("WORKER_LEASE_STARTED", lease)
            return leases

    def release(self, lease_id: str, *, org_id: Optional[str] = None, by: str,
                reason: str = "customer_release") -> Dict[str, Any]:
        """End a lease. `org_id` given = the caller is that customer and may only release its own lease. Releasing
        a lease that is already ending or over changes nothing."""
        with self._lock:
            lease = self.cp.worker_leases.get(lease_id)
            if lease is None or (org_id is not None and lease["org_id"] != org_id):
                raise LeaseNotFound("No such lease.")
            if lease["state"] == "active":
                self.cp.worker_leases.mark_ending(lease_id, reason, by, self.clock().isoformat())
                self.registry.set_lease(lease["node_id"], lease["org_id"], "ending")
                self._emit("WORKER_LEASE_ENDING", self.cp.worker_leases.get(lease_id))
                self._advance(self.cp.worker_leases.get(lease_id))
            return self.cp.worker_leases.get(lease_id)

    def sanitize_held_node(self, node_id: str, by: str) -> Dict[str, Any]:
        """Staff: have a held (or never-used) hosted worker cleaned and, if it answers clean, return it to the pool."""
        with self._lock:
            row = self.cp.fleet_nodes.get(node_id)
            if row is None:
                raise NotAHostedWorker(f"'{node_id}' is not a hosted-fleet worker.")
            if self.cp.worker_leases.get_open_for_node(node_id) is not None:
                raise BadRequest("This worker has an open lease; release the lease first.")
            info = self.registry.snapshot().get(node_id)
            if info is None or not node_connected(info["node"]) or info["status"] != "idle":
                raise BadRequest("The worker must be connected and idle to be cleaned.")
            ref = "clean_" + uuid.uuid4().hex[:12]
            self.cp.fleet_nodes.begin_sanitize(node_id, ref)
            if not (self.send_sanitize and self.send_sanitize(node_id, ref)):
                self.cp.fleet_nodes.clear_sanitize(node_id)
                raise BadRequest("Could not reach the worker to ask it to clean itself.")
            return self.cp.fleet_nodes.get(node_id)

    # ------------------------------------------------------------------ the worker's answer
    def on_sanitize_result(self, node_id: str, ref: str, ok: bool, detail: str) -> None:
        with self._lock:
            lease = self.cp.worker_leases.get(ref) if ref and ref.startswith("lease_") else None
            if lease and lease["node_id"] == node_id and lease["state"] == "sanitizing":
                self._finish_sanitize(lease, ok, detail)
                return
            row = self.cp.fleet_nodes.get(node_id)
            if row and row.get("sanitize_ref") == ref and self.cp.worker_leases.get_open_for_node(node_id) is None:
                if ok:
                    self.cp.fleet_nodes.release_hold(node_id)
                    self._emit("WORKER_RETURNED_TO_POOL", {"node_id": node_id, "via": "staff clean-up"})
                else:
                    self.cp.fleet_nodes.hold(node_id, f"clean-up failed: {detail}")
                return
            logger.warning("Ignoring a clean-up answer from '%s' that matches nothing waiting (ref %r)", node_id, ref)

    # ------------------------------------------------------------------ the reaper
    def tick(self) -> None:
        """Move leases along. Called every few seconds by the coordinator's health loop."""
        with self._lock:
            now = self.clock()
            snapshot = self.registry.snapshot()
            for lease in self.cp.worker_leases.list_open():
                try:
                    self._tick_one(lease, snapshot.get(lease["node_id"]), now)
                except Exception:
                    logger.exception("Lease %s could not be advanced", lease["lease_id"])
            # A staff-requested clean-up that nobody answered.
            for row in self.cp.fleet_nodes.list_all():
                since = row.get("sanitizing_since")
                if since and self.cp.worker_leases.get_open_for_node(row["node_id"]) is None:
                    if now - datetime.fromisoformat(since) > timedelta(seconds=self.sanitize_timeout_seconds):
                        self.cp.fleet_nodes.hold(row["node_id"], "clean-up timed out")

    def _tick_one(self, lease: Dict[str, Any], info: Optional[Dict[str, Any]], now: datetime) -> None:
        node_id = lease["node_id"]
        reachable = info is not None and info["status"] != "offline"
        if reachable:
            self._unseen_since.pop(node_id, None)
        else:
            first = self._unseen_since.setdefault(node_id, now)
            if now - first > timedelta(seconds=self.lost_grace_seconds):
                self._end_lost(lease)
                return

        state = lease["state"]
        if state == "active":
            if info is not None and (
                info["status"] == "busy" or info["running_jobs"] > 0
                or (info.get("last_claimed_at") and info["last_claimed_at"].isoformat() > lease["last_activity_at"])
            ):
                self.cp.worker_leases.touch(lease["lease_id"], now.isoformat())
            elif now - datetime.fromisoformat(lease["last_activity_at"]) > timedelta(seconds=self.idle_seconds):
                self.release(lease["lease_id"], by="system", reason="idle")
        elif state == "ending":
            self._advance(lease)
        elif state == "sanitizing":
            started = self.cp.fleet_nodes.get(node_id) or {}
            since = started.get("sanitizing_since")
            if since and now - datetime.fromisoformat(since) > timedelta(seconds=self.sanitize_timeout_seconds):
                self._finish_sanitize(lease, False, "the worker did not answer in time")

    # ------------------------------------------------------------------ internals
    def _advance(self, lease: Optional[Dict[str, Any]]) -> None:
        """ending -> sanitizing, once the worker has no job running and can be reached."""
        if lease is None or lease["state"] != "ending":
            return
        info = self.registry.snapshot().get(lease["node_id"])
        if info is None or info["status"] != "idle" or info["running_jobs"] > 0 or not node_connected(info["node"]):
            return
        if not self.cp.worker_leases.mark_sanitizing(lease["lease_id"]):
            return
        self.cp.fleet_nodes.begin_sanitize(lease["node_id"], lease["lease_id"])
        self.registry.set_lease(lease["node_id"], lease["org_id"], "sanitizing")
        if not (self.send_sanitize and self.send_sanitize(lease["node_id"], lease["lease_id"])):
            self._finish_sanitize(self.cp.worker_leases.get(lease["lease_id"]), False,
                                  "the clean-up request could not be delivered")

    def _finish_sanitize(self, lease: Dict[str, Any], ok: bool, detail: str) -> None:
        node_id = lease["node_id"]
        now = self.clock().isoformat()
        if ok:
            self.cp.worker_leases.mark_ended(lease["lease_id"], now, "clean")
            self.cp.fleet_nodes.clear_sanitize(node_id)
            self.registry.set_lease(node_id, None, None)
            self._emit("WORKER_LEASE_ENDED", self.cp.worker_leases.get(lease["lease_id"]))
        else:
            self.cp.worker_leases.mark_ended(lease["lease_id"], now, f"failed: {detail}")
            self.cp.fleet_nodes.hold(node_id, f"clean-up failed: {detail}")
            self.registry.set_lease(node_id, None, None)
            self._emit("WORKER_HELD", {"node_id": node_id, "reason": f"clean-up failed: {detail}"})
            self._emit("WORKER_LEASE_ENDED", self.cp.worker_leases.get(lease["lease_id"]))

    def _end_lost(self, lease: Dict[str, Any]) -> None:
        node_id = lease["node_id"]
        self._unseen_since.pop(node_id, None)
        self.cp.worker_leases.mark_ended(
            lease["lease_id"], self.clock().isoformat(), "not cleaned: the worker was lost",
            reason="worker_lost", by="system")
        self.cp.fleet_nodes.hold(node_id, "lost while leased; it must be cleaned before it is used again")
        self.registry.set_lease(node_id, None, None)
        self._emit("WORKER_HELD", {"node_id": node_id, "reason": "lost while leased"})
        self._emit("WORKER_LEASE_ENDED", self.cp.worker_leases.get(lease["lease_id"]))

    def _emit(self, event: str, payload: Dict[str, Any]) -> None:
        if self.publish is not None:
            try:
                self.publish(event, dict(payload))
            except Exception:
                logger.exception("Could not publish %s", event)
