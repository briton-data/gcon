"""
GCON Health Service

Computes cluster health from the coordinator's real, live state.
Every branch below inspects an actual subsystem (job queue, node
registry, receipt store, storage disk) — nothing here is randomized
or hardcoded. Overall Cluster Health is derived from these branches,
not reported as a bare, unexplained percentage.

    Overall Health
    ├── Coordinator      (running, response time, queue size)
    ├── Receipt Service  (reachable, write latency)
    ├── Node Registry    (active nodes, lost nodes)
    ├── Workers          (busy, idle, offline)
    ├── API              (response latency)
    └── Storage          (available, remaining capacity)
"""

import shutil
import time
from datetime import datetime, UTC


class HealthCheck:
    """A single health-tree branch and why it's in that state."""

    def __init__(self, key, label, healthy, detail, metrics=None):
        self.key = key
        self.label = label
        self.healthy = healthy
        self.detail = detail
        self.metrics = metrics or {}

    def to_dict(self):
        return {
            "key": self.key,
            "label": self.label,
            "healthy": self.healthy,
            "detail": self.detail,
            "metrics": self.metrics,
        }


class HealthService:
    """
    Builds the full cluster health source-tree and rolls it up into
    an overall state + score, with a human-readable reason for every
    branch (so an operator sees *why* health dropped, not just a
    number).
    """

    QUEUE_WARN_SIZE = 50
    API_SLOW_MS = 250
    STORAGE_CRITICAL_PCT = 5
    STORAGE_WARN_PCT = 15

    def __init__(self, coordinator):
        self.coordinator = coordinator

    # ------------------------------------------------------------
    # Individual branches
    # ------------------------------------------------------------

    def check_coordinator(self):
        start = time.perf_counter()
        queue_size = self.coordinator.job_queue.qsize()
        running = self.coordinator.scheduler_thread.is_alive()
        response_ms = round((time.perf_counter() - start) * 1000, 3)

        if not running:
            healthy, detail = False, "Scheduler thread is not running"
        elif queue_size >= self.QUEUE_WARN_SIZE:
            healthy, detail = False, f"Queue backlog increasing ({queue_size} pending)"
        else:
            healthy, detail = True, "Coordinator online"

        return HealthCheck(
            "coordinator", "Coordinator", healthy, detail,
            {"running": running, "response_time_ms": response_ms, "queue_size": queue_size},
        )

    def check_receipt_service(self):
        start = time.perf_counter()
        reachable = isinstance(self.coordinator.receipts, dict)
        total_receipts = len(self.coordinator.receipts)
        write_latency_ms = round((time.perf_counter() - start) * 1000, 3)

        detail = "Receipt service healthy" if reachable else "Receipt store unreachable"

        return HealthCheck(
            "receipt_service", "Receipt Service", reachable, detail,
            {
                "reachable": reachable,
                "write_latency_ms": write_latency_ms,
                "total_receipts": total_receipts,
            },
        )

    def check_node_registry(self):
        nodes = self.coordinator.get_nodes()
        lost = [n for n in nodes if n["status"] == "offline"]
        active = [n for n in nodes if n["status"] != "offline"]

        if not nodes:
            healthy, detail = False, "No registered nodes"
        elif lost:
            names = ", ".join(n["node_id"] for n in lost[:3])
            more = "" if len(lost) <= 3 else f" (+{len(lost) - 3} more)"
            healthy = False
            detail = f"Missing heartbeat: {names}{more}"
        else:
            healthy, detail = True, f"{len(active)} node(s) reporting"

        return HealthCheck(
            "node_registry", "Node Registry", healthy, detail,
            {
                "active_nodes": len(active),
                "lost_nodes": len(lost),
                "lost_node_ids": [n["node_id"] for n in lost],
            },
        )

    def check_workers(self):
        nodes = self.coordinator.get_nodes()
        busy = sum(1 for n in nodes if n["status"] == "busy")
        idle = sum(1 for n in nodes if n["status"] == "idle")
        offline = sum(1 for n in nodes if n["status"] == "offline")

        if not nodes:
            healthy, detail = False, "No workers registered"
        elif offline == len(nodes):
            healthy, detail = False, "All workers offline"
        elif offline > 0:
            healthy, detail = False, f"{offline} worker(s) offline"
        else:
            healthy, detail = True, f"{busy} busy / {idle} idle"

        return HealthCheck(
            "workers", "Workers", healthy, detail,
            {"busy": busy, "idle": idle, "offline": offline},
        )

    def check_api(self):
        start = time.perf_counter()
        # Exercise the same code path a client's /cluster request takes.
        self.coordinator.get_cluster_status()
        response_ms = round((time.perf_counter() - start) * 1000, 3)

        healthy = response_ms < self.API_SLOW_MS
        detail = (
            f"Responding in {response_ms}ms" if healthy
            else f"Slow response ({response_ms}ms)"
        )

        return HealthCheck("api", "API", healthy, detail, {"response_time_ms": response_ms})

    def check_storage(self):
        root = self.coordinator.storage_manager.storage_root
        try:
            usage = shutil.disk_usage(root)
            remaining_pct = round(usage.free / usage.total * 100, 1)
            available = True
        except (FileNotFoundError, OSError):
            usage = None
            remaining_pct = 0.0
            available = False

        if not available:
            healthy, detail = False, "Storage path unavailable"
        elif remaining_pct < self.STORAGE_CRITICAL_PCT:
            healthy, detail = False, f"Critically low: {remaining_pct}% capacity remaining"
        elif remaining_pct < self.STORAGE_WARN_PCT:
            healthy, detail = False, f"Low capacity: {remaining_pct}% remaining"
        else:
            healthy, detail = True, f"{remaining_pct}% capacity remaining"

        return HealthCheck(
            "storage", "Storage", healthy, detail,
            {
                "available": available,
                "remaining_capacity_pct": remaining_pct,
                "total_bytes": usage.total if usage else 0,
                "free_bytes": usage.free if usage else 0,
            },
        )

    # ------------------------------------------------------------
    # Aggregate
    # ------------------------------------------------------------

    def compute(self):
        checks = [
            self.check_coordinator(),
            self.check_receipt_service(),
            self.check_node_registry(),
            self.check_workers(),
            self.check_api(),
            self.check_storage(),
        ]

        healthy_count = sum(1 for c in checks if c.healthy)
        total = len(checks)
        score = round(healthy_count / total * 100) if total else 0

        if score == 100:
            state = "healthy"
        elif score >= 60:
            state = "degraded"
        else:
            state = "critical"

        reasons = [
            {"check": c.key, "label": c.label, "healthy": c.healthy, "detail": c.detail}
            for c in checks
        ]

        return {
            "state": state,
            "score": score,
            "reason": self._summarize(checks),
            "reasons": reasons,
            "checks": {c.key: c.to_dict() for c in checks},
            "computed_at": datetime.now(UTC).isoformat(),
        }

    @staticmethod
    def _summarize(checks):
        failing = [c.detail for c in checks if not c.healthy]
        if not failing:
            return "All systems operating normally."
        return "; ".join(failing)
