"""
Control-plane observability: why work is waiting, SLIs, per-customer and
per-node aggregates, sampled metric history, and incidents.

Everything here is DERIVED from state the coordinator already holds (jobs,
registry, receipts counters, control-plane tables) -- no second source of
truth. Thresholds come from environment variables (defaults below) so
nothing is a hardcoded policy.

It is never on the dispatch path: tick() is called from the coordinator's
background health loop inside its own try/except, and a failure here only
costs a missing sample.

    GCON_METRICS_SNAPSHOT_INTERVAL_SECONDS  sampling period            (30)
    GCON_METRICS_RETENTION_HOURS            history kept               (168)
    GCON_SLI_WINDOW_SECONDS                 window for SLIs            (3600)
    GCON_ALERT_QUEUE_AGE_SECONDS            oldest-queued alert        (300)
    GCON_ALERT_DB_WRITE_MS                  slow DB write alert        (250)
"""
from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timedelta, UTC
from typing import Any, Dict, List, Optional

logger = logging.getLogger("gcon.observability")

REASON_LABELS = {
    "standby_coordinator": "This coordinator is a standby (not the leader)",
    "scheduler_paused": "Scheduler is paused",
    "no_workers": "No workers are online",
    "all_workers_busy": "All online workers are busy",
    "insufficient_replica_workers": "Fewer idle workers than the job's replica count",
    "awaiting_matching_worker": "Idle worker exists; job has requirements to match",
    "awaiting_dispatch": "Idle worker exists; about to be dispatched or none is eligible",
}


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, default))
    except (TypeError, ValueError):
        return float(default)


def _parse(ts) -> Optional[datetime]:
    try:
        d = datetime.fromisoformat(ts)
    except (TypeError, ValueError):
        return None
    return d if d.tzinfo else d.replace(tzinfo=UTC)


def _percentile(values: List[float], p: float) -> Optional[float]:
    if not values:
        return None
    s = sorted(values)
    k = (len(s) - 1) * p
    lo, hi = int(k), min(int(k) + 1, len(s) - 1)
    return s[lo] + (s[hi] - s[lo]) * (k - lo)


class ObservabilityService:
    def __init__(self, coordinator):
        self.c = coordinator
        self._last_tick = 0.0
        self._last_prune = 0.0
        self._last_write_ms: Optional[float] = None

    # ------------------------------------------------------------ config
    @property
    def interval(self) -> float:
        return _env_float("GCON_METRICS_SNAPSHOT_INTERVAL_SECONDS", 30)

    # -------------------------------------------------------------- jobs
    def _jobs(self) -> List[Dict[str, Any]]:
        with self.c.jobs_lock:
            out = [dict(j) for j in self.c.jobs.values()]
        # Live job dicts record when they were queued as `created_at` (only
        # rows loaded from the durable table carry `submitted_at`). Every
        # queue-age / wait / end-to-end figure keys off submitted_at, so
        # without this they were all blank for jobs submitted since startup.
        for j in out:
            if not j.get("submitted_at"):
                j["submitted_at"] = j.get("created_at")
        return out

    def _is_leader(self) -> bool:
        e = getattr(self.c, "leader_elector", None)
        return e is None or e.is_leader

    # ------------------------------------------------------------- scope
    def _region_map(self) -> Dict[str, str]:
        """{node_id: region}. A worker's region is its registered capability
        "region" (set with run_worker.py --capability region=eu-west) -- no new
        field, capabilities are already free-form and persisted per node."""
        cp = getattr(self.c, "control_plane", None)
        if cp is None:
            return {}
        try:
            return {n: v for n, v in cp.node_capabilities.values_for_key("region").items() if v}
        except Exception as e:
            logger.warning("region lookup failed: %r", e)
            return {}

    def scope_options(self) -> Dict[str, Any]:
        """Values the dashboard's global filters can offer, all read from live state."""
        regions = self._region_map()
        nodes = list(self.c.registry.nodes)
        return {
            "regions": sorted(set(regions.values())),
            "unlabelled_workers": sum(1 for n in nodes if n not in regions),
            # One coordinator is one environment; it comes from config, not a guess.
            "environment": os.environ.get("GCON_ENVIRONMENT") or None,
            "queue_age_alert_seconds": _env_float("GCON_ALERT_QUEUE_AGE_SECONDS", 300),
        }

    # --------------------------------------------------- why work is waiting
    def waiting_reasons(self, jobs: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
        jobs = self._jobs() if jobs is None else jobs
        now = datetime.now(UTC)
        nodes = list(self.c.registry.nodes.values())
        active = sum(1 for n in nodes if n.get("status") != "offline")
        idle = len(self.c.registry.available_nodes())
        paused = bool(getattr(self.c, "scheduler_paused", False))
        standby = not self._is_leader()

        by_reason: Dict[str, Dict[str, Any]] = {}
        for job in jobs:
            if (job.get("status") or "").lower() != "pending":
                continue
            replicas = ((job.get("verify") or {}).get("replicas") or 0)
            if standby:
                reason = "standby_coordinator"
            elif paused:
                reason = "scheduler_paused"
            elif active == 0:
                reason = "no_workers"
            elif idle == 0:
                reason = "all_workers_busy"
            elif replicas and replicas > idle:
                reason = "insufficient_replica_workers"
            elif job.get("requires"):
                reason = "awaiting_matching_worker"
            else:
                reason = "awaiting_dispatch"
            entry = by_reason.setdefault(reason, {"count": 0, "oldest_age_seconds": None})
            entry["count"] += 1
            created = _parse(job.get("submitted_at"))
            if created is not None:
                age = max(0.0, (now - created).total_seconds())
                if entry["oldest_age_seconds"] is None or age > entry["oldest_age_seconds"]:
                    entry["oldest_age_seconds"] = age
        for reason, entry in by_reason.items():
            entry["label"] = REASON_LABELS[reason]
        return {"by_reason": by_reason, "idle_workers": idle, "active_workers": active}

    # ---------------------------------------------------------- scheduler
    def scheduler(self) -> Dict[str, Any]:
        """
        Everything the Scheduler page shows, in one call: the scheduler's
        state, queue pressure, dispatch activity, scheduling failures,
        retry/attempt pressure and recent pause/resume events.

        State precedence: dead (thread gone) > standby (not the HA leader)
        > paused > stalled (loop has not ticked recently) > running.
        """
        c = self.c
        stats = c.scheduler_stats.snapshot()
        jobs = self._jobs()
        pending = [j for j in jobs if (j.get("status") or "").lower() == "pending"]
        paused = bool(getattr(c, "scheduler_paused", False))
        thread = getattr(c, "scheduler_thread", None)
        alive = bool(thread is not None and thread.is_alive())
        stall_after = _env_float("GCON_SCHEDULER_STALL_SECONDS", 10)
        loop_age = stats["loop_age_seconds"]
        if not alive:
            state = "dead"
        elif not self._is_leader():
            state = "standby"
        elif paused:
            state = "paused"
        elif loop_age is not None and loop_age > stall_after:
            state = "stalled"
        else:
            state = "running"

        # Queued jobs that have already failed at least one placement pass and
        # are still waiting (cancelled/finished jobs are not "blocked").
        reported = set(getattr(c, "_dispatch_failure_reported", set()))
        # (job dicts carry no job_id key; the id is the key of c.jobs)
        with c.jobs_lock:
            blocked_now = sum(1 for jid, j in c.jobs.items()
                              if (j.get("status") or "").lower() == "pending" and jid in reported)

        max_attempts = getattr(c, "_max_job_attempts", None)
        retried = sum(1 for j in jobs if (j.get("attempt_number") or 0) > 1)
        at_cap = sum(1 for j in jobs if (j.get("status") or "").lower() == "failed"
                     and max_attempts and (j.get("attempt_number") or 0) >= max_attempts)

        ev = [e for e in c.get_all_events() if getattr(e, "event_type", "") in ("SCHEDULER_PAUSED", "SCHEDULER_RESUMED")]
        recent_control = [
            {"event_type": e.event_type, "at": e.timestamp.isoformat()}
            for e in sorted(ev, key=lambda e: e.timestamp, reverse=True)[:8]
        ]

        waiting = self.waiting_reasons(jobs)
        oldest = max((r["oldest_age_seconds"] or 0 for r in waiting["by_reason"].values()), default=None) if waiting["by_reason"] else None
        return {
            "state": state,
            "thread_alive": alive,
            "paused": paused,
            "leader": self._is_leader(),
            "stall_threshold_seconds": stall_after,
            "sandbox_policy": getattr(c, "_sandbox_policy", None),
            "queue": {
                "depth": c.job_queue.qsize(),
                "pending_jobs": len(pending),
                "oldest_pending_seconds": oldest,
                "waiting": waiting,
                "blocked_now": blocked_now,
            },
            "dispatch": {
                "total": stats["dispatched_total"],
                "last_at": stats["last_dispatch_at"],
                "rate_per_minute": stats["rate_per_minute"],
                "loop_state": stats["loop_state"],
                "loop_age_seconds": loop_age,
            },
            "failures": {
                "failed_passes_total": stats["failed_passes_total"],
                "by_kind": stats["failed_passes_by_kind"],
                "last_at": stats["last_failure_at"],
                "last_kind": stats["last_failure_kind"],
                "last_message": stats["last_failure_message"],
            },
            "retry": {
                "max_attempts": max_attempts,
                "jobs_retried": retried,
                "failed_at_attempt_cap": at_cap,
                # Honest: the scheduler has no delay between attempts. A job
                # that cannot be placed goes to the back of the queue and the
                # loop moves on; a failed job is retried only when asked.
                "backoff": "none",
            },
            "recent_control": recent_control,
            "counting_since": stats["since"],
        }

    # -------------------------------------------------------------- SLIs
    def sli(self, jobs: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
        jobs = self._jobs() if jobs is None else jobs
        window = _env_float("GCON_SLI_WINDOW_SECONDS", 3600)
        now = datetime.now(UTC)
        cutoff = now - timedelta(seconds=window)
        done = failed = 0
        e2e: List[float] = []
        waits: List[float] = []
        for job in jobs:
            submitted = _parse(job.get("submitted_at"))
            status = (job.get("status") or "").lower()
            finished = _parse(job.get("completed_at"))
            if finished is not None and finished >= cutoff:
                if status == "completed":
                    done += 1
                    if submitted is not None:
                        e2e.append((finished - submitted).total_seconds())
                elif status == "failed":
                    failed += 1
            first = _parse(job.get("first_dispatched_at"))
            if first is not None and first >= cutoff and submitted is not None:
                waits.append(max(0.0, (first - submitted).total_seconds()))
        finished_total = done + failed
        return {
            "window_seconds": window,
            "completed": done,
            "failed": failed,
            "jobs_per_hour": finished_total * 3600.0 / window if window else None,
            "failure_pct": (failed * 100.0 / finished_total) if finished_total else None,
            "completion_p50_seconds": _percentile(e2e, 0.5),
            "completion_p95_seconds": _percentile(e2e, 0.95),
            "queue_wait_p50_seconds": _percentile(waits, 0.5),
            "queue_wait_p95_seconds": _percentile(waits, 0.95),
            "queue_wait_samples": len(waits),
            "retained_jobs": len(jobs),
        }

    # --------------------------------------------------------- per customer
    def per_org(self, jobs: Optional[List[Dict[str, Any]]] = None, limit: int = 50,
                region: Optional[str] = None) -> List[Dict[str, Any]]:
        jobs = self._jobs() if jobs is None else jobs
        if region:
            # A job has a region only once a worker has run it (job["node_id"]);
            # waiting jobs have none, so a region filter excludes them.
            regions = self._region_map()
            jobs = [j for j in jobs if regions.get(j.get("node_id")) == region]
        now = datetime.now(UTC)
        orgs: Dict[str, Dict[str, Any]] = {}
        for job in jobs:
            key = job.get("org_id") or "(no organization)"
            o = orgs.setdefault(key, {"org_id": key, "pending": 0, "running": 0, "completed": 0,
                                      "failed": 0, "cancelled": 0, "oldest_pending_age_seconds": None})
            status = (job.get("status") or "").lower()
            if status in ("pending", "running", "completed", "failed", "cancelled"):
                o[status] += 1
            if status == "pending":
                created = _parse(job.get("submitted_at"))
                if created is not None:
                    age = max(0.0, (now - created).total_seconds())
                    if o["oldest_pending_age_seconds"] is None or age > o["oldest_pending_age_seconds"]:
                        o["oldest_pending_age_seconds"] = age
        for o in orgs.values():
            finished = o["completed"] + o["failed"]
            o["failure_pct"] = (o["failed"] * 100.0 / finished) if finished else None
        ranked = sorted(orgs.values(), key=lambda o: (o["pending"] + o["running"], o["failed"]), reverse=True)
        return ranked[:limit]

    # ---------------------------------------------------------- per node
    def node_trust(self) -> List[Dict[str, Any]]:
        receipts = {}
        cp = getattr(self.c, "control_plane", None)
        if cp is not None:
            try:
                receipts = {r["node_id"]: r for r in cp.obs_queries.node_receipt_trust()}
            except Exception as e:
                logger.warning("node trust query failed: %r", e)
        streaks = getattr(self.c, "_node_verification_failure_streak", {}) or {}
        out = []
        for node_id, info in self.c.registry.nodes.items():
            r = receipts.get(node_id, {})
            total, ok = r.get("receipts", 0), r.get("verified", 0)
            out.append({
                "node_id": node_id,
                "status": info.get("status"),
                "quarantined": bool(info.get("quarantined")),
                "quarantine_reason": info.get("quarantine_reason"),
                "verification_failure_streak": streaks.get(node_id, 0),
                "receipts": total,
                "verified": ok,
                "verified_pct": (ok * 100.0 / total) if total else None,
            })
        out.sort(key=lambda n: (not n["quarantined"], -n["verification_failure_streak"], n["node_id"]))
        return out

    # ---------------------------------------------------------- database
    def database(self) -> Dict[str, Any]:
        cp = getattr(self.c, "control_plane", None)
        if cp is None:
            return {"available": False}
        db = cp.db
        info: Dict[str, Any] = {"available": True, "dialect": db.dialect.name}
        t0 = time.perf_counter()
        db.query_one("SELECT 1 AS ok")
        info["read_ms"] = (time.perf_counter() - t0) * 1000.0
        info["write_ms"] = self._last_write_ms
        try:
            info["schema_version"] = cp.obs_queries.schema_version()
            if db.dialect.name == "sqlite":
                path = getattr(db, "path", None)
                info["size_bytes"] = os.path.getsize(path) if path and path != ":memory:" and os.path.exists(path) else None
            else:
                info["size_bytes"] = db.query_one("SELECT pg_database_size(current_database()) AS s")["s"]
            info["snapshots_stored"] = db.query_one("SELECT COUNT(*) AS n FROM metric_snapshots")["n"]
        except Exception as e:
            logger.warning("database info query failed: %r", e)
        return info

    # ----------------------------------------------------- compact snapshot
    def snapshot(self) -> Dict[str, Any]:
        jobs = self._jobs()
        counts = {k: 0 for k in ("pending", "running", "completed", "failed", "cancelled")}
        disputed = 0
        for j in jobs:
            s = (j.get("status") or "").lower()
            if s in counts:
                counts[s] += 1
            if ((j.get("verification") or {}).get("outcome")) == "disputed":
                disputed += 1
        waiting = self.waiting_reasons(jobs)
        oldest = [e["oldest_age_seconds"] for e in waiting["by_reason"].values()
                  if e["oldest_age_seconds"] is not None]
        nodes = list(self.c.registry.nodes.values())
        sli = self.sli(jobs)
        snap: Dict[str, Any] = {
            **counts,
            "oldest_pending_age_seconds": max(oldest) if oldest else None,
            "workers_total": len(nodes),
            "workers_active": waiting["active_workers"],
            "workers_idle": waiting["idle_workers"],
            "workers_quarantined": sum(1 for n in nodes if n.get("quarantined")),
            "scheduler_on": bool(not getattr(self.c, "scheduler_paused", False)),
            "receipts_verified": getattr(self.c, "_verified_receipt_count", None),
            "receipts_unverified": getattr(self.c, "_unverified_receipt_count", None),
            "disputed_jobs": disputed,
            "db_write_ms": self._last_write_ms,
            "jobs_per_hour": sli["jobs_per_hour"],
            "failure_pct": sli["failure_pct"],
            "queue_wait_p95_seconds": sli["queue_wait_p95_seconds"],
            "completion_p95_seconds": sli["completion_p95_seconds"],
        }
        snap["_waiting"] = {r: e["count"] for r, e in waiting["by_reason"].items()}
        return snap

    # --------------------------------------------------------- incidents
    def evaluate_rules(self, snap: Dict[str, Any]) -> List[Dict[str, Any]]:
        firing: List[Dict[str, Any]] = []

        def fire(rule, subject, severity, title, **detail):
            firing.append({"rule": rule, "subject": subject, "severity": severity,
                           "title": title, "detail": detail})

        pending = snap.get("pending") or 0
        if pending and not snap.get("workers_active"):
            fire("no_workers", "cluster", "critical",
                 f"{pending} job(s) waiting and no worker is online", pending=pending,
                 impact=f"{pending} job(s) cannot start")
        age = snap.get("oldest_pending_age_seconds")
        limit = _env_float("GCON_ALERT_QUEUE_AGE_SECONDS", 300)
        if age is not None and age > limit and snap.get("scheduler_on"):
            fire("queue_age", "cluster", "warning",
                 f"Oldest queued job has waited {int(age)}s (alert at {int(limit)}s)",
                 oldest_age_seconds=age, threshold_seconds=limit, waiting=snap.get("_waiting"),
                 impact=f"{pending} job(s) queued, oldest {int(age)}s")
        if pending and not snap.get("scheduler_on"):
            fire("scheduler_paused", "scheduler", "warning",
                 f"Scheduler is paused with {pending} job(s) waiting", pending=pending,
                 impact=f"{pending} job(s) will not be dispatched")
        if snap.get("receipts_unverified"):
            fire("receipts_unverified", "receipts", "warning",
                 f"{snap['receipts_unverified']} receipt(s) failing signature verification",
                 unverified=snap["receipts_unverified"],
                 impact=f"{snap['receipts_unverified']} receipt(s) not trustworthy")
        if snap.get("disputed_jobs"):
            fire("verification_disputed", "verification", "warning",
                 f"{snap['disputed_jobs']} job(s) with disagreeing replicas",
                 disputed=snap["disputed_jobs"],
                 impact=f"{snap['disputed_jobs']} job result(s) in dispute")
        if snap.get("workers_quarantined"):
            fire("nodes_quarantined", "nodes", "warning",
                 f"{snap['workers_quarantined']} worker(s) quarantined",
                 quarantined=snap["workers_quarantined"],
                 impact=f"{snap['workers_quarantined']} of {snap.get('workers_total') or 0} worker(s) out of rotation")
        write_limit = _env_float("GCON_ALERT_DB_WRITE_MS", 250)
        if snap.get("db_write_ms") is not None and snap["db_write_ms"] > write_limit:
            fire("db_slow_write", "database", "warning",
                 f"Control-plane DB write took {snap['db_write_ms']:.0f}ms (alert at {write_limit:.0f}ms)",
                 write_ms=snap["db_write_ms"], threshold_ms=write_limit)
        # Every health check the system already runs becomes an incident
        # when it is unhealthy -- one generic rule, no per-check list.
        try:
            for key, chk in (self.c.get_cluster_health().get("checks") or {}).items():
                if not chk.get("healthy", True) and key not in ("workers", "node_registry"):
                    fire(f"health_{key}", key, "warning", f"{chk.get('label', key)}: {chk.get('detail', 'unhealthy')}")
        except Exception as e:
            logger.warning("health rule evaluation failed: %r", e)
        # HA: no live leader at all.
        elector = getattr(self.c, "leader_elector", None)
        cp = getattr(self.c, "control_plane", None)
        if elector is not None and cp is not None:
            try:
                lease = cp.leases.read(elector.lease_name)
                expires = _parse(lease.get("expires_at")) if lease else None
                if expires is None or expires < datetime.now(UTC):
                    fire("no_leader", "ha", "critical", "No coordinator holds the leader lease")
            except Exception as e:
                logger.warning("lease rule evaluation failed: %r", e)
        # Webhook delivery trouble right now.
        if cp is not None:
            try:
                since = (datetime.now(UTC) - timedelta(seconds=_env_float("GCON_SLI_WINDOW_SECONDS", 3600))).isoformat()
                wh = cp.obs_queries.webhook_summary(since)
                if wh["failed_since"]:
                    fire("webhook_failures", "webhooks", "warning",
                         f"{wh['failed_since']} webhook delivery(ies) gave up in the last window",
                         failed=wh["failed_since"],
                         impact=f"{wh['failed_since']} customer callback(s) not delivered")
            except Exception as e:
                logger.warning("webhook rule evaluation failed: %r", e)
        return firing

    def sync_incidents(self, firing: List[Dict[str, Any]], now_iso: str) -> None:
        cp = self.c.control_plane
        for f in firing:
            cp.incidents.open_incident(f["rule"], f["severity"], f["subject"], f["title"], f["detail"], now_iso)
        cp.incidents.resolve_missing({(f["rule"], f["subject"]) for f in firing}, now_iso)

    # ------------------------------------------------------------- tick
    def maybe_tick(self) -> bool:
        """Called every health-loop pass; samples only when due, and only on the leader."""
        cp = getattr(self.c, "control_plane", None)
        if cp is None or not self._is_leader():
            return False
        if time.monotonic() - self._last_tick < self.interval:
            return False
        self._last_tick = time.monotonic()
        now_iso = datetime.now(UTC).isoformat()
        snap = self.snapshot()
        t0 = time.perf_counter()
        cp.metric_snapshots.record(now_iso, snap)
        self._last_write_ms = (time.perf_counter() - t0) * 1000.0
        self.sync_incidents(self.evaluate_rules(snap), now_iso)
        if time.monotonic() - self._last_prune > 3600:
            self._last_prune = time.monotonic()
            keep = _env_float("GCON_METRICS_RETENTION_HOURS", 168)
            cp.metric_snapshots.prune((datetime.now(UTC) - timedelta(hours=keep)).isoformat())
        return True

    # ----------------------------------------------------------- summary
    def summary(self, window_seconds: Optional[float] = None,
                region: Optional[str] = None) -> Dict[str, Any]:
        jobs = self._jobs()
        cp = getattr(self.c, "control_plane", None)
        out: Dict[str, Any] = {
            "waiting": self.waiting_reasons(jobs),
            "sli": self.sli(jobs),
            "customers": self.per_org(jobs, region=region),
            "nodes": self.node_trust(),
            "database": self.database(),
            "retries": None,
            "webhooks": None,
            "events": self.c.telemetry.derived_metrics(),
            "scope": self.scope_options(),
        }
        if cp is not None:
            try:
                out["retries"] = cp.obs_queries.retry_summary()
                window = float(window_seconds) if window_seconds else _env_float("GCON_SLI_WINDOW_SECONDS", 3600)
                since = (datetime.now(UTC) - timedelta(seconds=window)).isoformat()
                out["webhooks"] = cp.obs_queries.webhook_summary(since)
                lat = cp.obs_queries.delivery_latencies(since)
                out["webhooks"]["delivered_since"] = len(lat)
                out["webhooks"]["latency_p50_seconds"] = _percentile(lat, 0.5)
                out["webhooks"]["latency_p95_seconds"] = _percentile(lat, 0.95)
            except Exception as e:
                logger.warning("summary aggregate query failed: %r", e)
        return out

    def event_groups(self, minutes: float = 60, limit: int = 8, scan: int = 5000) -> Dict[str, Any]:
        """
        Cluster events rolled up by (event_type, source) over a window, with a
        count and first/last time, instead of one row per event -- a
        flapping health check becomes one line, not a waterfall. Reads the
        most recent `scan` events the bus holds, so on a very busy bus the
        window can be shorter than asked (`scanned_all` says whether the
        whole window was covered).
        """
        events = self.c.get_events(limit=scan)
        cutoff = datetime.now(UTC) - timedelta(minutes=minutes)
        groups: Dict[tuple, Dict[str, Any]] = {}
        total = 0
        oldest_scanned = None
        for ev in events:
            ts = ev.timestamp if ev.timestamp.tzinfo else ev.timestamp.replace(tzinfo=UTC)
            if oldest_scanned is None or ts < oldest_scanned:
                oldest_scanned = ts
            if ts < cutoff:
                continue
            total += 1
            g = groups.setdefault((ev.event_type, ev.source), {
                "event_type": ev.event_type, "source": ev.source, "count": 0,
                "first_at": ts, "last_at": ts,
            })
            g["count"] += 1
            g["first_at"] = min(g["first_at"], ts)
            g["last_at"] = max(g["last_at"], ts)
        ranked = sorted(groups.values(), key=lambda g: (g["count"], g["last_at"]), reverse=True)
        for g in ranked:
            g["first_at"] = g["first_at"].isoformat()
            g["last_at"] = g["last_at"].isoformat()
        return {
            "window_minutes": minutes,
            "total_events": total,
            "group_count": len(ranked),
            "groups": ranked[:limit],
            "scanned_all": len(events) < scan or (oldest_scanned is not None and oldest_scanned <= cutoff),
        }

    def history(self, since_minutes: float, limit: int = 2000) -> List[Dict[str, Any]]:
        cp = getattr(self.c, "control_plane", None)
        if cp is None:
            return []
        since = (datetime.now(UTC) - timedelta(minutes=since_minutes)).isoformat()
        # Snapshots come back oldest-first, so a plain LIMIT would return the
        # OLDEST `limit` rows and cut off the recent end of a long window
        # (24h at one sample per 30s is 2880 rows). Read the whole window
        # (bounded) and thin it evenly instead, keeping first and last.
        rows = cp.metric_snapshots.since(since, 20000)
        if len(rows) > limit:
            step = (len(rows) - 1) / (limit - 1)
            rows = [rows[round(i * step)] for i in range(limit)]
        for r in rows:
            r.pop("_waiting", None)
        return rows
