"""
Persistence for the observability layer: sampled metric history,
incidents, and the read-only aggregate queries the Reliability page
needs (retry and webhook-delivery summaries, per-node receipt trust).

Kept to one file because all of it serves a single consumer
(gcon.monitoring.observability) and none of it is on the job-dispatch
path -- a failure in here must never be able to affect scheduling, so
every caller wraps these in its own try/except.
"""
import json
import uuid
from typing import Any, Dict, List, Optional


class MetricSnapshotRepository:
    def __init__(self, db):
        self.db = db

    def record(self, taken_at: str, data: Dict[str, Any]) -> None:
        with self.db.transaction() as conn:
            conn.execute(
                "INSERT INTO metric_snapshots (snapshot_id, taken_at, data_json) "
                "VALUES (?, ?, ?)",
                (uuid.uuid4().hex, taken_at, json.dumps(data, default=str)),
            )

    def since(self, since_iso: str, limit: int = 2000) -> List[Dict[str, Any]]:
        """Oldest-first snapshots at or after `since_iso`."""
        rows = self.db.query(
            "SELECT taken_at, data_json FROM metric_snapshots "
            "WHERE taken_at >= ? ORDER BY taken_at ASC LIMIT ?",
            (since_iso, int(limit)),
        )
        return [dict(json.loads(r["data_json"]), taken_at=r["taken_at"]) for r in rows]

    def latest(self) -> Optional[Dict[str, Any]]:
        row = self.db.query_one(
            "SELECT taken_at, data_json FROM metric_snapshots "
            "ORDER BY taken_at DESC LIMIT 1"
        )
        return dict(json.loads(row["data_json"]), taken_at=row["taken_at"]) if row else None

    def prune(self, before_iso: str) -> None:
        with self.db.transaction() as conn:
            conn.execute("DELETE FROM metric_snapshots WHERE taken_at < ?", (before_iso,))


class IncidentRepository:
    def __init__(self, db):
        self.db = db

    @staticmethod
    def _row(r) -> Dict[str, Any]:
        d = dict(r)
        raw = d.pop("detail_json", None)
        d["detail"] = json.loads(raw) if raw else {}
        return d

    def open_incidents(self) -> List[Dict[str, Any]]:
        rows = self.db.query(
            "SELECT * FROM incidents WHERE status = 'open' ORDER BY first_seen ASC"
        )
        return [self._row(r) for r in rows]

    def recent_resolved(self, limit: int = 50) -> List[Dict[str, Any]]:
        rows = self.db.query(
            "SELECT * FROM incidents WHERE status = 'resolved' "
            "ORDER BY resolved_at DESC LIMIT ?",
            (int(limit),),
        )
        return [self._row(r) for r in rows]

    def open_incident(self, rule: str, severity: str, subject: str, title: str,
                      detail: Dict[str, Any], now_iso: str) -> None:
        """
        Record that `rule` is firing for `subject`. If an open incident
        for that exact (rule, subject) already exists it is only
        refreshed (last_seen, severity, detail) -- one continuous
        problem is one incident, not one row per sampling tick.
        """
        existing = self.db.query_one(
            "SELECT incident_id FROM incidents "
            "WHERE rule = ? AND subject = ? AND status = 'open'",
            (rule, subject),
        )
        with self.db.transaction() as conn:
            if existing:
                conn.execute(
                    "UPDATE incidents SET last_seen = ?, severity = ?, title = ?, "
                    "detail_json = ? WHERE incident_id = ?",
                    (now_iso, severity, title, json.dumps(detail, default=str),
                     existing["incident_id"]),
                )
            else:
                conn.execute(
                    "INSERT INTO incidents (incident_id, rule, subject, severity, "
                    "title, status, first_seen, last_seen, detail_json) "
                    "VALUES (?, ?, ?, ?, ?, 'open', ?, ?, ?)",
                    (uuid.uuid4().hex, rule, subject, severity, title,
                     now_iso, now_iso, json.dumps(detail, default=str)),
                )

    def resolve_missing(self, firing_keys: set, now_iso: str) -> int:
        """Resolve every open incident whose (rule, subject) is not firing now."""
        resolved = 0
        for inc in self.open_incidents():
            if (inc["rule"], inc["subject"]) not in firing_keys:
                with self.db.transaction() as conn:
                    conn.execute(
                        "UPDATE incidents SET status = 'resolved', resolved_at = ? "
                        "WHERE incident_id = ?",
                        (now_iso, inc["incident_id"]),
                    )
                resolved += 1
        return resolved

    def set_owner(self, incident_id: str, owner: Optional[str], now_iso: str) -> bool:
        """
        Take (owner=<who>) or release (owner=None) an OPEN incident.
        Returns False if the incident does not exist or is already
        resolved -- ownership of a closed incident is meaningless.
        """
        row = self.db.query_one(
            "SELECT status FROM incidents WHERE incident_id = ?", (incident_id,)
        )
        if not row or row["status"] != "open":
            return False
        with self.db.transaction() as conn:
            conn.execute(
                "UPDATE incidents SET owner = ?, owner_at = ? WHERE incident_id = ?",
                (owner, now_iso if owner else None, incident_id),
            )
        return True


class ObservabilityQueries:
    """Read-only aggregates over tables owned by other repositories."""

    def __init__(self, db):
        self.db = db

    def retry_summary(self) -> Dict[str, Any]:
        total = self.db.query_one("SELECT COUNT(*) AS n FROM job_attempts")["n"]
        retried = self.db.query_one(
            "SELECT COUNT(*) AS n FROM ("
            "SELECT job_id FROM job_attempts GROUP BY job_id HAVING COUNT(*) > 1) t"
        )["n"]
        by_status = {
            r["status"]: r["n"]
            for r in self.db.query(
                "SELECT status, COUNT(*) AS n FROM job_attempts GROUP BY status"
            )
        }
        return {"total_attempts": total, "jobs_retried": retried, "attempts_by_status": by_status}

    def webhook_summary(self, since_iso: str) -> Dict[str, Any]:
        by_status = {
            r["status"]: r["n"]
            for r in self.db.query(
                "SELECT status, COUNT(*) AS n FROM webhook_deliveries GROUP BY status"
            )
        }
        oldest = self.db.query_one(
            "SELECT MIN(created_at) AS t FROM webhook_deliveries "
            "WHERE status IN ('pending', 'retrying')"
        )
        recent_failed = self.db.query_one(
            "SELECT COUNT(*) AS n FROM webhook_deliveries "
            "WHERE status = 'failed' AND last_attempt_at >= ?",
            (since_iso,),
        )["n"]
        return {
            "by_status": by_status,
            "oldest_undelivered_at": oldest["t"] if oldest else None,
            # Terminal failures are cumulative forever; only RECENT ones
            # say whether delivery is broken right now (an alert that
            # could never resolve would be noise).
            "failed_since": recent_failed,
        }

    def delivery_latencies(self, since_iso: str, limit: int = 5000) -> List[float]:
        """
        Seconds from a webhook delivery being queued to the attempt that
        finally succeeded (last_attempt_at on a status='success' row), for
        deliveries that succeeded since `since_iso`. It includes any retry
        back-off, because that is the delay the customer actually saw.
        There is no separate delivered_at column; none is needed for this.
        """
        from datetime import datetime
        rows = self.db.query(
            "SELECT created_at, last_attempt_at FROM webhook_deliveries "
            "WHERE status = 'success' AND last_attempt_at >= ? "
            "ORDER BY last_attempt_at DESC LIMIT ?",
            (since_iso, int(limit)),
        )
        out: List[float] = []
        for r in rows:
            try:
                dt = (datetime.fromisoformat(r["last_attempt_at"])
                      - datetime.fromisoformat(r["created_at"])).total_seconds()
            except (TypeError, ValueError):
                continue
            out.append(max(0.0, dt))
        return out

    def node_receipt_trust(self) -> List[Dict[str, Any]]:
        rows = self.db.query(
            "SELECT node_id, COUNT(*) AS receipts, SUM(verified) AS verified "
            "FROM receipts WHERE node_id IS NOT NULL GROUP BY node_id"
        )
        return [
            {"node_id": r["node_id"], "receipts": r["receipts"], "verified": int(r["verified"] or 0)}
            for r in rows
        ]

    def schema_version(self) -> Optional[int]:
        row = self.db.query_one("SELECT MAX(version) AS v FROM schema_migrations")
        return row["v"] if row else None
