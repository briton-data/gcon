"""
Leases of hosted workers to customers (table `worker_leases`).

Not to be confused with `leases.py` (LeaseRepository), which is the coordinators' leader-election lease.

States: active -> ending -> sanitizing -> ended. See gcon.cluster.worker_leases for what moves a lease along.
"""
import uuid
from datetime import UTC, datetime
from typing import Any, Dict, List, Optional

OPEN_STATES = ("active", "ending", "sanitizing")


class WorkerLeaseRepository:
    def __init__(self, db):
        self.db = db

    def create_many(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Insert several leases in ONE transaction: all of them or none. A worker that already has an open
        lease violates the unique index and rolls the whole batch back (raises db.IntegrityError)."""
        now = datetime.now(UTC).isoformat()
        created = []
        with self.db.transaction() as conn:
            for row in rows:
                lease_id = "lease_" + uuid.uuid4().hex[:16]
                conn.execute(
                    "INSERT INTO worker_leases (lease_id, node_id, org_id, state, requested_by, started_at, "
                    "last_activity_at) VALUES (?, ?, ?, 'active', ?, ?, ?)",
                    (lease_id, row["node_id"], row["org_id"], row.get("requested_by"), now, now))
                created.append(lease_id)
        return [self.get(lease_id) for lease_id in created]

    def get(self, lease_id: str) -> Optional[Dict[str, Any]]:
        row = self.db.query_one("SELECT * FROM worker_leases WHERE lease_id = ?", (lease_id,))
        return dict(row) if row else None

    def get_open_for_node(self, node_id: str) -> Optional[Dict[str, Any]]:
        row = self.db.query_one(
            "SELECT * FROM worker_leases WHERE node_id = ? AND state != 'ended'", (node_id,))
        return dict(row) if row else None

    def list_open(self) -> List[Dict[str, Any]]:
        return [dict(r) for r in self.db.query(
            "SELECT * FROM worker_leases WHERE state != 'ended' ORDER BY started_at")]

    def list_for_org(self, org_id: str, recent_ended: int = 20) -> List[Dict[str, Any]]:
        open_rows = self.db.query(
            "SELECT * FROM worker_leases WHERE org_id = ? AND state != 'ended' ORDER BY started_at", (org_id,))
        ended = self.db.query(
            "SELECT * FROM worker_leases WHERE org_id = ? AND state = 'ended' ORDER BY ended_at DESC LIMIT ?",
            (org_id, int(recent_ended)))
        return [dict(r) for r in open_rows] + [dict(r) for r in ended]

    def list_for_node(self, node_id: str) -> List[Dict[str, Any]]:
        """Every lease this worker has ever had, oldest first: which customer held it, and when."""
        return [dict(r) for r in self.db.query(
            "SELECT * FROM worker_leases WHERE node_id = ? ORDER BY started_at", (node_id,))]

    def list_recent_ended(self, limit: int = 50) -> List[Dict[str, Any]]:
        return [dict(r) for r in self.db.query(
            "SELECT * FROM worker_leases WHERE state = 'ended' ORDER BY ended_at DESC LIMIT ?", (int(limit),))]

    def count_open_for_org(self, org_id: str) -> int:
        row = self.db.query_one(
            "SELECT COUNT(*) AS n FROM worker_leases WHERE org_id = ? AND state != 'ended'", (org_id,))
        return int(row["n"]) if row else 0

    def touch(self, lease_id: str, when_iso: str) -> None:
        with self.db.transaction() as conn:
            conn.execute("UPDATE worker_leases SET last_activity_at = ? WHERE lease_id = ?", (when_iso, lease_id))

    def mark_ending(self, lease_id: str, reason: str, by: str, when_iso: str) -> bool:
        """active -> ending. False if the lease was not active (already ending, or over): releasing twice is harmless."""
        with self.db.transaction() as conn:
            cur = conn.execute(
                "UPDATE worker_leases SET state = 'ending', ending_at = ?, end_reason = ?, ended_by = ? "
                "WHERE lease_id = ? AND state = 'active'", (when_iso, reason, by, lease_id))
            return cur.rowcount == 1

    def mark_sanitizing(self, lease_id: str) -> bool:
        with self.db.transaction() as conn:
            cur = conn.execute(
                "UPDATE worker_leases SET state = 'sanitizing' WHERE lease_id = ? AND state = 'ending'", (lease_id,))
            return cur.rowcount == 1

    def mark_ended(self, lease_id: str, when_iso: str, sanitize_result: str, reason: Optional[str] = None,
                   by: Optional[str] = None) -> bool:
        """-> ended, from any open state. `reason`/`by` are filled in only if nothing recorded them yet (a lease lost
        straight from active has no ending step)."""
        with self.db.transaction() as conn:
            cur = conn.execute(
                "UPDATE worker_leases SET state = 'ended', ended_at = ?, sanitize_result = ?, "
                "end_reason = COALESCE(end_reason, ?), ended_by = COALESCE(ended_by, ?) "
                "WHERE lease_id = ? AND state != 'ended'", (when_iso, sanitize_result, reason, by, lease_id))
            return cur.rowcount == 1
