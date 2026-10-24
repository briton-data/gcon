"""
The GCON-hosted fleet: workers GCON itself runs, enrolled with an operator-only fleet token.

Membership is recorded when a worker enrolls with a fleet token. It is never declared by the worker, and never
inferred from a worker having no organization (the legacy shared token also yields workers with no organization).
A fleet worker serves a customer only while that customer holds a lease on it -- see gcon.cluster.worker_leases.
"""
from datetime import UTC, datetime
from typing import Any, Dict, List, Optional


class FleetNodeRepository:
    def __init__(self, db):
        self.db = db

    def is_member(self, node_id: str) -> bool:
        return self.db.query_one("SELECT 1 AS present FROM fleet_nodes WHERE node_id = ?", (node_id,)) is not None

    def get(self, node_id: str) -> Optional[Dict[str, Any]]:
        row = self.db.query_one("SELECT * FROM fleet_nodes WHERE node_id = ?", (node_id,))
        return dict(row) if row else None

    def list_all(self) -> List[Dict[str, Any]]:
        return [dict(r) for r in self.db.query("SELECT * FROM fleet_nodes ORDER BY node_id")]

    def add(self, node_id: str, enroll_token_id: Optional[str]) -> None:
        """Enrolling again keeps the existing record (and any hold on it): a worker cannot clear its own hold by
        enrolling afresh."""
        with self.db.transaction() as conn:
            conn.execute(
                "INSERT INTO fleet_nodes (node_id, enrolled_at, enroll_token_id) VALUES (?, ?, ?) "
                "ON CONFLICT (node_id) DO NOTHING",
                (node_id, datetime.now(UTC).isoformat(), enroll_token_id),
            )

    def hold(self, node_id: str, reason: str) -> None:
        """Keep the worker out of leasing until it has been cleaned."""
        with self.db.transaction() as conn:
            conn.execute(
                "UPDATE fleet_nodes SET held_reason = ?, held_at = ?, sanitizing_since = NULL, sanitize_ref = NULL "
                "WHERE node_id = ?", (reason, datetime.now(UTC).isoformat(), node_id))

    def release_hold(self, node_id: str) -> None:
        with self.db.transaction() as conn:
            conn.execute(
                "UPDATE fleet_nodes SET held_reason = NULL, held_at = NULL, sanitizing_since = NULL, "
                "sanitize_ref = NULL WHERE node_id = ?", (node_id,))

    def begin_sanitize(self, node_id: str, ref: str) -> None:
        with self.db.transaction() as conn:
            conn.execute(
                "UPDATE fleet_nodes SET sanitizing_since = ?, sanitize_ref = ? WHERE node_id = ?",
                (datetime.now(UTC).isoformat(), ref, node_id))

    def clear_sanitize(self, node_id: str) -> None:
        with self.db.transaction() as conn:
            conn.execute(
                "UPDATE fleet_nodes SET sanitizing_since = NULL, sanitize_ref = NULL WHERE node_id = ?", (node_id,))
