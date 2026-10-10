"""
The workers GCON's operator has explicitly put in the shared pool.

Membership is a decision by a GCON Owner/Administrator, recorded here. It is
never inferred (a worker without an organization is not automatically pool),
and never declared by the worker itself.
"""
from datetime import UTC, datetime


class SharedPoolNodeRepository:
    def __init__(self, db):
        self.db = db

    def is_member(self, node_id: str) -> bool:
        return self.db.query_one(
            "SELECT 1 AS present FROM shared_pool_nodes WHERE node_id = ?", (node_id,)
        ) is not None

    def add(self, node_id: str, added_by: str) -> None:
        with self.db.transaction() as conn:
            conn.execute(
                "INSERT INTO shared_pool_nodes (node_id, added_by, added_at) VALUES (?, ?, ?) "
                "ON CONFLICT (node_id) DO NOTHING",
                (node_id, added_by, datetime.now(UTC).isoformat()),
            )

    def remove(self, node_id: str) -> None:
        with self.db.transaction() as conn:
            conn.execute("DELETE FROM shared_pool_nodes WHERE node_id = ?", (node_id,))

    def list_members(self):
        return [r["node_id"] for r in self.db.query(
            "SELECT node_id FROM shared_pool_nodes ORDER BY node_id")]
