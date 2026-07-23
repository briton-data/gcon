from __future__ import annotations

import sqlite3
from datetime import datetime, UTC
from typing import Any, Dict, List, Optional

from gcon.persistence.db import ControlPlaneDatabase


class HeartbeatRepository:
    """
    Append-only heartbeat log. `(node_id, sequence)` is UNIQUE, where
    `sequence` is a monotonic counter owned by the agent -- redelivery
    of the same heartbeat (an agent retrying after not seeing an ack
    from the coordinator during a brief reconnect) is a no-op rather
    than a duplicate row.
    """

    def __init__(self, db: ControlPlaneDatabase):
        self.db = db

    def record(
        self,
        node_id: str,
        sequence: int,
        status: str,
        cpu_percent: Optional[float] = None,
        memory_percent: Optional[float] = None,
        running_jobs: Optional[int] = None,
    ) -> bool:
        """Returns True if a new row was recorded, False if this was a
        duplicate (already-seen) sequence number for this node."""
        try:
            self.db.execute(
                """
                INSERT INTO heartbeats (
                    node_id, sequence, status, cpu_percent,
                    memory_percent, running_jobs, received_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    node_id,
                    sequence,
                    status,
                    cpu_percent,
                    memory_percent,
                    running_jobs,
                    datetime.now(UTC).isoformat(),
                ),
            )
            return True
        except sqlite3.IntegrityError:
            return False

    def latest_for_node(self, node_id: str) -> Optional[Dict[str, Any]]:
        row = self.db.query_one(
            "SELECT * FROM heartbeats WHERE node_id = ? ORDER BY sequence DESC LIMIT 1",
            (node_id,),
        )
        return dict(row) if row else None

    def last_sequence(self, node_id: str) -> int:
        row = self.db.query_one(
            "SELECT COALESCE(MAX(sequence), 0) AS m FROM heartbeats WHERE node_id = ?",
            (node_id,),
        )
        return row["m"] if row else 0

    def recent_for_node(self, node_id: str, limit: int = 100) -> List[Dict[str, Any]]:
        rows = self.db.query(
            "SELECT * FROM heartbeats WHERE node_id = ? ORDER BY sequence DESC LIMIT ?",
            (node_id, limit),
        )
        return [dict(r) for r in rows]
