from __future__ import annotations

import uuid
from datetime import datetime, UTC
from typing import Dict, List

from gcon.persistence.db import ControlPlaneDatabase


class NodeCapabilityRepository:
    """
    Key/value capability facts about a node (gpu_name, gpu_memory_total,
    cpu_cores, ...), reported at registration and refreshed over time.
    Normalized as rows rather than a JSON blob on `nodes` so individual
    capabilities can be indexed/queried and updated independently.
    """

    def __init__(self, db: ControlPlaneDatabase):
        self.db = db

    def set_capability(self, node_id: str, key: str, value: str) -> None:
        now = datetime.now(UTC).isoformat()
        existing = self.db.query_one(
            "SELECT capability_id FROM node_capabilities WHERE node_id = ? AND capability_key = ?",
            (node_id, key),
        )
        with self.db.transaction() as conn:
            if existing is None:
                conn.execute(
                    """
                    INSERT INTO node_capabilities
                        (capability_id, node_id, capability_key, capability_value, updated_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (uuid.uuid4().hex, node_id, key, value, now),
                )
            else:
                conn.execute(
                    "UPDATE node_capabilities SET capability_value = ?, updated_at = ? "
                    "WHERE capability_id = ?",
                    (value, now, existing["capability_id"]),
                )

    def set_capabilities(self, node_id: str, capabilities: Dict[str, str]) -> None:
        for key, value in capabilities.items():
            self.set_capability(node_id, key, str(value))

    def get_capabilities(self, node_id: str) -> Dict[str, str]:
        rows = self.db.query(
            "SELECT capability_key, capability_value FROM node_capabilities WHERE node_id = ?",
            (node_id,),
        )
        return {r["capability_key"]: r["capability_value"] for r in rows}

    def list_for_node(self, node_id: str) -> List[Dict]:
        rows = self.db.query(
            "SELECT * FROM node_capabilities WHERE node_id = ? ORDER BY capability_key",
            (node_id,),
        )
        return [dict(r) for r in rows]
