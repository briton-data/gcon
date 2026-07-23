"""
NodeRepository — durable inventory of worker nodes known to the
control plane (this is the durable counterpart to the in-memory,
process-lifetime `NodeRegistry` in `gcon.cluster.Noderegistry`; that
registry still owns *live* scheduling state such as which object
holds the open transport channel, this repository owns the *durable*
record so a coordinator restart doesn't forget a node ever existed).
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, UTC
from typing import Any, Dict, Optional

from gcon.persistence.db import ControlPlaneDatabase


class NodeRepository:
    def __init__(self, db: ControlPlaneDatabase):
        self.db = db

    def upsert(
        self,
        node_id: str,
        hostname: str,
        status: str = "unknown",
        transport_endpoint: Optional[str] = None,
        agent_version: Optional[str] = None,
        auth_fingerprint: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Idempotent registration: safe to call every time an agent
        (re)connects, including after a coordinator restart or an
        agent reconnect following a network blip.
        """
        now = datetime.now(UTC).isoformat()
        existing = self.get(node_id)

        with self.db.transaction() as conn:
            if existing is None:
                conn.execute(
                    """
                    INSERT INTO nodes (
                        node_id, hostname, status, transport_endpoint,
                        agent_version, auth_fingerprint, registered_at,
                        last_seen_at, draining, metadata_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, ?)
                    """,
                    (
                        node_id,
                        hostname,
                        status,
                        transport_endpoint,
                        agent_version,
                        auth_fingerprint,
                        now,
                        now,
                        json.dumps(metadata or {}),
                    ),
                )
            else:
                conn.execute(
                    """
                    UPDATE nodes
                    SET hostname = ?, status = ?, transport_endpoint = ?,
                        agent_version = ?, auth_fingerprint = ?,
                        last_seen_at = ?, metadata_json = ?
                    WHERE node_id = ?
                    """,
                    (
                        hostname,
                        status,
                        transport_endpoint,
                        agent_version,
                        auth_fingerprint,
                        now,
                        json.dumps(metadata or {}),
                        node_id,
                    ),
                )

    def get(self, node_id: str) -> Optional[Dict[str, Any]]:
        row = self.db.query_one("SELECT * FROM nodes WHERE node_id = ?", (node_id,))
        return self._row_to_dict(row)

    def get_by_fingerprint(self, auth_fingerprint: str) -> Optional[Dict[str, Any]]:
        row = self.db.query_one(
            "SELECT * FROM nodes WHERE auth_fingerprint = ?", (auth_fingerprint,)
        )
        return self._row_to_dict(row)

    def list_all(self):
        rows = self.db.query("SELECT * FROM nodes ORDER BY registered_at")
        return [self._row_to_dict(r) for r in rows]

    def set_status(self, node_id: str, status: str) -> None:
        self.db.execute(
            "UPDATE nodes SET status = ?, last_seen_at = ? WHERE node_id = ?",
            (status, datetime.now(UTC).isoformat(), node_id),
        )

    def set_draining(self, node_id: str, draining: bool) -> None:
        self.db.execute(
            "UPDATE nodes SET draining = ? WHERE node_id = ?",
            (1 if draining else 0, node_id),
        )

    def touch_last_seen(self, node_id: str) -> None:
        self.db.execute(
            "UPDATE nodes SET last_seen_at = ? WHERE node_id = ?",
            (datetime.now(UTC).isoformat(), node_id),
        )

    def remove(self, node_id: str) -> None:
        self.db.execute("DELETE FROM nodes WHERE node_id = ?", (node_id,))

    @staticmethod
    def new_id() -> str:
        return uuid.uuid4().hex

    @staticmethod
    def _row_to_dict(row) -> Optional[Dict[str, Any]]:
        if row is None:
            return None
        d = dict(row)
        d["draining"] = bool(d["draining"])
        d["metadata"] = json.loads(d.pop("metadata_json") or "{}")
        return d
