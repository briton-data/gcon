from __future__ import annotations

import json
from datetime import datetime, UTC
from typing import Any, Dict, List, Optional

from gcon.persistence.db import ControlPlaneDatabase


class ClusterEventRepository:
    """
    Durable, append-only audit trail of cluster-level events
    (node registered/disconnected/reconnected, job dispatched,
    receipt verification failed, ...). This is the persisted
    counterpart to the in-memory `EventBus` (`gcon.events.event_bus`)
    -- the event bus is for live, in-process pub/sub to subscribers
    such as the dashboard; this table is for history that must
    survive a coordinator restart.
    """

    def __init__(self, db: ControlPlaneDatabase):
        self.db = db

    def record(
        self,
        event_type: str,
        node_id: Optional[str] = None,
        job_id: Optional[str] = None,
        payload: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.db.execute(
            """
            INSERT INTO cluster_events (event_type, node_id, job_id, payload_json, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                event_type,
                node_id,
                job_id,
                json.dumps(payload) if payload is not None else None,
                datetime.now(UTC).isoformat(),
            ),
        )

    def recent(self, limit: int = 100) -> List[Dict[str, Any]]:
        rows = self.db.query(
            "SELECT * FROM cluster_events ORDER BY id DESC LIMIT ?", (limit,)
        )
        return [self._row_to_dict(r) for r in rows]

    def for_node(self, node_id: str, limit: int = 100) -> List[Dict[str, Any]]:
        rows = self.db.query(
            "SELECT * FROM cluster_events WHERE node_id = ? ORDER BY id DESC LIMIT ?",
            (node_id, limit),
        )
        return [self._row_to_dict(r) for r in rows]

    @staticmethod
    def _row_to_dict(row) -> Dict[str, Any]:
        d = dict(row)
        d["payload"] = json.loads(d.pop("payload_json")) if d.get("payload_json") else None
        return d
