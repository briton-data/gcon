"""
NodeEnrollmentAuditRepository -- durable "who/where brought this
worker online" trail for the Enroll RPC (see
transport/grpc_transport.py's Enroll() handler).

Before this repository existed, an enrollment's presented token and
real source IP (context.peer()) were only ever written to a single
log.info() line -- useful for the minute you're tailing logs, gone
once that line rotates out. This repository makes that a durable,
queryable record instead: one row per enrollment *attempt* (accepted
or rejected), so an operator investigating "which credential and
which IP actually registered node X" (or "who's been trying invalid
tokens against us") has a real answer.

See migrations/registry.py version 7 for why this is its own table
rather than new columns on `nodes` or a row in `cluster_events`/
`telemetry_events`: both of those FK-reference nodes(node_id), which
doesn't exist yet at Enroll() time -- a node's row is only created
later, at Register().
"""

from __future__ import annotations

from datetime import datetime, UTC
from typing import Any, Dict, List, Optional

from gcon.persistence.db import ControlPlaneDatabase


class NodeEnrollmentAuditRepository:
    def __init__(self, db: ControlPlaneDatabase):
        self.db = db

    def record(
        self,
        node_id: str,
        accepted: bool,
        org_id: Optional[str] = None,
        enroll_token_id: Optional[str] = None,
        source_ip: Optional[str] = None,
        reason: Optional[str] = None,
    ) -> None:
        self.db.execute(
            """
            INSERT INTO node_enrollment_audit
                (node_id, org_id, enroll_token_id, source_ip, accepted, reason, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                node_id, org_id, enroll_token_id, source_ip,
                1 if accepted else 0, reason, datetime.now(UTC).isoformat(),
            ),
        )

    def get_latest_for_node(self, node_id: str) -> Optional[Dict[str, Any]]:
        """The one call the dashboard actually needs: the most recent
        *accepted* enrollment for this node_id (a node's current
        identity), not its full history of rejected attempts under
        that same claimed node_id from anyone who tried."""
        row = self.db.query_one(
            """
            SELECT * FROM node_enrollment_audit
            WHERE node_id = ? AND accepted = 1
            ORDER BY created_at DESC LIMIT 1
            """,
            (node_id,),
        )
        return dict(row) if row else None

    def list_for_node(self, node_id: str) -> List[Dict[str, Any]]:
        rows = self.db.query(
            "SELECT * FROM node_enrollment_audit WHERE node_id = ? ORDER BY created_at DESC",
            (node_id,),
        )
        return [dict(r) for r in rows]
