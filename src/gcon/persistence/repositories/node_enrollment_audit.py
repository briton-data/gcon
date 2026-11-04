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

import os
import threading
from datetime import datetime, UTC
from typing import Any, Dict, List, Optional

from gcon.persistence.db import ControlPlaneDatabase


DEFAULT_REJECTED_KEEP = 10_000


class NodeEnrollmentAuditRepository:
    """
    Rejected attempts are bounded: anyone who can reach the enroll port can generate them, so they are kept only up
    to the newest GCON_ENROLL_AUDIT_REJECTED_KEEP rows (default 10000; 0 keeps them all). ACCEPTED enrollments, the
    record of who brought a node online and under which organization, are never removed here.
    """

    _prune_every = 200          # rejected inserts between prunes; a prune is one bounded DELETE

    def __init__(self, db: ControlPlaneDatabase):
        self.db = db
        self._rejected_since_prune = 0
        self._prune_lock = threading.Lock()

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
        if not accepted:
            with self._prune_lock:
                self._rejected_since_prune += 1
                due = self._rejected_since_prune >= self._prune_every
                if due:
                    self._rejected_since_prune = 0
            if due:
                self.prune_rejected()

    def prune_rejected(self, keep: Optional[int] = None) -> None:
        """Delete rejected attempts beyond the newest `keep` (default from the environment). Accepted rows are untouched."""
        if keep is None:
            try:
                keep = int(os.environ.get("GCON_ENROLL_AUDIT_REJECTED_KEEP", DEFAULT_REJECTED_KEEP))
            except ValueError:
                keep = DEFAULT_REJECTED_KEEP
        if keep <= 0:
            return
        self.db.execute(
            """
            DELETE FROM node_enrollment_audit
            WHERE accepted = 0 AND id NOT IN (
                SELECT id FROM node_enrollment_audit WHERE accepted = 0 ORDER BY id DESC LIMIT ?
            )
            """,
            (keep,),
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
