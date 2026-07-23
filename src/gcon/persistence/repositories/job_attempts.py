"""
JobAttemptRepository — one row per dispatch of a job to a node.

`request_message_id` is the idempotency key carried on the wire (see
`gcon.transport.idempotency`): the coordinator generates one UUID per
*intended* dispatch, and if a reconnect or retry causes the same
dispatch to be sent twice, `record_attempt` recognizes the duplicate
and returns the original attempt instead of creating a second one or
raising — this is what makes job dispatch idempotent end to end, not
just at the transport layer.
"""

from __future__ import annotations

import sqlite3
import uuid
from datetime import datetime, UTC
from typing import Any, Dict, List, Optional

from gcon.persistence.db import ControlPlaneDatabase


class JobAttemptRepository:
    def __init__(self, db: ControlPlaneDatabase):
        self.db = db

    def record_attempt(
        self,
        job_id: str,
        node_id: Optional[str],
        request_message_id: str,
    ) -> Dict[str, Any]:
        """
        Idempotently record that a dispatch attempt was made. Returns
        the (possibly pre-existing) attempt row. Safe to call more
        than once with the same `request_message_id`.
        """
        existing = self.get_by_request_id(request_message_id)
        if existing is not None:
            return existing

        attempt_id = uuid.uuid4().hex
        now = datetime.now(UTC).isoformat()

        with self.db.transaction() as conn:
            next_attempt_number = 1 + (
                conn.execute(
                    "SELECT COALESCE(MAX(attempt_number), 0) AS m FROM job_attempts WHERE job_id = ?",
                    (job_id,),
                ).fetchone()["m"]
            )
            try:
                conn.execute(
                    """
                    INSERT INTO job_attempts (
                        attempt_id, job_id, node_id, attempt_number,
                        status, request_message_id, dispatched_at
                    ) VALUES (?, ?, ?, ?, 'dispatched', ?, ?)
                    """,
                    (
                        attempt_id,
                        job_id,
                        node_id,
                        next_attempt_number,
                        request_message_id,
                        now,
                    ),
                )
            except sqlite3.IntegrityError:
                # Lost a race with a concurrent duplicate dispatch of the
                # same request_message_id -- fall through and read back
                # whichever row actually won.
                pass

        return self.get_by_request_id(request_message_id)

    def set_status(
        self,
        attempt_id: str,
        status: str,
        error: Optional[str] = None,
        completed: bool = False,
    ) -> None:
        if completed:
            self.db.execute(
                "UPDATE job_attempts SET status = ?, error = ?, completed_at = ? "
                "WHERE attempt_id = ?",
                (status, error, datetime.now(UTC).isoformat(), attempt_id),
            )
        else:
            self.db.execute(
                "UPDATE job_attempts SET status = ?, error = ? WHERE attempt_id = ?",
                (status, error, attempt_id),
            )

    def get(self, attempt_id: str) -> Optional[Dict[str, Any]]:
        row = self.db.query_one(
            "SELECT * FROM job_attempts WHERE attempt_id = ?", (attempt_id,)
        )
        return dict(row) if row else None

    def get_by_request_id(self, request_message_id: str) -> Optional[Dict[str, Any]]:
        row = self.db.query_one(
            "SELECT * FROM job_attempts WHERE request_message_id = ?",
            (request_message_id,),
        )
        return dict(row) if row else None

    def list_for_job(self, job_id: str) -> List[Dict[str, Any]]:
        rows = self.db.query(
            "SELECT * FROM job_attempts WHERE job_id = ? ORDER BY attempt_number",
            (job_id,),
        )
        return [dict(r) for r in rows]
