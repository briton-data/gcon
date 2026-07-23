from __future__ import annotations

import sqlite3
from datetime import datetime, UTC
from typing import Any, Dict, List, Optional

from gcon.persistence.db import ControlPlaneDatabase


class ExecutionLogRepository:
    """
    Durable store for streamed job log lines. `(attempt_id, stream,
    sequence)` is UNIQUE, where `sequence` is a monotonic per-attempt,
    per-stream counter owned by the agent -- this makes the log
    streaming RPC idempotent: if the agent resends a chunk after a
    reconnect because it never saw the ack, the duplicate is dropped
    instead of appearing twice in the log.
    """

    def __init__(self, db: ControlPlaneDatabase):
        self.db = db

    def append(
        self,
        job_id: str,
        attempt_id: Optional[str],
        node_id: Optional[str],
        stream: str,
        sequence: int,
        content: str,
    ) -> bool:
        """Returns True if the line was newly recorded, False if it was
        a duplicate delivery of an already-seen sequence number."""
        try:
            self.db.execute(
                """
                INSERT INTO execution_logs (
                    job_id, attempt_id, node_id, stream, sequence, content, logged_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    job_id,
                    attempt_id,
                    node_id,
                    stream,
                    sequence,
                    content,
                    datetime.now(UTC).isoformat(),
                ),
            )
            return True
        except sqlite3.IntegrityError:
            return False

    def for_attempt(self, attempt_id: str) -> List[Dict[str, Any]]:
        rows = self.db.query(
            "SELECT * FROM execution_logs WHERE attempt_id = ? ORDER BY stream, sequence",
            (attempt_id,),
        )
        return [dict(r) for r in rows]

    def for_job(self, job_id: str) -> List[Dict[str, Any]]:
        rows = self.db.query(
            "SELECT * FROM execution_logs WHERE job_id = ? ORDER BY id", (job_id,)
        )
        return [dict(r) for r in rows]

    def last_sequence(self, attempt_id: str, stream: str) -> int:
        row = self.db.query_one(
            "SELECT COALESCE(MAX(sequence), 0) AS m FROM execution_logs "
            "WHERE attempt_id = ? AND stream = ?",
            (attempt_id, stream),
        )
        return row["m"] if row else 0
