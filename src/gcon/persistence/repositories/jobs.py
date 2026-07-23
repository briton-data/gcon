from __future__ import annotations

import json
from datetime import datetime, UTC
from typing import Any, Dict, List, Optional

from gcon.persistence.db import ControlPlaneDatabase


class JobRepository:
    def __init__(self, db: ControlPlaneDatabase):
        self.db = db

    def create(
        self,
        job_id: str,
        command: str,
        priority: int = 0,
        workflow_id: Optional[str] = None,
        created_by: Optional[str] = None,
        timeout_seconds: Optional[int] = None,
    ) -> None:
        self.db.execute(
            """
            INSERT INTO jobs (
                job_id, command, status, priority, workflow_id,
                created_by, timeout_seconds, submitted_at
            ) VALUES (?, ?, 'pending', ?, ?, ?, ?, ?)
            """,
            (
                job_id,
                command,
                priority,
                workflow_id,
                created_by,
                timeout_seconds,
                datetime.now(UTC).isoformat(),
            ),
        )

    def ensure_exists(
        self,
        job_id: str,
        command: str,
        priority: int = 0,
        workflow_id: Optional[str] = None,
        created_by: Optional[str] = None,
        timeout_seconds: Optional[int] = None,
    ) -> None:
        """
        Idempotent get-or-create. `job_attempts.job_id` is a foreign
        key into this table, but the coordinator's in-memory job
        objects (created by the scheduler, which this task does not
        touch) are never explicitly persisted here on submission --
        so the transport layer calls this immediately before
        recording a dispatch attempt, guaranteeing the referenced row
        exists no matter which subsystem created the job_id first.
        Safe to call for a job_id that already exists (no-op).
        """
        if self.get(job_id) is not None:
            return
        try:
            self.create(
                job_id, command, priority=priority, workflow_id=workflow_id,
                created_by=created_by, timeout_seconds=timeout_seconds,
            )
        except Exception:
            # Lost a race with a concurrent ensure_exists/create for the
            # same job_id -- fine as long as the row exists now.
            if self.get(job_id) is None:
                raise

    def set_status(
        self,
        job_id: str,
        status: str,
        result: Optional[Dict[str, Any]] = None,
        completed: bool = False,
    ) -> None:
        if completed:
            self.db.execute(
                "UPDATE jobs SET status = ?, result_json = ?, completed_at = ? WHERE job_id = ?",
                (
                    status,
                    json.dumps(result) if result is not None else None,
                    datetime.now(UTC).isoformat(),
                    job_id,
                ),
            )
        else:
            self.db.execute(
                "UPDATE jobs SET status = ? WHERE job_id = ?", (status, job_id)
            )

    def get(self, job_id: str) -> Optional[Dict[str, Any]]:
        row = self.db.query_one("SELECT * FROM jobs WHERE job_id = ?", (job_id,))
        return self._row_to_dict(row)

    def list_by_status(self, status: str) -> List[Dict[str, Any]]:
        rows = self.db.query(
            "SELECT * FROM jobs WHERE status = ? ORDER BY submitted_at", (status,)
        )
        return [self._row_to_dict(r) for r in rows]

    def list_all(self) -> List[Dict[str, Any]]:
        rows = self.db.query("SELECT * FROM jobs ORDER BY submitted_at")
        return [self._row_to_dict(r) for r in rows]

    @staticmethod
    def _row_to_dict(row) -> Optional[Dict[str, Any]]:
        if row is None:
            return None
        d = dict(row)
        d["result"] = json.loads(d.pop("result_json")) if d.get("result_json") else None
        return d
