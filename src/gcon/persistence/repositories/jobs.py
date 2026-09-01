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
        org_id: Optional[str] = None,
    ) -> None:
        self.db.execute(
            """
            INSERT INTO jobs (
                job_id, command, status, priority, workflow_id,
                created_by, timeout_seconds, submitted_at, org_id
            ) VALUES (?, ?, 'pending', ?, ?, ?, ?, ?, ?)
            """,
            (
                job_id,
                command,
                priority,
                workflow_id,
                created_by,
                timeout_seconds,
                datetime.now(UTC).isoformat(),
                org_id,
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
        org_id: Optional[str] = None,
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
                org_id=org_id,
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

    def search_paginated(
        self, status: str = None, search: str = None, org_id: str = None,
        limit: int = 50, offset: int = 0,
    ) -> tuple:
        """
        Real server-side pagination against the full durable history --
        not the coordinator's bounded in-memory job set (see
        GCONCoordinator.get_jobs_page for why this exists: at real
        throughput, "fetch everything and filter/paginate in the
        browser" is the direct cause of an unusably huge payload and a
        hung tab, whether or not the DOM itself is paginated).

        `search` matches job_id or command via a substring LIKE (case-
        sensitive on the default SQLite build -- fine for the job-ID/
        command-fragment lookups this powers; a case-insensitive full-
        text search would need FTS5, real infra work not justified by
        this dashboard's own search box). `org_id`, `status`, and
        `search` all compose freely (each just ANDs another WHERE
        clause) -- e.g. the Executions tab's company filter and search
        box together stay on this one indexed query path rather than
        falling back to an in-memory scan. Returns (rows, total_count)
        where total_count is the *filtered* count (before limit/
        offset), for computing page numbers.
        """
        where = []
        params: List[Any] = []
        if status:
            where.append("status = ?")
            params.append(status)
        if org_id:
            where.append("org_id = ?")
            params.append(org_id)
        if search:
            where.append("(job_id LIKE ? OR command LIKE ?)")
            like = f"%{search}%"
            params.extend([like, like])
        where_sql = f"WHERE {' AND '.join(where)}" if where else ""

        count_row = self.db.query_one(f"SELECT COUNT(*) AS n FROM jobs {where_sql}", tuple(params))
        total = count_row["n"] if count_row else 0

        rows = self.db.query(
            f"SELECT * FROM jobs {where_sql} ORDER BY submitted_at DESC LIMIT ? OFFSET ?",
            tuple(params) + (limit, offset),
        )
        return [self._row_to_dict(r) for r in rows], total

    def count_by_status(self) -> Dict[str, int]:
        """One grouped COUNT query for the Executions tab's summary
        tiles (total/queued/running/completed/failed) -- replaces
        computing these by fetching and filtering the entire job list
        client- or server-side, which is exactly the O(total jobs)
        cost this whole pagination pass exists to remove."""
        rows = self.db.query("SELECT status, COUNT(*) AS n FROM jobs GROUP BY status")
        return {r["status"]: r["n"] for r in rows}

    def delete_by_status(self, status: str) -> int:
        """Permanently deletes every job in `status` from the durable
        store. This is what "Clear Failed" actually needs and
        previously didn't have: GCONCoordinator.clear_failed_jobs()
        only ever removed jobs from its own bounded in-memory dict --
        anything beyond that bound (or already evicted from memory)
        stayed in this table untouched, and restore_from_persistence()
        would load it right back on the next restart. Never touches
        pending/running jobs -- same "never delete anything in flight"
        rule as purge_terminal_older_than, just triggered by an
        explicit action instead of an age/count threshold. Returns
        the number of rows removed."""
        if status in ("pending", "running"):
            raise ValueError(f"refusing to delete '{status}' jobs -- they are not terminal")
        cursor = self.db.execute("DELETE FROM jobs WHERE status = ?", (status,))
        return cursor.rowcount

    def list_all(self) -> List[Dict[str, Any]]:
        rows = self.db.query("SELECT * FROM jobs ORDER BY submitted_at")
        return [self._row_to_dict(r) for r in rows]

    def list_non_terminal(self) -> List[Dict[str, Any]]:
        """Every job not yet in a terminal state (pending/running) --
        unbounded, deliberately: these need reconciliation on restart
        (see GCONCoordinator.restore_from_persistence) no matter how
        many there are, so bounding this the way list_recent_terminal
        bounds finished jobs would silently drop in-flight jobs from
        recovery. In practice this set stays small on its own -- it
        can never exceed however many jobs are actually
        pending/dispatched at once, which is bounded by cluster size,
        not history."""
        rows = self.db.query(
            "SELECT * FROM jobs WHERE status IN ('pending', 'running') ORDER BY submitted_at"
        )
        return [self._row_to_dict(r) for r in rows]

    def list_recent_terminal(self, limit: int) -> List[Dict[str, Any]]:
        """The most recent `limit` finished jobs (completed/failed/
        cancelled), newest first. Used instead of list_all() by
        restore_from_persistence so a coordinator restart never has
        to load a table that's grown across months of operation just
        to repopulate the dashboard's recent-history view -- see this
        module's retention.py for the complementary DB-side purge
        that keeps the table itself from growing unbounded in the
        first place."""
        rows = self.db.query(
            """
            SELECT * FROM jobs WHERE status NOT IN ('pending', 'running')
            ORDER BY submitted_at DESC LIMIT ?
            """,
            (limit,),
        )
        return [self._row_to_dict(r) for r in rows]

    def count_terminal(self) -> int:
        row = self.db.query_one(
            "SELECT COUNT(*) AS n FROM jobs WHERE status NOT IN ('pending', 'running')"
        )
        return row["n"] if row else 0

    def purge_terminal_older_than(self, cutoff_iso: str) -> int:
        """Physically deletes terminal jobs whose completed_at (or
        submitted_at, for the rare terminal job that never got a
        completed_at) is older than `cutoff_iso`. Returns the number
        of rows removed. Never touches pending/running jobs regardless
        of age -- an old but still-in-flight job is a bug to
        investigate, not something retention should make disappear."""
        cursor = self.db.execute(
            """
            DELETE FROM jobs
            WHERE status NOT IN ('pending', 'running')
              AND COALESCE(completed_at, submitted_at) < ?
            """,
            (cutoff_iso,),
        )
        return cursor.rowcount

    def purge_terminal_keep_newest(self, keep: int) -> int:
        """Deletes terminal jobs beyond the newest `keep`, by
        submitted_at. Row-count-based retention, for a deployment that
        cares about "keep the DB under X rows" more than "keep Y days
        of history"."""
        cursor = self.db.execute(
            """
            DELETE FROM jobs WHERE job_id IN (
                SELECT job_id FROM jobs WHERE status NOT IN ('pending', 'running')
                ORDER BY submitted_at DESC LIMIT -1 OFFSET ?
            )
            """,
            (keep,),
        )
        return cursor.rowcount

    @staticmethod
    def _row_to_dict(row) -> Optional[Dict[str, Any]]:
        if row is None:
            return None
        d = dict(row)
        d["result"] = json.loads(d.pop("result_json")) if d.get("result_json") else None
        return d
