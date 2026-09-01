from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import datetime, UTC
from typing import Any, Dict, List, Optional

from gcon.persistence.db import ControlPlaneDatabase


class ReceiptRepository:
    """
    Durable store for signed execution receipts uploaded by agents.
    `receipt_hash` is UNIQUE, so a receipt re-uploaded after a
    connection drop (before the agent got the ack) is a no-op rather
    than a duplicate row -- this is the idempotency guarantee for the
    receipt-upload RPC.
    """

    def __init__(self, db: ControlPlaneDatabase):
        self.db = db

    def upload(
        self,
        job_id: str,
        payload: Dict[str, Any],
        receipt_hash: str,
        attempt_id: Optional[str] = None,
        node_id: Optional[str] = None,
        signature: Optional[str] = None,
    ) -> Dict[str, Any]:
        existing = self.get_by_hash(receipt_hash)
        if existing is not None:
            return existing

        receipt_id = uuid.uuid4().hex
        try:
            self.db.execute(
                """
                INSERT INTO receipts (
                    receipt_id, job_id, attempt_id, node_id, receipt_hash,
                    signature, payload_json, uploaded_at, verified
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0)
                """,
                (
                    receipt_id,
                    job_id,
                    attempt_id,
                    node_id,
                    receipt_hash,
                    signature,
                    json.dumps(payload),
                    datetime.now(UTC).isoformat(),
                ),
            )
        except sqlite3.IntegrityError as e:
            # UNIQUE(receipt_hash) really can legitimately race (two
            # concurrent uploads of the identical receipt) -- but this
            # bare except used to swallow EVERY IntegrityError the same
            # way, including a genuine FK violation (job_id/node_id
            # referencing a row that doesn't exist), which silently
            # discarded a real receipt with zero trace: upload()
            # returned None, no exception surfaced, no log line -- the
            # caller (GCONCoordinator._run_job) had no way to know its
            # receipt was never actually persisted. Now: if a row with
            # this exact hash exists after the failed insert, it really
            # was the intended duplicate-race case (return it, as
            # before). If not, this was a different constraint
            # violation entirely -- log it loudly and re-raise, so it
            # reaches _run_job's own try/except and prints a [WARN]
            # instead of vanishing.
            existing = self.get_by_hash(receipt_hash)
            if existing is not None:
                return existing
            print(
                f"[RECEIPT PERSIST] INSERT failed for job '{job_id}' "
                f"(not a duplicate -- likely a missing FK row for "
                f"node_id={node_id!r} or job_id={job_id!r}): {e!r}"
            )
            raise

        return self.get_by_hash(receipt_hash)

    def mark_verified(self, receipt_id: str, verified: bool = True) -> None:
        self.db.execute(
            "UPDATE receipts SET verified = ? WHERE receipt_id = ?",
            (1 if verified else 0, receipt_id),
        )

    def mark_verified_by_job_id(self, job_id: str, verified: bool = True) -> bool:
        """Same as mark_verified, but keyed by job_id instead of
        receipt_id. Exists because the caller that actually knows a
        fresh verification result (Coordinator._commit_receipt_
        verification) only has the in-memory receipt's job_id to work
        with -- upload() mints its own receipt_id (a fresh UUID) at
        persist time, unrelated to anything the in-memory dict might
        carry, so job_id is the one key guaranteed to correlate the
        two. Returns False (no-op) if this job has no persisted
        receipt yet -- not an error, just means _run_job's own
        control_plane.receipts.upload() call for it hasn't landed (or
        never will, e.g. no control_plane at all)."""
        row = self.db.query_one(
            "SELECT receipt_id FROM receipts WHERE job_id = ? ORDER BY uploaded_at DESC LIMIT 1",
            (job_id,),
        )
        if row is None:
            return False
        self.mark_verified(row["receipt_id"], verified)
        return True

    def get_by_hash(self, receipt_hash: str) -> Optional[Dict[str, Any]]:
        row = self.db.query_one(
            "SELECT * FROM receipts WHERE receipt_hash = ?", (receipt_hash,)
        )
        return self._row_to_dict(row)

    def list_for_job(self, job_id: str) -> List[Dict[str, Any]]:
        rows = self.db.query(
            "SELECT * FROM receipts WHERE job_id = ? ORDER BY uploaded_at", (job_id,)
        )
        return [self._row_to_dict(r) for r in rows]

    def search_paginated(
        self, verified: bool = None, search: str = None, limit: int = 50, offset: int = 0
    ) -> tuple:
        """Real server-side pagination against the full durable
        receipt history -- see JobRepository.search_paginated's
        docstring, same reasoning applies here. `verified` filters
        against the DB column, which is only meaningful now that
        Coordinator._commit_receipt_verification actually writes to
        it (see migration 4's docstring) -- before that it was always
        the DEFAULT 0 regardless of a receipt's real verification
        state. `search` matches receipt_id or job_id via LIKE."""
        where = []
        params: List[Any] = []
        if verified is not None:
            where.append("verified = ?")
            params.append(1 if verified else 0)
        if search:
            where.append("(receipt_id LIKE ? OR job_id LIKE ?)")
            like = f"%{search}%"
            params.extend([like, like])
        where_sql = f"WHERE {' AND '.join(where)}" if where else ""

        count_row = self.db.query_one(f"SELECT COUNT(*) AS n FROM receipts {where_sql}", tuple(params))
        total = count_row["n"] if count_row else 0

        rows = self.db.query(
            f"SELECT * FROM receipts {where_sql} ORDER BY uploaded_at DESC LIMIT ? OFFSET ?",
            tuple(params) + (limit, offset),
        )
        return [self._row_to_dict(r) for r in rows], total

    def count_by_verified(self) -> Dict[str, int]:
        """One grouped COUNT query for the Receipts tab's (and the
        websocket bootstrap payload's) verified/unverified summary
        tiles -- replaces PresentationLayer.get_receipts_summary's
        previous approach of calling Coordinator.get_receipts() (an
        O(total receipts) full-list build) just to sum() a boolean
        over it, on every dashboard refresh tick regardless of how
        much history exists."""
        rows = self.db.query("SELECT verified, COUNT(*) AS n FROM receipts GROUP BY verified")
        verified = sum(r["n"] for r in rows if r["verified"])
        total = sum(r["n"] for r in rows)
        return {"total": total, "verified": verified, "unverified": total - verified}

    def list_all(self) -> List[Dict[str, Any]]:
        """Every persisted receipt, oldest first. Used to rehydrate the
        coordinator's receipt view on startup (see
        GCONCoordinator.restore_from_persistence)."""
        rows = self.db.query("SELECT * FROM receipts ORDER BY uploaded_at")
        return [self._row_to_dict(r) for r in rows]

    def list_recent(self, limit: int) -> List[Dict[str, Any]]:
        """The most recent `limit` receipts, newest first -- bounded
        alternative to list_all() used by restore_from_persistence.
        See jobs.py's list_recent_terminal for why: a receipt, unlike
        a job, has no in-flight state to reconcile, so there's no
        subset that must always be loaded regardless of the cap."""
        rows = self.db.query(
            "SELECT * FROM receipts ORDER BY uploaded_at DESC LIMIT ?", (limit,)
        )
        return [self._row_to_dict(r) for r in rows]

    def count_all(self) -> int:
        row = self.db.query_one("SELECT COUNT(*) AS n FROM receipts")
        return row["n"] if row else 0

    def purge_older_than(self, cutoff_iso: str) -> int:
        cursor = self.db.execute(
            "DELETE FROM receipts WHERE uploaded_at < ?", (cutoff_iso,)
        )
        return cursor.rowcount

    def purge_keep_newest(self, keep: int) -> int:
        cursor = self.db.execute(
            """
            DELETE FROM receipts WHERE receipt_id IN (
                SELECT receipt_id FROM receipts ORDER BY uploaded_at DESC LIMIT -1 OFFSET ?
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
        d["payload"] = json.loads(d.pop("payload_json"))
        d["verified"] = bool(d["verified"])
        return d