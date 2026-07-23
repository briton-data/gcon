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
        except sqlite3.IntegrityError:
            pass  # lost race with a concurrent duplicate upload

        return self.get_by_hash(receipt_hash)

    def mark_verified(self, receipt_id: str, verified: bool = True) -> None:
        self.db.execute(
            "UPDATE receipts SET verified = ? WHERE receipt_id = ?",
            (1 if verified else 0, receipt_id),
        )

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

    @staticmethod
    def _row_to_dict(row) -> Optional[Dict[str, Any]]:
        if row is None:
            return None
        d = dict(row)
        d["payload"] = json.loads(d.pop("payload_json"))
        d["verified"] = bool(d["verified"])
        return d
