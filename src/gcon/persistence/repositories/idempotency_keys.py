"""
Durable (org_id, idempotency_key) -> job_id mapping for POST /jobs.
See migrations/registry.py's job_submission_idempotency_keys
migration for the full reasoning; this repository is deliberately
small (two operations) since that's the entire contract this needs.
"""
from typing import Any, Dict, Optional


class IdempotencyKeyRepository:
    def __init__(self, db):
        self.db = db

    @staticmethod
    def _norm_org_id(org_id: Optional[str]) -> str:
        # PRIMARY KEY (org_id, idempotency_key) can't hold NULL in
        # org_id sensibly across both SQLite and Postgres -- '' is the
        # "no org" sentinel, consistent for lookups either way.
        return org_id or ""

    def get_job_id(self, org_id: Optional[str], idempotency_key: str) -> Optional[str]:
        row = self.db.query_one(
            "SELECT job_id FROM job_submission_idempotency_keys "
            "WHERE org_id = ? AND idempotency_key = ?",
            (self._norm_org_id(org_id), idempotency_key),
        )
        return row["job_id"] if row else None

    def get(self, org_id: Optional[str], idempotency_key: str) -> Optional[Dict[str, Any]]:
        """The full stored mapping (job_id, request_hash, created_at), or None."""
        row = self.db.query_one(
            "SELECT job_id, request_hash, created_at FROM job_submission_idempotency_keys "
            "WHERE org_id = ? AND idempotency_key = ?",
            (self._norm_org_id(org_id), idempotency_key),
        )
        return dict(row) if row else None

    def record(
        self, org_id: Optional[str], idempotency_key: str, job_id: str, created_at: str,
        request_hash: Optional[str] = None,
    ) -> str:
        """
        Record that `idempotency_key` (scoped to `org_id`) resolves to
        `job_id`. Returns the job_id that's actually durably recorded
        for this key -- almost always `job_id` itself, but if a
        concurrent request with the same (org_id, idempotency_key)
        already won the race (same PRIMARY KEY -> IntegrityError here),
        this returns THAT job_id instead, same "lost the race, defer
        to the winner" pattern as JobAttemptRepository.record_attempt,
        so two concurrent identical submissions can never both create
        a job for the same key -- exactly the case this table exists
        to prevent, and the race an API-level idempotency key has to
        survive to be worth anything.
        """
        norm_org_id = self._norm_org_id(org_id)
        try:
            with self.db.transaction() as conn:
                conn.execute(
                    "INSERT INTO job_submission_idempotency_keys "
                    "(org_id, idempotency_key, job_id, created_at, request_hash) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (norm_org_id, idempotency_key, job_id, created_at, request_hash),
                )
        except self.db.IntegrityError:
            existing = self.get_job_id(norm_org_id, idempotency_key)
            if existing is not None:
                return existing
            raise
        return job_id
