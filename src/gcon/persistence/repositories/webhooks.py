from __future__ import annotations

import json
import secrets
import uuid
from datetime import datetime, UTC
from typing import Any, Dict, List, Optional

from gcon.persistence.db import ControlPlaneDatabase


class WebhookRepository:
    """Durable store for org-level webhook subscriptions and every
    delivery attempt made against them (see
    `gcon.transport.webhooks.WebhookDispatcher`)."""

    def __init__(self, db: ControlPlaneDatabase):
        self.db = db

    # ------------------------------------------------------ subscriptions
    def create_subscription(
        self, org_id: str, url: str, event_types: List[str], secret: Optional[str] = None
    ) -> Dict[str, Any]:
        subscription_id = uuid.uuid4().hex
        secret = secret or secrets.token_hex(32)
        now = datetime.now(UTC).isoformat()
        self.db.execute(
            """
            INSERT INTO webhook_subscriptions
                (subscription_id, org_id, url, secret, event_types_json, active, created_at)
            VALUES (?, ?, ?, ?, ?, 1, ?)
            """,
            (subscription_id, org_id, url, secret, json.dumps(event_types), now),
        )
        return self.get_subscription(subscription_id)

    def get_subscription(self, subscription_id: str) -> Optional[Dict[str, Any]]:
        row = self.db.query_one(
            "SELECT * FROM webhook_subscriptions WHERE subscription_id = ?", (subscription_id,)
        )
        return self._sub_to_dict(row)

    def list_for_org(self, org_id: str, active_only: bool = True) -> List[Dict[str, Any]]:
        if active_only:
            rows = self.db.query(
                "SELECT * FROM webhook_subscriptions WHERE org_id = ? AND active = 1 ORDER BY created_at",
                (org_id,),
            )
        else:
            rows = self.db.query(
                "SELECT * FROM webhook_subscriptions WHERE org_id = ? ORDER BY created_at",
                (org_id,),
            )
        return [self._sub_to_dict(r) for r in rows]

    def deactivate(self, subscription_id: str) -> None:
        self.db.execute(
            "UPDATE webhook_subscriptions SET active = 0 WHERE subscription_id = ?",
            (subscription_id,),
        )

    @staticmethod
    def _sub_to_dict(row) -> Optional[Dict[str, Any]]:
        if row is None:
            return None
        d = dict(row)
        d["event_types"] = json.loads(d.pop("event_types_json"))
        d["active"] = bool(d["active"])
        return d

    # --------------------------------------------------------- deliveries
    def enqueue_delivery(
        self,
        event_type: str,
        payload: Dict[str, Any],
        url: str,
        secret: str,
        subscription_id: Optional[str] = None,
        org_id: Optional[str] = None,
        job_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """`subscription_id` is None for an ad-hoc, per-job
        `callback_url` delivery (see JobSubmitRequest.callback_url) --
        `url`/`secret` are then supplied directly instead of looked
        up from a standing subscription row."""
        delivery_id = uuid.uuid4().hex
        now = datetime.now(UTC).isoformat()
        self.db.execute(
            """
            INSERT INTO webhook_deliveries
                (delivery_id, subscription_id, org_id, url, secret, job_id, event_type,
                 payload_json, status, attempt_count, created_at, next_attempt_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', 0, ?, ?)
            """,
            (
                delivery_id, subscription_id, org_id, url, secret, job_id, event_type,
                json.dumps(payload), now, now,
            ),
        )
        return self.get_delivery(delivery_id)

    def get_delivery(self, delivery_id: str) -> Optional[Dict[str, Any]]:
        row = self.db.query_one(
            "SELECT * FROM webhook_deliveries WHERE delivery_id = ?", (delivery_id,)
        )
        return self._delivery_to_dict(row)

    def due_deliveries(self, now_iso: str, limit: int = 50) -> List[Dict[str, Any]]:
        """Pending or previously-failed deliveries whose next retry
        time has arrived, oldest first."""
        rows = self.db.query(
            """
            SELECT * FROM webhook_deliveries
            WHERE status IN ('pending', 'retrying') AND next_attempt_at <= ?
            ORDER BY next_attempt_at LIMIT ?
            """,
            (now_iso, limit),
        )
        return [self._delivery_to_dict(r) for r in rows]

    def record_attempt(
        self,
        delivery_id: str,
        success: bool,
        response_code: Optional[int],
        error: Optional[str],
        next_attempt_at: Optional[str],
        give_up: bool,
    ) -> None:
        if success:
            status = "success"
        elif give_up:
            status = "failed"
        else:
            status = "retrying"
        now = datetime.now(UTC).isoformat()
        self.db.execute(
            """
            UPDATE webhook_deliveries
            SET status = ?, attempt_count = attempt_count + 1, response_code = ?,
                last_error = ?, last_attempt_at = ?, next_attempt_at = ?
            WHERE delivery_id = ?
            """,
            (status, response_code, error, now, next_attempt_at, delivery_id),
        )

    def list_for_job(self, job_id: str) -> List[Dict[str, Any]]:
        rows = self.db.query(
            "SELECT * FROM webhook_deliveries WHERE job_id = ? ORDER BY created_at", (job_id,)
        )
        return [self._delivery_to_dict(r) for r in rows]

    @staticmethod
    def _delivery_to_dict(row) -> Optional[Dict[str, Any]]:
        if row is None:
            return None
        d = dict(row)
        d["payload"] = json.loads(d.pop("payload_json"))
        return d
