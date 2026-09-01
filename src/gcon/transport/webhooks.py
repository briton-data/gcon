"""
WebhookDispatcher — outbound delivery for job-completion callbacks.

Two independent ways to receive a callback, both landing in the same
`webhook_deliveries` queue/retry machinery:

  1. Per-job, ad-hoc: `POST /jobs` with `callback_url` set (see
     `api_v1.JobSubmitRequest.callback_url` / `jobs.callback_url`
     column). No standing registration needed -- the common case for
     "notify me when this one batch finishes". Signed with a
     per-delivery random secret (nothing durable to leak).
  2. Standing, org-level: `webhook_subscriptions`
     (`ControlPlane.webhooks.create_subscription`), matched by
     `event_types`, for an org that wants every job (or every
     receipt-verification-failure, etc.) pushed to one endpoint
     without repeating `callback_url` on every submission.

Delivery is asynchronous and non-blocking: `dispatch_job_event` only
enqueues a `webhook_deliveries` row (a fast DB write) from the
`_run_job` worker thread; the actual HTTP POST happens on
`WebhookDispatcher`'s own background thread, polling
`due_deliveries()`. A slow or dead endpoint therefore can never stall
job execution or the health-check tick, and a coordinator restart
picks up any deliveries still `pending`/`retrying` from the DB (they
survive a crash the same way jobs/receipts do).

Signing
--------
Every POST carries `X-GCON-Signature: sha256=<hex hmac>` over the
raw JSON body, keyed by the subscription's (or ad-hoc delivery's)
secret -- the same "prove you hold the shared secret, not just that
you can produce a plausible-looking payload" primitive `verifier.py`
already uses for receipts. `X-GCON-Event` carries the event type,
`X-GCON-Delivery` the delivery_id, so a receiving endpoint can
de-duplicate retried attempts by delivery_id.

Retries
--------
Exponential backoff, capped attempts (`GCON_WEBHOOK_MAX_ATTEMPTS`,
default 6 -- roughly 1m/2m/4m/8m/16m/32m by default schedule), then
marked `failed` for good (surfaced via NODE... no, via
`WEBHOOK_DELIVERY_FAILED`, see coordinator wiring). A 2xx response is
the only success condition; anything else (including a connection
error/timeout) is a retry candidate.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
import threading
import time
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timedelta, UTC
from typing import Any, Dict, Optional

from gcon.persistence.control_plane import ControlPlane

logger = logging.getLogger(__name__)


def sign_payload(secret: str, body: bytes) -> str:
    return "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()


def dispatch_job_event(
    control_plane: ControlPlane, job: Dict[str, Any], event_type: str
) -> None:
    """Enqueues delivery to whichever of the two mechanisms above
    apply to this job -- a no-op (no DB write) if the job has no
    `callback_url` and its org has no matching subscription. Cheap
    and synchronous; safe to call directly from `_run_job`'s terminal
    path."""
    job_id = job.get("job_id") or job.get("id")
    org_id = job.get("org_id")
    payload = {
        "event": event_type,
        "job_id": job_id,
        "status": job.get("status"),
        "org_id": org_id,
        "completed_at": job.get("completed_at"),
        "result": _redact_result(job.get("result")),
    }

    callback_url = job.get("callback_url")
    if callback_url:
        secret = uuid.uuid4().hex  # ad-hoc: only needs to exist for this delivery's signature
        control_plane.webhooks.enqueue_delivery(
            event_type=event_type, payload=payload, url=callback_url,
            secret=secret, subscription_id=None, org_id=org_id, job_id=job_id,
        )

    if org_id:
        for sub in control_plane.webhooks.list_for_org(org_id, active_only=True):
            if event_type in sub["event_types"]:
                control_plane.webhooks.enqueue_delivery(
                    event_type=event_type, payload=payload, url=sub["url"],
                    secret=sub["secret"], subscription_id=sub["subscription_id"],
                    org_id=org_id, job_id=job_id,
                )


def _redact_result(result: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Job stdout can run to 8KB (see coordinator.py's output capture)
    -- too large and not useful for a webhook body. Keep the
    small/structured parts, drop raw output; the receiving side can
    always fetch the full job via the API using job_id."""
    if not isinstance(result, dict):
        return None
    return {
        "status": result.get("status"),
        "runtime_seconds": result.get("runtime_seconds"),
        "error": result.get("error") or result.get("message"),
    }


class WebhookDispatcher:
    def __init__(self, control_plane: ControlPlane, event_bus=None, poll_interval: float = 2.0):
        self.control_plane = control_plane
        self.event_bus = event_bus
        self.poll_interval = poll_interval
        self.max_attempts = int(os.environ.get("GCON_WEBHOOK_MAX_ATTEMPTS", "6"))
        self.timeout_seconds = float(os.environ.get("GCON_WEBHOOK_TIMEOUT_SECONDS", "10"))
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=5)

    def _loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                self.drain_due()
            except Exception as e:  # never let a delivery bug kill the loop
                logger.warning("webhook dispatch tick failed: %r", e)
            self._stop_event.wait(self.poll_interval)

    def drain_due(self) -> int:
        now_iso = datetime.now(UTC).isoformat()
        due = self.control_plane.webhooks.due_deliveries(now_iso, limit=50)
        for delivery in due:
            self._attempt(delivery)
        return len(due)

    def _attempt(self, delivery: Dict[str, Any]) -> None:
        body = json.dumps(delivery["payload"]).encode()
        signature = sign_payload(delivery["secret"], body)
        req = urllib.request.Request(
            delivery["url"], data=body, method="POST",
            headers={
                "Content-Type": "application/json",
                "X-GCON-Signature": signature,
                "X-GCON-Event": delivery["event_type"],
                "X-GCON-Delivery": delivery["delivery_id"],
            },
        )
        error: Optional[str] = None
        response_code: Optional[int] = None
        success = False
        try:
            with urllib.request.urlopen(req, timeout=self.timeout_seconds) as resp:
                response_code = resp.status
                success = 200 <= resp.status < 300
                if not success:
                    error = f"non-2xx response: {resp.status}"
        except urllib.error.HTTPError as e:
            response_code = e.code
            error = f"HTTP {e.code}: {e.reason}"
        except Exception as e:
            error = repr(e)

        attempt_number = delivery["attempt_count"] + 1
        give_up = (not success) and attempt_number >= self.max_attempts
        next_attempt_at = None
        if not success and not give_up:
            backoff_seconds = min(60 * 30, 60 * (2 ** (attempt_number - 1)))
            next_attempt_at = (
                datetime.now(UTC) + timedelta(seconds=backoff_seconds)
            ).isoformat()

        self.control_plane.webhooks.record_attempt(
            delivery["delivery_id"], success=success, response_code=response_code,
            error=error, next_attempt_at=next_attempt_at, give_up=give_up,
        )

        if give_up and self.event_bus is not None:
            from gcon.events.event import Event
            from gcon.events.event_types import EventType
            self.event_bus.publish(Event(
                event_type=EventType.WEBHOOK_DELIVERY_FAILED,
                source="WebhookDispatcher",
                payload={
                    "delivery_id": delivery["delivery_id"], "url": delivery["url"],
                    "attempt_count": attempt_number, "error": error,
                    "job_id": delivery.get("job_id"),
                },
            ))
