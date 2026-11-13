"""
Stripe webhook handling: proving a request really came from Stripe, and turning its events into invoice status.

Stripe calls POST /api/v1/webhooks/stripe when something happens to an invoice GCON issued. Nothing in a request
can be trusted until `verify_signature` has checked it: the endpoint is public, so the signature is the only thing
separating Stripe from anyone who knows the URL.

`apply_event` is idempotent. Stripe delivers events at least once and sometimes out of order, so every handler
looks at the invoice's CURRENT status and only moves it forward; a repeated or late event changes nothing.
"""

from __future__ import annotations

import hashlib
import hmac
import logging
import time
from typing import Any, Dict, Optional

_log = logging.getLogger(__name__)

# How far a signed timestamp may be from now. Stripe's own libraries default to five minutes; a captured request
# cannot be replayed after that.
SIGNATURE_TOLERANCE_SECONDS = 300


class WebhookSignatureError(ValueError):
    """The request was not signed by the configured Stripe webhook secret (or is too old)."""


class WebhookNotConfigured(RuntimeError):
    """No GCON_STRIPE_WEBHOOK_SECRET is set, so no request can be verified."""


def verify_signature(payload: bytes, header: str, secret: str,
                     tolerance: int = SIGNATURE_TOLERANCE_SECONDS, now: Optional[float] = None) -> None:
    """Raise WebhookSignatureError unless `header` (the Stripe-Signature header, `t=...,v1=...`) is a valid signature
    of exactly these bytes made with `secret`. Stripe may send several v1 values while a secret is being rolled;
    any one matching is enough."""
    if not secret:
        raise WebhookNotConfigured("GCON_STRIPE_WEBHOOK_SECRET is not set.")
    timestamp: Optional[str] = None
    candidates = []
    for part in (header or "").split(","):
        key, _, value = part.strip().partition("=")
        if key == "t":
            timestamp = value
        elif key == "v1" and value:
            candidates.append(value)
    if not timestamp or not candidates:
        raise WebhookSignatureError("Missing or malformed Stripe-Signature header.")
    try:
        signed_at = int(timestamp)
    except ValueError:
        raise WebhookSignatureError("Malformed timestamp in Stripe-Signature.") from None
    expected = hmac.new(secret.encode("utf-8"), f"{timestamp}.".encode("utf-8") + payload, hashlib.sha256).hexdigest()
    if not any(hmac.compare_digest(expected, c) for c in candidates):
        raise WebhookSignatureError("Signature does not match.")
    if abs((time.time() if now is None else now) - signed_at) > tolerance:
        raise WebhookSignatureError("Signature timestamp is outside the allowed window.")


def _find_invoice(control_plane, stripe_invoice: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    gcon_id = (stripe_invoice.get("metadata") or {}).get("gcon_invoice_id")
    if gcon_id:
        found = control_plane.invoices.get(gcon_id)
        if found is not None:
            return found
    if stripe_invoice.get("id"):
        return control_plane.invoices.get_by_provider_ref("stripe", stripe_invoice["id"])
    return None


def apply_event(control_plane, event: Dict[str, Any]) -> Dict[str, Any]:
    """Apply one verified Stripe event. Returns {"outcome": ..., "invoice_id": ... or None}; `outcome` says what
    happened ('paid', 'duplicate', 'ignored', 'unknown_invoice', ...). Only a few outcomes need a person to look
    (they are logged at warning level): 'paid_after_void', 'total_mismatch', 'invoice_mismatch'."""
    etype = event.get("type", "")
    obj = (event.get("data") or {}).get("object") or {}
    if etype not in ("invoice.paid", "invoice.payment_succeeded", "invoice.payment_failed", "invoice.voided"):
        return {"outcome": "ignored", "invoice_id": None}
    invoice = _find_invoice(control_plane, obj)
    if invoice is None:
        # Not one of ours (the Stripe account may serve other things). 200 so Stripe does not retry forever.
        return {"outcome": "unknown_invoice", "invoice_id": None}
    iid = invoice["invoice_id"]
    status = invoice["status"]
    if invoice.get("provider") != "stripe" or invoice.get("provider_charge_id") != obj.get("id"):
        _log.warning("Stripe event %s for %s does not match the Stripe invoice recorded on it (%s vs %s)",
                     event.get("id"), iid, obj.get("id"), invoice.get("provider_charge_id"))
        return {"outcome": "invoice_mismatch", "invoice_id": iid}

    if etype in ("invoice.paid", "invoice.payment_succeeded"):
        if status == "paid":
            return {"outcome": "duplicate", "invoice_id": iid}
        if status == "void":
            _log.warning("Stripe says invoice %s was paid but GCON has it void: needs a person.", iid)
            return {"outcome": "paid_after_void", "invoice_id": iid}
        if status not in ("open", "failed"):
            return {"outcome": "not_issued", "invoice_id": iid}
        if obj.get("total") is not None and int(obj["total"]) != int(invoice["amount_cents"]):
            _log.warning("Stripe invoice %s was paid for a total of %s but GCON invoice %s is %s: not marked paid.",
                         obj.get("id"), obj.get("total"), iid, invoice["amount_cents"])
            return {"outcome": "total_mismatch", "invoice_id": iid}
        control_plane.invoices.mark_status(iid, "paid", provider="stripe", provider_charge_id=obj.get("id"))
        return {"outcome": "paid", "invoice_id": iid}

    if etype == "invoice.payment_failed":
        if status != "open":
            return {"outcome": "ignored", "invoice_id": iid}
        # Stripe lets the customer try again from the same page, so this is a state, not an end: a later
        # invoice.paid still moves it to paid.
        control_plane.invoices.mark_status(
            iid, "failed", provider="stripe", provider_charge_id=obj.get("id"),
            provider_error="A payment attempt failed; the customer can retry from the invoice page.")
        return {"outcome": "payment_failed", "invoice_id": iid}

    # invoice.voided
    if status in ("open", "failed"):
        control_plane.invoices.mark_status(iid, "void", provider="stripe", provider_charge_id=obj.get("id"))
        return {"outcome": "voided", "invoice_id": iid}
    return {"outcome": "ignored", "invoice_id": iid}
