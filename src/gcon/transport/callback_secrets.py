"""
An organization's callback signing secret.

A job's `callback_url` is signed (see webhooks.py), but until now with a random secret generated per delivery and
known only to GCON, so the receiving endpoint could not check anything: anyone who learned the URL could post a
forged "job completed". Each organization now has ONE secret it can read when it creates or rotates it and then
use to verify every callback_url delivery of its own.

Stored sealed (secret_box) in the control-plane settings, one row per organization. Reading a status never returns
the secret. A rotation applies to deliveries created afterwards; one already queued keeps the secret it was
queued with (it is copied into the delivery row, sealed).
"""
import json
import logging
import secrets
from datetime import datetime, UTC
from typing import Any, Dict, Optional

from gcon.persistence import secret_box

logger = logging.getLogger(__name__)

_KEY_PREFIX = "callback_secret:"


def _key(org_id: str) -> str:
    return f"{_KEY_PREFIX}{org_id}"


def _load(control_plane, org_id: str) -> Optional[Dict[str, Any]]:
    if control_plane is None or not org_id:
        return None
    raw = control_plane.settings.get(_key(org_id))
    if not raw:
        return None
    try:
        return json.loads(raw)
    except ValueError:
        logger.error("Ignoring unreadable callback secret record for org %s", org_id)
        return None


def rotate(control_plane, org_id: str, updated_by: Optional[str] = None) -> Dict[str, str]:
    """Create the organization's secret, or replace it. Returns {"secret", "created_at"}: the only time the secret is shown."""
    secret = secrets.token_urlsafe(32)
    created_at = datetime.now(UTC).isoformat()
    control_plane.settings.set(
        _key(org_id), json.dumps({"sealed": secret_box.seal(secret), "created_at": created_at}), updated_by=updated_by,
    )
    return {"secret": secret, "created_at": created_at}


def get(control_plane, org_id: str) -> Optional[str]:
    """The organization's secret, or None if it has none or it can no longer be decrypted (never raises)."""
    record = _load(control_plane, org_id)
    if not record:
        return None
    try:
        return secret_box.open_(record["sealed"])
    except Exception as exc:
        logger.error("The callback secret of org %s cannot be decrypted (%s); its callbacks fall back to unverifiable signing", org_id, exc)
        return None


def status(control_plane, org_id: str) -> Dict[str, Any]:
    """{"configured": bool, "created_at": str|None}. Never contains the secret."""
    record = _load(control_plane, org_id)
    if not record:
        return {"configured": False, "created_at": None}
    return {"configured": True, "created_at": record.get("created_at")}
