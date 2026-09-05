"""
EnrollTokenRepository -- durable, per-org bootstrap secrets for the
Enroll RPC (see grpc_transport.py). Replaces the single shared
GCON_ENROLL_TOKEN env var as the source of truth for *which company
a newly-enrolling worker belongs to*: previously org_id was a
self-reported field in the Register RPC's capabilities map (trivial
to spoof by any worker holding the one shared token); now org_id is
resolved here, at Enroll time, from the specific token presented,
and gets burned into the signed certificate itself (see tls.py's
sign_agent_csr org_id param) so it's a TLS-authenticated fact for
every RPC that worker makes afterward, not a value it merely claims.

Tokens are stored as a SHA-256 hash, never in plaintext -- the same
model as most API-key systems (GitHub PATs, Stripe keys, etc). The
plaintext value is generated here and returned exactly once, to the
caller that mints it (see scripts/create_enroll_token.py); there is
no way to recover it from the database afterward, only revoke it and
mint a new one.
"""

from __future__ import annotations

import hashlib
import secrets
import uuid
from datetime import datetime, UTC
from typing import Any, Dict, List, Optional

from gcon.persistence.db import ControlPlaneDatabase


def _hash_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


class EnrollTokenRepository:
    def __init__(self, db: ControlPlaneDatabase):
        self.db = db

    def create_token(self, org_id: str, label: Optional[str] = None) -> str:
        """Mints a new token for `org_id`, stores only its hash, and
        returns the plaintext -- the one and only time it's ever
        available. Callers (e.g. scripts/create_enroll_token.py) are
        responsible for handing it to the customer securely; GCON
        itself never displays it again."""
        if not org_id:
            raise ValueError("org_id is required")
        token = "gcon_enroll_" + secrets.token_urlsafe(32)
        token_id = uuid.uuid4().hex
        now = datetime.now(UTC).isoformat()
        self.db.execute(
            """
            INSERT INTO enroll_tokens
                (token_id, org_id, token_hash, label, created_at, revoked_at)
            VALUES (?, ?, ?, ?, ?, NULL)
            """,
            (token_id, org_id, _hash_token(token), label, now),
        )
        return token

    def lookup_org_id(self, token: str) -> Optional[str]:
        """The one call the Enroll RPC actually makes: given a
        presented token, which org (if any) does it authorize
        enrollment for? Returns None for a missing, unknown, or
        revoked token -- the Enroll handler treats all three
        identically (reject), so this deliberately doesn't
        distinguish them in its return value; use get_by_token() if
        you need to know why for logging/admin purposes."""
        if not token:
            return None
        row = self.db.query_one(
            "SELECT org_id FROM enroll_tokens WHERE token_hash = ? AND revoked_at IS NULL",
            (_hash_token(token),),
        )
        return row["org_id"] if row else None

    def get_by_token(self, token: str) -> Optional[Dict[str, Any]]:
        row = self.db.query_one(
            "SELECT * FROM enroll_tokens WHERE token_hash = ?", (_hash_token(token),)
        )
        return dict(row) if row else None

    def list_for_org(self, org_id: str) -> List[Dict[str, Any]]:
        rows = self.db.query(
            "SELECT * FROM enroll_tokens WHERE org_id = ? ORDER BY created_at", (org_id,)
        )
        return [dict(r) for r in rows]

    def revoke(self, token_id: str) -> None:
        self.db.execute(
            "UPDATE enroll_tokens SET revoked_at = ? WHERE token_id = ?",
            (datetime.now(UTC).isoformat(), token_id),
        )
