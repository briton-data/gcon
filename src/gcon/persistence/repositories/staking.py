from __future__ import annotations

import uuid
from datetime import datetime, UTC
from typing import Any, Dict, List, Optional

from gcon.persistence.db import ControlPlaneDatabase


class StakeRepository:
    """
    Durable store for `gcon.execution.staking.StakeLedger`. Balances
    are integer "stake units" (see the staking module docstring for
    why this isn't wired to a real token) -- kept as plain INTEGER
    columns rather than floats so bond/slash arithmetic never drifts.
    """

    def __init__(self, db: ControlPlaneDatabase):
        self.db = db

    def _ensure_row(self, node_id: str) -> None:
        now = datetime.now(UTC).isoformat()
        self.db.execute(
            """
            INSERT INTO node_stakes (node_id, bonded_amount, unbonding_amount,
                                      unbonding_release_at, slashed_total, updated_at)
            VALUES (?, 0, 0, NULL, 0, ?)
            ON CONFLICT(node_id) DO NOTHING
            """,
            (node_id, now),
        )

    def get(self, node_id: str) -> Dict[str, Any]:
        self._ensure_row(node_id)
        row = self.db.query_one(
            "SELECT * FROM node_stakes WHERE node_id = ?", (node_id,)
        )
        return dict(row)

    def bond(self, node_id: str, amount: int) -> Dict[str, Any]:
        self._ensure_row(node_id)
        now = datetime.now(UTC).isoformat()
        self.db.execute(
            "UPDATE node_stakes SET bonded_amount = bonded_amount + ?, updated_at = ? "
            "WHERE node_id = ?",
            (amount, now, node_id),
        )
        self._log_event(node_id, "bond", amount, reason=None)
        return self.get(node_id)

    def request_unbond(self, node_id: str, amount: int, release_at: str) -> Dict[str, Any]:
        """Moves `amount` from bonded to unbonding, held until
        `release_at` (an ISO-8601 timestamp) -- the unbonding period
        that lets a slash still land on a node that just asked to
        withdraw, closing the "misbehave then instantly exit" hole."""
        self._ensure_row(node_id)
        state = self.get(node_id)
        if amount > state["bonded_amount"]:
            raise ValueError(
                f"cannot unbond {amount}, only {state['bonded_amount']} bonded"
            )
        now = datetime.now(UTC).isoformat()
        self.db.execute(
            "UPDATE node_stakes SET bonded_amount = bonded_amount - ?, "
            "unbonding_amount = unbonding_amount + ?, unbonding_release_at = ?, "
            "updated_at = ? WHERE node_id = ?",
            (amount, amount, release_at, now, node_id),
        )
        self._log_event(node_id, "unbond_request", amount, reason=None)
        return self.get(node_id)

    def release_matured_unbonding(self, node_id: str, now_iso: str) -> int:
        """Finalizes any unbonding amount whose release time has
        passed, returning the released amount (the caller is
        responsible for actually crediting it back, e.g. an invoice
        credit or an off-ledger payout -- this only clears the hold)."""
        state = self.get(node_id)
        if state["unbonding_amount"] <= 0 or not state["unbonding_release_at"]:
            return 0
        if state["unbonding_release_at"] > now_iso:
            return 0
        released = state["unbonding_amount"]
        self.db.execute(
            "UPDATE node_stakes SET unbonding_amount = 0, unbonding_release_at = NULL, "
            "updated_at = ? WHERE node_id = ?",
            (now_iso, node_id),
        )
        self._log_event(node_id, "unbond_release", released, reason=None)
        return released

    def slash(
        self,
        node_id: str,
        amount: int,
        reason: str,
        job_id: Optional[str] = None,
        receipt_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Slashes from bonded first, then unbonding (a node mid-exit
        is still on the hook) -- never goes negative; a node with
        less staked than the nominal slash amount just loses
        everything it has left, it isn't put into debt."""
        self._ensure_row(node_id)
        state = self.get(node_id)
        from_bonded = min(amount, state["bonded_amount"])
        remaining = amount - from_bonded
        from_unbonding = min(remaining, state["unbonding_amount"])
        actually_slashed = from_bonded + from_unbonding
        now = datetime.now(UTC).isoformat()
        self.db.execute(
            "UPDATE node_stakes SET bonded_amount = bonded_amount - ?, "
            "unbonding_amount = unbonding_amount - ?, slashed_total = slashed_total + ?, "
            "updated_at = ? WHERE node_id = ?",
            (from_bonded, from_unbonding, actually_slashed, now, node_id),
        )
        self._log_event(
            node_id, "slash", actually_slashed, reason=reason,
            job_id=job_id, receipt_id=receipt_id,
        )
        return self.get(node_id)

    def _log_event(
        self,
        node_id: str,
        event_type: str,
        amount: int,
        reason: Optional[str],
        job_id: Optional[str] = None,
        receipt_id: Optional[str] = None,
    ) -> None:
        self.db.execute(
            """
            INSERT INTO stake_events (event_id, node_id, event_type, amount, reason,
                                       job_id, receipt_id, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                uuid.uuid4().hex, node_id, event_type, amount, reason,
                job_id, receipt_id, datetime.now(UTC).isoformat(),
            ),
        )

    def list_events(self, node_id: Optional[str] = None, limit: int = 200) -> List[Dict[str, Any]]:
        if node_id:
            rows = self.db.query(
                "SELECT * FROM stake_events WHERE node_id = ? ORDER BY created_at DESC LIMIT ?",
                (node_id, limit),
            )
        else:
            rows = self.db.query(
                "SELECT * FROM stake_events ORDER BY created_at DESC LIMIT ?", (limit,)
            )
        return [dict(r) for r in rows]

    def list_all(self) -> List[Dict[str, Any]]:
        rows = self.db.query("SELECT * FROM node_stakes ORDER BY node_id")
        return [dict(r) for r in rows]
