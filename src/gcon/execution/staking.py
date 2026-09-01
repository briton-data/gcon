"""
GCON Staking — bonded deposits per node, slashed on failed or
fraudulent receipt verification.

What this is (and isn't)
-------------------------
This is an in-app accounting ledger (`node_stakes` / `stake_events`
in the control-plane DB, see `gcon.persistence.repositories.staking`)
-- integer "stake units" bonded against a node's identity, debited on
slash, nothing more. It is NOT an on-chain token, an escrow contract,
or a real custody system: there is no wallet integration, no
transfer of real value in or out, and "bonding" just increments a
number in GCON's own database. Standing this up as a real economic
system (real token, real custody, a chain or L2 to settle slashes on)
is a substantial follow-on project; this module is the scaffolding
that a real implementation would sit behind -- the parts that are
GCON-specific (what counts as a slashable offense, how much, when a
node's stake gates scheduling) are real and enforced end-to-end
today, even though the units themselves are notional.

Enforcement is opt-in
----------------------
Every existing deployment (local dev, the test suite, anyone who
hasn't set up staking) has nodes with zero bonded stake. Making
`min_stake_required` default to anything above 0 would silently
un-schedule every node everywhere. So gating is off
(`GCON_STAKING_REQUIRED` unset/false) unless explicitly enabled, at
which point `Scheduler.select_node()` (see `cluster/scheduler.py`)
filters out any node below `GCON_MIN_NODE_STAKE`.

Slashing trigger
------------------
Scoped exactly to what was asked for: a receipt that fails
`ExecutionVerifier.validate_proof()` (bad/missing HMAC signature --
this is either fraud, an implementation bug on the node, or gross
data corruption; the ledger can't distinguish those, only flag the
evidence). Ordinary job failures (timeout, node offline, non-zero
exit) are NOT slashable on their own -- that's normal operation of
an untrusted, best-effort compute node and slashing it would make
running a GCON node economically hostile. This mirrors how
`_sample_health_and_trust` already treats verification failure as
the one thing that moves the trust score independent of raw job
success rate.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, UTC
from typing import Any, Dict, Optional

from gcon.persistence.control_plane import ControlPlane


class StakeLedger:
    def __init__(self, control_plane: ControlPlane):
        self.control_plane = control_plane
        # Fraction of a node's current bonded stake burned per
        # verification failure. Bounded to (0, 1] -- a fraction over
        # 1 makes no sense (slash() already floors at what's bonded)
        # and 0 would make "staking_required" toothless.
        self.slash_fraction = min(
            1.0, max(0.0, float(os.environ.get("GCON_SLASH_FRACTION", "0.1")))
        )
        self.unbonding_period_days = float(
            os.environ.get("GCON_UNBONDING_PERIOD_DAYS", "7")
        )
        self.min_stake_required = int(os.environ.get("GCON_MIN_NODE_STAKE", "0"))
        self.staking_required = os.environ.get(
            "GCON_STAKING_REQUIRED", "false"
        ).strip().lower() in ("1", "true", "yes")

    # ------------------------------------------------------------ bonding
    def bond(self, node_id: str, amount: int) -> Dict[str, Any]:
        if amount <= 0:
            raise ValueError("bond amount must be positive")
        return self.control_plane.stakes.bond(node_id, amount)

    def request_unbond(self, node_id: str, amount: int) -> Dict[str, Any]:
        if amount <= 0:
            raise ValueError("unbond amount must be positive")
        release_at = (
            datetime.now(UTC) + timedelta(days=self.unbonding_period_days)
        ).isoformat()
        return self.control_plane.stakes.request_unbond(node_id, amount, release_at)

    def release_matured(self, node_id: str) -> int:
        """Finalizes any unbonding amount past its release time.
        Returns the amount released (still the operator's
        responsibility to actually pay it back out -- see module
        docstring; this only clears GCON's own hold on it)."""
        return self.control_plane.stakes.release_matured_unbonding(
            node_id, datetime.now(UTC).isoformat()
        )

    def get(self, node_id: str) -> Dict[str, Any]:
        return self.control_plane.stakes.get(node_id)

    def list_all(self):
        return self.control_plane.stakes.list_all()

    def list_events(self, node_id: Optional[str] = None, limit: int = 200):
        return self.control_plane.stakes.list_events(node_id, limit)

    # --------------------------------------------------------- gating
    def meets_minimum(self, node_id: str) -> bool:
        """True if staking gating is off, or this node clears the
        configured minimum. Called by the scheduler before a node is
        eligible for `staking_required` deployments."""
        if not self.staking_required:
            return True
        return self.get(node_id)["bonded_amount"] >= self.min_stake_required

    # --------------------------------------------------------- slashing
    def slash_for_failed_verification(
        self, node_id: str, job_id: Optional[str], receipt_id: Optional[str]
    ) -> Optional[Dict[str, Any]]:
        """Slashes `slash_fraction` of whatever this node currently
        has bonded+unbonding. A no-op (returns None) for a node with
        nothing staked, rather than a slash of 0 cluttering
        stake_events -- most deployments won't have staking enabled
        at all, and this keeps that case silent."""
        state = self.get(node_id)
        total = state["bonded_amount"] + state["unbonding_amount"]
        if total <= 0:
            return None
        amount = max(1, int(total * self.slash_fraction))
        return self.control_plane.stakes.slash(
            node_id, amount,
            reason="failed_receipt_verification",
            job_id=job_id, receipt_id=receipt_id,
        )
