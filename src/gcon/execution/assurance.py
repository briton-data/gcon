"""
GCON Assurance Synthesis.

get_receipt_detail() already computes three independent trust signals
for a receipt:
  - crypto verification (ExecutionVerifier.validate_proof -- the HMAC
    signature over the receipt's own claimed content)
  - worker attestation (ExecutionVerifier.validate_worker_attestation --
    the node's own Ed25519 signature, independent of GCON's HMAC key)
  - policy trust (PolicyEngine.evaluate -- did this job's own reported
    metrics stay within configured resource/runtime limits)
and, for a verify-tagged (replicated) job only, a fourth:
  - replication agreement (gcon.execution.replication.compare_results --
    did N independently-dispatched nodes report the same result)

Each of these is real and independently useful, but until now they
were left as four unreconciled sibling fields on the receipt detail
payload -- every caller (dashboard, API consumer, SDK user) had to
know all four existed and write their own logic for what to do when
they disagree. This module is that reconciliation, done once, in one
place: synthesize_assurance() takes the four already-computed signals
and returns a single explicit decision.

Deliberately a pure function, not a coordinator method: it has no
dependency on GCONCoordinator, ControlPlane, or any locking -- it only
combines values its caller already computed. Kept in its own module
(matching gcon.execution.replication's own precedent) so it can be
unit-tested standalone, without spinning up a coordinator.

Precedence, worst to best (a receipt gets exactly one level, the
worst one that applies):
  1. "invalid"              -- the HMAC signature itself doesn't
                                check out. Nothing else here can be
                                trusted if this fails: the receipt's
                                own claimed content -- including the
                                other three signals' inputs -- cannot
                                be trusted, so the other three
                                are not even weighed once this fires.
  2. "disputed"              -- signature is valid, but independently
                                dispatched replicas disagree on the
                                result. This is a *worse* signal than
                                a plain policy violation (it means the
                                computation's own correctness is in
                                question, not just its resource use),
                                so it outranks policy_violation.
  3. "attestation_mismatch"  -- signature is valid, replicas agree (or
                                there was no replication), but the
                                node's own attestation, when present,
                                failed to verify -- either a forged/
                                hijacked identity or a real bookkeeping
                                bug; either way not routine.
  4. "policy_violation"      -- everything above passed (or wasn't
                                applicable), but the job itself
                                exceeded a configured resource/runtime
                                limit.
  5. "verified"              -- every applicable signal passed. The
                                only level where `assured` is True.

Worker attestation and replication are each optional per receipt (an
older agent build has no attestation; only verify=N jobs get an
execution_proof) -- their absence is never itself a reason to lower
the level below "verified". Only an explicit failed check does.
"""

from typing import Any, Dict, Optional

# Ordered worst-to-best; only used to assert the precedence above in
# tests, not read at runtime.
LEVELS = (
    "invalid",
    "disputed",
    "attestation_mismatch",
    "policy_violation",
    "verified",
)


def synthesize_assurance(
    crypto_verified: bool,
    crypto_message: Optional[str] = None,
    worker_attestation: Optional[Dict[str, Any]] = None,
    policy_report: Optional[Dict[str, Any]] = None,
    execution_proof: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Combine the four receipt-level trust signals into one decision.

    Args:
        crypto_verified: result of ExecutionVerifier.validate_proof().
        crypto_message: the accompanying message from validate_proof(),
            surfaced as the reason when crypto_verified is False.
        worker_attestation: the dict get_receipt_detail() already
            builds -- {"verified": bool, "verification_message": str,
            ...} -- or None if this receipt has no attestation block
            at all (not a failure; see module docstring).
        policy_report: PolicyEngine.evaluate()'s own report -- {
            "trusted": bool, "checks": [{"name", "passed", "message"},
            ...]} -- or None if no policy evaluation exists for this
            receipt yet.
        execution_proof: the receipt-level block gcon.execution.
            replication.build_execution_proof() produces -- {
            "agreement": bool, "mismatches": [...], ...} (note:
            "agreement", not compare_results()'s own internal "agree"
            key -- build_execution_proof renames it when packaging the
            receipt-level block, and this is that packaged form, as
            stored on receipt["execution_proof"]) -- or None for a
            non-replicated job.

    Returns:
        {
            "level": one of LEVELS,
            "assured": bool,          # True only for "verified"
            "reasons": [str, ...],    # what drove this decision
            "signals": {
                "crypto": {"applicable": True, "passed": bool},
                "worker_attestation": {"applicable": bool, "passed": bool | None},
                "policy": {"applicable": bool, "passed": bool | None},
                "replication": {"applicable": bool, "passed": bool | None},
            },
        }
    """
    signals = {
        "crypto": {"applicable": True, "passed": bool(crypto_verified)},
        "worker_attestation": {
            "applicable": worker_attestation is not None,
            "passed": (worker_attestation or {}).get("verified") if worker_attestation is not None else None,
        },
        "policy": {
            "applicable": policy_report is not None,
            "passed": (policy_report or {}).get("trusted") if policy_report is not None else None,
        },
        "replication": {
            "applicable": execution_proof is not None,
            "passed": (execution_proof or {}).get("agreement") if execution_proof is not None else None,
        },
    }

    if not crypto_verified:
        return {
            "level": "invalid",
            "assured": False,
            "reasons": [crypto_message or "Cryptographic signature is invalid"],
            "signals": signals,
        }

    if execution_proof is not None and not execution_proof.get("agreement", True):
        mismatched_fields = [m.get("field") for m in execution_proof.get("mismatches", []) if m.get("field")]
        reason = "Replicated executions did not agree"
        if mismatched_fields:
            reason += f" (mismatched: {', '.join(mismatched_fields)})"
        return {
            "level": "disputed",
            "assured": False,
            "reasons": [reason],
            "signals": signals,
        }

    if worker_attestation is not None and not worker_attestation.get("verified", False):
        return {
            "level": "attestation_mismatch",
            "assured": False,
            "reasons": [
                worker_attestation.get("verification_message")
                or "Worker attestation failed to verify"
            ],
            "signals": signals,
        }

    if policy_report is not None and not policy_report.get("trusted", True):
        failed_checks = [
            c.get("message") for c in policy_report.get("checks", []) if not c.get("passed", True)
        ]
        return {
            "level": "policy_violation",
            "assured": False,
            "reasons": failed_checks or ["Policy evaluation reported this job as untrusted"],
            "signals": signals,
        }

    reasons = ["Cryptographic signature is valid"]
    if worker_attestation is not None:
        reasons.append("Worker attestation is valid")
    if execution_proof is not None:
        reasons.append("Replicated executions agree")
    if policy_report is not None:
        reasons.append("Policy checks passed")

    return {
        "level": "verified",
        "assured": True,
        "reasons": reasons,
        "signals": signals,
    }
