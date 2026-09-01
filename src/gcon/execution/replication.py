"""
GCON Replicated-Execution Verification.

HMAC-signed receipts (see gcon.execution.verifier.ExecutionVerifier)
prove that a specific, trusted signing key vouches for a record --
they do NOT prove the record describes a computation that actually
happened as claimed. A compromised or dishonest node can sign a
fabricated result just as easily as a real one; the crypto only
protects the packaging, not the computation inside it.

This module adds an independent, additive layer on top: dispatch the
same job to N nodes and compare their reported results. Agreement
across independently-selected nodes is evidence the computation was
actually performed, in a way no signature alone can provide.

Design note (see gcon-rebuild verification-design discussion):
  - ZK proof of real training workloads isn't tractable yet
    (proving overhead for real workloads, float-vs-finite-field
    mismatch) -- ruled out for now, not attempted here.
  - TEE needs confidential-computing hardware (e.g. Nvidia H100
    confidential mode) the current fleet (T4s) doesn't have -- ruled
    out for now, not attempted here.
  - Redundancy/replication is the one of the three actually
    buildable with what GCON has today -- this module.

This module never imports or modifies ExecutionVerifier. Each
replica still gets its own independently HMAC-signed receipt exactly
as before (see coordinator.py's _run_replicated_job). The comparison
result computed here is attached to each receipt as a sibling
"execution_proof" field, OUTSIDE the signed "proof" dict -- it is
deliberately not part of the signed payload. It doesn't need to be:
the two (or more) things it points to -- each replica's own signed
receipt -- are already independently tamper-evident, so any auditor
holding those receipts can redo this exact comparison themselves.
Folding the comparison into the signature would also require
delaying signing until every replica finishes, which would change
ExecutionVerifier's contract; keeping it a separate, unsigned,
independently-reproducible annotation avoids that.
"""

from typing import Any, Dict, List, Optional

DEFAULT_TOLERANCE = 0.02  # 2% relative tolerance on comparable numeric fields

# output_hash must match exactly across replicas if every replica
# reported one (it's the same hash_data(stdout) computation
# GCONCoordinator already does for the primary receipt) -- any
# deviation here means the replicas produced genuinely different
# output, not just noisy timing.
_STRICT_FIELDS = ("output_hash",)

# Numeric fields that legitimately vary a little between honest runs
# on different hardware (clock speed, thermal throttling, etc.) --
# compared within `tolerance` instead of exactly. Deliberately does
# NOT include cpu_percent/memory_percent/gpu_memory_used -- those
# reflect the *node's* load at the time, not the computation's
# correctness, and comparing them would produce false disagreements
# between two perfectly honest nodes.
_TOLERANT_METRIC_FIELDS = ("runtime_seconds",)


def compare_results(
    results: List[Dict[str, Any]],
    tolerance: float = DEFAULT_TOLERANCE,
) -> Dict[str, Any]:
    """
    Compare N replicas' execution results for agreement.

    Args:
        results: one dict per successful replica, each shaped like:
            {"output_hash": <str>, "metrics": {"runtime_seconds": ..., ...}}
            (see coordinator.py's _run_replicated_job for how these
            are built from the same `result`/output_hash values the
            single-node path already computes).
        tolerance: relative tolerance (0.02 == 2%) applied to
            _TOLERANT_METRIC_FIELDS.

    Returns:
        {
            "agree": bool,
            "compared_fields": [str, ...],
            "max_deviation": float,   # 0.0 if nothing tolerant-compared
            "mismatches": [{"field": ..., "values": [...], ...}, ...],
        }

    Fewer than 2 results can't be compared -- returns agree=False
    with an explicit "not enough replicas" mismatch rather than
    silently claiming agreement on a single, unwitnessed result.
    """
    if len(results) < 2:
        return {
            "agree": False,
            "compared_fields": [],
            "max_deviation": 0.0,
            "mismatches": [{
                "field": None,
                "reason": f"only {len(results)} successful replica(s) to compare; need at least 2",
            }],
        }

    compared_fields: List[str] = []
    mismatches: List[Dict[str, Any]] = []
    max_deviation = 0.0

    for field in _STRICT_FIELDS:
        values = [r.get(field) for r in results]
        if any(v is None for v in values):
            continue
        compared_fields.append(field)
        if len(set(values)) > 1:
            mismatches.append({"field": field, "values": values})

    for field in _TOLERANT_METRIC_FIELDS:
        raw_values = [r.get("metrics", {}).get(field) for r in results]
        if any(v is None for v in raw_values):
            continue
        try:
            values = [float(v) for v in raw_values]
        except (TypeError, ValueError):
            continue
        compared_fields.append(field)
        base = values[0]
        field_max_deviation = 0.0
        for v in values[1:]:
            if base == 0 and v == 0:
                deviation = 0.0
            elif base == 0:
                deviation = float("inf")
            else:
                deviation = abs(v - base) / abs(base)
            field_max_deviation = max(field_max_deviation, deviation)
        max_deviation = max(max_deviation, field_max_deviation)
        if field_max_deviation > tolerance:
            mismatches.append({
                "field": field,
                "values": values,
                "deviation": field_max_deviation,
                "tolerance": tolerance,
            })

    return {
        "agree": len(mismatches) == 0,
        "compared_fields": compared_fields,
        "max_deviation": max_deviation,
        "mismatches": mismatches,
    }


def build_execution_proof(
    witnesses: List[str],
    comparison: Dict[str, Any],
    replica_group_id: str,
) -> Dict[str, Any]:
    """
    Package a comparison result into the receipt-level
    "execution_proof" field attached to every replica's receipt.
    Kept as a small separate function (rather than inlined at each
    call site) so every replica in a group gets a byte-identical
    execution_proof block.
    """
    return {
        "type": "replicated",
        "replica_group_id": replica_group_id,
        "witnesses": list(witnesses),
        "agreement": comparison["agree"],
        "compared_fields": comparison["compared_fields"],
        "max_deviation": comparison["max_deviation"],
        "mismatches": comparison["mismatches"],
    }
