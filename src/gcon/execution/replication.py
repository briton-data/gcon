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

# Numeric fields that legitimately vary between honest runs on
# different hardware (clock speed, thermal throttling, scheduling
# jitter, ...). Measured against `tolerance` and REPORTED
# (compare_results()'s "metric_outliers" / "max_deviation") but they do
# NOT decide agreement: timing says nothing about whether the
# computation was performed correctly, and letting it decide made
# honest replicas disagree -- 18 of 20 identical `echo hi` runs on two
# healthy nodes came out "disputed" on runtime alone. Also
# deliberately excludes cpu_percent/memory_percent/gpu_memory_used --
# those reflect the *node's* load, not the computation.
_TOLERANT_METRIC_FIELDS = ("runtime_seconds",)


def compare_results(
    results: List[Dict[str, Any]],
    tolerance: float = DEFAULT_TOLERANCE,
) -> Dict[str, Any]:
    """
    Compare N replicas' execution results for agreement.

    Agreement is decided by the strict fields only (_STRICT_FIELDS --
    currently output_hash, which must match exactly on every replica).
    Timing-style metrics (_TOLERANT_METRIC_FIELDS) are measured and
    reported but never cause a disagreement; see the comment on that
    constant for why.

    Args:
        results: one dict per successful replica, each shaped like:
            {"output_hash": <str>, "metrics": {"runtime_seconds": ..., ...}}
            (see coordinator.py's _run_replicated_job for how these
            are built from the same `result`/output_hash values the
            single-node path already computes).
        tolerance: relative spread (0.02 == 2%) beyond which a metric
            is listed in "metric_outliers". Informational only.

    Returns:
        {
            "agree": bool,
            "compared_fields": [str, ...],   # the fields that decided "agree"
            "max_deviation": float,          # largest metric spread, 0.0 if none
            "mismatches": [{"field": ..., "values": [...]}, ...],
            "metric_outliers": [{"field": ..., "values": [...],
                                 "deviation": ..., "tolerance": ...}, ...],
        }

    Cases that are NOT agreement, returned as agree=False with an
    explicit reason rather than a silent True:
      - fewer than 2 results: a single, unwitnessed result.
      - no strict field present on every replica: nothing was
        actually compared, so there is nothing to agree ON. (Missing
        data on one replica is skipped rather than compared as
        None-vs-value, but only as long as at least one field remains.)
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
            "metric_outliers": [],
        }

    compared_fields: List[str] = []
    mismatches: List[Dict[str, Any]] = []

    for field in _STRICT_FIELDS:
        values = [r.get(field) for r in results]
        if any(v is None for v in values):
            continue
        compared_fields.append(field)
        if len(set(values)) > 1:
            mismatches.append({"field": field, "values": values})

    if not compared_fields:
        return {
            "agree": False,
            "compared_fields": [],
            "max_deviation": 0.0,
            "mismatches": [{
                "field": None,
                "reason": (
                    "no strict field (" + ", ".join(_STRICT_FIELDS) + ") was present "
                    "on every replica, so nothing could be compared"
                ),
            }],
            "metric_outliers": [],
        }

    # Informational only -- never affects "agree". The spread is taken
    # over the whole set and scaled by the largest magnitude, so it
    # doesn't depend on which replica happened to be listed first
    # (measuring against results[0] made the same two runtimes agree or
    # disagree depending on completion order).
    max_deviation = 0.0
    metric_outliers: List[Dict[str, Any]] = []
    for field in _TOLERANT_METRIC_FIELDS:
        raw_values = [r.get("metrics", {}).get(field) for r in results]
        if any(v is None for v in raw_values):
            continue
        try:
            values = [float(v) for v in raw_values]
        except (TypeError, ValueError):
            continue
        spread = max(values) - min(values)
        scale = max(abs(v) for v in values)
        deviation = 0.0 if spread == 0 else spread / scale
        max_deviation = max(max_deviation, deviation)
        if deviation > tolerance:
            metric_outliers.append({
                "field": field,
                "values": values,
                "deviation": deviation,
                "tolerance": tolerance,
            })

    return {
        "agree": len(mismatches) == 0,
        "compared_fields": compared_fields,
        "max_deviation": max_deviation,
        "mismatches": mismatches,
        "metric_outliers": metric_outliers,
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
