"""
Invoice generation from usage-metering data.

What this is (and isn't) -- read before wiring this to real money
--------------------------------------------------------------------
GCON has usage *metering* (`ManagementLayer.get_org_usage_summary`,
built from real measured job runtime and opt-in usage reports) but no
billing system: no charge amounts, no invoicing, no payment-provider
integration. This module is the first two of those three -- it turns
metering data for a completed billing period into a durable Invoice
(line items, a total, a status) via `gcon.persistence.repositories
.billing.InvoiceRepository`. It does NOT move any money. Actually
charging a customer is `gcon.billing.providers.PaymentProvider`'s
job, and today the only real implementation of that interface is
`MockPaymentProvider` -- charging a live card/bank account requires
a real provider account and API credentials this environment doesn't
have (see `providers.py`'s docstring for exactly what a real
`StripePaymentProvider` would need).

Where usage data comes from
-----------------------------
`get_org_usage_summary()` reads `self.coordinator.get_jobs()`, which
is the coordinator's *current in-memory* job set -- fine for "what's
running right now" on a dashboard, wrong for "every job this org ran
in July" once jobs older than `GCON_MAX_JOBS_IN_MEMORY` have been
evicted (see coordinator.py's `_evict_completed_if_over_capacity`).
So invoicing queries the control-plane DB directly instead
(`control_plane.jobs`, filtered by `org_id` and `completed_at` inside
the period) -- durable, unaffected by in-memory eviction. This does
inherit that table's own fidelity limit (documented in
`_evict_completed_if_over_capacity`'s docstring): only
command/status/completed_at/result/org_id survive per row, which is
exactly what a runtime-seconds/token-usage rollup needs anyway.

Metering previously stopped at compute + LLM tokens -- Execution and
(partially) Evidence in GCON's own pipeline terms, but nothing here
ever counted Verification (a replicated-execution comparison actually
run), Assurance decision (a PolicyEngine.evaluate() actually run), or
Receipt/proof (the signed receipt itself, the actual customer-facing
deliverable) as billable at all, even though all three are real,
already-happening events with their own durable record. Those three
are metered the same way as jobs above: `_receipts_for_period` reads
`control_plane.receipts` directly (joined to `jobs` for org_id, same
join `ReceiptRepository.search_paginated` already uses), and
`compute_receipt_usage_totals` counts them from each receipt's
`payload_json` -- `execution_proof`/`policy_report` keys are only
ever set once, at receipt-creation time (coordinator.py's
create_receipt/_run_replicated_job/policy-engine wiring), so the
payload already IS the durable record of whether each stage ran for
that receipt, not a separately-tracked (and separately-driftable)
counter.
"""

from __future__ import annotations

import json as _json
import logging
import math
import os
from datetime import datetime, timedelta, UTC
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Callable, Dict, List, Optional, Tuple

from gcon.billing.pricing import PricingConfig, load_pricing
from gcon.persistence.control_plane import ControlPlane

_log = logging.getLogger("gcon.billing")

# A job's measured runtime can never be billed beyond what the customer allowed it (its own timeout_seconds),
# and a job with no timeout is held to this sanity ceiling so a corrupt value can't produce a huge charge.
_DEFAULT_MAX_JOB_SECONDS = 7 * 24 * 3600


def _jobs_for_period(
    control_plane: ControlPlane, org_id: str, period_start: str, period_end: str
) -> List[Dict[str, Any]]:
    """Completed jobs for this org whose completed_at falls inside
    [period_start, period_end) -- both ISO-8601 strings, compared as
    strings (safe: ISO-8601 sorts lexicographically same as
    chronologically, the same assumption every other ordering in this
    codebase's persistence layer already makes)."""
    rows = control_plane.db.query(
        """
        SELECT * FROM jobs
        WHERE org_id = ? AND status = 'completed'
          AND completed_at >= ? AND completed_at < ?
        """,
        (org_id, period_start, period_end),
    )
    out = []
    for row in rows:
        d = dict(row)
        import json as _json
        d["result"] = _json.loads(d["result_json"]) if d.get("result_json") else None
        out.append(d)
    return out


def _max_job_seconds() -> float:
    try:
        value = float(os.environ.get("GCON_BILLING_MAX_JOB_SECONDS", _DEFAULT_MAX_JOB_SECONDS))
    except ValueError:
        return float(_DEFAULT_MAX_JOB_SECONDS)
    return value if value > 0 else float(_DEFAULT_MAX_JOB_SECONDS)


def _clean_runtime(value: Any, job: Dict[str, Any], max_job_seconds: float, excluded: Dict[str, int]) -> float:
    """Billable seconds for one job: 0 if the runtime is missing or not a usable number, never more than the
    job's own timeout (or the sanity ceiling when it has none). Counts what it set aside in `excluded`."""
    if value is None:
        return 0.0
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
        excluded["invalid_runtime"] += 1
        return 0.0
    timeout = job.get("timeout_seconds")
    cap = float(timeout) if isinstance(timeout, (int, float)) and not isinstance(timeout, bool) and timeout > 0 else max_job_seconds
    if value > cap:
        excluded["clamped_runtime"] += 1
        return float(cap)
    return float(value)


def _clean_tokens(value: Any) -> Optional[int]:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
        return None
    return int(value)


def measure_jobs(
    jobs: List[Dict[str, Any]],
    org_id: Optional[str] = None,
    job_nodes: Optional[Dict[str, str]] = None,
    node_orgs: Optional[Dict[str, Optional[str]]] = None,
    max_job_seconds: Optional[float] = None,
) -> Dict[str, Any]:
    """Usage of completed jobs, with bad values set aside instead of billed (or crashing the run).

    `compute_seconds` is all billable runtime; `own_worker_compute_seconds` is the part of it that ran on a node
    enrolled to this same organization (job_nodes: job_id -> node_id, node_orgs: node_id -> org_id). A job whose
    node is unknown counts as GCON-provided compute, i.e. at the standard rate.
    """
    ceiling = max_job_seconds if max_job_seconds is not None else _max_job_seconds()
    excluded = {"invalid_runtime": 0, "clamped_runtime": 0, "invalid_token_usage": 0}
    compute_seconds = own_seconds = 0.0
    llm_input_tokens = llm_output_tokens = 0
    for job in jobs:
        result = job.get("result")
        result = result if isinstance(result, dict) else {}
        seconds = _clean_runtime(result.get("runtime_seconds"), job, ceiling, excluded)
        compute_seconds += seconds
        node_id = (job_nodes or {}).get(job.get("job_id"))
        if org_id and node_id and (node_orgs or {}).get(node_id) == org_id:
            own_seconds += seconds
        usage = result.get("usage")
        tokens = usage.get("llm_tokens") if isinstance(usage, dict) else None
        if isinstance(tokens, dict):
            tin, tout = _clean_tokens(tokens.get("input", 0) or 0), _clean_tokens(tokens.get("output", 0) or 0)
            if tin is None or tout is None:
                excluded["invalid_token_usage"] += 1
            else:
                llm_input_tokens += tin
                llm_output_tokens += tout
    return {
        "job_count": len(jobs),
        "compute_seconds": compute_seconds,
        "own_worker_compute_seconds": own_seconds,
        "llm_input_tokens": llm_input_tokens,
        "llm_output_tokens": llm_output_tokens,
        "excluded": excluded,
    }


def compute_usage_totals(jobs: List[Dict[str, Any]]) -> Dict[str, float]:
    measured = measure_jobs(jobs)
    return {k: measured[k] for k in ("job_count", "compute_seconds", "llm_input_tokens", "llm_output_tokens")}


def _job_nodes(control_plane: ControlPlane, job_ids: List[str]) -> Dict[str, str]:
    """job_id -> the node that ran it: the completed attempt when attempts are recorded, else the node that
    issued the job's receipt."""
    found: Dict[str, str] = {}
    for i in range(0, len(job_ids), 400):
        chunk = job_ids[i:i + 400]
        marks = ",".join("?" for _ in chunk)
        for row in control_plane.db.query(
            f"SELECT job_id, node_id FROM receipts WHERE job_id IN ({marks}) AND node_id IS NOT NULL "
            "ORDER BY uploaded_at", tuple(chunk)):
            found[row["job_id"]] = row["node_id"]
        for row in control_plane.db.query(
            f"SELECT job_id, node_id FROM job_attempts WHERE job_id IN ({marks}) AND node_id IS NOT NULL "
            "AND status = 'completed' ORDER BY attempt_number", tuple(chunk)):
            found[row["job_id"]] = row["node_id"]
    return found


def _node_orgs(control_plane: ControlPlane) -> Dict[str, Optional[str]]:
    return {row["node_id"]: row["org_id"] for row in control_plane.db.query("SELECT node_id, org_id FROM nodes")}


def _receipts_for_period(
    control_plane: ControlPlane, org_id: str, period_start: str, period_end: str
) -> List[Dict[str, Any]]:
    """Uploaded receipts for this org's jobs whose uploaded_at falls
    inside [period_start, period_end) -- joined against jobs the same
    way ReceiptRepository.search_paginated's org_id filter already
    does (a receipt row has no org_id column of its own -- only
    jobs.org_id does)."""
    rows = control_plane.db.query(
        """
        SELECT r.* FROM receipts r
        JOIN jobs j ON j.job_id = r.job_id
        WHERE j.org_id = ? AND j.status = 'completed'
          AND r.uploaded_at >= ? AND r.uploaded_at < ?
        """,
        (org_id, period_start, period_end),
    )
    out = []
    for row in rows:
        d = dict(row)
        d["payload"] = _json.loads(d["payload_json"]) if d.get("payload_json") else {}
        out.append(d)
    return out


def make_receipt_validator(verifier) -> Callable[[Dict[str, Any]], bool]:
    """A check that a stored receipt is the genuine, unaltered one: the signed proof verifies and the fields beside
    it match what was signed. This is the same check the coordinator runs on every receipt."""
    from gcon.execution.verifier import ExecutionVerifier

    def check(receipt: Dict[str, Any]) -> bool:
        try:
            proof_ok, _ = verifier.validate_proof(receipt.get("proof") or {})
            if not proof_ok:
                return False
            fields_ok, _ = ExecutionVerifier.validate_receipt_fields(receipt)
            return bool(fields_ok)
        except Exception:
            return False
    return check


def validate_receipts(
    receipts: List[Dict[str, Any]],
    validator: Optional[Callable[[Dict[str, Any]], bool]] = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    """The receipts that may be billed, and how many were set aside and why.

    A receipt is billed only if its payload is a real record for the same job, it is not a duplicate of one
    already counted, and (when a `validator` is given) its signature checks out. Without a validator only the
    structural checks apply; an unsigned receipt is rejected only when a validator is in play.
    """
    valid: List[Dict[str, Any]] = []
    rejected = {"malformed_receipt": 0, "job_mismatch_receipt": 0, "duplicate_receipt": 0, "bad_signature_receipt": 0}
    seen = set()
    for receipt in receipts:
        payload = receipt.get("payload")
        if not isinstance(payload, dict) or not payload:
            rejected["malformed_receipt"] += 1
        elif payload.get("job_id") not in (None, receipt.get("job_id")):
            rejected["job_mismatch_receipt"] += 1
        elif receipt.get("receipt_id") in seen:
            rejected["duplicate_receipt"] += 1
        elif validator is not None and (not receipt.get("signature") or not validator(payload)):
            rejected["bad_signature_receipt"] += 1
        else:
            seen.add(receipt.get("receipt_id"))
            valid.append(receipt)
    return valid, rejected


def compute_receipt_usage_totals(receipts: List[Dict[str, Any]]) -> Dict[str, int]:
    """
    Metering for the three billable stages that stopped at "trust
    signal" and were never counted as usage at all: every uploaded
    receipt is one billable Receipt/proof deliverable
    (`receipt_count`); one whose payload has `execution_proof` had a
    real replicated-execution Verification comparison run
    (`verification_count`); one whose payload has `policy_report` had
    a real Assurance decision (PolicyEngine.evaluate()) run
    (`assurance_decision_count`). See module docstring for why these
    are read from payload_json rather than separate counter columns.
    """
    receipt_count = 0
    verification_count = 0
    assurance_decision_count = 0
    for receipt in receipts:
        receipt_count += 1
        payload = receipt.get("payload") or {}
        if payload.get("execution_proof") is not None:
            verification_count += 1
        if payload.get("policy_report") is not None:
            assurance_decision_count += 1
    return {
        "receipt_count": receipt_count,
        "verification_count": verification_count,
        "assurance_decision_count": assurance_decision_count,
    }


def _cents(value: float) -> int:
    """Whole cents, halves rounded up (Python's round() sends 0.5 to 0, which silently drops small charges)."""
    return int(Decimal(repr(float(value))).quantize(Decimal(1), rounding=ROUND_HALF_UP))


def _line(description: str, quantity: float, unit: str, unit_price: float) -> Dict[str, Any]:
    exact = quantity * unit_price
    return {
        "description": description, "quantity": quantity, "unit": unit, "unit_price_cents": unit_price,
        "amount_cents": _cents(exact), "exact_cents": exact,
    }


def build_line_items(usage: Dict[str, float], pricing: PricingConfig) -> List[Dict[str, Any]]:
    items = []
    compute = usage["compute_seconds"]
    own = min(max(float(usage.get("own_worker_compute_seconds", 0.0) or 0.0), 0.0), compute)
    own_rate = pricing.own_worker_gpu_second_cents
    if compute > 0 and own > 0 and own_rate is not None and own_rate != pricing.gpu_second_cents:
        # Compute is priced by where it ran: GCON's workers at the standard rate, the customer's own at theirs.
        if compute - own > 0:
            items.append(_line("Compute (shared pool)", round(compute - own, 2), "gpu_second", pricing.gpu_second_cents))
        items.append(_line("Compute (your own workers)", round(own, 2), "gpu_second", own_rate))
    elif compute > 0:
        items.append(_line("Compute (measured job runtime)", round(compute, 2), "gpu_second", pricing.gpu_second_cents))
    if usage["llm_input_tokens"] > 0:
        items.append(_line("LLM input tokens", usage["llm_input_tokens"], "token", pricing.llm_input_token_cents))
    if usage["llm_output_tokens"] > 0:
        items.append(_line("LLM output tokens", usage["llm_output_tokens"], "token", pricing.llm_output_token_cents))
    if usage.get("verification_count", 0) > 0:
        items.append(_line("Verification (replicated-execution checks run)", usage["verification_count"],
                           "verification", pricing.verification_check_cents))
    if usage.get("assurance_decision_count", 0) > 0:
        items.append(_line("Assurance decisions (policy evaluations run)", usage["assurance_decision_count"],
                           "decision", pricing.assurance_decision_cents))
    if usage.get("receipt_count", 0) > 0:
        items.append(_line("Receipts issued (signed proof of execution)", usage["receipt_count"],
                           "receipt", pricing.receipt_cents))
    if pricing.flat_fee_per_job_cents > 0 and usage["job_count"] > 0:
        items.append(_line("Per-job platform fee", usage["job_count"], "job", pricing.flat_fee_per_job_cents))
    return items


def with_rounding_adjustment(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Make the invoice total the exact usage rounded ONCE, not the sum of separately rounded lines.

    Rounding every line on its own can lose (or invent) several cents, and at GCON's per-second and per-token
    rates it can turn a small but real bill into $0.00. Any difference between the two is shown as its own
    'Rounding adjustment' line, so the lines always add up to the total and nothing is hidden."""
    if not items:
        return items
    target = _cents(sum(item.get("exact_cents", item["amount_cents"]) for item in items))
    diff = target - sum(item["amount_cents"] for item in items)
    if diff == 0:
        return items
    return items + [{"description": "Rounding adjustment", "quantity": 1, "unit": "adjustment",
                     "unit_price_cents": 0, "amount_cents": diff, "exact_cents": float(diff)}]


def with_free_allowance(items: List[Dict[str, Any]], allowance_cents: int) -> List[Dict[str, Any]]:
    """A plan's free monthly allowance: a credit line that takes up to `allowance_cents` off what the usage costs,
    never below zero. It is its own visible line, so the lines still add up to the total."""
    total = sum(item["amount_cents"] for item in items)
    credit = min(int(allowance_cents or 0), total)
    if credit <= 0:
        return items
    return items + [{"description": "Free monthly allowance", "quantity": 1, "unit": "credit",
                     "unit_price_cents": 0, "amount_cents": -credit, "exact_cents": float(-credit)}]


# ---------------------------------------------------------------
# Billing periods
# ---------------------------------------------------------------

def normalize_period(
    period_start: Any, period_end: Any, now: Optional[datetime] = None, require_ended: bool = True
) -> Tuple[str, str]:
    """A billing period as canonical UTC ISO strings (so '2026-09-01', '2026-09-01T00:00:00Z' and
    '2026-09-01T00:00:00+00:00' are the one period), or ValueError if it can't be invoiced.

    It must run forwards, span at most MAX_PREVIEW_DAYS, and have ENDED: invoicing a period that is still running
    would leave out everything that happens before it closes."""
    start = _parse_instant(period_start, "period_start")
    end = _parse_instant(period_end, "period_end")
    if end <= start:
        raise ValueError("period_end must be after period_start.")
    if end - start > timedelta(days=MAX_PREVIEW_DAYS):
        raise ValueError(f"A billing period can span at most {MAX_PREVIEW_DAYS} days.")
    now = (now or datetime.now(UTC)).astimezone(UTC)
    if require_ended and end > now:
        raise ValueError(
            f"This billing period has not ended yet (it ends {end.isoformat()}). Invoice it once it has ended, "
            "so no usage is left out."
        )
    return start.isoformat(), end.isoformat()


def _period_content(
    control_plane: ControlPlane, org_id: str, start_iso: str, end_iso: str, pricing: PricingConfig,
    receipt_validator: Optional[Callable[[Dict[str, Any]], bool]] = None,
    allowance_cents: int = 0,
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """Usage and line items for one org and period. The ONE place that computes them, used by both the
    preview and the invoice, so the two can never disagree."""
    jobs = _jobs_for_period(control_plane, org_id, start_iso, end_iso)
    receipts = _receipts_for_period(control_plane, org_id, start_iso, end_iso)
    valid_receipts, rejected = validate_receipts(receipts, receipt_validator)
    measured = measure_jobs(jobs, org_id, _job_nodes(control_plane, [j["job_id"] for j in jobs]) if jobs else {},
                            _node_orgs(control_plane) if jobs else {})
    excluded = {**measured.pop("excluded"), **rejected}
    usage = {**measured, **compute_receipt_usage_totals(valid_receipts), "excluded": excluded}
    return usage, with_free_allowance(with_rounding_adjustment(build_line_items(usage, pricing)), allowance_cents)


def generate_invoice(
    control_plane: ControlPlane,
    org_id: str,
    period_start: str,
    period_end: str,
    pricing: Optional[PricingConfig] = None,
    *,
    receipt_validator: Optional[Callable[[Dict[str, Any]], bool]] = None,
    now: Optional[datetime] = None,
    skip_empty: bool = False,
    allowance_cents: int = 0,
) -> Optional[Dict[str, Any]]:
    """
    Idempotent: re-calling this for a period that already has an invoice returns the existing invoice
    unchanged rather than double-billing, however the period was spelled. Generating a draft never attempts a
    charge (see `gcon.billing.providers`), so it is safe to run speculatively or from a scheduled job.

    The period must have ended and must not overlap any invoice the organization already has (ValueError, or
    InvoiceOverlapError, otherwise). `skip_empty=True` creates nothing and returns None when the period has no
    billable usage. Pass `receipt_validator` (see make_receipt_validator) so only receipts whose signatures check
    out are billed.
    """
    start_iso, end_iso = normalize_period(period_start, period_end, now=now)
    pricing = pricing or load_pricing(control_plane)
    usage, line_items = _period_content(control_plane, org_id, start_iso, end_iso, pricing, receipt_validator, allowance_cents)
    if skip_empty and not line_items:
        return None
    if any(usage["excluded"].values()):
        _log.warning("invoice for %s %s..%s set aside invalid usage: %s", org_id, start_iso, end_iso,
                     {k: v for k, v in usage["excluded"].items() if v})
    return control_plane.invoices.create(
        org_id=org_id,
        period_start=start_iso,
        period_end=end_iso,
        currency=pricing.currency,
        line_items=line_items,
    )


def generate_invoices_for_all_orgs(
    control_plane: ControlPlane,
    org_ids: List[str],
    period_start: str,
    period_end: str,
    pricing: Optional[PricingConfig] = None,
    **kwargs: Any,
) -> List[Dict[str, Any]]:
    pricing = pricing or load_pricing(control_plane)
    return [
        generate_invoice(control_plane, org_id, period_start, period_end, pricing=pricing, **kwargs)
        for org_id in org_ids
    ]


def finalize_invoice(
    control_plane: ControlPlane,
    invoice_id: str,
    provider=None,
    org_billing_ref: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Attempts to charge a draft invoice via `provider` (defaults to
    `gcon.billing.providers.get_configured_provider()`) and records
    the outcome. A zero-amount invoice (no billable usage that
    period) is marked `paid` without ever calling the provider --
    there's nothing to charge, and a $0 charge attempt is a
    meaningless (and, on some real gateways, rejected) API call.
    """
    from gcon.billing.providers import get_configured_provider

    invoice = control_plane.invoices.get(invoice_id)
    if invoice is None:
        raise ValueError(f"No invoice '{invoice_id}'")
    if invoice["status"] != "draft":
        return invoice

    if invoice["amount_cents"] <= 0:
        return control_plane.invoices.mark_status(invoice_id, "paid", provider="none")

    provider = provider or get_configured_provider()
    result = provider.charge(invoice, org_billing_ref)
    if result.success:
        return control_plane.invoices.mark_status(
            invoice_id, "paid", provider=result.provider,
            provider_charge_id=result.provider_charge_id,
        )
    return control_plane.invoices.mark_status(
        invoice_id, "failed", provider=result.provider, provider_error=result.error,
    )


# ---------------------------------------------------------------
# Customer-facing reads (GET /api/v1/billing/...)
# ---------------------------------------------------------------

# A usage preview scans the org's jobs and receipts for the period; a year is
# enough for any real invoice question and keeps one request from asking for
# an unbounded scan.
MAX_PREVIEW_DAYS = 366


def default_billing_period(now: Optional[datetime] = None):
    """The current UTC calendar month as (start, end), end exclusive."""
    now = now or datetime.now(UTC)
    start = now.astimezone(UTC).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if start.month == 12:
        end = start.replace(year=start.year + 1, month=1)
    else:
        end = start.replace(month=start.month + 1)
    return start, end


def _parse_instant(value: Any, name: str) -> datetime:
    """An ISO-8601 date or timestamp as an aware UTC datetime. No timezone
    means UTC."""
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be an ISO-8601 date or timestamp.")
    try:
        parsed = datetime.fromisoformat(value.strip())
    except ValueError:
        raise ValueError(
            f"{name} '{value}' is not a valid ISO-8601 date or timestamp "
            "(for example 2026-10-01 or 2026-10-01T00:00:00Z)."
        ) from None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def preview_usage(
    control_plane: ControlPlane,
    org_id: str,
    period_start: Optional[str] = None,
    period_end: Optional[str] = None,
    pricing: Optional[PricingConfig] = None,
    now: Optional[datetime] = None,
    receipt_validator: Optional[Callable[[Dict[str, Any]], bool]] = None,
    allowance_cents: int = 0,
) -> Dict[str, Any]:
    """
    What an invoice for this org and period would contain right now, computed
    with exactly the helpers `generate_invoice` uses but WITHOUT creating or
    changing anything. Same completed jobs, same receipts, same line items --
    so if staff later generate an invoice for the identical period the numbers
    match, unless more usage was recorded in between.

    Give both `period_start` and `period_end` (end exclusive), or neither for
    the current UTC calendar month. Raises ValueError for anything else.
    """
    if not org_id:
        raise ValueError("An organization is required to preview usage.")
    if (period_start is None) != (period_end is None):
        raise ValueError(
            "Give both period_start and period_end, or neither "
            "(neither means the current UTC calendar month)."
        )
    if period_start is None:
        start, end = default_billing_period(now)
    else:
        start = _parse_instant(period_start, "period_start")
        end = _parse_instant(period_end, "period_end")
    if end <= start:
        raise ValueError("period_end must be after period_start.")
    if end - start > timedelta(days=MAX_PREVIEW_DAYS):
        raise ValueError(f"A usage period can span at most {MAX_PREVIEW_DAYS} days.")

    pricing = pricing or load_pricing(control_plane)
    start_iso, end_iso = start.isoformat(), end.isoformat()
    usage, line_items = _period_content(control_plane, org_id, start_iso, end_iso, pricing, receipt_validator, allowance_cents)
    return {
        "period_start": start_iso,
        "period_end": end_iso,
        "currency": pricing.currency,
        "usage": usage,
        "line_items": line_items,
        "amount_cents": sum(item["amount_cents"] for item in line_items),
    }


_CUSTOMER_INVOICE_FIELDS = (
    "invoice_id", "period_start", "period_end", "currency", "amount_cents",
    "status", "created_at", "finalized_at", "paid_at",
)
_CUSTOMER_LINE_ITEM_FIELDS = ("description", "quantity", "unit", "unit_price_cents", "amount_cents")


def customer_invoice_view(invoice: Dict[str, Any]) -> Dict[str, Any]:
    """An invoice as a customer may see it. Deliberately leaves out the
    payment-gateway fields (provider, provider_charge_id, provider_error) and
    the org id: those are internal bookkeeping, not part of the invoice."""
    view = {field: invoice.get(field) for field in _CUSTOMER_INVOICE_FIELDS}
    view["line_items"] = [
        {field: item.get(field) for field in _CUSTOMER_LINE_ITEM_FIELDS}
        for item in invoice.get("line_items") or []
    ]
    return view
