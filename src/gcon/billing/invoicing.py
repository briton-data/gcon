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
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from gcon.billing.pricing import PricingConfig, load_pricing
from gcon.persistence.control_plane import ControlPlane


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


def compute_usage_totals(jobs: List[Dict[str, Any]]) -> Dict[str, float]:
    compute_seconds = 0.0
    llm_input_tokens = 0
    llm_output_tokens = 0
    for job in jobs:
        result = job.get("result") or {}
        compute_seconds += float(result.get("runtime_seconds") or 0)
        usage = result.get("usage")
        if isinstance(usage, dict):
            tokens = usage.get("llm_tokens")
            if isinstance(tokens, dict):
                llm_input_tokens += int(tokens.get("input", 0) or 0)
                llm_output_tokens += int(tokens.get("output", 0) or 0)
    return {
        "job_count": len(jobs),
        "compute_seconds": compute_seconds,
        "llm_input_tokens": llm_input_tokens,
        "llm_output_tokens": llm_output_tokens,
    }


def build_line_items(usage: Dict[str, float], pricing: PricingConfig) -> List[Dict[str, Any]]:
    items = []
    if usage["compute_seconds"] > 0:
        amount = round(usage["compute_seconds"] * pricing.gpu_second_cents)
        items.append({
            "description": "Compute (measured job runtime)",
            "quantity": round(usage["compute_seconds"], 2),
            "unit": "gpu_second",
            "unit_price_cents": pricing.gpu_second_cents,
            "amount_cents": int(amount),
        })
    if usage["llm_input_tokens"] > 0:
        amount = round(usage["llm_input_tokens"] * pricing.llm_input_token_cents)
        items.append({
            "description": "LLM input tokens",
            "quantity": usage["llm_input_tokens"],
            "unit": "token",
            "unit_price_cents": pricing.llm_input_token_cents,
            "amount_cents": int(amount),
        })
    if usage["llm_output_tokens"] > 0:
        amount = round(usage["llm_output_tokens"] * pricing.llm_output_token_cents)
        items.append({
            "description": "LLM output tokens",
            "quantity": usage["llm_output_tokens"],
            "unit": "token",
            "unit_price_cents": pricing.llm_output_token_cents,
            "amount_cents": int(amount),
        })
    if pricing.flat_fee_per_job_cents > 0 and usage["job_count"] > 0:
        amount = round(usage["job_count"] * pricing.flat_fee_per_job_cents)
        items.append({
            "description": "Per-job platform fee",
            "quantity": usage["job_count"],
            "unit": "job",
            "unit_price_cents": pricing.flat_fee_per_job_cents,
            "amount_cents": int(amount),
        })
    return items


def generate_invoice(
    control_plane: ControlPlane,
    org_id: str,
    period_start: str,
    period_end: str,
    pricing: Optional[PricingConfig] = None,
) -> Dict[str, Any]:
    """
    Idempotent: re-calling this for a period that already has an
    invoice (see `InvoiceRepository.create`'s UNIQUE constraint)
    returns the existing invoice unchanged rather than double-billing
    -- generating a draft invoice never itself attempts a charge (see
    `gcon.billing.providers` for that step), so this is safe to run
    speculatively/repeatedly (e.g. from a scheduled job) without
    financial consequences.
    """
    pricing = pricing or load_pricing(control_plane)
    jobs = _jobs_for_period(control_plane, org_id, period_start, period_end)
    usage = compute_usage_totals(jobs)
    line_items = build_line_items(usage, pricing)
    return control_plane.invoices.create(
        org_id=org_id,
        period_start=period_start,
        period_end=period_end,
        currency=pricing.currency,
        line_items=line_items,
    )


def generate_invoices_for_all_orgs(
    control_plane: ControlPlane,
    org_ids: List[str],
    period_start: str,
    period_end: str,
    pricing: Optional[PricingConfig] = None,
) -> List[Dict[str, Any]]:
    pricing = pricing or load_pricing(control_plane)
    return [
        generate_invoice(control_plane, org_id, period_start, period_end, pricing=pricing)
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
