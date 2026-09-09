"""
Pricing configuration for GCON's usage-based invoicing.

Precedence, matching `gcon.transport.config.TransportConfig`'s
existing pattern (env > DB settings > hardcoded default): an env var
always wins if set, then the `settings` table (so an operator can
change pricing without redeploying), then these defaults. Defaults
are deliberately nominal placeholder numbers -- there is no market
research behind them, they exist so `generate_invoice` has something
non-zero to compute against out of the box; a real deployment sets
its own via `GCON_PRICE_*` or the settings API.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, asdict
from typing import Optional

from gcon.persistence.control_plane import ControlPlane

_SETTINGS_KEY = "billing.pricing"

_DEFAULTS = {
    "gpu_second_cents": 0.05,       # $0.0005 / GPU-second of measured runtime
    "llm_input_token_cents": 0.0001,
    "llm_output_token_cents": 0.0003,
    "flat_fee_per_job_cents": 0.0,
    # Previously-unmetered pipeline stages (Verification/Assurance
    # decision/Receipt-proof) -- see invoicing.py's
    # compute_receipt_usage_totals for what each actually counts.
    # Nominal placeholders like the rates above, not market-researched.
    "verification_check_cents": 0.02,   # per replicated-execution comparison actually run
    "assurance_decision_cents": 0.01,   # per PolicyEngine.evaluate() actually run
    "receipt_cents": 0.005,             # per signed receipt issued (the proof deliverable itself)
    "currency": "usd",
}


@dataclass(frozen=True)
class PricingConfig:
    gpu_second_cents: float
    llm_input_token_cents: float
    llm_output_token_cents: float
    flat_fee_per_job_cents: float
    verification_check_cents: float
    assurance_decision_cents: float
    receipt_cents: float
    currency: str

    def to_dict(self) -> dict:
        return asdict(self)


def load_pricing(control_plane: Optional[ControlPlane] = None) -> PricingConfig:
    values = dict(_DEFAULTS)

    if control_plane is not None:
        stored = control_plane.settings.get(_SETTINGS_KEY)
        if stored:
            try:
                values.update(json.loads(stored))
            except (json.JSONDecodeError, TypeError):
                pass

    env_map = {
        "gpu_second_cents": "GCON_PRICE_GPU_SECOND_CENTS",
        "llm_input_token_cents": "GCON_PRICE_LLM_INPUT_TOKEN_CENTS",
        "llm_output_token_cents": "GCON_PRICE_LLM_OUTPUT_TOKEN_CENTS",
        "flat_fee_per_job_cents": "GCON_PRICE_FLAT_FEE_PER_JOB_CENTS",
        "verification_check_cents": "GCON_PRICE_VERIFICATION_CHECK_CENTS",
        "assurance_decision_cents": "GCON_PRICE_ASSURANCE_DECISION_CENTS",
        "receipt_cents": "GCON_PRICE_RECEIPT_CENTS",
    }
    for field, env_var in env_map.items():
        raw = os.environ.get(env_var)
        if raw is not None:
            try:
                values[field] = float(raw)
            except ValueError:
                pass
    currency_override = os.environ.get("GCON_BILLING_CURRENCY")
    if currency_override:
        values["currency"] = currency_override

    return PricingConfig(**values)


def save_pricing(control_plane: ControlPlane, pricing: PricingConfig, updated_by: Optional[str] = None) -> None:
    """Persists an operator-set price schedule to the DB tier of the
    precedence above. Does not touch env vars, which always take
    priority over this when set."""
    control_plane.settings.set(_SETTINGS_KEY, json.dumps(pricing.to_dict()), updated_by=updated_by)


def estimate_job_cost(execution_result: dict, pricing: PricingConfig, receipt: Optional[dict] = None) -> dict:
    """
    Per-job cost estimate for a single completed job's raw result dict
    (the same shape stored at jobs.result_json / receipt["usage"] --
    i.e. `execution_result.get("runtime_seconds")` and
    `execution_result.get("usage", {}).get("llm_tokens")`).

    `receipt`, if given, is the job's full signed receipt dict (see
    coordinator.get_receipt_detail) -- used only to detect whether
    verification (execution_proof present) and an assurance decision
    (policy_report present) actually ran for this specific job, so
    their per-unit costs can be estimated too. receipt=None (the
    default) skips those two components entirely, so every existing
    caller that only ever had execution_result keeps working
    unchanged. The receipt itself, once it exists, is always exactly
    one billable unit -- receipt_cents is added unconditionally
    whenever a receipt is given, since reaching this function at all
    means one was issued.

    Deliberately separate from invoicing.build_line_items(), which
    exists to produce actual invoice line items and rounds each one to
    whole cents -- correct for a monthly aggregate, but at these
    nominal per-second/per-token rates a single job's cost is almost
    always a small fraction of a cent, so rounding it the same way
    here would show "$0.00" for nearly every job regardless of real
    usage. This returns un-rounded, full-precision cents instead, and
    is explicitly an ESTIMATE using current pricing -- not a promise
    of what an actual invoice line for this job will show, since a
    real invoice aggregates a whole billing period and rounds once at
    the end, not per job.

    Returns cents (float, unrounded) per component plus a total, or
    None for a component the job has no usage for (so the caller/UI
    can tell "reported zero" apart from "never reported").
    """
    runtime_seconds = execution_result.get("runtime_seconds")
    compute_cents = (
        runtime_seconds * pricing.gpu_second_cents
        if runtime_seconds is not None else None
    )

    usage = execution_result.get("usage")
    tokens = usage.get("llm_tokens") if isinstance(usage, dict) else None
    input_tokens = tokens.get("input") if isinstance(tokens, dict) else None
    output_tokens = tokens.get("output") if isinstance(tokens, dict) else None
    input_cents = (
        input_tokens * pricing.llm_input_token_cents
        if input_tokens is not None else None
    )
    output_cents = (
        output_tokens * pricing.llm_output_token_cents
        if output_tokens is not None else None
    )

    verification_cents = None
    assurance_cents = None
    receipt_cents = None
    if receipt is not None:
        receipt_cents = pricing.receipt_cents
        if receipt.get("execution_proof") is not None:
            verification_cents = pricing.verification_check_cents
        if receipt.get("policy_report") is not None:
            assurance_cents = pricing.assurance_decision_cents

    total_cents = (
        (compute_cents or 0)
        + (input_cents or 0)
        + (output_cents or 0)
        + (verification_cents or 0)
        + (assurance_cents or 0)
        + (receipt_cents or 0)
        + pricing.flat_fee_per_job_cents
    )

    return {
        "compute_cents": compute_cents,
        "llm_input_cents": input_cents,
        "llm_output_cents": output_cents,
        "verification_cents": verification_cents,
        "assurance_decision_cents": assurance_cents,
        "receipt_cents": receipt_cents,
        "flat_fee_cents": pricing.flat_fee_per_job_cents,
        "total_cents": total_cents,
        "currency": pricing.currency,
        "estimated": True,  # never a real charge -- see docstring
    }
