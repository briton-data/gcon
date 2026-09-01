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
    "currency": "usd",
}


@dataclass(frozen=True)
class PricingConfig:
    gpu_second_cents: float
    llm_input_token_cents: float
    llm_output_token_cents: float
    flat_fee_per_job_cents: float
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
