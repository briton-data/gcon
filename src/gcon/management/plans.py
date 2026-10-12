"""
Customer plans: Standard, Pro and Enterprise.

What a plan controls
--------------------
Exactly three things, no more:

  * the plan's name, shown to staff and to the customer,
  * `requests_per_minute`  -- how fast the organization may call /api/v1
    (enforced by require_scope in gcon.api.api_v1),
  * `max_concurrent_jobs`  -- how many of its jobs may be pending or running at
    once (enforced by GCONCoordinator.submit_job, for every way a job can be
    submitted, workflows included).

Pricing is NOT part of a plan: rates stay in gcon.billing.pricing and are the
same for every organization.

Where the numbers live
----------------------
`DEFAULT_PLAN_LIMITS` below are the starting values. Staff change them in the
control plane (PUT /management/plans/{plan}); the table is stored as one JSON
value in the control-plane `settings` table under `plans.limits`, so a change
applies on the very next request and survives a restart. A stored value that
cannot be read is ignored and that tier falls back to its default -- a damaged
setting must never leave a customer without limits.

A limit of 0 means "no limit".

Environment overrides
---------------------
Same rule as pricing (env > settings > default): when set, these apply to EVERY
organization whatever its plan, and staff edits to the tiers then have no
effect on that limit:

  GCON_API_RATE_LIMIT_PER_MINUTE    N > 0 = everyone gets N; 0 = request
                                    limiting off for everyone.
  GCON_MAX_CONCURRENT_JOBS_PER_ORG  N > 0 = everyone gets N.

Organizations whose stored plan label is not one of the three (for example the
old "Free") are treated as Standard for limits; their label is shown as stored
until staff pick a real plan.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, Optional

log = logging.getLogger(__name__)

PLAN_NAMES = ("Standard", "Pro", "Enterprise")
DEFAULT_PLAN = "Standard"
LIMIT_FIELDS = ("requests_per_minute", "max_concurrent_jobs")
MAX_LIMIT_VALUE = 1_000_000
SETTINGS_KEY = "plans.limits"

# Starting values, to be tuned by staff in the control plane. Standard's request
# rate equals the single limit the API had before plans existed (600/minute).
DEFAULT_PLAN_LIMITS: Dict[str, Dict[str, int]] = {
    "Standard": {"requests_per_minute": 600, "max_concurrent_jobs": 50},
    "Pro": {"requests_per_minute": 3000, "max_concurrent_jobs": 200},
    "Enterprise": {"requests_per_minute": 12000, "max_concurrent_jobs": 1000},
}


def canonical_plan(name: Any) -> str:
    """The canonical spelling of a plan name (case-insensitive). ValueError
    if it is not one of PLAN_NAMES."""
    if isinstance(name, str):
        for plan in PLAN_NAMES:
            if plan.lower() == name.strip().lower():
                return plan
    raise ValueError(f"Plan must be one of: {', '.join(PLAN_NAMES)}.")


def plan_for_limits(stored_label: Any) -> str:
    """The tier whose limits apply to an organization with this stored label.
    Never raises: an unknown or empty label gets the default tier."""
    try:
        return canonical_plan(stored_label)
    except ValueError:
        return DEFAULT_PLAN


def validate_limits(requests_per_minute: Any, max_concurrent_jobs: Any) -> Dict[str, int]:
    """Both limits as ints in 0..MAX_LIMIT_VALUE (0 = no limit). ValueError
    otherwise. Booleans and fractions are rejected, not rounded."""
    clean: Dict[str, int] = {}
    for field, value in (
        ("requests_per_minute", requests_per_minute),
        ("max_concurrent_jobs", max_concurrent_jobs),
    ):
        if isinstance(value, bool) or not isinstance(value, int):
            raise ValueError(f"{field} must be a whole number (0 means no limit).")
        if not 0 <= value <= MAX_LIMIT_VALUE:
            raise ValueError(f"{field} must be between 0 and {MAX_LIMIT_VALUE} (0 means no limit).")
        clean[field] = value
    return clean


def load_plan_limits(settings: Any) -> Dict[str, Dict[str, int]]:
    """All three tiers' limits: defaults, overlaid with whatever staff have
    stored. `settings` is control_plane.settings, or None (defaults only)."""
    table = {plan: dict(limits) for plan, limits in DEFAULT_PLAN_LIMITS.items()}
    if settings is None:
        return table
    try:
        raw = settings.get(SETTINGS_KEY)
        stored = json.loads(raw) if raw else {}
    except (ValueError, TypeError) as exc:
        log.error("Ignoring unreadable %s setting (%s); using default plan limits.", SETTINGS_KEY, exc)
        return table
    if not isinstance(stored, dict):
        log.error("Ignoring %s setting: not an object; using default plan limits.", SETTINGS_KEY)
        return table
    for plan in PLAN_NAMES:
        entry = stored.get(plan)
        if entry is None:
            continue
        try:
            table[plan] = validate_limits(
                entry.get("requests_per_minute"), entry.get("max_concurrent_jobs")
            )
        except (ValueError, AttributeError):
            log.error("Ignoring stored limits for plan %s (invalid); using its defaults.", plan)
    return table


def save_plan_limits(
    settings: Any,
    plan: str,
    requests_per_minute: Any,
    max_concurrent_jobs: Any,
    updated_by: Optional[str] = None,
) -> Dict[str, int]:
    """Validate and store one tier's limits; the other tiers are kept as they
    are. Returns the stored values."""
    plan = canonical_plan(plan)
    values = validate_limits(requests_per_minute, max_concurrent_jobs)
    table = load_plan_limits(settings)
    table[plan] = values
    settings.set(SETTINGS_KEY, json.dumps(table), updated_by=updated_by)
    return values


def effective_requests_per_minute(plan_value: int, env_override: Optional[int]) -> int:
    """0 = no limit. `env_override` is None when the environment sets nothing."""
    return plan_value if env_override is None else env_override


def effective_max_concurrent_jobs(plan_value: int, env_override: int) -> int:
    """0 = no limit. `env_override` is 0 when the environment sets nothing."""
    return env_override if env_override > 0 else plan_value
