"""
Customer plans: Standard, Pro and Enterprise.

What a plan controls
--------------------
Exactly these, no more:

  * the plan's name, shown to staff and to the customer,
  * `requests_per_minute`  -- how fast the organization may call /api/v1
    (enforced by require_scope in gcon.api.api_v1),
  * `max_concurrent_jobs`  -- how many of its jobs may be pending or running at
    once (enforced by GCONCoordinator.submit_job, for every way a job can be
    submitted, workflows included),
  * `allowed_programs`     -- the programs its jobs may start with; "*" = any
    (enforced by GCONCoordinator.submit_job, see gcon.execution.command_check
    for exactly what that does and does not guarantee),
  * `max_runtime_seconds`  -- how long one of its jobs may run before the worker
    kills it (enforced by passing the limit to the worker when the job is
    dispatched).

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

For the two numeric limits and the runtime, 0 means "no limit". For
`allowed_programs` the way to say "no restriction" is the entry "*".

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
import re
from typing import Any, Dict, List, Optional

log = logging.getLogger(__name__)

PLAN_NAMES = ("Standard", "Pro", "Enterprise")
DEFAULT_PLAN = "Standard"
LIMIT_FIELDS = ("requests_per_minute", "max_concurrent_jobs", "allowed_programs", "max_runtime_seconds")
MAX_LIMIT_VALUE = 1_000_000
MAX_RUNTIME_SECONDS = 30 * 24 * 3600
MAX_PROGRAMS = 100
SETTINGS_KEY = "plans.limits"
_PROGRAM_NAME = re.compile(r"^(\*|[A-Za-z0-9][A-Za-z0-9._+-]{0,63})$")

# Starting values, to be tuned by staff in the control plane. Standard's request
# rate equals the single limit the API had before plans existed (600/minute).
DEFAULT_PLAN_LIMITS: Dict[str, Dict[str, Any]] = {
    "Standard": {
        "requests_per_minute": 600, "max_concurrent_jobs": 50,
        "allowed_programs": ["python", "python3", "nvidia-smi", "echo"],
        "max_runtime_seconds": 3600,
    },
    "Pro": {
        "requests_per_minute": 3000, "max_concurrent_jobs": 200,
        "allowed_programs": ["python", "python3", "nvidia-smi", "echo", "bash", "sh", "pip", "pip3"],
        "max_runtime_seconds": 14400,
    },
    "Enterprise": {
        "requests_per_minute": 12000, "max_concurrent_jobs": 1000,
        "allowed_programs": ["*"],
        "max_runtime_seconds": 86400,
    },
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


def _copy_table(table: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    return {
        plan: {k: (list(v) if isinstance(v, list) else v) for k, v in limits.items()}
        for plan, limits in table.items()
    }


def validate_programs(programs: Any) -> List[str]:
    """A clean list of program names ("*" = any program). ValueError for
    anything else, including an empty list: a plan that allows no program at all
    is almost certainly a mistake, and "*" is the way to say "any"."""
    if isinstance(programs, str) or not isinstance(programs, (list, tuple)):
        raise ValueError("allowed_programs must be a list of program names, or [\"*\"] for any.")
    clean: List[str] = []
    for item in programs:
        if not isinstance(item, str) or not _PROGRAM_NAME.match(item.strip()):
            raise ValueError(
                f"{item!r} is not a program name (letters, digits and . _ + - only; "
                "write the name only, not a path)."
            )
        name = item.strip()
        if name not in clean:
            clean.append(name)
    if not clean:
        raise ValueError("allowed_programs cannot be empty: list the programs, or use * for any.")
    if len(clean) > MAX_PROGRAMS:
        raise ValueError(f"allowed_programs can hold at most {MAX_PROGRAMS} programs.")
    return ["*"] if "*" in clean else clean


def validate_limits(
    requests_per_minute: Any,
    max_concurrent_jobs: Any,
    allowed_programs: Any,
    max_runtime_seconds: Any,
) -> Dict[str, Any]:
    """All four limits, cleaned. The numbers must be ints (0 = no limit; the
    request rate and job cap up to MAX_LIMIT_VALUE, the runtime up to 30 days);
    booleans and fractions are rejected, not rounded. ValueError otherwise."""
    clean: Dict[str, Any] = {}
    for field, value, ceiling in (
        ("requests_per_minute", requests_per_minute, MAX_LIMIT_VALUE),
        ("max_concurrent_jobs", max_concurrent_jobs, MAX_LIMIT_VALUE),
        ("max_runtime_seconds", max_runtime_seconds, MAX_RUNTIME_SECONDS),
    ):
        if isinstance(value, bool) or not isinstance(value, int):
            raise ValueError(f"{field} must be a whole number (0 means no limit).")
        if not 0 <= value <= ceiling:
            raise ValueError(f"{field} must be between 0 and {ceiling} (0 means no limit).")
        clean[field] = value
    clean["allowed_programs"] = validate_programs(allowed_programs)
    return {field: clean[field] for field in LIMIT_FIELDS}


def load_plan_limits(settings: Any) -> Dict[str, Dict[str, int]]:
    """All three tiers' limits: defaults, overlaid with whatever staff have
    stored. `settings` is control_plane.settings, or None (defaults only)."""
    table = _copy_table(DEFAULT_PLAN_LIMITS)
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
            # Fields an older stored value does not have yet (added later)
            # take the plan's default instead of discarding the whole entry.
            merged = {**DEFAULT_PLAN_LIMITS[plan], **entry}
            table[plan] = validate_limits(
                merged.get("requests_per_minute"), merged.get("max_concurrent_jobs"),
                merged.get("allowed_programs"), merged.get("max_runtime_seconds"),
            )
        except (ValueError, AttributeError, TypeError):
            log.error("Ignoring stored limits for plan %s (invalid); using its defaults.", plan)
    return table


def save_plan_limits(
    settings: Any,
    plan: str,
    requests_per_minute: Any,
    max_concurrent_jobs: Any,
    allowed_programs: Any = None,
    max_runtime_seconds: Any = None,
    updated_by: Optional[str] = None,
) -> Dict[str, Any]:
    """Validate and store one tier's limits; the other tiers are kept as they
    are. `allowed_programs` / `max_runtime_seconds` left as None keep that
    tier's current value. Returns the stored values."""
    plan = canonical_plan(plan)
    table = load_plan_limits(settings)
    current = table[plan]
    values = validate_limits(
        requests_per_minute,
        max_concurrent_jobs,
        current["allowed_programs"] if allowed_programs is None else allowed_programs,
        current["max_runtime_seconds"] if max_runtime_seconds is None else max_runtime_seconds,
    )
    table[plan] = values
    settings.set(SETTINGS_KEY, json.dumps(table), updated_by=updated_by)
    return values


def effective_requests_per_minute(plan_value: int, env_override: Optional[int]) -> int:
    """0 = no limit. `env_override` is None when the environment sets nothing."""
    return plan_value if env_override is None else env_override


def effective_max_concurrent_jobs(plan_value: int, env_override: int) -> int:
    """0 = no limit. `env_override` is 0 when the environment sets nothing."""
    return env_override if env_override > 0 else plan_value
