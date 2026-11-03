"""
What a period is billed is decided by the plan, and the plan's prices, in force when the period STARTED.

Both can change after a month closes (an organization is moved to another plan, a price is edited), and
before this module the invoice for that closed month used whatever was current when it was made. This keeps a
small append-only history, written at the moment of each change, in the control-plane settings (no schema
change):

    key("org_plan:<org_id>")  ->  [[when, plan label], ...]
    key("plan_prices")        ->  [[when, {tier: {"monthly_fee_cents", "included_usage_cents"}}], ...]

The first time something changes, what it was BEFORE is recorded as in force since the beginning of time, so
a change made today never reaches back to periods that ended before it. Nothing recorded (a store that
predates this, or no settings store at all) means "use the current value", exactly as before.

Rule that follows: the plan on the first day of a month decides that month. A plan changed in the middle of a
month applies from the next one; proration is a pricing decision nobody has made, so none is applied.
"""
import json
import logging
from datetime import datetime, UTC
from typing import Any, List, Optional

log = logging.getLogger(__name__)

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_MAX_ROWS = 500


def key(subject: str) -> str:
    return f"billing_history:{subject}"


def _aware(value: Any) -> datetime:
    dt = value if isinstance(value, datetime) else datetime.fromisoformat(str(value))
    return dt if dt.tzinfo else dt.replace(tzinfo=UTC)


def _load(settings, subject: str) -> List[list]:
    if settings is None:
        return []
    raw = settings.get(key(subject))
    if not raw:
        return []
    try:
        rows = json.loads(raw)
        return [[_aware(when).isoformat(), value] for when, value in rows]
    except (ValueError, TypeError) as exc:
        log.error("Ignoring unreadable billing history %r (%s); current values apply.", subject, exc)
        return []


def record_change(settings, subject: str, previous: Any, new: Any, when: Optional[datetime] = None) -> None:
    """Note that `subject` changed from `previous` to `new` now (or `when`). A change to the same value records nothing."""
    if settings is None or previous == new:
        return
    rows = _load(settings, subject) or [[_EPOCH.isoformat(), previous]]
    rows.append([_aware(when or datetime.now(UTC)).isoformat(), new])
    settings.set(key(subject), json.dumps(rows[-_MAX_ROWS:]))


def value_at(settings, subject: str, instant: Any, current: Any) -> Any:
    """What `subject` was at `instant`: the last recorded value that took effect at or before it. `current` when
    nothing is recorded."""
    rows = _load(settings, subject)
    if not rows:
        return current
    moment = _aware(instant)
    chosen = rows[0][1]
    for when, value in rows:
        if _aware(when) <= moment:
            chosen = value
    return chosen
