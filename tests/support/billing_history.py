"""Test helper: make the plan prices on a ManagementLayer look as if they had been in place since `since`
(billing history otherwise dates a price from the moment it was saved)."""
import json
from datetime import datetime, UTC

from gcon.billing import terms_history


def backdate_plan_prices(management, since=datetime(2025, 1, 1, tzinfo=UTC)):
    settings = management._plan_settings()
    rows = json.loads(settings.get(terms_history.key("plan_prices")) or "[]")
    if rows:
        rows[-1][0] = since.isoformat()
        settings.set(terms_history.key("plan_prices"), json.dumps(rows))
