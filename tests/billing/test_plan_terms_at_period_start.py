"""
The plan fee and included usage for a period come from the plan (and the plan's prices) in force when the
period STARTED, not whatever they are by the time the invoice is made.

Before: an organization upgraded on the 3rd of the next month, or a price raised after the month closed,
changed what the closed month was billed.
"""
from datetime import datetime, UTC

import pytest

from gcon.billing import terms_history
from gcon.cluster.coordinator import GCONCoordinator
from gcon.dashboard.presentation import PresentationLayer
from gcon.dashboard.web_server import WebServer
from gcon.persistence import ControlPlane
from tests.support.billing_history import backdate_plan_prices


class _Actor:
    role = "Owner"
    name = "Owner"


@pytest.fixture
def mgmt(tmp_path, monkeypatch):
    monkeypatch.setenv("GCON_DB_PATH", str(tmp_path / "gcon.db"))
    monkeypatch.setenv("GCON_OWNER_PASSWORD", "owner-pw-123")
    coordinator = GCONCoordinator(control_plane=ControlPlane(path=str(tmp_path / "cp.db")))
    server = WebServer(PresentationLayer(coordinator))
    yield server.management
    coordinator.shutdown()


def _last_month():
    now = datetime.now(UTC)
    first_this = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    prev_year, prev_month = (first_this.year - 1, 12) if first_this.month == 1 else (first_this.year, first_this.month - 1)
    first_prev = first_this.replace(year=prev_year, month=prev_month)
    return first_prev.isoformat(), first_this.isoformat()


def _old_org(m, plan="Standard"):
    org = m.org_registry.add_organization("Acme", plan=plan)
    org.created_at = datetime(2025, 1, 1, tzinfo=UTC)
    m.org_registry._persist_org(org)
    return org.org_id


def _set_fee(m, tier, cents, included=None, *, long_ago=False):
    """Save a price. `long_ago=True` makes it a price that was already in force before last month."""
    cur = m.get_plan_limits()[tier]
    m.set_plan_limits(tier, cur["requests_per_minute"], cur["max_concurrent_jobs"],
                      monthly_fee_cents=cents, included_usage_cents=included, actor=_Actor())
    if long_ago:
        backdate_plan_prices(m)


class TestFeeIsThePlanAtPeriodStart:
    def test_upgrading_after_the_month_closed_does_not_charge_the_new_fee_for_it(self, mgmt):
        _set_fee(mgmt, "Pro", 4900, long_ago=True)
        org_id = _old_org(mgmt, "Standard")
        start, end = _last_month()
        mgmt.update_organization(org_id, plan="Pro")            # upgraded today, after last month ended
        assert mgmt.invoice_terms(org_id, start, end)["fee_cents"] == 0

    def test_downgrading_after_the_month_closed_still_charges_the_fee_it_was_on(self, mgmt):
        _set_fee(mgmt, "Pro", 4900, long_ago=True)
        org_id = _old_org(mgmt, "Pro")
        start, end = _last_month()
        mgmt.update_organization(org_id, plan="Standard")
        terms = mgmt.invoice_terms(org_id, start, end)
        assert terms["fee_cents"] == 4900
        assert terms["fee_label"].startswith("Pro")

    def test_raising_the_price_after_the_month_closed_does_not_reprice_it(self, mgmt):
        _set_fee(mgmt, "Pro", 4900, long_ago=True)
        org_id = _old_org(mgmt, "Pro")
        start, end = _last_month()
        _set_fee(mgmt, "Pro", 9900)
        assert mgmt.invoice_terms(org_id, start, end)["fee_cents"] == 4900

    def test_included_usage_follows_the_same_rule(self, mgmt):
        _set_fee(mgmt, "Standard", 0, included=500, long_ago=True)
        org_id = _old_org(mgmt, "Standard")
        start, end = _last_month()
        _set_fee(mgmt, "Standard", 0, included=50)
        assert mgmt.invoice_terms(org_id, start, end)["included_usage_cents"] == 500

    def test_with_no_changes_the_current_plan_applies_as_before(self, mgmt):
        _set_fee(mgmt, "Pro", 4900, long_ago=True)
        org_id = _old_org(mgmt, "Pro")
        start, end = _last_month()
        assert mgmt.invoice_terms(org_id, start, end)["fee_cents"] == 4900

    def test_a_price_first_set_today_does_not_reach_back_to_last_month(self, mgmt):
        """A fee configured for the first time today was not in force when last month began, so last month
        is billed without it. It applies from the next month that starts after it was set."""
        org_id = _old_org(mgmt, "Pro")
        start, end = _last_month()
        _set_fee(mgmt, "Pro", 4900)
        assert mgmt.invoice_terms(org_id, start, end)["fee_cents"] == 0

    def test_a_saved_price_that_changes_nothing_records_nothing(self, mgmt):
        _set_fee(mgmt, "Pro", 4900)
        settings = mgmt._plan_settings()
        before = settings.get(terms_history.key("plan_prices"))
        _set_fee(mgmt, "Pro", 4900)
        assert settings.get(terms_history.key("plan_prices")) == before


class TestHistoryHelper:
    class _Settings:
        def __init__(self):
            self.d = {}

        def get(self, k):
            return self.d.get(k)

        def set(self, k, v, updated_by=None):
            self.d[k] = v

    def _at(self, day):
        return datetime(2026, 10, day, tzinfo=UTC)

    def test_a_change_in_the_middle_of_a_period_does_not_apply_to_it(self):
        s = self._Settings()
        terms_history.record_change(s, "org_plan:o1", "Standard", "Pro", when=self._at(20))
        assert terms_history.value_at(s, "org_plan:o1", self._at(1), "Pro") == "Standard"
        assert terms_history.value_at(s, "org_plan:o1", self._at(20), "Pro") == "Pro"
        assert terms_history.value_at(s, "org_plan:o1", self._at(28), "Pro") == "Pro"

    def test_later_changes_stack_in_order(self):
        s = self._Settings()
        terms_history.record_change(s, "k", "A", "B", when=self._at(10))
        terms_history.record_change(s, "k", "B", "C", when=self._at(20))
        got = [terms_history.value_at(s, "k", self._at(d), "C") for d in (5, 15, 25)]
        assert got == ["A", "B", "C"]

    def test_unreadable_history_falls_back_to_the_current_value(self):
        s = self._Settings()
        s.d[terms_history.key("k")] = "not json"
        assert terms_history.value_at(s, "k", self._at(5), "now") == "now"

    def test_no_settings_store_means_current_value(self):
        assert terms_history.value_at(None, "k", self._at(5), "now") == "now"
        terms_history.record_change(None, "k", "a", "b")        # must not raise
