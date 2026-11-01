"""A plan's monthly fee and included usage on invoices, and invoicing by hand (issue, then record the payment)."""
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from gcon.billing.automation import BillingAutomation
from gcon.billing.invoicing import (finalize_invoice, generate_invoice, preview_usage, record_manual_payment,
                                    with_included_usage, with_plan_fee)
from gcon.billing.pricing import PricingConfig
from gcon.cluster.coordinator import GCONCoordinator
from gcon.management.management_layer import ManagementLayer
from gcon.persistence.control_plane import ControlPlane

OWNER = SimpleNamespace(name="Olive", role="Owner")
START, END = "2026-01-01T00:00:00+00:00", "2026-02-01T00:00:00+00:00"
INSIDE = "2026-01-15T12:00:00+00:00"
LATER = datetime(2026, 3, 1, tzinfo=UTC)
ONE_CENT_A_SECOND = PricingConfig(gpu_second_cents=1.0, llm_input_token_cents=0.0, llm_output_token_cents=0.0,
                                  flat_fee_per_job_cents=0.0, verification_check_cents=0.0,
                                  assurance_decision_cents=0.0, receipt_cents=0.0, currency="usd")


def make_job(plane, job_id, org_id, runtime, completed_at=INSIDE):
    plane.jobs.ensure_exists(job_id, "echo hi", org_id=org_id)
    plane.jobs.set_status(job_id, "completed", result={"runtime_seconds": runtime}, completed=True)
    plane.db.execute("UPDATE jobs SET completed_at = ? WHERE job_id = ?", (completed_at, job_id))


@pytest.fixture
def stack(tmp_path):
    coordinator = GCONCoordinator(control_plane=ControlPlane(path=str(tmp_path / "cp.db")))
    management = ManagementLayer(coordinator=coordinator, db_path=str(tmp_path / "m.db"))
    management.set_plan_limits("Pro", 600, 50, included_usage_cents=2000, monthly_fee_cents=4900, actor=OWNER)
    yield coordinator, management
    coordinator.shutdown()


def customer(management, name, plan, since):
    org = management.create_organization(name, plan)
    management.org_registry.organizations[org["org_id"]].created_at = since
    return org["org_id"]


class TestTheLines:
    def test_included_usage_offsets_usage_only_never_the_fee(self):
        usage = [{"amount_cents": 900}]
        lines = with_plan_fee(with_included_usage(usage, 2000), 4900, "Pro plan, monthly fee")
        assert [l["amount_cents"] for l in lines] == [4900, 900, -900]
        assert sum(l["amount_cents"] for l in lines) == 4900          # usage fully covered; the fee is still owed

    def test_a_bill_over_the_included_usage_pays_the_difference_plus_the_fee(self):
        lines = with_plan_fee(with_included_usage([{"amount_cents": 3000}], 2000), 4900, "fee")
        assert sum(l["amount_cents"] for l in lines) == 4900 + 1000


class TestWhenTheFeeApplies:
    def test_a_whole_month_for_a_customer_who_was_there_on_its_first_day(self, stack):
        _, management = stack
        org = customer(management, "Acme", "Pro", datetime(2025, 12, 1, tzinfo=UTC))
        assert management.invoice_terms(org, START, END) == {
            "included_usage_cents": 2000, "fee_cents": 4900, "fee_label": "Pro plan, monthly fee"}

    def test_not_in_the_month_they_joined(self, stack):
        _, management = stack
        org = customer(management, "Acme", "Pro", datetime(2026, 1, 10, tzinfo=UTC))
        assert management.invoice_terms(org, START, END)["fee_cents"] == 0

    def test_not_for_a_period_that_is_not_one_whole_month(self, stack):
        _, management = stack
        org = customer(management, "Acme", "Pro", datetime(2025, 12, 1, tzinfo=UTC))
        assert management.invoice_terms(org, START, "2026-01-16T00:00:00+00:00")["fee_cents"] == 0

    def test_standard_has_no_fee_until_staff_set_one(self, stack):
        _, management = stack
        org = customer(management, "Free Co", "Standard", datetime(2025, 12, 1, tzinfo=UTC))
        assert management.invoice_terms(org, START, END)["fee_cents"] == 0

    def test_the_preview_and_the_invoice_match(self, stack):
        coordinator, management = stack
        org = customer(management, "Acme", "Pro", datetime(2025, 12, 1, tzinfo=UTC))
        make_job(coordinator.control_plane, "j1", org, 900)
        terms = lambda s, e: management.invoice_terms(org, s, e)
        preview = preview_usage(coordinator.control_plane, org, START, END, pricing=ONE_CENT_A_SECOND, terms=terms)
        invoice = generate_invoice(coordinator.control_plane, org, START, END, pricing=ONE_CENT_A_SECOND, now=LATER, terms=terms)
        assert preview["amount_cents"] == invoice["amount_cents"] == 4900
        assert invoice["line_items"][0]["description"] == "Pro plan, monthly fee"


class TestTheMonthlyRun:
    def test_a_customer_with_a_fee_and_no_jobs_is_invoiced_but_not_for_the_month_they_joined(self, stack):
        coordinator, management = stack
        long_standing = customer(management, "Old Co", "Pro", datetime(2025, 12, 1, tzinfo=UTC))
        joined_in_february = customer(management, "New Co", "Pro", datetime(2026, 2, 10, tzinfo=UTC))
        BillingAutomation(coordinator).run_once(now=LATER, months=1)          # February is the month that just ended
        invoices = lambda org: coordinator.control_plane.invoices.list_for_org(org)
        assert [i["amount_cents"] for i in invoices(long_standing)] == [4900]  # no jobs at all, still invoiced the fee
        assert invoices(joined_in_february) == []                              # their first month has no fee, so nothing to invoice


class TestInvoicingByHand:
    def test_issuing_does_not_pretend_it_was_paid_and_staff_record_the_payment(self, stack, monkeypatch):
        coordinator, management = stack
        monkeypatch.delenv("GCON_PAYMENT_PROVIDER", raising=False)             # production's default
        plane = coordinator.control_plane
        org = customer(management, "Acme", "Pro", datetime(2025, 12, 1, tzinfo=UTC))
        draft = generate_invoice(plane, org, START, END, now=LATER, terms=lambda s, e: management.invoice_terms(org, s, e))
        with pytest.raises(ValueError, match="issued"):
            record_manual_payment(plane, draft["invoice_id"], "TX-1")         # a draft cannot be marked paid
        issued = finalize_invoice(plane, draft["invoice_id"])
        assert issued["status"] == "open" and issued["provider"] == "manual" and issued["paid_at"] is None
        with pytest.raises(ValueError, match="reference"):
            record_manual_payment(plane, draft["invoice_id"], "  ")
        paid = management.record_invoice_payment(draft["invoice_id"], "BANK-7731")
        assert paid["status"] == "paid" and paid["provider_charge_id"] == "BANK-7731" and paid["paid_at"]
        with pytest.raises(ValueError, match="issued"):
            record_manual_payment(plane, draft["invoice_id"], "again")        # already settled
