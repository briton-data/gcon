"""A plan's free monthly allowance: a visible credit line on previews and invoices, never below zero."""
import pytest

from gcon.billing.invoicing import finalize_invoice, generate_invoice, preview_usage, with_free_allowance
from gcon.billing.pricing import PricingConfig
from gcon.cluster.coordinator import GCONCoordinator
from gcon.management.management_layer import ManagementLayer
from gcon.persistence.control_plane import ControlPlane

START = "2026-01-01T00:00:00+00:00"
END = "2026-02-01T00:00:00+00:00"
INSIDE = "2026-01-15T12:00:00+00:00"
NOW = "2026-03-01T00:00:00+00:00"


@pytest.fixture
def cp(tmp_path):
    plane = ControlPlane(path=str(tmp_path / "allowance.db"))
    yield plane
    plane.close()


@pytest.fixture
def pricing():
    # One cent per second of runtime, nothing else, so the arithmetic in the tests is plain.
    return PricingConfig(gpu_second_cents=1.0, llm_input_token_cents=0.0, llm_output_token_cents=0.0,
                         flat_fee_per_job_cents=0.0, verification_check_cents=0.0,
                         assurance_decision_cents=0.0, receipt_cents=0.0, currency="usd")


def make_job(cp, job_id, org_id, runtime):
    cp.jobs.ensure_exists(job_id, "echo hi", org_id=org_id)
    cp.jobs.set_status(job_id, "completed", result={"runtime_seconds": runtime}, completed=True)
    cp.db.execute("UPDATE jobs SET completed_at = ? WHERE job_id = ?", (INSIDE, job_id))


class TestWithFreeAllowance:
    def test_takes_the_allowance_off_a_bigger_bill(self):
        out = with_free_allowance([{"amount_cents": 900}], 500)
        assert out[-1]["description"] == "Free monthly allowance" and out[-1]["amount_cents"] == -500
        assert sum(i["amount_cents"] for i in out) == 400

    def test_never_takes_a_bill_below_zero(self):
        out = with_free_allowance([{"amount_cents": 320}], 500)
        assert sum(i["amount_cents"] for i in out) == 0

    def test_no_allowance_or_no_usage_adds_nothing(self):
        assert with_free_allowance([{"amount_cents": 300}], 0) == [{"amount_cents": 300}]
        assert with_free_allowance([], 500) == []


class TestPreviewAndInvoice:
    def test_preview_shows_the_credit_and_the_reduced_total(self, cp, pricing):
        make_job(cp, "j1", "org-a", 900)
        result = preview_usage(cp, "org-a", START, END, pricing=pricing, allowance_cents=500)
        assert result["amount_cents"] == 400
        assert [i["description"] for i in result["line_items"]][-1] == "Free monthly allowance"

    def test_the_invoice_matches_the_preview(self, cp, pricing):
        make_job(cp, "j1", "org-a", 900)
        preview = preview_usage(cp, "org-a", START, END, pricing=pricing, allowance_cents=500)
        invoice = generate_invoice(cp, "org-a", START, END, pricing=pricing, now=__import__("datetime").datetime.fromisoformat(NOW),
                                   allowance_cents=500)
        assert invoice["amount_cents"] == preview["amount_cents"] == 400

    def test_usage_inside_the_allowance_is_a_zero_invoice_that_is_paid_without_a_charge(self, cp, pricing):
        make_job(cp, "j1", "org-a", 300)
        invoice = generate_invoice(cp, "org-a", START, END, pricing=pricing, now=__import__("datetime").datetime.fromisoformat(NOW),
                                   allowance_cents=500)
        assert invoice["amount_cents"] == 0
        assert finalize_invoice(cp, invoice["invoice_id"])["status"] == "paid"

    def test_without_an_allowance_nothing_changes(self, cp, pricing):
        make_job(cp, "j1", "org-a", 900)
        assert preview_usage(cp, "org-a", START, END, pricing=pricing)["amount_cents"] == 900


class TestThePlansDecideIt:
    def test_standard_gets_the_allowance_and_pro_does_not(self, tmp_path):
        coordinator = GCONCoordinator(control_plane=ControlPlane(path=str(tmp_path / "cp.db")))
        management = ManagementLayer(coordinator=coordinator, db_path=str(tmp_path / "m.db"))
        try:
            standard = management.create_organization("Acme", "Standard")["org_id"]
            pro = management.create_organization("Bolt", "Pro")["org_id"]
            plane = coordinator.control_plane
            make_job(plane, "a", standard, 36000)     # 10 hours at the default rate
            make_job(plane, "b", pro, 36000)
            std = management.preview_org_usage(standard, START, END)
            prof = management.preview_org_usage(pro, START, END)
            assert std["amount_cents"] == prof["amount_cents"] - 500
            assert any(i["description"] == "Free monthly allowance" for i in std["line_items"])
            assert not any(i["description"] == "Free monthly allowance" for i in prof["line_items"])
        finally:
            coordinator.shutdown()


class TestTheMonthlyRun:
    def test_the_scheduled_invoice_carries_the_plans_allowance(self, cp):
        from datetime import datetime, UTC
        from types import SimpleNamespace
        from gcon.billing.automation import BillingAutomation
        make_job(cp, "s1", "acme", 36000)
        cp.db.execute("UPDATE jobs SET completed_at = ? WHERE job_id = ?", ("2026-09-10T12:00:00+00:00", "s1"))
        coordinator = SimpleNamespace(control_plane=cp, verifier=None, leader_elector=SimpleNamespace(is_leader=True),
                                      org_job_rules_resolver=lambda org_id: {"free_allowance_cents": 500})
        BillingAutomation(coordinator).run_once(now=datetime(2026, 10, 15, tzinfo=UTC))
        invoice = cp.invoices.list_for_org("acme")[0] if hasattr(cp.invoices, "list_for_org") else None
        assert invoice is not None and invoice["amount_cents"] == 1800 - 500
