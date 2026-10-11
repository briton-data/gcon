"""
preview_usage / customer_invoice_view / list_issued_for_org.

Real ControlPlane + SQLite (tmp_path), same approach as test_invoicing.py:
the thing under test is real SQL over jobs/receipts and real JSON parsing.
"""
from datetime import datetime, UTC

import pytest

from gcon.billing.invoicing import (
    MAX_PREVIEW_DAYS,
    customer_invoice_view,
    default_billing_period,
    generate_invoice,
    preview_usage,
)
from gcon.billing.pricing import PricingConfig
from gcon.persistence.control_plane import ControlPlane

START = "2026-01-01T00:00:00+00:00"
END = "2026-02-01T00:00:00+00:00"
INSIDE = "2026-01-15T12:00:00+00:00"
OUTSIDE = "2026-02-15T12:00:00+00:00"


@pytest.fixture
def cp(tmp_path):
    control_plane = ControlPlane(path=str(tmp_path / "preview.db"))
    yield control_plane
    control_plane.close()


@pytest.fixture
def pricing():
    return PricingConfig(
        gpu_second_cents=1.0, llm_input_token_cents=0.1, llm_output_token_cents=0.2,
        flat_fee_per_job_cents=0.0, verification_check_cents=5.0,
        assurance_decision_cents=2.0, receipt_cents=1.0, currency="usd",
    )


def make_job(cp, job_id, org_id, runtime=10.0, completed_at=INSIDE):
    cp.jobs.ensure_exists(job_id, "echo hi", org_id=org_id)
    cp.jobs.set_status(job_id, "completed", result={"runtime_seconds": runtime}, completed=True)
    cp.db.execute("UPDATE jobs SET completed_at = ? WHERE job_id = ?", (completed_at, job_id))


class TestDefaultPeriod:
    def test_current_utc_month(self):
        start, end = default_billing_period(datetime(2026, 10, 11, 15, 30, tzinfo=UTC))
        assert start.isoformat() == "2026-10-01T00:00:00+00:00"
        assert end.isoformat() == "2026-11-01T00:00:00+00:00"

    def test_december_rolls_into_next_year(self):
        start, end = default_billing_period(datetime(2026, 12, 31, 23, 59, tzinfo=UTC))
        assert start.isoformat() == "2026-12-01T00:00:00+00:00"
        assert end.isoformat() == "2027-01-01T00:00:00+00:00"


class TestPreviewUsage:
    def test_counts_only_this_orgs_completed_jobs_inside_the_period(self, cp, pricing):
        make_job(cp, "mine-1", "org-a", runtime=10)
        make_job(cp, "mine-2", "org-a", runtime=5)
        make_job(cp, "late", "org-a", runtime=100, completed_at=OUTSIDE)
        make_job(cp, "theirs", "org-b", runtime=999)
        cp.jobs.ensure_exists("pending", "echo hi", org_id="org-a")

        result = preview_usage(cp, "org-a", START, END, pricing=pricing)

        assert result["usage"]["job_count"] == 2
        assert result["usage"]["compute_seconds"] == 15.0
        assert result["amount_cents"] == 15
        assert result["currency"] == "usd"
        assert [i["unit"] for i in result["line_items"]] == ["gpu_second"]

    def test_period_is_normalised_to_utc_iso(self, cp, pricing):
        result = preview_usage(cp, "org-a", "2026-01-01", "2026-02-01T00:00:00Z", pricing=pricing)
        assert result["period_start"] == START
        assert result["period_end"] == END

    def test_timezone_offsets_are_converted_to_utc(self, cp, pricing):
        result = preview_usage(
            cp, "org-a", "2026-01-01T02:00:00+02:00", "2026-02-01T00:00:00+00:00", pricing=pricing,
        )
        assert result["period_start"] == START

    def test_matches_the_invoice_generated_for_the_same_period(self, cp, pricing):
        make_job(cp, "j1", "org-a", runtime=40)
        make_job(cp, "j2", "org-a", runtime=2)
        preview = preview_usage(cp, "org-a", START, END, pricing=pricing)
        invoice = generate_invoice(cp, "org-a", START, END, pricing=pricing)
        assert preview["amount_cents"] == invoice["amount_cents"]
        assert [(i["unit"], i["amount_cents"]) for i in preview["line_items"]] == [
            (i["unit"], i["amount_cents"]) for i in invoice["line_items"]
        ]

    def test_previewing_creates_no_invoice(self, cp, pricing):
        make_job(cp, "j1", "org-a", runtime=40)
        preview_usage(cp, "org-a", START, END, pricing=pricing)
        assert cp.invoices.list_for_org("org-a") == []

    def test_empty_period_is_zero_with_no_line_items(self, cp, pricing):
        result = preview_usage(cp, "org-a", START, END, pricing=pricing)
        assert result["amount_cents"] == 0
        assert result["line_items"] == []
        assert result["usage"]["job_count"] == 0

    def test_default_period_is_the_current_month(self, cp, pricing):
        now = datetime(2026, 1, 20, tzinfo=UTC)
        make_job(cp, "j1", "org-a", runtime=7)
        result = preview_usage(cp, "org-a", pricing=pricing, now=now)
        assert result["period_start"] == START and result["period_end"] == END
        assert result["usage"]["job_count"] == 1

    @pytest.mark.parametrize("kwargs, fragment", [
        ({"period_start": START}, "both period_start and period_end"),
        ({"period_end": END}, "both period_start and period_end"),
        ({"period_start": "nonsense", "period_end": END}, "period_start 'nonsense'"),
        ({"period_start": START, "period_end": ""}, "period_end must be"),
        ({"period_start": END, "period_end": START}, "after period_start"),
        ({"period_start": START, "period_end": START}, "after period_start"),
        ({"period_start": "2025-01-01", "period_end": "2026-06-01"}, f"{MAX_PREVIEW_DAYS} days"),
    ])
    def test_bad_periods_are_rejected(self, cp, pricing, kwargs, fragment):
        with pytest.raises(ValueError) as exc:
            preview_usage(cp, "org-a", pricing=pricing, **kwargs)
        assert fragment in str(exc.value)

    def test_an_org_is_required(self, cp, pricing):
        with pytest.raises(ValueError):
            preview_usage(cp, "", START, END, pricing=pricing)
        with pytest.raises(ValueError):
            preview_usage(cp, None, START, END, pricing=pricing)


class TestCustomerView:
    def _issued(self, cp, org_id="org-a", period=("2026-03-01", "2026-04-01"), status="paid"):
        invoice = cp.invoices.create(
            org_id, period[0], period[1], "usd",
            [{"description": "Compute", "quantity": 10, "unit": "gpu_second",
              "unit_price_cents": 1.0, "amount_cents": 10}],
        )
        if status != "draft":
            cp.invoices.mark_status(
                invoice["invoice_id"], status, provider="mock",
                provider_charge_id="mock_ch_1", provider_error="card declined" if status == "failed" else None,
            )
        return cp.invoices.get(invoice["invoice_id"])

    def test_view_hides_gateway_fields_and_org(self, cp):
        view = customer_invoice_view(self._issued(cp, status="failed"))
        assert set(view) == {
            "invoice_id", "period_start", "period_end", "currency", "amount_cents",
            "status", "created_at", "finalized_at", "paid_at", "line_items",
        }
        assert view["status"] == "failed"
        assert set(view["line_items"][0]) == {
            "description", "quantity", "unit", "unit_price_cents", "amount_cents",
        }
        flat = repr(view)
        assert "mock" not in flat and "card declined" not in flat and "org-a" not in flat

    def test_list_issued_for_org_excludes_drafts_and_other_orgs(self, cp):
        paid = self._issued(cp, period=("2026-03-01", "2026-04-01"), status="paid")
        open_ = self._issued(cp, period=("2026-04-01", "2026-05-01"), status="open")
        self._issued(cp, period=("2026-05-01", "2026-06-01"), status="draft")
        self._issued(cp, org_id="org-b", period=("2026-03-01", "2026-04-01"), status="paid")

        ids = [i["invoice_id"] for i in cp.invoices.list_issued_for_org("org-a")]
        assert ids == [open_["invoice_id"], paid["invoice_id"]]   # newest period first

    def test_list_issued_respects_limit(self, cp):
        for month in (3, 4, 5):
            self._issued(cp, period=(f"2026-0{month}-01", f"2026-0{month + 1}-01"))
        assert len(cp.invoices.list_issued_for_org("org-a", limit=2)) == 2
