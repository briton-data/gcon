"""
Billing/invoicing tests -- `gcon.billing.invoicing` and
`gcon.billing.pricing` had ZERO test coverage before this session
despite being real, DB-backed, wired code (confirmed only via ad-hoc
manual runs in an earlier chat session, never a pytest file). This
covers both the pre-existing compute/LLM-token metering (regression
coverage it never had) and the new Verification/Assurance-decision/
Receipt-proof metering added this session.

Works directly against a real ControlPlane + SQLite DB (tmp_path),
not mocks -- the thing actually being verified is real SQL joins
(_receipts_for_period joining receipts to jobs on org_id) and real
JSON parsing of payload_json, which a mocked repository would hide
bugs in.
"""
import json

import pytest

from gcon.billing.invoicing import (
    build_line_items,
    compute_receipt_usage_totals,
    compute_usage_totals,
    generate_invoice,
)
from gcon.billing.pricing import PricingConfig, estimate_job_cost
from gcon.persistence.control_plane import ControlPlane

PERIOD_START = "2026-01-01T00:00:00+00:00"
PERIOD_END = "2026-02-01T00:00:00+00:00"
IN_PERIOD = "2026-01-15T12:00:00+00:00"


@pytest.fixture
def control_plane(tmp_path):
    cp = ControlPlane(path=str(tmp_path / "billing_test.db"))
    yield cp
    cp.close()


@pytest.fixture
def pricing():
    return PricingConfig(
        gpu_second_cents=1.0,
        llm_input_token_cents=0.1,
        llm_output_token_cents=0.2,
        flat_fee_per_job_cents=0.0,
        verification_check_cents=5.0,
        assurance_decision_cents=2.0,
        receipt_cents=1.0,
        currency="usd",
    )


def _make_job(control_plane, job_id, org_id, runtime_seconds=10.0, tokens=None, completed_at=IN_PERIOD):
    control_plane.jobs.ensure_exists(job_id, "echo hi", org_id=org_id)
    result = {"runtime_seconds": runtime_seconds}
    if tokens is not None:
        result["usage"] = {"llm_tokens": tokens}
    control_plane.jobs.set_status(job_id, "completed", result=result, completed=True)
    # set_status always stamps completed_at with "now" -- backdate it
    # directly so period-filtering tests are deterministic regardless
    # of when the test actually runs.
    control_plane.db.execute(
        "UPDATE jobs SET completed_at = ? WHERE job_id = ?", (completed_at, job_id)
    )


def _make_node(control_plane, node_id, org_id=None):
    control_plane.nodes.upsert(node_id, hostname="test-host", org_id=org_id)


def _make_receipt(control_plane, job_id, node_id, payload, uploaded_at=IN_PERIOD):
    receipt = control_plane.receipts.upload(
        job_id, payload, receipt_hash=f"hash-{job_id}-{node_id}", node_id=node_id,
    )
    control_plane.db.execute(
        "UPDATE receipts SET uploaded_at = ? WHERE receipt_id = ?",
        (uploaded_at, receipt["receipt_id"]),
    )
    return receipt


class TestComputeUsageTotals:
    def test_sums_runtime_and_tokens_across_jobs(self):
        jobs = [
            {"result": {"runtime_seconds": 10.0, "usage": {"llm_tokens": {"input": 100, "output": 50}}}},
            {"result": {"runtime_seconds": 5.0}},
        ]
        totals = compute_usage_totals(jobs)
        assert totals["job_count"] == 2
        assert totals["compute_seconds"] == 15.0
        assert totals["llm_input_tokens"] == 100
        assert totals["llm_output_tokens"] == 50

    def test_empty_job_list_is_all_zeros(self):
        totals = compute_usage_totals([])
        assert totals == {
            "job_count": 0, "compute_seconds": 0.0,
            "llm_input_tokens": 0, "llm_output_tokens": 0,
        }


class TestComputeReceiptUsageTotals:
    def test_every_receipt_counts_once_regardless_of_content(self):
        receipts = [{"payload": {}}, {"payload": {}}, {"payload": {}}]
        totals = compute_receipt_usage_totals(receipts)
        assert totals["receipt_count"] == 3
        assert totals["verification_count"] == 0
        assert totals["assurance_decision_count"] == 0

    def test_execution_proof_present_counts_as_verification(self):
        receipts = [
            {"payload": {"execution_proof": {"agreement": True}}},
            {"payload": {}},
        ]
        totals = compute_receipt_usage_totals(receipts)
        assert totals["receipt_count"] == 2
        assert totals["verification_count"] == 1

    def test_policy_report_present_counts_as_assurance_decision(self):
        receipts = [
            {"payload": {"policy_report": {"trusted": True}}},
            {"payload": {"policy_report": {"trusted": False}}},
            {"payload": {}},
        ]
        totals = compute_receipt_usage_totals(receipts)
        assert totals["receipt_count"] == 3
        # Both trusted AND untrusted decisions are still real decisions
        # that ran -- billed the same either way, this meters whether
        # PolicyEngine.evaluate() ran, not what it concluded.
        assert totals["assurance_decision_count"] == 2

    def test_empty_receipt_list_is_all_zeros(self):
        assert compute_receipt_usage_totals([]) == {
            "receipt_count": 0, "verification_count": 0, "assurance_decision_count": 0,
        }


class TestBuildLineItems:
    def test_zero_usage_produces_no_line_items(self, pricing):
        usage = {
            "job_count": 0, "compute_seconds": 0.0,
            "llm_input_tokens": 0, "llm_output_tokens": 0,
            "receipt_count": 0, "verification_count": 0, "assurance_decision_count": 0,
        }
        assert build_line_items(usage, pricing) == []

    def test_all_six_usage_types_each_produce_their_own_line_item(self, pricing):
        usage = {
            "job_count": 4, "compute_seconds": 100.0,
            "llm_input_tokens": 1000, "llm_output_tokens": 500,
            "receipt_count": 4, "verification_count": 2, "assurance_decision_count": 3,
        }
        items = build_line_items(usage, pricing)
        descriptions = {item["description"] for item in items}
        assert "Compute (measured job runtime)" in descriptions
        assert "LLM input tokens" in descriptions
        assert "LLM output tokens" in descriptions
        assert "Verification (replicated-execution checks run)" in descriptions
        assert "Assurance decisions (policy evaluations run)" in descriptions
        assert "Receipts issued (signed proof of execution)" in descriptions

        verification_item = next(i for i in items if "Verification" in i["description"])
        assert verification_item["quantity"] == 2
        assert verification_item["amount_cents"] == round(2 * pricing.verification_check_cents)

        receipt_item = next(i for i in items if "Receipts issued" in i["description"])
        assert receipt_item["quantity"] == 4
        assert receipt_item["amount_cents"] == round(4 * pricing.receipt_cents)

    def test_missing_new_usage_keys_do_not_crash_backward_compat(self, pricing):
        """A caller still passing the old (pre-this-session) usage
        dict shape -- no receipt_count/verification_count/
        assurance_decision_count keys at all -- must not KeyError."""
        usage = {
            "job_count": 1, "compute_seconds": 10.0,
            "llm_input_tokens": 0, "llm_output_tokens": 0,
        }
        items = build_line_items(usage, pricing)
        assert any("Compute" in i["description"] for i in items)


class TestEstimateJobCost:
    def test_without_receipt_behaves_exactly_as_before(self, pricing):
        result = estimate_job_cost({"runtime_seconds": 10.0}, pricing)
        assert result["compute_cents"] == 10.0 * pricing.gpu_second_cents
        assert result["verification_cents"] is None
        assert result["assurance_decision_cents"] is None
        assert result["receipt_cents"] is None

    def test_with_receipt_but_no_verification_or_policy_report(self, pricing):
        result = estimate_job_cost({"runtime_seconds": 10.0}, pricing, receipt={})
        # Reaching this function with a receipt at all means one was
        # issued -- receipt_cents is unconditional.
        assert result["receipt_cents"] == pricing.receipt_cents
        assert result["verification_cents"] is None
        assert result["assurance_decision_cents"] is None

    def test_with_receipt_carrying_execution_proof_and_policy_report(self, pricing):
        receipt = {
            "execution_proof": {"agreement": True},
            "policy_report": {"trusted": True},
        }
        result = estimate_job_cost({"runtime_seconds": 10.0}, pricing, receipt=receipt)
        assert result["verification_cents"] == pricing.verification_check_cents
        assert result["assurance_decision_cents"] == pricing.assurance_decision_cents
        assert result["receipt_cents"] == pricing.receipt_cents
        expected_total = (
            10.0 * pricing.gpu_second_cents
            + pricing.verification_check_cents
            + pricing.assurance_decision_cents
            + pricing.receipt_cents
        )
        assert result["total_cents"] == pytest.approx(expected_total)


class TestGenerateInvoiceEndToEnd:
    """Real ControlPlane + SQLite, real jobs/nodes/receipts rows --
    verifies the actual SQL join in _receipts_for_period and the full
    generate_invoice() pipeline, not just the pure functions above."""

    def test_invoice_includes_all_six_line_item_types(self, control_plane, pricing):
        _make_node(control_plane, "node-1", org_id="acme")
        _make_job(control_plane, "job-1", org_id="acme", runtime_seconds=20.0,
                   tokens={"input": 200, "output": 100})
        _make_receipt(control_plane, "job-1", "node-1", payload={
            "execution_proof": {"agreement": True},
            "policy_report": {"trusted": True},
        })

        invoice = generate_invoice(control_plane, "acme", PERIOD_START, PERIOD_END, pricing=pricing)

        descriptions = {item["description"] for item in invoice["line_items"]}
        assert len(descriptions) == 6  # compute, input tokens, output tokens, verification, assurance, receipt
        assert invoice["amount_cents"] > 0

    def test_receipts_outside_the_period_are_excluded(self, control_plane, pricing):
        _make_node(control_plane, "node-1", org_id="acme")
        _make_job(control_plane, "job-old", org_id="acme", completed_at="2025-06-01T00:00:00+00:00")
        _make_receipt(control_plane, "job-old", "node-1", payload={"execution_proof": {}},
                       uploaded_at="2025-06-01T00:00:00+00:00")

        invoice = generate_invoice(control_plane, "acme", PERIOD_START, PERIOD_END, pricing=pricing)
        assert invoice["line_items"] == []
        assert invoice["amount_cents"] == 0

    def test_another_orgs_receipts_are_never_billed_to_this_org(self, control_plane, pricing):
        """The same cross-tenant isolation concern as the receipts
        API fix earlier this session, at the billing layer: org A
        must never be charged for org B's verification/receipt
        activity."""
        _make_node(control_plane, "acme-node", org_id="acme")
        _make_node(control_plane, "globex-node", org_id="globex")
        _make_job(control_plane, "job-acme", org_id="acme")
        _make_job(control_plane, "job-globex", org_id="globex")
        _make_receipt(control_plane, "job-acme", "acme-node", payload={"execution_proof": {}})
        _make_receipt(control_plane, "job-globex", "globex-node", payload={"execution_proof": {}})

        acme_invoice = generate_invoice(control_plane, "acme", PERIOD_START, PERIOD_END, pricing=pricing)
        verification_item = next(
            (i for i in acme_invoice["line_items"] if "Verification" in i["description"]), None
        )
        assert verification_item is not None
        assert verification_item["quantity"] == 1  # only acme's own receipt, not globex's too

    def test_idempotent_regenerating_same_period_returns_existing_invoice(self, control_plane, pricing):
        _make_node(control_plane, "node-1", org_id="acme")
        _make_job(control_plane, "job-1", org_id="acme")
        _make_receipt(control_plane, "job-1", "node-1", payload={})

        first = generate_invoice(control_plane, "acme", PERIOD_START, PERIOD_END, pricing=pricing)
        second = generate_invoice(control_plane, "acme", PERIOD_START, PERIOD_END, pricing=pricing)
        assert first["invoice_id"] == second["invoice_id"]
