"""
Billing hardening: only genuine usage is billed, periods can't be double-billed or invoiced early, pricing
follows where the compute ran, the total is rounded once, and invoicing runs by itself every month.

Real ControlPlane + SQLite and, where it matters, a real coordinator and verifier -- no mocked repositories.
"""
import json
import time
from datetime import datetime, UTC
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from gcon.billing.automation import BillingAutomation, previous_months
from gcon.billing.invoicing import (
    build_line_items, generate_invoice, make_receipt_validator, measure_jobs, normalize_period, preview_usage,
    validate_receipts, with_rounding_adjustment,
)
from gcon.billing.pricing import PricingConfig
from gcon.persistence.control_plane import ControlPlane
from gcon.persistence.repositories.billing import InvoiceOverlapError

JAN = ("2026-01-01T00:00:00+00:00", "2026-02-01T00:00:00+00:00")
IN_JAN = "2026-01-15T12:00:00+00:00"
NOW = datetime(2026, 10, 15, tzinfo=UTC)


@pytest.fixture
def cp(tmp_path):
    plane = ControlPlane(path=str(tmp_path / "hardening.db"))
    yield plane
    plane.close()


@pytest.fixture
def pricing():
    return PricingConfig(gpu_second_cents=1.0, llm_input_token_cents=0.1, llm_output_token_cents=0.2,
                         flat_fee_per_job_cents=0.0, verification_check_cents=5.0, assurance_decision_cents=2.0,
                         receipt_cents=1.0, currency="usd")


def make_job(cp, job_id, org_id, runtime=10.0, tokens=None, completed_at=IN_JAN, timeout=None, raw_result=None):
    cp.jobs.ensure_exists(job_id, "echo hi", org_id=org_id)
    result = raw_result if raw_result is not None else {"runtime_seconds": runtime}
    if tokens is not None and raw_result is None:
        result["usage"] = {"llm_tokens": tokens}
    cp.jobs.set_status(job_id, "completed", result=result, completed=True)
    cp.db.execute("UPDATE jobs SET completed_at = ?, timeout_seconds = ? WHERE job_id = ?", (completed_at, timeout, job_id))


def make_node(cp, node_id, org_id=None):
    cp.nodes.upsert(node_id, hostname="h", org_id=org_id)


def make_receipt(cp, job_id, node_id, payload=None, signature="sig", uploaded_at=IN_JAN):
    if cp.db.query_one("SELECT 1 FROM nodes WHERE node_id = ?", (node_id,)) is None:
        make_node(cp, node_id)                       # receipts.node_id is a foreign key
    r = cp.receipts.upload(job_id, payload if payload is not None else {"job_id": job_id},
                           receipt_hash=f"hash-{job_id}-{node_id}", node_id=node_id, signature=signature)
    cp.db.execute("UPDATE receipts SET uploaded_at = ? WHERE receipt_id = ?", (uploaded_at, r["receipt_id"]))
    return r


# ------------------------------------------------------------------ 1. receipt / usage validation
class TestUsageValidation:
    def test_unusable_runtimes_are_set_aside_not_billed(self):
        jobs = [{"result": {"runtime_seconds": v}} for v in (10.0, -5, float("nan"), float("inf"), "12", True, None)]
        m = measure_jobs(jobs)
        assert m["compute_seconds"] == 10.0
        assert m["excluded"]["invalid_runtime"] == 5          # negative, NaN, inf, string, bool; None is just "no runtime"

    def test_a_runtime_is_capped_at_the_jobs_own_timeout(self):
        m = measure_jobs([{"timeout_seconds": 60, "result": {"runtime_seconds": 9999.0}}])
        assert m["compute_seconds"] == 60.0 and m["excluded"]["clamped_runtime"] == 1

    def test_a_job_without_a_timeout_is_held_to_the_sanity_ceiling(self):
        m = measure_jobs([{"result": {"runtime_seconds": 1e12}}], max_job_seconds=3600)
        assert m["compute_seconds"] == 3600.0

    def test_garbage_token_counts_do_not_crash_the_invoice(self, cp, pricing):
        make_job(cp, "j1", "acme", raw_result={"runtime_seconds": 4, "usage": {"llm_tokens": {"input": "lots", "output": -3}}})
        inv = generate_invoice(cp, "acme", *JAN, pricing=pricing, now=NOW)        # used to raise ValueError
        assert not any("LLM" in i["description"] for i in inv["line_items"])

    def test_without_a_validator_receipts_are_counted_as_before(self, cp, pricing):
        make_job(cp, "j1", "acme"); make_receipt(cp, "j1", "n1", signature=None)
        inv = generate_invoice(cp, "acme", *JAN, pricing=pricing, now=NOW)
        assert any("Receipts issued" in i["description"] for i in inv["line_items"])

    def test_with_a_validator_unsigned_and_forged_receipts_are_not_billed(self):
        receipts = [
            {"receipt_id": "ok", "job_id": "a", "signature": "s", "payload": {"job_id": "a", "good": True}},
            {"receipt_id": "unsigned", "job_id": "b", "signature": None, "payload": {"job_id": "b", "good": True}},
            {"receipt_id": "forged", "job_id": "c", "signature": "s", "payload": {"job_id": "c", "good": False}},
            {"receipt_id": "other-job", "job_id": "d", "signature": "s", "payload": {"job_id": "zzz", "good": True}},
            {"receipt_id": "empty", "job_id": "e", "signature": "s", "payload": {}},
            {"receipt_id": "ok", "job_id": "a", "signature": "s", "payload": {"job_id": "a", "good": True}},
        ]
        valid, rejected = validate_receipts(receipts, lambda p: p["good"])
        assert [r["receipt_id"] for r in valid] == ["ok"]
        assert rejected == {"malformed_receipt": 1, "job_mismatch_receipt": 1, "duplicate_receipt": 1, "bad_signature_receipt": 2}

    def test_a_receipt_for_a_job_that_is_not_completed_is_not_billed(self, cp, pricing):
        make_job(cp, "j1", "acme"); make_receipt(cp, "j1", "n1")
        cp.jobs.set_status("j1", "failed", result={})
        inv = generate_invoice(cp, "acme", *JAN, pricing=pricing, now=NOW)
        assert inv["line_items"] == []

    def test_real_receipts_pass_the_real_verifier_and_tampered_ones_do_not(self, tmp_path):
        from gcon.cluster.coordinator import GCONCoordinator
        from gcon.execution.agent import GCONAgent

        class Node(GCONAgent):
            sandboxed = True

            def execute_job(self, job_id, job_script, timeout=None, usage_report_path=None, stage_report_path=None):
                return {"job_id": job_id, "status": "success", "return_code": 0, "runtime_seconds": 2.0, "stdout": "ok\n",
                        "stderr": "", "usage": None, "stages": [], "timestamp": datetime.now(UTC).isoformat(),
                        "metrics": {"job_id": job_id, "gpu_name": "x", "gpu_memory_total": 0, "gpu_memory_used": 0,
                                    "cpu_percent": 0, "memory_percent": 0, "runtime_seconds": 2.0,
                                    "timestamp": datetime.now(UTC).isoformat()}}

        plane = ControlPlane(path=str(tmp_path / "real.db"))
        co = GCONCoordinator(control_plane=plane)
        try:
            node = Node("n1"); node.org_id = "acme"; co.register_agent(node)
            for jid in ("good", "forged"):
                co.submit_job(jid, "echo hi", org_id="acme")
            deadline = time.time() + 30
            while time.time() < deadline and len(plane.db.query("SELECT 1 FROM receipts")) < 2:
                time.sleep(0.2)
            rows = plane.db.query("SELECT receipt_id, job_id, payload_json FROM receipts")
            assert len(rows) == 2
            for row in rows:
                plane.db.execute("UPDATE receipts SET uploaded_at = ? WHERE receipt_id = ?", (IN_JAN, row["receipt_id"]))
                plane.db.execute("UPDATE jobs SET completed_at = ? WHERE job_id = ?", (IN_JAN, row["job_id"]))
            forged = next(r for r in rows if r["job_id"] == "forged")
            payload = json.loads(forged["payload_json"]); payload["proof"]["job_id"] = "somebody-else"
            plane.db.execute("UPDATE receipts SET payload_json = ? WHERE receipt_id = ?", (json.dumps(payload), forged["receipt_id"]))

            validator = make_receipt_validator(co.verifier)
            usage = preview_usage(plane, "acme", *JAN, now=NOW, receipt_validator=validator)["usage"]
            assert usage["receipt_count"] == 1                      # the forged one is not billed
            assert usage["excluded"]["bad_signature_receipt"] == 1
            assert usage["job_count"] == 2                          # the jobs themselves still ran
        finally:
            co.shutdown()


# ------------------------------------------------------------------ 2. billing periods
class TestPeriods:
    @pytest.mark.parametrize("a,b", [("2026-01-01", "2026-02-01"), ("2026-01-01T00:00:00Z", "2026-02-01T00:00:00Z"),
                                      ("2026-01-01T00:00:00+00:00", "2026-02-01T00:00:00+00:00"),
                                      ("2026-01-01T02:00:00+02:00", "2026-02-01T02:00:00+02:00")])
    def test_every_spelling_of_a_period_is_the_same_period(self, a, b):
        assert normalize_period(a, b, now=NOW) == JAN

    def test_bad_periods_are_refused_with_a_reason(self):
        with pytest.raises(ValueError, match="after period_start"):
            normalize_period(JAN[1], JAN[0], now=NOW)
        with pytest.raises(ValueError, match="at most"):
            normalize_period("2024-01-01", "2026-01-01", now=NOW)
        with pytest.raises(ValueError, match="not ended yet"):
            normalize_period("2026-10-01", "2026-11-01", now=NOW)
        with pytest.raises(ValueError, match="ISO-8601"):
            normalize_period("last month", "this month", now=NOW)

    def test_the_same_period_spelled_two_ways_makes_one_invoice(self, cp, pricing):
        make_job(cp, "j1", "acme")
        a = generate_invoice(cp, "acme", "2026-01-01", "2026-02-01", pricing=pricing, now=NOW)
        b = generate_invoice(cp, "acme", "2026-01-01T00:00:00Z", "2026-02-01T00:00:00Z", pricing=pricing, now=NOW)
        assert a["invoice_id"] == b["invoice_id"]
        assert len(cp.invoices.list_for_org("acme")) == 1

    def test_an_overlapping_period_is_refused_but_adjacent_and_other_orgs_are_fine(self, cp, pricing):
        generate_invoice(cp, "acme", *JAN, pricing=pricing, now=NOW)
        with pytest.raises(InvoiceOverlapError, match="overlaps invoice"):
            generate_invoice(cp, "acme", "2026-01-15", "2026-02-15", pricing=pricing, now=NOW)
        with pytest.raises(InvoiceOverlapError):
            generate_invoice(cp, "acme", "2025-12-15", "2026-01-02", pricing=pricing, now=NOW)
        with pytest.raises(InvoiceOverlapError):                     # fully inside
            generate_invoice(cp, "acme", "2026-01-10", "2026-01-20", pricing=pricing, now=NOW)
        generate_invoice(cp, "acme", "2026-02-01", "2026-03-01", pricing=pricing, now=NOW)       # adjacent
        generate_invoice(cp, "other", *JAN, pricing=pricing, now=NOW)                             # different org
        assert len(cp.invoices.list_for_org("acme")) == 2

    def test_an_older_invoice_stored_with_another_spelling_still_counts(self, cp, pricing):
        cp.invoices.create("acme", "2026-01-01", "2026-02-01", "usd", [])        # as earlier versions stored it
        again = generate_invoice(cp, "acme", *JAN, pricing=pricing, now=NOW)
        assert again["period_start"] == "2026-01-01" and len(cp.invoices.list_for_org("acme")) == 1
        with pytest.raises(InvoiceOverlapError):
            generate_invoice(cp, "acme", "2026-01-20", "2026-02-20", pricing=pricing, now=NOW)

    def test_a_period_still_running_cannot_be_invoiced(self, cp, pricing):
        with pytest.raises(ValueError, match="not ended yet"):
            generate_invoice(cp, "acme", "2026-10-01", "2026-11-01", pricing=pricing, now=NOW)
        assert cp.invoices.list_for_org("acme") == []


# ------------------------------------------------------------------ 3. rounding and pool pricing
class TestRounding:
    def _usage(self, **kw):
        base = {"job_count": 1, "compute_seconds": 0.0, "llm_input_tokens": 0, "llm_output_tokens": 0}
        return {**base, **kw}

    def test_half_a_cent_rounds_up_not_to_zero(self, pricing):
        p = PricingConfig(**{**pricing.to_dict(), "gpu_second_cents": 0.05})
        items = build_line_items(self._usage(compute_seconds=10.0), p)          # 0.5 cents
        assert items[0]["amount_cents"] == 1

    def test_the_total_is_the_exact_usage_rounded_once(self, pricing):
        p = PricingConfig(**{**pricing.to_dict(), "gpu_second_cents": 0.4, "receipt_cents": 0.4})
        items = with_rounding_adjustment(build_line_items(self._usage(compute_seconds=1, receipt_count=1), p))
        assert [i["amount_cents"] for i in items[:2]] == [0, 0]                  # each line alone would be 0
        assert sum(i["amount_cents"] for i in items) == 1                        # 0.8 cents rounds to 1
        assert items[-1]["description"] == "Rounding adjustment" and items[-1]["amount_cents"] == 1

    def test_lines_that_round_up_are_corrected_back_down(self, pricing):
        p = PricingConfig(**{**pricing.to_dict(), "gpu_second_cents": 0.5, "receipt_cents": 0.5})
        items = with_rounding_adjustment(build_line_items(self._usage(compute_seconds=1, receipt_count=1), p))
        assert sum(i["amount_cents"] for i in items) == 1 and items[-1]["amount_cents"] == -1

    def test_no_adjustment_line_when_nothing_needs_adjusting(self, pricing):
        items = with_rounding_adjustment(build_line_items(self._usage(compute_seconds=100.0), pricing))
        assert all(i["description"] != "Rounding adjustment" for i in items)

    def test_the_preview_and_the_invoice_agree_to_the_cent(self, cp, pricing):
        p = PricingConfig(**{**pricing.to_dict(), "gpu_second_cents": 0.05, "receipt_cents": 0.005})
        for n in range(5):
            make_job(cp, f"j{n}", "acme", runtime=2.0); make_receipt(cp, f"j{n}", "n1")
        preview = preview_usage(cp, "acme", *JAN, pricing=p, now=NOW)
        invoice = generate_invoice(cp, "acme", *JAN, pricing=p, now=NOW)
        assert invoice["amount_cents"] == preview["amount_cents"] == 1                # 10s x 0.05 + 5 x 0.005 = 0.525
        assert sum(i["amount_cents"] for i in invoice["line_items"]) == invoice["amount_cents"]


class TestPoolPricing:
    def _setup(self, cp):
        make_node(cp, "mine", org_id="acme"); make_node(cp, "pool", org_id=None); make_node(cp, "theirs", org_id="other")
        for jid, node, secs in (("a", "mine", 10.0), ("b", "pool", 30.0), ("c", "theirs", 5.0), ("d", None, 20.0)):
            make_job(cp, jid, "acme", runtime=secs)
            if node:
                make_receipt(cp, jid, node)

    def test_own_worker_compute_is_priced_apart_from_shared_pool_compute(self, cp, pricing):
        self._setup(cp)
        p = PricingConfig(**{**pricing.to_dict(), "own_worker_gpu_second_cents": 0.25, "receipt_cents": 0.0})
        items = generate_invoice(cp, "acme", *JAN, pricing=p, now=NOW)["line_items"]
        shared = next(i for i in items if i["description"] == "Compute (shared pool)")
        own = next(i for i in items if i["description"] == "Compute (your own workers)")
        assert (own["quantity"], own["unit_price_cents"], own["amount_cents"]) == (10.0, 0.25, 3)   # 2.5 -> 3
        assert shared["quantity"] == 55.0 and shared["amount_cents"] == 55       # another org's node, the pool and an unknown node

    def test_without_an_own_worker_rate_nothing_changes(self, cp, pricing):
        self._setup(cp)
        items = generate_invoice(cp, "acme", *JAN, pricing=pricing, now=NOW)["line_items"]
        compute = [i for i in items if i["description"].startswith("Compute")]
        assert len(compute) == 1 and compute[0]["description"] == "Compute (measured job runtime)" and compute[0]["quantity"] == 65.0

    def test_an_own_rate_equal_to_the_standard_rate_is_one_line(self, cp, pricing):
        self._setup(cp)
        p = PricingConfig(**{**pricing.to_dict(), "own_worker_gpu_second_cents": 1.0})
        assert len([i for i in generate_invoice(cp, "acme", *JAN, pricing=p, now=NOW)["line_items"]
                    if i["description"].startswith("Compute")]) == 1

    def test_a_free_own_worker_rate_removes_that_compute_from_the_bill(self, cp, pricing):
        make_node(cp, "mine", org_id="acme"); make_job(cp, "a", "acme", runtime=100.0); make_receipt(cp, "a", "mine")
        p = PricingConfig(**{**pricing.to_dict(), "own_worker_gpu_second_cents": 0.0, "receipt_cents": 0.0})
        inv = generate_invoice(cp, "acme", *JAN, pricing=p, now=NOW)
        assert inv["amount_cents"] == 0


# ------------------------------------------------------------------ 4. automatic monthly invoicing
SEP = "2026-09-10T12:00:00+00:00"
AUG = "2026-08-10T12:00:00+00:00"


def automation(cp, leader=True, verifier=None):
    return BillingAutomation(SimpleNamespace(control_plane=cp, verifier=verifier,
                                              leader_elector=SimpleNamespace(is_leader=leader)))


class TestAutomaticInvoicing:
    def test_the_window_is_the_complete_months_before_now(self):
        months = previous_months(datetime(2026, 1, 20, tzinfo=UTC), 3)
        assert [m[0].strftime("%Y-%m") for m in months] == ["2025-10", "2025-11", "2025-12"]
        assert months[-1][1].isoformat() == "2026-01-01T00:00:00+00:00"

    def test_every_org_with_usage_gets_one_draft_per_month_and_nothing_else(self, cp):
        make_job(cp, "s1", "acme", completed_at=SEP); make_job(cp, "s2", "acme", completed_at=SEP)
        make_job(cp, "a1", "acme", completed_at=AUG); make_job(cp, "s3", "beta", completed_at=SEP)
        make_job(cp, "cur", "gamma", completed_at="2026-10-05T00:00:00+00:00")       # the month still running
        out = automation(cp).run_once(now=NOW)
        assert out["generated"] == 3 and out["months"] == ["2026-07", "2026-08", "2026-09"]
        assert {(i["org_id"], i["period_start"][:7], i["status"]) for i in cp.invoices.list_all(limit=50)} == {
            ("acme", "2026-09", "draft"), ("acme", "2026-08", "draft"), ("beta", "2026-09", "draft")}

    def test_running_again_creates_nothing_new(self, cp):
        make_job(cp, "s1", "acme", completed_at=SEP)
        a = automation(cp)
        a.run_once(now=NOW)
        again = a.run_once(now=NOW)
        assert again["generated"] == 0 and again["already_invoiced"] == 1 and len(cp.invoices.list_all(limit=50)) == 1

    def test_a_month_missed_is_picked_up_on_the_next_run(self, cp):
        make_job(cp, "s1", "acme", completed_at=AUG)
        automation(cp).run_once(now=datetime(2026, 9, 3, tzinfo=UTC), months=1)          # only July..August window
        assert len(cp.invoices.list_all(limit=50)) == 1
        make_job(cp, "s2", "acme", completed_at=SEP)
        automation(cp).run_once(now=NOW)
        assert {i["period_start"][:7] for i in cp.invoices.list_all(limit=50)} == {"2026-08", "2026-09"}

    def test_a_hand_made_invoice_that_overlaps_is_skipped_and_reported(self, cp):
        make_job(cp, "s1", "acme", completed_at=SEP)
        generate_invoice(cp, "acme", "2026-09-15", "2026-10-01", now=NOW)               # staff billed half of September
        out = automation(cp).run_once(now=NOW)
        assert out["generated"] == 0 and out["overlap_skipped"] == 1 and "overlaps invoice" in out["errors"][0]

    def test_drafts_are_only_charged_when_the_operator_opts_in(self, cp, monkeypatch):
        make_job(cp, "s1", "acme", runtime=1000.0, completed_at=SEP)
        out = automation(cp).run_once(now=NOW)
        assert out["finalized"] == 0 and cp.invoices.list_all(limit=5)[0]["status"] == "draft"
        monkeypatch.setenv("GCON_BILLING_AUTO_FINALIZE", "1")
        out = automation(cp).run_once(now=NOW)                  # the existing draft is charged on the next run
        assert out["finalized"] == 1 and cp.invoices.list_all(limit=5)[0]["status"] == "paid"

    def test_an_invalid_receipt_is_not_billed_by_the_automatic_run(self, cp):
        make_job(cp, "s1", "acme", completed_at=SEP); make_receipt(cp, "s1", "n1", uploaded_at=SEP)
        rejecting = SimpleNamespace(validate_proof=lambda proof: (False, "bad"))
        automation(cp, verifier=rejecting).run_once(now=NOW)
        items = cp.invoices.list_all(limit=5)[0]["line_items"]
        assert not any("Receipts issued" in i["description"] for i in items)

    def test_the_result_is_remembered_and_shown_in_the_status(self, cp):
        make_job(cp, "s1", "acme", completed_at=SEP)
        a = automation(cp)
        a.run_once(now=NOW, trigger="manual")
        status = a.status()
        assert status["last_run"]["generated"] == 1 and status["last_run"]["trigger"] == "manual"
        assert status["payment_provider"] == "mock" and status["moves_real_money"] is False
        assert status["enabled"] is True and status["auto_finalize"] is False

    def test_it_runs_only_on_the_leader_and_only_when_enabled(self, cp, monkeypatch):
        monkeypatch.setenv("GCON_BILLING_FIRST_RUN_DELAY", "0")
        assert automation(cp, leader=False).maybe_tick() is False
        monkeypatch.setenv("GCON_BILLING_AUTO_INVOICE", "0")
        assert automation(cp).maybe_tick() is False

    def test_the_scheduled_run_starts_when_due_and_not_twice(self, cp, monkeypatch):
        monkeypatch.setenv("GCON_BILLING_FIRST_RUN_DELAY", "0")
        a = automation(cp)
        assert a.maybe_tick() is True and a.maybe_tick() is False       # second call: not due again for an hour
        deadline = time.time() + 10
        while time.time() < deadline and (a.last_run() is None or a.status()["running"]):
            time.sleep(0.05)
        assert a.last_run()["trigger"] == "schedule"

    def test_a_manual_run_while_one_is_in_progress_is_refused(self, cp):
        a = automation(cp)
        assert a._run_lock.acquire(blocking=False)
        try:
            with pytest.raises(ValueError, match="already in progress"):
                a.run_once(now=NOW)
        finally:
            a._run_lock.release()


# ------------------------------------------------------------------ dashboard routes (staff)
@pytest.fixture
def staff(tmp_path, monkeypatch):
    from gcon.cluster.coordinator import GCONCoordinator
    from gcon.dashboard.presentation import PresentationLayer
    from gcon.dashboard.web_server import WebServer
    monkeypatch.setenv("GCON_DB_PATH", str(tmp_path / "gcon.db"))
    monkeypatch.setenv("GCON_OWNER_PASSWORD", "owner-pw-123")
    coordinator = GCONCoordinator(control_plane=ControlPlane(path=str(tmp_path / "cp.db")))
    server = WebServer(PresentationLayer(coordinator))
    with TestClient(server.app) as client:
        yield client, coordinator
    coordinator.shutdown()


def login(client):
    from tests.integration.smoke_management_gaps import OWNER_EMAIL
    r = client.post("/auth/login", json={"email": OWNER_EMAIL, "password": "owner-pw-123"})
    assert r.status_code == 200, r.text


class TestStaffBillingRoutes:
    def test_billing_routes_need_a_login(self, staff):
        client, _ = staff
        for method, path in (("get", "/management/billing/status"), ("post", "/management/billing/run"),
                             ("post", "/management/billing/pricing"), ("post", "/management/billing/invoices/finalize-drafts")):
            assert getattr(client, method)(path).status_code in (401, 403), path

    def test_status_says_what_automation_and_payments_are_doing(self, staff):
        client, _ = staff; login(client)
        body = client.get("/management/billing/status").json()
        assert body["available"] and body["enabled"] and body["payment_provider"] == "mock" and body["moves_real_money"] is False

    def test_run_now_returns_the_summary_and_rejects_a_silly_window(self, staff):
        client, co = staff; login(client)
        make_job(co.control_plane, "s1", "acme", completed_at="2026-01-10T00:00:00+00:00")
        out = client.post("/management/billing/run", json={"months": 24}).json()
        assert out["trigger"] == "manual" and out["generated"] == 1 and len(out["months"]) == 24
        assert client.post("/management/billing/run", json={"months": 0}).status_code == 400
        assert client.post("/management/billing/run", json={"months": "3"}).status_code == 400

    def test_prices_can_be_changed_and_bad_ones_are_refused(self, staff, monkeypatch):
        client, _ = staff; login(client)
        ok = client.post("/management/billing/pricing", json={"own_worker_gpu_second_cents": 0.01})
        assert ok.status_code == 200 and ok.json()["own_worker_gpu_second_cents"] == 0.01
        assert client.get("/management/billing/pricing").json()["own_worker_gpu_second_cents"] == 0.01
        assert client.post("/management/billing/pricing", json={"own_worker_gpu_second_cents": None}).json()["own_worker_gpu_second_cents"] is None
        for bad in ({"gpu_second_cents": -1}, {"gpu_second_cents": "free"}, {"nonsense": 1}, {}, {"currency": "dollars"}):
            assert client.post("/management/billing/pricing", json=bad).status_code == 400, bad
        monkeypatch.setenv("GCON_PRICE_GPU_SECOND_CENTS", "0.07")
        r = client.post("/management/billing/pricing", json={"gpu_second_cents": 0.2})
        assert r.status_code == 400 and "GCON_PRICE_GPU_SECOND_CENTS" in r.json()["detail"]

    def test_generate_enforces_the_period_rules(self, staff):
        client, _ = staff; login(client)
        gen = lambda s, e: client.post("/management/billing/invoices/generate", json={"org_id": "acme", "period_start": s, "period_end": e})
        assert gen("2099-01-01", "2099-02-01").status_code == 400
        assert gen("2026-01-01", "2026-02-01").status_code == 200
        dup = gen("2026-01-01T00:00:00Z", "2026-02-01T00:00:00Z")
        assert dup.status_code == 200
        overlap = gen("2026-01-20", "2026-02-20")
        assert overlap.status_code == 400 and "overlaps invoice" in overlap.json()["detail"]

    def test_finalize_all_drafts_charges_each_and_reports_counts(self, staff):
        client, co = staff; login(client)
        make_job(co.control_plane, "j1", "acme", runtime=1000.0); make_job(co.control_plane, "j2", "beta", runtime=1000.0)
        for org in ("acme", "beta"):
            client.post("/management/billing/invoices/generate", json={"org_id": org, "period_start": "2026-01-01", "period_end": "2026-02-01"})
        out = client.post("/management/billing/invoices/finalize-drafts", json={}).json()
        assert out["drafts"] == 2 and out["paid"] == 2 and out["failed"] == 0
        assert client.post("/management/billing/invoices/finalize-drafts", json={}).json()["drafts"] == 0


# ------------------------------------------------------------------ the dashboard can't run a stale script
class TestStaticAssetVersion:
    def test_pages_reference_static_files_with_a_version(self, staff):
        import re
        client, _ = staff
        html = client.get("/login").text
        assert re.search(r"/static/css/dashboard\.css\?v=[0-9a-f]{10}\b", html)

    def test_the_version_changes_when_a_static_file_changes(self, tmp_path):
        from gcon.dashboard.web_server import WebServer
        (tmp_path / "dashboard.js").write_text("one")
        first = WebServer._asset_version(str(tmp_path))
        assert first == WebServer._asset_version(str(tmp_path))          # stable while nothing changes
        (tmp_path / "dashboard.js").write_text("two")
        assert WebServer._asset_version(str(tmp_path)) != first
        assert WebServer._asset_version(str(tmp_path / "missing")) == "0"


# ------------------------------------------------------------------ line items are stored portably
class TestLineItemStorage:
    PRICES = (0.05, 0.005, 0.0001, 12.0, 0.3)

    def test_exact_prices_and_order_survive(self, cp):
        inv = cp.invoices.create("acme", *JAN, "usd", [
            {"description": f"line {i}", "quantity": 1, "unit": "u", "unit_price_cents": p, "amount_cents": i}
            for i, p in enumerate(self.PRICES)])
        got = cp.invoices.get(inv["invoice_id"])
        assert [li["description"] for li in got["line_items"]] == [f"line {i}" for i in range(5)]
        assert [li["unit_price_cents"] for li in got["line_items"]] == list(self.PRICES)
        assert set(got["line_items"][0]) == {"line_item_id", "invoice_id", "description", "quantity", "unit",
                                              "unit_price_cents", "amount_cents"}

    def test_an_invoice_written_before_the_new_columns_still_reads_the_same(self, cp):
        cp.db.execute("INSERT INTO invoices (invoice_id, org_id, period_start, period_end, currency, amount_cents, status, created_at) "
                      "VALUES ('legacy', 'acme', '2025-01-01', '2025-02-01', 'usd', 3, 'draft', '2025-02-01')")
        for i, price in enumerate((0.05, 2.0, 0.5)):
            cp.db.execute("INSERT INTO invoice_line_items (line_item_id, invoice_id, description, quantity, unit, unit_price_cents, amount_cents) "
                          "VALUES (?, 'legacy', ?, 1, 'u', ?, 1)", (f"l{i}", f"old {i}", price))
        items = cp.invoices.get("legacy")["line_items"]
        assert [li["description"] for li in items] == ["old 0", "old 1", "old 2"]          # storage order, as before
        assert [li["unit_price_cents"] for li in items] == [0.05, 2.0, 0.5]                 # the legacy column is used
