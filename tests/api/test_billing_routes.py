"""
GET /billing/pricing, /billing/usage, /billing/invoices[/{id}] -- the
customer-facing, read-only billing surface.

Covers: org isolation (404 never 403, a customer cannot enumerate or read
another company's invoices), drafts and gateway details never shown, the
usage preview using only the caller's own data, input validation, and auth.
Invoice generation/finalization stay staff-only; there is deliberately no
customer route that creates or charges anything.
"""
import pytest
from tests.support.label_client import LabelClient as TestClient

from gcon.api.api_v1 import create_api_v1_app
from gcon.cluster.coordinator import GCONCoordinator
from gcon.dashboard.presentation import PresentationLayer
from gcon.management.management_layer import ManagementLayer
from gcon.persistence.control_plane import ControlPlane

INSIDE = "2026-01-15T12:00:00+00:00"


@pytest.fixture
def env(tmp_path, monkeypatch):
    monkeypatch.setenv("GCON_API_RATE_LIMIT_PER_MINUTE", "0")
    coordinator = GCONCoordinator(control_plane=ControlPlane(path=str(tmp_path / "cp.db")))
    management = ManagementLayer(coordinator=coordinator, db_path=str(tmp_path / "mgmt.db"))
    client = TestClient(create_api_v1_app(management, PresentationLayer(coordinator)))
    yield client, coordinator.control_plane, management
    coordinator.shutdown()


def signup(client, org="Acme", email="a@acme.example"):
    r = client.post(
        "/auth/signup",
        json={"org_name": org, "name": "Ann", "email": email, "password": "correct-horse-1"},
    )
    assert r.status_code == 200, r.text
    body = r.json()
    return body["organization"]["org_id"], {"Authorization": f"Bearer {body['api_key']['secret']}"}


def completed_job(cp, job_id, org_id, runtime, completed_at=INSIDE):
    cp.jobs.ensure_exists(job_id, "echo hi", org_id=org_id)
    cp.jobs.set_status(job_id, "completed", result={"runtime_seconds": runtime}, completed=True)
    cp.db.execute("UPDATE jobs SET completed_at = ? WHERE job_id = ?", (completed_at, job_id))


def invoice(cp, org_id, start, end, status="paid", amount=10):
    inv = cp.invoices.create(
        org_id, start, end, "usd",
        [{"description": "Compute (measured job runtime)", "quantity": 10, "unit": "gpu_second",
          "unit_price_cents": 1.0, "amount_cents": amount}],
    )
    if status != "draft":
        cp.invoices.mark_status(
            inv["invoice_id"], status, provider="mock", provider_charge_id="mock_ch_secret",
            provider_error="gateway said no" if status == "failed" else None,
        )
    return inv["invoice_id"]


class TestAuth:
    @pytest.mark.parametrize("path", [
        "/billing/pricing", "/billing/usage", "/billing/invoices", "/billing/invoices/x",
    ])
    def test_every_route_requires_an_api_key(self, env, path):
        client, _, _ = env
        assert client.get(path).status_code == 401
        assert client.get(path, headers={"Authorization": "Bearer gcon_nope"}).status_code == 401


class TestPricing:
    def test_returns_the_price_list(self, env):
        client, _, _ = env
        _, h = signup(client)
        r = client.get("/billing/pricing", headers=h)
        assert r.status_code == 200
        body = r.json()
        assert body["currency"] == "usd"
        assert body["gpu_second_cents"] > 0
        assert set(body) == {
            "currency", "gpu_second_cents", "llm_input_token_cents", "llm_output_token_cents",
            "flat_fee_per_job_cents", "verification_check_cents", "assurance_decision_cents",
            "receipt_cents",
        }


class TestUsage:
    def test_previews_only_the_callers_own_usage(self, env):
        client, cp, _ = env
        acme, h = signup(client, "Acme", "a@acme.example")
        globex, _ = signup(client, "Globex", "g@globex.example")
        completed_job(cp, "a1", acme, runtime=1000)
        completed_job(cp, "g1", globex, runtime=5000)

        r = client.get("/billing/usage",
                       params={"period_start": "2026-01-01", "period_end": "2026-02-01"}, headers=h)
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["usage"]["job_count"] == 1
        assert body["usage"]["compute_seconds"] == 1000.0
        assert body["amount_cents"] == sum(i["amount_cents"] for i in body["line_items"]) > 0
        assert body["period_start"] == "2026-01-01T00:00:00+00:00"

    def test_default_period_needs_no_parameters(self, env):
        client, _, _ = env
        _, h = signup(client)
        r = client.get("/billing/usage", headers=h)
        assert r.status_code == 200
        assert r.json()["period_start"].endswith("-01T00:00:00+00:00")
        assert r.json()["usage"]["job_count"] == 0

    def test_previewing_creates_no_invoice(self, env):
        client, cp, _ = env
        acme, h = signup(client)
        completed_job(cp, "a1", acme, runtime=1000)
        client.get("/billing/usage", params={"period_start": "2026-01-01", "period_end": "2026-02-01"}, headers=h)
        assert cp.invoices.list_for_org(acme) == []

    @pytest.mark.parametrize("params", [
        {"period_start": "2026-01-01"},
        {"period_end": "2026-02-01"},
        {"period_start": "garbage", "period_end": "2026-02-01"},
        {"period_start": "2026-02-01", "period_end": "2026-01-01"},
        {"period_start": "2020-01-01", "period_end": "2026-01-01"},
    ])
    def test_bad_periods_are_a_400_with_a_reason(self, env, params):
        client, _, _ = env
        _, h = signup(client)
        r = client.get("/billing/usage", params=params, headers=h)
        assert r.status_code == 400
        assert r.json()["detail"]


class TestInvoices:
    def test_lists_issued_invoices_of_the_callers_org_only(self, env):
        client, cp, _ = env
        acme, h = signup(client, "Acme", "a@acme.example")
        globex, _ = signup(client, "Globex", "g@globex.example")
        paid = invoice(cp, acme, "2026-03-01", "2026-04-01", "paid")
        open_ = invoice(cp, acme, "2026-04-01", "2026-05-01", "open")
        invoice(cp, acme, "2026-05-01", "2026-06-01", "draft")
        invoice(cp, globex, "2026-03-01", "2026-04-01", "paid")

        r = client.get("/billing/invoices", headers=h)
        assert r.status_code == 200
        assert [i["invoice_id"] for i in r.json()] == [open_, paid]

    def test_never_exposes_gateway_details_or_the_org_id(self, env):
        client, cp, _ = env
        acme, h = signup(client)
        failed = invoice(cp, acme, "2026-03-01", "2026-04-01", "failed")
        for response in (
            client.get("/billing/invoices", headers=h),
            client.get(f"/billing/invoices/{failed}", headers=h),
        ):
            text = response.text
            assert response.status_code == 200
            for secret in ("mock_ch_secret", "gateway said no", "provider", acme):
                assert secret not in text
        assert client.get(f"/billing/invoices/{failed}", headers=h).json()["status"] == "failed"

    def test_detail_includes_line_items(self, env):
        client, cp, _ = env
        acme, h = signup(client)
        inv = invoice(cp, acme, "2026-03-01", "2026-04-01", "paid", amount=42)
        body = client.get(f"/billing/invoices/{inv}", headers=h).json()
        assert body["invoice_id"] == inv
        assert body["amount_cents"] == 42
        assert body["line_items"][0]["unit"] == "gpu_second"
        assert body["paid_at"]

    def test_another_orgs_invoice_is_404_not_403(self, env):
        client, cp, _ = env
        acme, _ = signup(client, "Acme", "a@acme.example")
        _, globex_h = signup(client, "Globex", "g@globex.example")
        inv = invoice(cp, acme, "2026-03-01", "2026-04-01", "paid")
        r = client.get(f"/billing/invoices/{inv}", headers=globex_h)
        assert r.status_code == 404

    def test_drafts_are_404_even_for_the_owner(self, env):
        client, cp, _ = env
        acme, h = signup(client)
        draft = invoice(cp, acme, "2026-03-01", "2026-04-01", "draft")
        assert client.get(f"/billing/invoices/{draft}", headers=h).status_code == 404

    def test_unknown_invoice_is_404(self, env):
        client, _, _ = env
        _, h = signup(client)
        assert client.get("/billing/invoices/does-not-exist", headers=h).status_code == 404

    @pytest.mark.parametrize("limit, ok", [(1, True), (200, True), (0, False), (201, False), ("x", False)])
    def test_limit_is_validated(self, env, limit, ok):
        client, _, _ = env
        _, h = signup(client)
        r = client.get("/billing/invoices", params={"limit": limit}, headers=h)
        assert (r.status_code == 200) is ok
        if not ok:
            assert r.status_code == 422

    def test_limit_caps_the_result(self, env):
        client, cp, _ = env
        acme, h = signup(client)
        for month in (3, 4, 5):
            invoice(cp, acme, f"2026-0{month}-01", f"2026-0{month + 1}-01", "paid")
        assert len(client.get("/billing/invoices", params={"limit": 2}, headers=h).json()) == 2


class TestReadOnly:
    @pytest.mark.parametrize("method", ["post", "put", "delete", "patch"])
    def test_no_customer_route_creates_or_changes_invoices(self, env, method):
        client, _, _ = env
        _, h = signup(client)
        for path in ("/billing/invoices", "/billing/invoices/x", "/billing/usage", "/billing/pricing"):
            assert getattr(client, method)(path, headers=h).status_code in (404, 405)
