"""
Stripe hosted invoices: GCON issues the invoice at Stripe, the customer pays on Stripe's page, and a signed webhook
marks the GCON invoice paid.

Nothing here talks to Stripe. The provider takes its HTTP function as a parameter, so the tests give it a fake that
records every call and answers like Stripe does; the real HTTP function is covered by pointing urllib at errors.
The first run against Stripe itself is done with a test-mode key (see docs/DEPLOYMENT.md).
"""
import hashlib
import hmac
import io
import json
import time
import urllib.error

import pytest
from fastapi.testclient import TestClient

from gcon.api.api_v1 import create_api_v1_app
from gcon.billing import providers
from gcon.billing.invoicing import finalize_invoice
from gcon.billing.providers import PaymentProviderNotConfigured, StripeError, StripePaymentProvider
from gcon.billing.stripe_webhooks import (
    WebhookNotConfigured, WebhookSignatureError, apply_event, verify_signature,
)
from gcon.cluster.coordinator import GCONCoordinator
from gcon.dashboard.presentation import PresentationLayer
from gcon.management.management_layer import ManagementLayer
from gcon.persistence.control_plane import ControlPlane

SECRET = "whsec_test_secret"
ITEMS = [
    {"description": "GPU compute", "quantity": 100.0, "unit": "gpu-second", "unit_price_cents": 1.0, "amount_cents": 100},
    {"description": "Receipts", "quantity": 5.0, "unit": "receipt", "unit_price_cents": 1.0, "amount_cents": 5},
]


class FakeStripe:
    """Answers like Stripe and remembers what it was asked."""

    def __init__(self, fail_on=None, hosted_url="https://invoice.stripe.com/i/acct_1/test_abc"):
        self.calls = []
        self.fail_on = fail_on or set()
        self.hosted_url = hosted_url
        self._n = 0

    def __call__(self, method, path, params, idempotency_key):
        self.calls.append({"method": method, "path": path, "params": params, "key": idempotency_key})
        for fragment in self.fail_on:
            if path.endswith(fragment):
                raise StripeError(f"fake failure on {fragment}", status=500)
        self._n += 1
        if path == "/customers":
            return {"id": f"cus_{self._n}"}
        if path == "/invoices":
            return {"id": "in_123", "status": "draft"}
        if path.endswith("/finalize"):
            return {"id": "in_123", "status": "open", "hosted_invoice_url": self.hosted_url}
        return {"id": f"obj_{self._n}"}

    def paths(self):
        return [c["path"] for c in self.calls]


def stripe_provider(fake, key="sk_test_abc"):
    return StripePaymentProvider(secret_key=key, request_fn=fake, days_until_due=14)


@pytest.fixture
def cp(tmp_path):
    plane = ControlPlane(path=str(tmp_path / "stripe.db"))
    yield plane
    plane.close()


def make_invoice(cp, org="org_a", start="2026-09-01", end="2026-10-01", items=ITEMS):
    return cp.invoices.create(org_id=org, period_start=start, period_end=end, currency="usd",
                              line_items=[dict(i) for i in items])


def contact_for(org_id):
    return {"org_name": "Acme", "email": "billing@acme.example"}


def sign(payload: bytes, secret=SECRET, ts=None):
    ts = int(time.time()) if ts is None else ts
    digest = hmac.new(secret.encode(), f"{ts}.".encode() + payload, hashlib.sha256).hexdigest()
    return f"t={ts},v1={digest}"


def stripe_event(etype, invoice_id, stripe_id="in_123", total=105, event_id="evt_1"):
    return {"id": event_id, "type": etype,
            "data": {"object": {"id": stripe_id, "total": total, "metadata": {"gcon_invoice_id": invoice_id}}}}


# --------------------------------------------------------------- the provider
class TestProviderConfiguration:
    def test_a_missing_key_fails_loudly(self, monkeypatch):
        monkeypatch.delenv("STRIPE_SECRET_KEY", raising=False)
        with pytest.raises(PaymentProviderNotConfigured):
            StripePaymentProvider()

    def test_a_publishable_key_is_refused(self):
        with pytest.raises(PaymentProviderNotConfigured, match="publishable"):
            StripePaymentProvider(secret_key="pk_test_123")

    def test_the_mode_comes_from_the_key(self):
        assert stripe_provider(FakeStripe(), "sk_test_x").customer_scope == "stripe:test"
        assert stripe_provider(FakeStripe(), "rk_live_x").customer_scope == "stripe:live"

    def test_gcon_payment_provider_stripe_selects_it(self, monkeypatch):
        monkeypatch.setenv("GCON_PAYMENT_PROVIDER", "stripe")
        monkeypatch.setenv("STRIPE_SECRET_KEY", "sk_test_abc")
        assert providers.get_configured_provider().name == "stripe"


class TestIssuingAnInvoice:
    def test_creates_attaches_finalizes_and_sends_in_that_order(self, cp):
        fake = FakeStripe()
        invoice = make_invoice(cp)
        issued = stripe_provider(fake).issue(invoice, "cus_9")
        assert fake.paths() == ["/invoices", "/invoiceitems", "/invoiceitems", "/invoices/in_123/finalize",
                                "/invoices/in_123/send"]
        create = fake.calls[0]["params"]
        assert create["customer"] == "cus_9" and create["collection_method"] == "send_invoice"
        assert create["currency"] == "usd" and create["days_until_due"] == 14
        assert create["auto_advance"] is False and create["pending_invoice_items_behavior"] == "exclude"
        assert create["metadata"]["gcon_invoice_id"] == invoice["invoice_id"]
        assert [c["params"]["amount"] for c in fake.calls[1:3]] == [100, 5]
        assert all(c["params"]["invoice"] == "in_123" for c in fake.calls[1:3])
        assert issued.provider_invoice_id == "in_123" and issued.emailed is True
        assert issued.hosted_invoice_url == "https://invoice.stripe.com/i/acct_1/test_abc"

    def test_every_call_carries_an_idempotency_key_from_the_gcon_invoice(self, cp):
        fake = FakeStripe()
        invoice = make_invoice(cp)
        stripe_provider(fake).issue(invoice, "cus_9")
        assert all(c["key"] and invoice["invoice_id"] in c["key"] for c in fake.calls)
        assert len({c["key"] for c in fake.calls}) == len(fake.calls)

    def test_a_failed_email_does_not_undo_an_issued_invoice(self, cp):
        fake = FakeStripe(fail_on={"/send"})
        issued = stripe_provider(fake).issue(make_invoice(cp), "cus_9")
        assert issued.provider_invoice_id == "in_123" and issued.emailed is False
        assert issued.hosted_invoice_url

    def test_line_items_that_do_not_add_up_are_never_sent(self, cp):
        fake = FakeStripe()
        invoice = make_invoice(cp)
        invoice["amount_cents"] += 1
        with pytest.raises(StripeError, match="add up"):
            stripe_provider(fake).issue(invoice, "cus_9")
        assert fake.calls == []

    def test_zero_amount_lines_are_left_out(self, cp):
        fake = FakeStripe()
        zero = {"description": "Included usage", "quantity": 1.0, "unit": "x", "unit_price_cents": 0.0, "amount_cents": 0}
        stripe_provider(fake).issue(make_invoice(cp, items=ITEMS + [zero]), "cus_9")
        assert fake.paths().count("/invoiceitems") == 2

    def test_a_credit_line_is_sent_as_a_negative_amount(self, cp):
        fake = FakeStripe()
        credit = {"description": "Included usage", "quantity": 1.0, "unit": "credit", "unit_price_cents": -20.0, "amount_cents": -20}
        stripe_provider(fake).issue(make_invoice(cp, items=ITEMS + [credit]), "cus_9")
        assert [c["params"]["amount"] for c in fake.calls if c["path"] == "/invoiceitems"] == [100, 5, -20]

    def test_a_stripe_error_stops_the_sequence(self, cp):
        fake = FakeStripe(fail_on={"/invoiceitems"})
        with pytest.raises(StripeError):
            stripe_provider(fake).issue(make_invoice(cp), "cus_9")
        assert "/invoices/in_123/finalize" not in fake.paths()


class TestTheRealHttpFunction:
    def _provider(self):
        return StripePaymentProvider(secret_key="sk_test_SECRETVALUE")

    def test_a_stripe_error_body_becomes_its_message_without_the_key(self, monkeypatch):
        body = json.dumps({"error": {"message": "No such customer: 'cus_x'", "code": "resource_missing"}}).encode()

        def boom(request, timeout=None):
            raise urllib.error.HTTPError(request.full_url, 400, "Bad Request", {}, io.BytesIO(body))
        monkeypatch.setattr("urllib.request.urlopen", boom)
        with pytest.raises(StripeError) as caught:
            self._provider()._call("POST", "/invoices", {"customer": "cus_x"})
        assert "No such customer" in str(caught.value) and caught.value.status == 400
        assert "SECRETVALUE" not in str(caught.value)

    def test_an_unreachable_stripe_is_a_clear_error(self, monkeypatch):
        def down(request, timeout=None):
            raise urllib.error.URLError("connection refused")
        monkeypatch.setattr("urllib.request.urlopen", down)
        with pytest.raises(StripeError, match="Could not reach Stripe"):
            self._provider()._call("POST", "/customers", {})

    def test_the_request_is_form_encoded_with_bearer_auth_and_the_idempotency_key(self, monkeypatch):
        seen = {}

        class Response:
            def __enter__(self): return self
            def __exit__(self, *a): return False
            def read(self): return b'{"id": "cus_1"}'

        def capture(request, timeout=None):
            seen["url"], seen["data"], seen["headers"] = request.full_url, request.data.decode(), dict(request.header_items())
            return Response()
        monkeypatch.setattr("urllib.request.urlopen", capture)
        out = self._provider()._call("POST", "/customers", {"email": "a@b.c", "metadata": {"gcon_org_id": "org_1"}, "flag": True},
                                     idempotency_key="k-1")
        assert out == {"id": "cus_1"} and seen["url"] == "https://api.stripe.com/v1/customers"
        assert "email=a%40b.c" in seen["data"] and "metadata%5Bgcon_org_id%5D=org_1" in seen["data"]
        assert "flag=true" in seen["data"]
        assert seen["headers"]["Authorization"] == "Bearer sk_test_SECRETVALUE"
        assert seen["headers"]["Idempotency-key"] == "k-1"


# --------------------------------------------------------------- finalize_invoice
class TestFinalizeWithStripe:
    def test_the_invoice_becomes_open_with_stripes_id_and_pay_link(self, cp):
        fake = FakeStripe()
        invoice = make_invoice(cp)
        out = finalize_invoice(cp, invoice["invoice_id"], provider=stripe_provider(fake), contact_resolver=contact_for)
        assert out["status"] == "open" and out["provider"] == "stripe" and out["provider_charge_id"] == "in_123"
        assert out["hosted_invoice_url"].startswith("https://invoice.stripe.com/")
        assert out["paid_at"] is None            # nothing is paid until the webhook says so

    def test_the_customer_is_created_once_and_reused(self, cp):
        fake = FakeStripe()
        provider = stripe_provider(fake)
        for start, end in (("2026-08-01", "2026-09-01"), ("2026-09-01", "2026-10-01")):
            finalize_invoice(cp, make_invoice(cp, start=start, end=end)["invoice_id"], provider=provider,
                             contact_resolver=contact_for)
        assert fake.paths().count("/customers") == 1
        created = next(c for c in fake.calls if c["path"] == "/customers")
        assert created["params"]["email"] == "billing@acme.example" and created["params"]["name"] == "Acme"
        assert created["params"]["metadata"] == {"gcon_org_id": "org_a"}

    def test_a_test_key_customer_is_not_reused_under_a_live_key(self, cp):
        fake = FakeStripe()
        finalize_invoice(cp, make_invoice(cp, start="2026-08-01", end="2026-09-01")["invoice_id"],
                         provider=stripe_provider(fake, "sk_test_a"), contact_resolver=contact_for)
        finalize_invoice(cp, make_invoice(cp, start="2026-09-01", end="2026-10-01")["invoice_id"],
                         provider=stripe_provider(fake, "sk_live_a"), contact_resolver=contact_for)
        assert fake.paths().count("/customers") == 2

    def test_a_stripe_failure_leaves_the_invoice_a_draft_and_a_retry_works(self, cp):
        invoice = make_invoice(cp)
        broken = FakeStripe(fail_on={"/finalize"})
        with pytest.raises(StripeError):
            finalize_invoice(cp, invoice["invoice_id"], provider=stripe_provider(broken), contact_resolver=contact_for)
        assert cp.invoices.get(invoice["invoice_id"])["status"] == "draft"
        ok = finalize_invoice(cp, invoice["invoice_id"], provider=stripe_provider(FakeStripe()), contact_resolver=contact_for)
        assert ok["status"] == "open"

    def test_no_billing_email_means_stripe_is_never_called(self, cp):
        fake = FakeStripe()
        invoice = make_invoice(cp)
        for resolver in (None, lambda org: None, lambda org: {"org_name": "Acme", "email": "  "}):
            with pytest.raises(ValueError, match="no billing email"):
                finalize_invoice(cp, invoice["invoice_id"], provider=stripe_provider(fake), contact_resolver=resolver)
        assert fake.calls == [] and cp.invoices.get(invoice["invoice_id"])["status"] == "draft"

    def test_a_zero_amount_invoice_is_settled_without_calling_stripe(self, cp):
        fake = FakeStripe()
        empty = cp.invoices.create(org_id="org_a", period_start="2026-07-01", period_end="2026-08-01",
                                   currency="usd", line_items=[])
        out = finalize_invoice(cp, empty["invoice_id"], provider=stripe_provider(fake), contact_resolver=contact_for)
        assert out["status"] == "paid" and fake.calls == []

    def test_an_already_issued_invoice_is_not_sent_again(self, cp):
        fake = FakeStripe()
        invoice = make_invoice(cp)
        provider = stripe_provider(fake)
        finalize_invoice(cp, invoice["invoice_id"], provider=provider, contact_resolver=contact_for)
        before = len(fake.calls)
        finalize_invoice(cp, invoice["invoice_id"], provider=provider, contact_resolver=contact_for)
        assert len(fake.calls) == before


# --------------------------------------------------------------- the webhook
class TestSignature:
    BODY = b'{"id": "evt_1"}'

    def test_a_correctly_signed_request_passes(self):
        verify_signature(self.BODY, sign(self.BODY), SECRET)

    def test_the_wrong_secret_fails(self):
        with pytest.raises(WebhookSignatureError):
            verify_signature(self.BODY, sign(self.BODY, secret="whsec_other"), SECRET)

    def test_a_changed_body_fails(self):
        with pytest.raises(WebhookSignatureError):
            verify_signature(self.BODY + b" ", sign(self.BODY), SECRET)

    def test_an_old_signature_fails_even_if_genuine(self):
        with pytest.raises(WebhookSignatureError, match="window"):
            verify_signature(self.BODY, sign(self.BODY, ts=int(time.time()) - 3600), SECRET)

    def test_any_one_matching_v1_is_enough_while_a_secret_is_rolled(self):
        header = sign(self.BODY) + ",v1=" + "0" * 64
        verify_signature(self.BODY, header, SECRET)

    @pytest.mark.parametrize("header", ["", "garbage", "t=abc,v1=00", "v1=00", "t=1"])
    def test_a_missing_or_malformed_header_fails(self, header):
        with pytest.raises(WebhookSignatureError):
            verify_signature(self.BODY, header, SECRET)

    def test_no_secret_configured_is_its_own_error(self):
        with pytest.raises(WebhookNotConfigured):
            verify_signature(self.BODY, sign(self.BODY), "")


class TestEvents:
    @pytest.fixture
    def open_invoice(self, cp):
        invoice = make_invoice(cp)
        return finalize_invoice(cp, invoice["invoice_id"], provider=stripe_provider(FakeStripe()), contact_resolver=contact_for)

    def status(self, cp, invoice):
        return cp.invoices.get(invoice["invoice_id"])["status"]

    def test_paid_marks_the_invoice_paid(self, cp, open_invoice):
        out = apply_event(cp, stripe_event("invoice.paid", open_invoice["invoice_id"]))
        assert out == {"outcome": "paid", "invoice_id": open_invoice["invoice_id"]}
        row = cp.invoices.get(open_invoice["invoice_id"])
        assert row["status"] == "paid" and row["paid_at"] and row["hosted_invoice_url"]

    def test_the_same_event_twice_changes_nothing_the_second_time(self, cp, open_invoice):
        apply_event(cp, stripe_event("invoice.paid", open_invoice["invoice_id"]))
        paid_at = cp.invoices.get(open_invoice["invoice_id"])["paid_at"]
        assert apply_event(cp, stripe_event("invoice.paid", open_invoice["invoice_id"]))["outcome"] == "duplicate"
        assert cp.invoices.get(open_invoice["invoice_id"])["paid_at"] == paid_at

    def test_a_late_failure_never_undoes_a_payment(self, cp, open_invoice):
        apply_event(cp, stripe_event("invoice.paid", open_invoice["invoice_id"]))
        apply_event(cp, stripe_event("invoice.payment_failed", open_invoice["invoice_id"]))
        assert self.status(cp, open_invoice) == "paid"

    def test_a_failed_attempt_can_still_be_paid_afterwards(self, cp, open_invoice):
        assert apply_event(cp, stripe_event("invoice.payment_failed", open_invoice["invoice_id"]))["outcome"] == "payment_failed"
        assert self.status(cp, open_invoice) == "failed"
        apply_event(cp, stripe_event("invoice.paid", open_invoice["invoice_id"]))
        assert self.status(cp, open_invoice) == "paid"

    def test_voiding_in_stripe_voids_the_invoice_here(self, cp, open_invoice):
        assert apply_event(cp, stripe_event("invoice.voided", open_invoice["invoice_id"]))["outcome"] == "voided"
        assert self.status(cp, open_invoice) == "void"

    def test_a_payment_for_a_void_invoice_is_flagged_not_applied(self, cp, open_invoice):
        apply_event(cp, stripe_event("invoice.voided", open_invoice["invoice_id"]))
        assert apply_event(cp, stripe_event("invoice.paid", open_invoice["invoice_id"]))["outcome"] == "paid_after_void"
        assert self.status(cp, open_invoice) == "void"

    def test_a_total_that_differs_from_the_invoice_is_not_marked_paid(self, cp, open_invoice):
        out = apply_event(cp, stripe_event("invoice.paid", open_invoice["invoice_id"], total=1))
        assert out["outcome"] == "total_mismatch" and self.status(cp, open_invoice) == "open"

    def test_an_event_naming_a_different_stripe_invoice_is_not_applied(self, cp, open_invoice):
        out = apply_event(cp, stripe_event("invoice.paid", open_invoice["invoice_id"], stripe_id="in_other"))
        assert out["outcome"] == "invoice_mismatch" and self.status(cp, open_invoice) == "open"

    def test_the_invoice_is_also_found_by_stripes_own_id(self, cp, open_invoice):
        event = {"id": "evt_2", "type": "invoice.paid", "data": {"object": {"id": "in_123", "total": 105}}}
        assert apply_event(cp, event)["outcome"] == "paid"

    def test_an_invoice_that_is_not_ours_is_acknowledged_and_ignored(self, cp):
        out = apply_event(cp, stripe_event("invoice.paid", "not-a-gcon-invoice", stripe_id="in_unknown"))
        assert out["outcome"] == "unknown_invoice"

    def test_other_event_types_are_ignored(self, cp, open_invoice):
        assert apply_event(cp, stripe_event("customer.created", open_invoice["invoice_id"]))["outcome"] == "ignored"
        assert self.status(cp, open_invoice) == "open"

    def test_a_draft_invoice_is_never_marked_paid_by_an_event(self, cp):
        draft = make_invoice(cp)
        cp.invoices.mark_status(draft["invoice_id"], "draft", provider="stripe", provider_charge_id="in_123")
        assert apply_event(cp, stripe_event("invoice.paid", draft["invoice_id"]))["outcome"] == "not_issued"
        assert cp.invoices.get(draft["invoice_id"])["status"] == "draft"


# --------------------------------------------------------------- end to end through the real API
@pytest.fixture
def app(tmp_path, monkeypatch):
    monkeypatch.setenv("GCON_STRIPE_WEBHOOK_SECRET", SECRET)
    coordinator = GCONCoordinator(control_plane=ControlPlane(path=str(tmp_path / "e2e.db")))
    management = ManagementLayer(coordinator=coordinator, db_path=str(tmp_path / "e2e-mgmt.db"))
    client = TestClient(create_api_v1_app(management, PresentationLayer(coordinator)))
    yield client, coordinator, management
    coordinator.shutdown()


def signup(client, org, email):
    r = client.post("/auth/signup", json={"org_name": org, "name": "Ann", "email": email, "password": "correct-horse-1"})
    assert r.status_code == 200, r.text
    body = r.json()
    return body["organization"]["org_id"], {"Authorization": f"Bearer {body['api_key']['secret']}"}


class TestThroughTheApi:
    def post_event(self, client, event, secret=SECRET, signature=None):
        payload = json.dumps(event).encode()
        return client.post("/webhooks/stripe", content=payload,
                           headers={"Stripe-Signature": signature or sign(payload, secret)})

    def issue(self, app, monkeypatch, org_name="Acme", email="ann@acme.example"):
        client, coordinator, management = app
        org_id, headers = signup(client, org_name, email)
        invoice = make_invoice(coordinator.control_plane, org=org_id)
        fake = FakeStripe()
        monkeypatch.setattr(providers, "get_configured_provider", lambda: stripe_provider(fake))
        issued = management.finalize_invoice_now(invoice["invoice_id"])
        return org_id, headers, issued, fake

    def test_the_billing_contact_is_the_organizations_signup_email(self, app, monkeypatch):
        org_id, headers, issued, fake = self.issue(app, monkeypatch)
        created = next(c for c in fake.calls if c["path"] == "/customers")
        assert created["params"]["email"] == "ann@acme.example" and created["params"]["name"] == "Acme"
        assert issued["status"] == "open"

    def test_the_customer_sees_the_pay_link_and_only_their_own(self, app, monkeypatch):
        client = app[0]
        org_id, headers, issued, _ = self.issue(app, monkeypatch)
        _, other_headers = signup(client, "Beta", "bob@beta.example")
        mine = client.get("/billing/invoices", headers=headers).json()
        assert mine[0]["status"] == "open" and mine[0]["hosted_invoice_url"].startswith("https://invoice.stripe.com/")
        assert not {"provider", "provider_charge_id", "provider_error"} & set(mine[0])
        assert client.get("/billing/invoices", headers=other_headers).json() == []

    def test_a_signed_paid_event_marks_the_invoice_paid_for_the_customer(self, app, monkeypatch):
        client = app[0]
        org_id, headers, issued, _ = self.issue(app, monkeypatch)
        r = self.post_event(client, stripe_event("invoice.paid", issued["invoice_id"]))
        assert r.status_code == 200 and r.json() == {"received": True, "outcome": "paid"}
        assert client.get("/billing/invoices", headers=headers).json()[0]["status"] == "paid"
        again = self.post_event(client, stripe_event("invoice.paid", issued["invoice_id"]))
        assert again.status_code == 200 and again.json()["outcome"] == "duplicate"

    def test_a_forged_request_is_refused_and_changes_nothing(self, app, monkeypatch):
        client, coordinator, _ = app
        org_id, headers, issued, _ = self.issue(app, monkeypatch)
        r = self.post_event(client, stripe_event("invoice.paid", issued["invoice_id"]), secret="whsec_attacker")
        assert r.status_code == 400
        assert self.post_event(client, stripe_event("invoice.paid", issued["invoice_id"]), signature="nope").status_code == 400
        assert coordinator.control_plane.invoices.get(issued["invoice_id"])["status"] == "open"

    def test_no_webhook_secret_answers_503_so_stripe_retries(self, app, monkeypatch):
        monkeypatch.delenv("GCON_STRIPE_WEBHOOK_SECRET")
        r = app[0].post("/webhooks/stripe", content=b"{}", headers={"Stripe-Signature": "t=1,v1=00"})
        assert r.status_code == 503

    def test_a_signed_event_that_is_not_json_is_acknowledged_and_ignored(self, app):
        payload = b"not json"
        r = app[0].post("/webhooks/stripe", content=payload, headers={"Stripe-Signature": sign(payload)})
        assert r.status_code == 200 and r.json()["outcome"] == "ignored"

    def test_the_webhook_is_not_part_of_the_customer_api_documentation(self, app):
        assert "/webhooks/stripe" not in app[0].get("/openapi.json").json()["paths"]

    def test_finalize_all_counts_issued_invoices_as_issued_not_failed(self, app, monkeypatch):
        client, coordinator, management = app
        org_id, _ = signup(client, "Acme", "ann@acme.example")
        make_invoice(coordinator.control_plane, org=org_id)
        monkeypatch.setattr(providers, "get_configured_provider", lambda: stripe_provider(FakeStripe()))
        counts = management.finalize_draft_invoices()
        assert counts["drafts"] == 1 and counts["issued"] == 1 and counts["failed"] == 0 and counts["paid"] == 0

    def test_finalizing_for_an_org_with_no_customer_account_says_why(self, app, monkeypatch):
        client, coordinator, management = app
        invoice = make_invoice(coordinator.control_plane, org="org_with_nobody")
        monkeypatch.setattr(providers, "get_configured_provider", lambda: stripe_provider(FakeStripe()))
        with pytest.raises(ValueError, match="no billing email"):
            management.finalize_invoice_now(invoice["invoice_id"])


# --------------------------------------------------------------- the whole server, as Stripe and staff really reach it
@pytest.fixture
def server(tmp_path, monkeypatch):
    from gcon.dashboard.web_server import WebServer
    monkeypatch.setenv("GCON_DB_PATH", str(tmp_path / "gcon.db"))
    monkeypatch.setenv("GCON_OWNER_PASSWORD", "owner-pw-123")
    monkeypatch.setenv("GCON_STRIPE_WEBHOOK_SECRET", SECRET)
    coordinator = GCONCoordinator(control_plane=ControlPlane(path=str(tmp_path / "cp.db")))
    web = WebServer(PresentationLayer(coordinator))
    with TestClient(web.app) as client:
        yield client, coordinator
    coordinator.shutdown()


class TestThroughTheFullServer:
    def staff_login(self, client):
        from tests.integration.smoke_management_gaps import OWNER_EMAIL
        r = client.post("/auth/login", json={"email": OWNER_EMAIL, "password": "owner-pw-123"})
        assert r.status_code == 200, r.text

    def test_staff_finalize_shows_stripes_message_then_works_and_the_webhook_pays_it(self, server, monkeypatch):
        client, coordinator = server
        self.staff_login(client)
        org_id, customer_headers = self._signup(client)
        invoice = make_invoice(coordinator.control_plane, org=org_id)
        path = f"/management/billing/invoices/{invoice['invoice_id']}/finalize"

        monkeypatch.setattr(providers, "get_configured_provider",
                            lambda: stripe_provider(FakeStripe(fail_on={"/invoiceitems"})))
        refused = client.post(path)
        assert refused.status_code == 400 and "fake failure" in refused.json()["detail"]

        monkeypatch.setattr(providers, "get_configured_provider", lambda: stripe_provider(FakeStripe()))
        issued = client.post(path)
        assert issued.status_code == 200 and issued.json()["status"] == "open"
        assert issued.json()["hosted_invoice_url"].startswith("https://invoice.stripe.com/")

        payload = json.dumps(stripe_event("invoice.paid", invoice["invoice_id"])).encode()
        hook = client.post("/api/v1/webhooks/stripe", content=payload, headers={"Stripe-Signature": sign(payload)})
        assert hook.status_code == 200 and hook.json()["outcome"] == "paid"
        listed = client.get("/management/billing/invoices", params={"org_id": org_id}).json()
        assert listed[0]["status"] == "paid" and listed[0]["provider"] == "stripe"
        assert client.get("/api/v1/billing/invoices", headers=customer_headers).json()[0]["status"] == "paid"

    def _signup(self, client):
        r = client.post("/api/v1/auth/signup", json={"org_name": "Acme", "name": "Ann", "email": "ann@acme.example",
                                                     "password": "correct-horse-1"})
        assert r.status_code == 200, r.text
        body = r.json()
        return body["organization"]["org_id"], {"Authorization": f"Bearer {body['api_key']['secret']}"}
