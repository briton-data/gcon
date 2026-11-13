"""
PaymentProvider — the seam a real payment gateway integration plugs
into. Charging a real card/bank account needs a provider account and
API credentials this environment does not have and cannot fabricate,
so this module ships:

  * `PaymentProvider` -- the abstract interface `finalize_invoice`
    (billing/api_v1 route) calls.
  * `ManualInvoiceProvider` -- invoicing by hand, and the default. Nothing is charged: finalizing an invoice
    issues it (status `open`) and staff record the payment when it arrives (invoicing.record_manual_payment).
  * `MockPaymentProvider` -- a real, working implementation with no
    external dependency: it "charges" by deterministically succeeding
    or failing (configurable) and recording the attempt, useful for
    tests, demos, and any deployment that genuinely doesn't need real
    money to move (e.g. usage tracked for internal chargeback only).
  * `StripePaymentProvider` -- Stripe-hosted invoices. Finalizing an invoice creates the matching invoice at Stripe, which
    emails the customer and hosts the page where they pay; a signed webhook (see `gcon.billing.stripe_webhooks`)
    marks the GCON invoice paid. GCON never sees card details.
"""

from __future__ import annotations

import json
import logging
import os
import urllib.error
import urllib.parse
import urllib.request
import uuid
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Protocol, Tuple

_log = logging.getLogger(__name__)


class PaymentProviderNotConfigured(RuntimeError):
    pass


@dataclass
class ChargeResult:
    success: bool
    provider: str
    provider_charge_id: Optional[str]
    error: Optional[str] = None


class PaymentProvider(Protocol):
    name: str

    def charge(self, invoice: Dict[str, Any], org_billing_ref: Optional[str]) -> ChargeResult:
        """Attempt to charge `invoice['amount_cents']` (in
        `invoice['currency']`) against whatever payment method
        `org_billing_ref` identifies for this org. Must never raise
        for an ordinary decline/failure -- return
        `ChargeResult(success=False, ...)` instead; raising is
        reserved for the provider itself being unusable (e.g. not
        configured)."""
        ...


class ManualInvoiceProvider:
    """Invoicing by hand. `finalize_invoice` sees `issues_only` and issues the invoice instead of charging anything,
    so an invoice is never shown as paid until staff record a real payment."""

    name = "manual"
    issues_only = True

    def charge(self, invoice: Dict[str, Any], org_billing_ref: Optional[str]) -> ChargeResult:
        raise PaymentProviderNotConfigured("Invoices are issued by hand; record the payment instead of charging.")


class MockPaymentProvider:
    """No external calls. `always_succeed=True` (default) charges
    everything; set False (or export
    GCON_MOCK_PAYMENTS_ALWAYS_FAIL=true) to exercise the failure
    path -- e.g. for testing dunning/retry UI without a real
    provider."""

    name = "mock"

    def __init__(self, always_succeed: Optional[bool] = None):
        if always_succeed is None:
            always_succeed = os.environ.get(
                "GCON_MOCK_PAYMENTS_ALWAYS_FAIL", "false"
            ).strip().lower() not in ("1", "true", "yes")
        self.always_succeed = always_succeed

    def charge(self, invoice: Dict[str, Any], org_billing_ref: Optional[str]) -> ChargeResult:
        if self.always_succeed:
            return ChargeResult(
                success=True, provider=self.name,
                provider_charge_id=f"mock_ch_{uuid.uuid4().hex[:16]}",
            )
        return ChargeResult(
            success=False, provider=self.name, provider_charge_id=None,
            error="mock provider configured to always fail (GCON_MOCK_PAYMENTS_ALWAYS_FAIL)",
        )


class StripeError(ValueError):
    """Stripe refused a request or could not be reached. The message is Stripe's own (never contains the key).
    A ValueError so the staff finalize routes report it to the person who pressed the button."""

    def __init__(self, message: str, status: Optional[int] = None, code: Optional[str] = None):
        super().__init__(message)
        self.status = status
        self.code = code


@dataclass
class HostedInvoice:
    provider_invoice_id: str
    hosted_invoice_url: Optional[str]
    emailed: bool


def _form_pairs(params: Dict[str, Any], prefix: str = "") -> List[Tuple[str, str]]:
    """Stripe's form encoding: nested dicts become `metadata[key]=value`; booleans are 'true'/'false'."""
    pairs: List[Tuple[str, str]] = []
    for key, value in params.items():
        name = f"{prefix}[{key}]" if prefix else key
        if value is None:
            continue
        if isinstance(value, dict):
            pairs.extend(_form_pairs(value, name))
        elif isinstance(value, bool):
            pairs.append((name, "true" if value else "false"))
        else:
            pairs.append((name, str(value)))
    return pairs


class StripePaymentProvider:
    """
    Stripe-hosted invoices. Nothing is charged by GCON: finalizing a GCON invoice creates a matching invoice at
    Stripe (`collection_method=send_invoice`), finalizes it and asks Stripe to email it. The customer pays on
    Stripe's own hosted page, so card details never reach GCON. When the payment succeeds Stripe calls the
    webhook (POST /api/v1/webhooks/stripe, see `gcon.billing.stripe_webhooks`) and only then does GCON mark the
    invoice paid.

    Configuration (all environment variables):
      GCON_PAYMENT_PROVIDER=stripe
      STRIPE_SECRET_KEY            a secret (sk_...) or restricted (rk_...) key; sk_test_... is Stripe's test mode
      GCON_STRIPE_WEBHOOK_SECRET   the signing secret (whsec_...) of the webhook endpoint you add in Stripe
      GCON_STRIPE_DAYS_UNTIL_DUE   optional, default 14

    Raises `PaymentProviderNotConfigured` on construction without a usable key rather than behaving like the mock
    provider, so a misconfigured deployment fails loudly instead of issuing invoices that nobody can pay.

    Every request carries an Idempotency-Key derived from the GCON invoice id, so retrying a finalize that
    failed part-way never makes a second Stripe invoice (Stripe honours a key for 24 hours).
    """

    name = "stripe"
    hosted = True
    API_BASE = "https://api.stripe.com/v1"
    TIMEOUT_SECONDS = 30

    def __init__(self, secret_key: Optional[str] = None,
                 request_fn: Optional[Callable[..., Dict[str, Any]]] = None,
                 days_until_due: Optional[int] = None):
        key = (secret_key if secret_key is not None else os.environ.get("STRIPE_SECRET_KEY", "")).strip()
        if not key:
            raise PaymentProviderNotConfigured(
                "GCON_PAYMENT_PROVIDER=stripe needs STRIPE_SECRET_KEY (a secret key from the Stripe dashboard).")
        if not key.startswith(("sk_", "rk_")):
            raise PaymentProviderNotConfigured(
                "STRIPE_SECRET_KEY must be a secret key (sk_...) or restricted key (rk_...); "
                "a publishable key (pk_...) cannot create invoices.")
        self._key = key
        self.mode = "test" if key.startswith(("sk_test_", "rk_test_")) else "live"
        # A customer created under a test key does not exist under a live key, so they are remembered separately.
        self.customer_scope = f"stripe:{self.mode}"
        if days_until_due is None:
            try:
                days_until_due = int(os.environ.get("GCON_STRIPE_DAYS_UNTIL_DUE", "14"))
            except ValueError:
                days_until_due = 14
        self.days_until_due = max(1, min(365, days_until_due))
        self._request = request_fn or self._http_request

    def charge(self, invoice: Dict[str, Any], org_billing_ref: Optional[str]) -> ChargeResult:
        raise PaymentProviderNotConfigured(
            "Stripe invoices are hosted: finalize_invoice issues them with issue() and the customer pays on Stripe's page.")

    # ------------------------------------------------------------------ HTTP
    def _http_request(self, method: str, path: str, params: Dict[str, Any],
                      idempotency_key: Optional[str]) -> Dict[str, Any]:
        headers = {
            "Authorization": f"Bearer {self._key}",
            "Content-Type": "application/x-www-form-urlencoded",
            "User-Agent": "gcon-billing",
        }
        if idempotency_key:
            headers["Idempotency-Key"] = idempotency_key
        data = urllib.parse.urlencode(_form_pairs(params)).encode("utf-8") if params else None
        request = urllib.request.Request(self.API_BASE + path, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(request, timeout=self.TIMEOUT_SECONDS) as response:
                return json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            try:
                err = json.loads(e.read().decode("utf-8")).get("error", {})
            except Exception:                              # noqa: BLE001
                err = {}
            raise StripeError(err.get("message") or f"Stripe returned HTTP {e.code}",
                              status=e.code, code=err.get("code")) from None
        except (urllib.error.URLError, TimeoutError, OSError) as e:
            raise StripeError(f"Could not reach Stripe: {e}") from None
        except ValueError:
            raise StripeError("Stripe returned a response that is not JSON.") from None

    def _call(self, method: str, path: str, params: Optional[Dict[str, Any]] = None,
              idempotency_key: Optional[str] = None) -> Dict[str, Any]:
        return self._request(method, path, params or {}, idempotency_key)

    # ------------------------------------------------------------------ customers
    def ensure_customer(self, org_id: str, contact: Dict[str, Any]) -> str:
        """Create the Stripe customer for this organization (the caller remembers the id)."""
        customer = self._call("POST", "/customers", {
            "name": contact.get("org_name") or org_id,
            "email": contact["email"],
            "metadata": {"gcon_org_id": org_id},
        }, idempotency_key=f"gcon-customer-{org_id}-{self.mode}")
        return customer["id"]

    # ------------------------------------------------------------------ invoices
    @staticmethod
    def _line_description(item: Dict[str, Any]) -> str:
        quantity = item.get("quantity")
        unit = item.get("unit") or ""
        detail = f" ({quantity:g} {unit})" if isinstance(quantity, (int, float)) and unit else ""
        return f"{item.get('description', 'Usage')}{detail}"[:500]

    def issue(self, invoice: Dict[str, Any], customer_id: str) -> HostedInvoice:
        """Create, finalize and send the Stripe invoice that matches this GCON invoice."""
        items = [i for i in invoice.get("line_items", []) if int(i.get("amount_cents", 0)) != 0]
        total = sum(int(i["amount_cents"]) for i in items)
        if total != int(invoice["amount_cents"]):
            # Never bill a customer an amount that differs from the invoice they can see in GCON.
            raise StripeError(
                f"Invoice {invoice['invoice_id']}: line items add up to {total} but the invoice is "
                f"{invoice['amount_cents']}; not sent to Stripe.")
        if int(invoice["amount_cents"]) <= 0:
            raise StripeError(f"Invoice {invoice['invoice_id']} has nothing to charge; not sent to Stripe.")
        iid = invoice["invoice_id"]
        currency = str(invoice["currency"]).lower()
        created = self._call("POST", "/invoices", {
            "customer": customer_id,
            "collection_method": "send_invoice",
            "days_until_due": self.days_until_due,
            "currency": currency,
            "auto_advance": False,
            "pending_invoice_items_behavior": "exclude",
            "description": f"GCON usage {str(invoice['period_start'])[:10]} to {str(invoice['period_end'])[:10]}",
            "metadata": {"gcon_invoice_id": iid, "gcon_org_id": invoice.get("org_id")},
        }, idempotency_key=f"gcon-inv-{iid}")
        stripe_id = created["id"]
        for n, item in enumerate(items):
            self._call("POST", "/invoiceitems", {
                "customer": customer_id,
                "invoice": stripe_id,
                "currency": currency,
                "amount": int(item["amount_cents"]),
                "description": self._line_description(item),
            }, idempotency_key=f"gcon-inv-{iid}-item-{n}")
        finalized = self._call("POST", f"/invoices/{stripe_id}/finalize", {"auto_advance": False},
                               idempotency_key=f"gcon-inv-{iid}-finalize")
        emailed = True
        try:
            self._call("POST", f"/invoices/{stripe_id}/send", {}, idempotency_key=f"gcon-inv-{iid}-send")
        except StripeError as e:
            # The invoice exists and is payable from its page; only the email failed. Staff can resend it from the
            # Stripe dashboard, so this must not undo an invoice that was successfully issued.
            emailed = False
            _log.warning("Stripe invoice %s for %s was issued but not emailed: %s", stripe_id, iid, e)
        return HostedInvoice(provider_invoice_id=stripe_id,
                             hosted_invoice_url=finalized.get("hosted_invoice_url"), emailed=emailed)


def get_configured_provider() -> PaymentProvider:
    """`GCON_PAYMENT_PROVIDER` selects the provider. The default (and anything unrecognised) is invoicing by hand:
    an invoice is issued and stays open until staff record a payment, so nothing is ever marked paid by accident.
    "mock" pretends every charge succeeds (development and tests only). "stripe" only once real credentials and the
    STRIPE_SECRET_KEY (and, to see payments arrive, GCON_STRIPE_WEBHOOK_SECRET) exist."""
    choice = os.environ.get("GCON_PAYMENT_PROVIDER", "manual").strip().lower()
    if choice == "stripe":
        return StripePaymentProvider()
    if choice == "mock":
        return MockPaymentProvider()
    return ManualInvoiceProvider()
