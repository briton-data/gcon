"""
PaymentProvider — the seam a real payment gateway integration plugs
into. Charging a real card/bank account needs a provider account and
API credentials this environment does not have and cannot fabricate,
so this module ships:

  * `PaymentProvider` -- the abstract interface `finalize_invoice`
    (billing/api_v1 route) calls.
  * `MockPaymentProvider` -- a real, working implementation with no
    external dependency: it "charges" by deterministically succeeding
    or failing (configurable) and recording the attempt, useful for
    tests, demos, and any deployment that genuinely doesn't need real
    money to move (e.g. usage tracked for internal chargeback only).
  * `StripePaymentProvider` -- NOT a working integration. It documents
    exactly what a real one needs (a Stripe secret key, a customer ID
    per org, a Stripe `PaymentIntent`/`Charge` call) and raises
    `PaymentProviderNotConfigured` if instantiated without
    `STRIPE_SECRET_KEY` set, rather than silently no-opping or
    pretending to charge someone. Filling in the real HTTP calls
    (`stripe.PaymentIntent.create(...)`) is the entire remaining
    work, gated behind an operator actually providing credentials.
"""

from __future__ import annotations

import os
import uuid
from dataclasses import dataclass
from typing import Any, Dict, Optional, Protocol


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


class StripePaymentProvider:
    """
    Not implemented against the real Stripe API -- this environment
    has no Stripe account/credentials to test against, and shipping
    an untested integration against a real payment gateway is worse
    than shipping none. What a real implementation needs, concretely:

      1. `pip install stripe`, `STRIPE_SECRET_KEY` env var.
      2. A durable mapping from GCON org_id -> Stripe customer_id
         (not modeled anywhere in GCON today -- would need a column
         on `organizations` or a new small table) and -> a default
         payment method already attached to that customer (Stripe
         Checkout/Setup Intents handle the actual card-collection
         flow; GCON never touches raw card data, same boundary
         `key_manager.py`'s Ed25519 module already respects for
         signing keys vs. this module's boundary for payment data).
      3. `charge()` becomes roughly:
         `stripe.PaymentIntent.create(amount=invoice["amount_cents"],
         currency=invoice["currency"], customer=org_billing_ref,
         confirm=True, off_session=True)`, mapping Stripe's
         success/`card_error`/`invalid_request_error` outcomes onto
         `ChargeResult`.
      4. Idempotency: pass `idempotency_key=invoice["invoice_id"]` so
         a retried `finalize_invoice` call can't double-charge --
         mirrors the `receipt_hash` UNIQUE-constraint idempotency
         pattern already used for receipt uploads.

    Raises `PaymentProviderNotConfigured` on construction rather than
    silently behaving like `MockPaymentProvider` -- an operator who
    sets `GCON_PAYMENT_PROVIDER=stripe` without a key should get a
    loud, immediate error, not invoices that quietly never get
    charged.
    """

    name = "stripe"

    def __init__(self):
        if not os.environ.get("STRIPE_SECRET_KEY"):
            raise PaymentProviderNotConfigured(
                "StripePaymentProvider requires STRIPE_SECRET_KEY; see this "
                "class's docstring for what else a real integration needs."
            )
        raise NotImplementedError(
            "StripePaymentProvider is a documented integration point, not a "
            "working implementation -- see the class docstring for the "
            "concrete steps to finish it."
        )

    def charge(self, invoice: Dict[str, Any], org_billing_ref: Optional[str]) -> ChargeResult:
        raise NotImplementedError


def get_configured_provider() -> PaymentProvider:
    """`GCON_PAYMENT_PROVIDER` selects the provider; defaults to the
    mock so `finalize_invoice` always has something usable without
    any setup. Set to "stripe" only once real credentials and the
    customer-id mapping (see StripePaymentProvider's docstring)
    actually exist."""
    choice = os.environ.get("GCON_PAYMENT_PROVIDER", "mock").strip().lower()
    if choice == "stripe":
        return StripePaymentProvider()
    return MockPaymentProvider()
