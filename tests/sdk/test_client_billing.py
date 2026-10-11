"""
SDK billing methods (get_pricing, get_usage, list_invoices, get_invoice) --
GconClient (real `requests`) against a real uvicorn server, same approach as
test_client_new_methods.py: checks the client builds the right URL/params and
parses the real response, and that API errors surface as GconAPIError.
"""
import pytest

from gcon_sdk import GconAPIError

from tests.sdk.test_client_new_methods import (  # noqa: F401  (live_server is a fixture)
    LiveServer,
    _signup_and_client,
    live_server,
)

INSIDE = "2026-01-15T12:00:00+00:00"


@pytest.fixture(autouse=True)
def no_rate_limit(monkeypatch):
    monkeypatch.setenv("GCON_API_RATE_LIMIT_PER_MINUTE", "0")


def test_get_pricing(live_server):
    client, _ = _signup_and_client(live_server)
    pricing = client.get_pricing()
    assert pricing["currency"] == "usd"
    assert pricing["gpu_second_cents"] > 0


def test_get_usage_with_and_without_a_period(live_server):
    client, org_id = _signup_and_client(live_server)
    cp = live_server.coordinator.control_plane
    cp.jobs.ensure_exists("sdk-bill-1", "echo hi", org_id=org_id)
    cp.jobs.set_status("sdk-bill-1", "completed", result={"runtime_seconds": 1000}, completed=True)
    cp.db.execute("UPDATE jobs SET completed_at = ? WHERE job_id = ?", (INSIDE, "sdk-bill-1"))

    usage = client.get_usage("2026-01-01", "2026-02-01")
    assert usage["usage"]["job_count"] == 1
    assert usage["amount_cents"] > 0

    current = client.get_usage()
    assert current["period_start"].endswith("-01T00:00:00+00:00")

    with pytest.raises(GconAPIError) as exc:
        client.get_usage("2026-01-01")
    assert exc.value.status_code == 400


def test_list_and_get_invoice(live_server):
    client, org_id = _signup_and_client(live_server)
    cp = live_server.coordinator.control_plane
    inv = cp.invoices.create(
        org_id, "2026-03-01", "2026-04-01", "usd",
        [{"description": "Compute", "quantity": 10, "unit": "gpu_second",
          "unit_price_cents": 1.0, "amount_cents": 10}],
    )
    # still a draft: invisible to the customer
    assert client.list_invoices() == []
    with pytest.raises(GconAPIError) as exc:
        client.get_invoice(inv["invoice_id"])
    assert exc.value.status_code == 404

    cp.invoices.mark_status(inv["invoice_id"], "open", provider="mock")
    listed = client.list_invoices(limit=10)
    assert [i["invoice_id"] for i in listed] == [inv["invoice_id"]]
    one = client.get_invoice(inv["invoice_id"])
    assert one["status"] == "open" and one["line_items"][0]["unit"] == "gpu_second"
