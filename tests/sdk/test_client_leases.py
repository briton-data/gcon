"""SDK hosted-worker methods: GconClient against a real uvicorn server, like the other SDK tests."""
import pytest

from gcon_sdk.client import GconAPIError
from tests.sdk.test_client_new_methods import (  # noqa: F401  (live_server is a fixture)
    _signup_and_client,
    live_server,
)


def test_a_new_customer_holds_no_hosted_workers(live_server):
    client, _ = _signup_and_client(live_server)
    assert client.list_worker_leases() == []


def test_requesting_hosted_workers_when_none_are_free_is_a_409(live_server):
    client, _ = _signup_and_client(live_server)
    with pytest.raises(GconAPIError) as err:
        client.request_workers(1)
    assert err.value.status_code == 409


def test_releasing_a_lease_that_is_not_yours_is_a_404(live_server):
    client, _ = _signup_and_client(live_server)
    with pytest.raises(GconAPIError) as err:
        client.release_worker("lease_does_not_exist")
    assert err.value.status_code == 404
