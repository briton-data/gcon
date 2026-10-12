"""SDK get_plan(): GconClient against a real uvicorn server, like the other SDK tests."""
import pytest

from tests.sdk.test_client_new_methods import (  # noqa: F401  (live_server is a fixture)
    _signup_and_client,
    live_server,
)

pytestmark = pytest.mark.real_plan_defaults


@pytest.fixture(autouse=True)
def plan_limits_apply(monkeypatch):
    monkeypatch.delenv("GCON_API_RATE_LIMIT_PER_MINUTE", raising=False)
    monkeypatch.delenv("GCON_MAX_CONCURRENT_JOBS_PER_ORG", raising=False)


def test_get_plan_shows_plan_limits_and_usage(live_server):
    client, org_id = _signup_and_client(live_server)
    plan = client.get_plan()
    assert plan["plan"] == "Standard"
    assert plan["requests_per_minute"] == 600
    assert plan["max_concurrent_jobs"] == 50
    assert plan["jobs_in_flight"] == 0

    live_server.management.update_organization(org_id, plan="Enterprise")
    assert client.get_plan()["plan"] == "Enterprise"
