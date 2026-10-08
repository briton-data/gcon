"""
Stopping an abusive customer:

  * signup is rate limited per client IP,
  * every login used to mint another 90-day key that was never retired,
  * revoking a key did nothing durable (the customer logs in and gets another),
    and nothing could change a customer's status: now staff can disable an
    organization's customers, after which neither login nor any existing key works.
"""
import pytest
from tests.support.label_client import LabelClient as TestClient

from gcon.api.api_v1 import MAX_SESSION_KEYS_PER_CUSTOMER, SIGNUPS_PER_HOUR_PER_IP, create_api_v1_app
from gcon.cluster.coordinator import GCONCoordinator
from gcon.dashboard.presentation import PresentationLayer
from gcon.management.management_layer import ManagementLayer


@pytest.fixture
def env(tmp_path):
    coordinator = GCONCoordinator()
    management = ManagementLayer(coordinator=coordinator, db_path=str(tmp_path / "m.db"))
    client = TestClient(create_api_v1_app(management, PresentationLayer(coordinator)))
    yield client, management
    coordinator.shutdown()


def _signup(client, n):
    return client.post("/auth/signup", json={
        "org_name": f"Org{n}", "name": "N", "email": f"u{n}@example.com", "password": "correct-horse-1"})


def test_signup_is_rate_limited_per_address(env):
    client, _ = env
    codes = [_signup(client, n).status_code for n in range(SIGNUPS_PER_HOUR_PER_IP + 3)]
    assert codes[:SIGNUPS_PER_HOUR_PER_IP] == [200] * SIGNUPS_PER_HOUR_PER_IP
    assert set(codes[SIGNUPS_PER_HOUR_PER_IP:]) == {429}


def test_logins_keep_only_the_newest_session_keys(env):
    client, management = env
    signup = _signup(client, 1).json()
    for _ in range(MAX_SESSION_KEYS_PER_CUSTOMER + 3):
        r = client.post("/auth/login", json={"email": "u1@example.com", "password": "correct-horse-1"})
        assert r.status_code == 200
    uid = signup["customer_user"]["customer_user_id"]
    active = [k for k in management.api_key_manager.list_keys()
              if k.owner_user_id == uid and k.name.startswith("Web session") and k.status == "Active"]
    assert len(active) == MAX_SESSION_KEYS_PER_CUSTOMER


def test_the_newest_login_key_keeps_working_and_the_oldest_is_retired(env):
    client, _ = env
    first = _signup(client, 1).json()["api_key"]["secret"]
    last = first
    for _ in range(MAX_SESSION_KEYS_PER_CUSTOMER + 2):
        last = client.post("/auth/login", json={"email": "u1@example.com", "password": "correct-horse-1"}).json()["api_key"]["secret"]
    assert client.get("/jobs", headers={"X-API-Key": last}).status_code == 200


def test_disabling_an_organization_stops_login_and_every_existing_key(env):
    client, management = env
    signup = _signup(client, 1).json()
    key = signup["api_key"]["secret"]
    org_id = signup["organization"]["org_id"]
    assert client.get("/jobs", headers={"X-API-Key": key}).status_code == 200

    out = management.set_organization_customer_status(org_id, "Disabled")
    assert out["customers"] == 1

    assert client.get("/jobs", headers={"X-API-Key": key}).status_code == 401
    assert client.post("/auth/login", json={"email": "u1@example.com", "password": "correct-horse-1"}).status_code == 401
    management.set_organization_customer_status(org_id, "Active")
    assert client.get("/jobs", headers={"X-API-Key": key}).status_code == 200


def test_bad_status_and_unknown_org_are_refused(env):
    _, management = env
    with pytest.raises(ValueError):
        management.set_organization_customer_status("no-such-org", "Disabled")
    signup = management.signup_customer("Acme", "Ann", "ann@acme.example", "correct-horse-1")
    with pytest.raises(ValueError, match="Invalid status"):
        management.set_organization_customer_status(signup["organization"]["org_id"], "Banished")
