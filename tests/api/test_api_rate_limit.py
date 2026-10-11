"""
General request limit on /api/v1 (GCON_API_RATE_LIMIT_PER_MINUTE).

Counted per organization after the API key authenticates; a 429 carries
Retry-After. Unauthenticated requests, the auth routes (own limiters) and
other organizations are unaffected.
"""
import pytest
from tests.support.label_client import LabelClient as TestClient

from gcon.api.api_v1 import create_api_v1_app
from gcon.cluster.coordinator import GCONCoordinator
from gcon.dashboard.presentation import PresentationLayer
from gcon.management.management_layer import ManagementLayer
from gcon.persistence.control_plane import ControlPlane


def build(tmp_path, monkeypatch, limit):
    if limit is None:
        monkeypatch.delenv("GCON_API_RATE_LIMIT_PER_MINUTE", raising=False)
    else:
        monkeypatch.setenv("GCON_API_RATE_LIMIT_PER_MINUTE", str(limit))
    coordinator = GCONCoordinator(control_plane=ControlPlane(path=str(tmp_path / "cp.db")))
    management = ManagementLayer(coordinator=coordinator, db_path=str(tmp_path / "mgmt.db"))
    client = TestClient(create_api_v1_app(management, PresentationLayer(coordinator)))
    return client, coordinator, management


@pytest.fixture
def limited(tmp_path, monkeypatch):
    client, coordinator, management = build(tmp_path, monkeypatch, limit=3)
    yield client, coordinator, management
    coordinator.shutdown()


def signup(client, org="Acme", email="a@acme.example"):
    r = client.post(
        "/auth/signup",
        json={"org_name": org, "name": "Ann", "email": email, "password": "correct-horse-1"},
    )
    assert r.status_code == 200, r.text
    return r.json()


def h(secret):
    return {"Authorization": f"Bearer {secret}"}


def test_requests_over_the_limit_get_429_with_retry_after(limited):
    client, _, _ = limited
    key = signup(client)["api_key"]["secret"]
    for _ in range(3):
        assert client.get("/jobs", headers=h(key)).status_code == 200
    r = client.get("/jobs", headers=h(key))
    assert r.status_code == 429
    assert "Rate limit exceeded" in r.json()["detail"]
    retry_after = int(r.headers["Retry-After"])
    assert 1 <= retry_after <= 60


def test_the_limit_is_shared_across_every_key_and_route_of_an_organization(limited):
    client, _, _ = limited
    first = signup(client)["api_key"]["secret"]
    second = client.post("/auth/api-keys", json={"name": "second"}, headers=h(first))
    assert second.status_code == 200, second.text
    second_secret = second.json()["secret"]

    # the api-keys call above already counted once; two more fill the allowance
    assert client.get("/jobs", headers=h(first)).status_code == 200
    assert client.get("/nodes", headers=h(second_secret)).status_code == 200
    # a fresh key does not buy a fresh allowance
    assert client.get("/receipts", headers=h(second_secret)).status_code == 429
    assert client.get("/jobs", headers=h(first)).status_code == 429


def test_another_organization_is_not_affected(limited):
    client, _, _ = limited
    acme = signup(client, "Acme", "a@acme.example")["api_key"]["secret"]
    globex = signup(client, "Globex", "g@globex.example")["api_key"]["secret"]
    for _ in range(3):
        client.get("/jobs", headers=h(acme))
    assert client.get("/jobs", headers=h(acme)).status_code == 429
    assert client.get("/jobs", headers=h(globex)).status_code == 200


def test_failed_authentication_does_not_use_up_anyones_allowance(limited):
    client, _, _ = limited
    key = signup(client)["api_key"]["secret"]
    for _ in range(10):
        assert client.get("/jobs", headers=h("gcon_not_a_real_key")).status_code == 401
        assert client.get("/jobs").status_code == 401
    for _ in range(3):
        assert client.get("/jobs", headers=h(key)).status_code == 200


def test_auth_routes_are_not_counted(limited):
    client, _, _ = limited
    key = signup(client)["api_key"]["secret"]
    for i in range(6):
        r = client.post("/auth/login", json={"email": "a@acme.example", "password": "correct-horse-1"})
        assert r.status_code == 200, (i, r.text)
    assert client.get("/jobs", headers=h(key)).status_code == 200


def test_429_is_documented_in_openapi(limited):
    client, _, _ = limited
    paths = client.get("/openapi.json").json()["paths"]
    assert "429" in paths["/jobs"]["get"]["responses"]
    assert "429" in paths["/billing/invoices"]["get"]["responses"]


def test_zero_turns_the_limit_off(tmp_path, monkeypatch):
    client, coordinator, _ = build(tmp_path, monkeypatch, limit=0)
    try:
        key = signup(client)["api_key"]["secret"]
        assert all(client.get("/jobs", headers=h(key)).status_code == 200 for _ in range(40))
    finally:
        coordinator.shutdown()


def test_default_is_on_and_generous(tmp_path, monkeypatch):
    client, coordinator, _ = build(tmp_path, monkeypatch, limit=None)
    try:
        key = signup(client)["api_key"]["secret"]
        # 600 / minute: ordinary use never notices it
        assert all(client.get("/jobs", headers=h(key)).status_code == 200 for _ in range(60))
    finally:
        coordinator.shutdown()
