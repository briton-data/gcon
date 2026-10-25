"""The customer's side of hosted workers: lease some, see them, release them -- and never see or touch anyone else's."""
from types import SimpleNamespace

import pytest
from tests.support.label_client import LabelClient as TestClient

from gcon.api.api_v1 import create_api_v1_app
from gcon.cluster.coordinator import GCONCoordinator
from gcon.dashboard.presentation import PresentationLayer
from gcon.management.management_layer import ManagementLayer
from gcon.persistence.control_plane import ControlPlane


@pytest.fixture
def env(tmp_path):
    coordinator = GCONCoordinator(control_plane=ControlPlane(path=str(tmp_path / "cp.db")))
    management = ManagementLayer(coordinator=coordinator, db_path=str(tmp_path / "mgmt.db"))
    coordinator.org_leased_worker_cap_resolver = lambda org_id: 3          # tests choose the plan's allowance
    client = TestClient(create_api_v1_app(management, PresentationLayer(coordinator)))
    yield client, coordinator
    coordinator.shutdown()


def _signup(client, org_name, email):
    r = client.post("/auth/signup", json={
        "org_name": org_name, "name": "Ann", "email": email, "password": "correct-horse-1"})
    assert r.status_code == 200, r.text
    return {"Authorization": f"Bearer {r.json()['api_key']['secret']}"}, r.json()


def _hosted(coordinator, *names):
    for n in names:
        coordinator.control_plane.fleet_nodes.add(n, None)
        coordinator.registry.register(SimpleNamespace(
            node_id=n, org_id=None, sandboxed=True, agent_version="1.1.0", status="idle",
            is_connected=lambda: True, address="10.0.0.9:50051"))


def test_a_customer_leases_and_sees_its_workers(env):
    client, coordinator = env
    acme, _ = _signup(client, "Acme", "a@acme.example")
    _hosted(coordinator, "h1", "h2", "h3")
    r = client.post("/workers/leases", json={"count": 2}, headers=acme)
    assert r.status_code == 200, r.text
    leases = r.json()
    assert len(leases) == 2 and {l["state"] for l in leases} == {"active"}
    listed = client.get("/workers/leases", headers=acme).json()
    assert {l["lease_id"] for l in listed} == {l["lease_id"] for l in leases}
    assert set(leases[0]) == {"lease_id", "node_id", "state", "started_at", "ending_at", "ended_at", "end_reason"}


def test_each_organization_sees_only_its_own_workers_and_cannot_touch_anothers(env):
    client, coordinator = env
    acme, _ = _signup(client, "Acme", "a@acme.example")
    globex, _ = _signup(client, "Globex", "g@globex.example")
    _hosted(coordinator, "h1", "h2", "h3")
    [lease] = client.post("/workers/leases", json={"count": 1}, headers=acme).json()
    assert client.get("/workers/leases", headers=globex).json() == []
    assert client.delete(f"/workers/leases/{lease['lease_id']}", headers=globex).status_code == 404
    assert client.delete("/workers/leases/lease_made_up", headers=globex).status_code == 404       # same answer
    assert client.get("/workers/leases", headers=acme).json()[0]["state"] == "active"
    # In the node list: the leasing organization sees the worker (without where it lives), nobody else does.
    mine = {n["node_id"]: n for n in client.get("/nodes", headers=acme).json()}
    assert lease["node_id"] in mine and mine[lease["node_id"]]["address"] is None
    assert [n["node_id"] for n in client.get("/nodes", headers=globex).json()] == []


def test_releasing_starts_the_clean_up_and_the_worker_is_not_free_until_it_is_clean(env):
    client, coordinator = env
    acme, _ = _signup(client, "Acme", "a@acme.example")
    _hosted(coordinator, "h1")
    [lease] = client.post("/workers/leases", json={"count": 1}, headers=acme).json()
    r = client.delete(f"/workers/leases/{lease['lease_id']}", headers=acme)
    assert r.status_code == 200 and r.json()["state"] in ("ending", "sanitizing", "ended")
    assert coordinator.worker_leases.available_nodes() == []          # nobody has said it is clean


def test_the_limits_answer_with_a_code_a_client_can_act_on(env):
    client, coordinator = env
    acme, _ = _signup(client, "Acme", "a@acme.example")
    _hosted(coordinator, "h1", "h2")
    too_many = client.post("/workers/leases", json={"count": 3}, headers=acme)
    assert too_many.status_code == 409 and too_many.json()["code"] == "not_enough_workers" \
        and too_many.json()["available"] == 2
    assert client.post("/workers/leases", json={"count": 2}, headers=acme).status_code == 200
    over = client.post("/workers/leases", json={"count": 2}, headers=acme)
    assert over.status_code == 403 and over.json()["code"] == "plan_limit"
    assert client.post("/workers/leases", json={"count": 0}, headers=acme).status_code == 422
    assert len(client.get("/workers/leases", headers=acme).json()) == 2              # nothing was half-done


def test_a_plan_without_hosted_workers_cannot_lease(env):
    client, coordinator = env
    coordinator.org_leased_worker_cap_resolver = lambda org_id: 0
    acme, _ = _signup(client, "Acme", "a@acme.example")
    _hosted(coordinator, "h1")
    r = client.post("/workers/leases", json={"count": 1}, headers=acme)
    assert r.status_code == 403 and r.json()["code"] == "plan_limit"


def test_it_requires_authentication(env):
    client, _ = env
    assert client.post("/workers/leases", json={"count": 1}).status_code == 401
    assert client.get("/workers/leases").status_code == 401
    assert client.delete("/workers/leases/lease_x").status_code == 401


def test_the_plan_endpoint_tells_a_customer_what_hosted_workers_it_may_hold(env):
    client, coordinator = env
    acme, _ = _signup(client, "Acme", "a@acme.example")
    plan = client.get("/account/plan", headers=acme).json()
    assert plan["max_leased_workers"] == 1 and plan["leased_workers_held"] == 0      # Standard: one hosted worker
    assert plan["max_workers"] == 5
    _hosted(coordinator, "h1", "h2")
    client.post("/workers/leases", json={"count": 2}, headers=acme)
    assert client.get("/account/plan", headers=acme).json()["leased_workers_held"] == 2
