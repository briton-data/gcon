"""
GET /jobs?status=&limit= and GET /receipts?verified=&limit= -- #9's
"filters on list_jobs/list_receipts" SDK-parity item. Both were
previously unfiltered at the API layer even though the real filtering
logic (presentation.get_jobs's status/limit, presentation.
get_receipts_page's verified/limit) already existed and was already
used by the internal dashboard -- this only covers the new query
params' own contract (unfiltered default is unchanged, a real filter
actually filters). The underlying filter logic itself already has its
own coverage elsewhere (dashboard tests), not duplicated here.
"""
import time

import pytest
from fastapi.testclient import TestClient

from gcon.api.api_v1 import create_api_v1_app
from gcon.cluster.coordinator import GCONCoordinator
from gcon.dashboard.presentation import PresentationLayer
from gcon.execution.agent import GCONAgent
from gcon.management.management_layer import ManagementLayer
from gcon.persistence.control_plane import ControlPlane


@pytest.fixture
def env(tmp_path):
    db = str(tmp_path / "cp.db")
    coordinator = GCONCoordinator(control_plane=ControlPlane(path=db))
    management = ManagementLayer(coordinator=coordinator, db_path=str(tmp_path / "mgmt.db"))
    client = TestClient(create_api_v1_app(management, PresentationLayer(coordinator)))
    yield client, coordinator
    coordinator.shutdown()


def _signup(client, org_name="Acme", email="a@acme.example"):
    r = client.post(
        "/auth/signup",
        json={"org_name": org_name, "name": "Ann", "email": email, "password": "correct-horse-1"},
    )
    assert r.status_code == 200, r.text
    return r.json()


def _h(secret):
    return {"Authorization": f"Bearer {secret}"}


def _wait(pred, timeout=6.0):
    end = time.time() + timeout
    while time.time() < end:
        if pred():
            return True
        time.sleep(0.05)
    return False


def test_unfiltered_get_jobs_is_unchanged(env):
    """No query params at all -> byte-for-byte the same request this
    route always handled, still returning every one of the caller's
    own jobs."""
    client, coordinator = env
    acme = _signup(client)
    key = acme["api_key"]["secret"]
    node = GCONAgent(node_id="acme-node")
    node.org_id = acme["organization"]["org_id"]
    coordinator.register_agent(node)

    client.post("/jobs", json={"job_id": "j1", "command": "echo one"}, headers=_h(key))
    client.post("/jobs", json={"job_id": "j2", "command": "exit 1"}, headers=_h(key))
    assert _wait(lambda: coordinator.jobs["j1"]["status"] == "completed")
    assert _wait(lambda: coordinator.jobs["j2"]["status"] == "failed")

    r = client.get("/jobs", headers=_h(key))
    assert r.status_code == 200
    ids = {j["job_id"] for j in r.json()}
    assert ids == {"j1", "j2"}


def test_status_filter_narrows_to_matching_jobs_only(env):
    client, coordinator = env
    acme = _signup(client)
    key = acme["api_key"]["secret"]
    node = GCONAgent(node_id="acme-node")
    node.org_id = acme["organization"]["org_id"]
    coordinator.register_agent(node)

    client.post("/jobs", json={"job_id": "j-ok", "command": "echo one"}, headers=_h(key))
    client.post("/jobs", json={"job_id": "j-bad", "command": "exit 1"}, headers=_h(key))
    assert _wait(lambda: coordinator.jobs["j-ok"]["status"] == "completed")
    assert _wait(lambda: coordinator.jobs["j-bad"]["status"] == "failed")

    r = client.get("/jobs", params={"status": "failed"}, headers=_h(key))
    assert r.status_code == 200
    ids = {j["job_id"] for j in r.json()}
    assert ids == {"j-bad"}


def test_limit_caps_the_number_of_jobs_returned(env):
    client, coordinator = env
    acme = _signup(client)
    key = acme["api_key"]["secret"]
    node = GCONAgent(node_id="acme-node")
    node.org_id = acme["organization"]["org_id"]
    coordinator.register_agent(node)

    for i in range(5):
        client.post("/jobs", json={"job_id": f"j-{i}", "command": "echo hi"}, headers=_h(key))
    assert _wait(lambda: all(coordinator.jobs[f"j-{i}"]["status"] == "completed" for i in range(5)))

    r = client.get("/jobs", params={"limit": 2}, headers=_h(key))
    assert r.status_code == 200
    assert len(r.json()) == 2


def test_job_filters_stay_org_scoped(env):
    """A filter param must never widen visibility past org isolation
    -- same isolation guarantee the unfiltered route already has."""
    client, coordinator = env
    acme = _signup(client, org_name="Acme", email="a@acme.example")
    globex = _signup(client, org_name="Globex", email="g@globex.example")

    acme_node = GCONAgent(node_id="acme-node")
    acme_node.org_id = acme["organization"]["org_id"]
    coordinator.register_agent(acme_node)
    globex_node = GCONAgent(node_id="globex-node")
    globex_node.org_id = globex["organization"]["org_id"]
    coordinator.register_agent(globex_node)

    client.post("/jobs", json={"job_id": "acme-job", "command": "echo hi"},
                headers=_h(acme["api_key"]["secret"]))
    client.post("/jobs", json={"job_id": "globex-job", "command": "echo hi"},
                headers=_h(globex["api_key"]["secret"]))
    assert _wait(lambda: coordinator.jobs["acme-job"]["status"] == "completed")
    assert _wait(lambda: coordinator.jobs["globex-job"]["status"] == "completed")

    r = client.get("/jobs", params={"status": "completed"}, headers=_h(acme["api_key"]["secret"]))
    assert r.status_code == 200
    ids = {j["job_id"] for j in r.json()}
    assert ids == {"acme-job"}


def test_unfiltered_get_receipts_is_unchanged(env):
    client, coordinator = env
    acme = _signup(client)
    key = acme["api_key"]["secret"]
    node = GCONAgent(node_id="acme-node")
    node.org_id = acme["organization"]["org_id"]
    coordinator.register_agent(node)
    client.post("/jobs", json={"job_id": "r1", "command": "echo hi"}, headers=_h(key))
    assert _wait(lambda: "r1" in coordinator.receipts)

    r = client.get("/receipts", headers=_h(key))
    assert r.status_code == 200
    assert any(rc["job_id"] == "r1" for rc in r.json())


def test_receipts_verified_filter_uses_the_real_paginated_backend(env):
    """Only asserts the route actually returns real, correctly-org-
    scoped data through the verified/limit path (get_receipts_page) --
    every real job's receipt in this fixture genuinely verifies (real
    HMAC signature), so verified=true should include it and
    verified=false should not.

    The verified= filter reads control_plane.receipts's own `verified`
    column, only committed once health_check_loop's real 3s tick runs
    its verification pass (see get_receipts_page's docstring) -- not
    instantly at receipt creation -- so this polls for it to settle
    rather than asserting on the very first check."""
    client, coordinator = env
    acme = _signup(client)
    key = acme["api_key"]["secret"]
    node = GCONAgent(node_id="acme-node")
    node.org_id = acme["organization"]["org_id"]
    coordinator.register_agent(node)
    client.post("/jobs", json={"job_id": "r2", "command": "echo hi"}, headers=_h(key))
    assert _wait(lambda: "r2" in coordinator.receipts)

    def _r2_is_listed_as_verified():
        r = client.get("/receipts", params={"verified": "true"}, headers=_h(key))
        assert r.status_code == 200
        return any(rc["job_id"] == "r2" for rc in r.json())

    assert _wait(_r2_is_listed_as_verified, timeout=15)

    r = client.get("/receipts", params={"verified": "false"}, headers=_h(key))
    assert r.status_code == 200
    assert all(rc["job_id"] != "r2" for rc in r.json())


def test_receipts_limit_caps_the_number_returned(env):
    client, coordinator = env
    acme = _signup(client)
    key = acme["api_key"]["secret"]
    node = GCONAgent(node_id="acme-node")
    node.org_id = acme["organization"]["org_id"]
    coordinator.register_agent(node)
    for i in range(4):
        client.post("/jobs", json={"job_id": f"rl-{i}", "command": "echo hi"}, headers=_h(key))
    assert _wait(lambda: all(f"rl-{i}" in coordinator.receipts for i in range(4)))

    r = client.get("/receipts", params={"limit": 2}, headers=_h(key))
    assert r.status_code == 200
    assert len(r.json()) == 2
