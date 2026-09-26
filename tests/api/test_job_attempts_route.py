"""
GET /jobs/{job_id}/attempts -- durable dispatch-attempt history,
previously unqueryable even though job_attempts rows were already
being recorded (over the real gRPC transport -- see
tests/transport/test_coordinator_receipt_attempt_linking.py for the
end-to-end proof the data itself is correct). This file only covers
the route's auth/org-isolation/404 contract, same pattern as
test_node_enrollment_history.py; the LocalTransport this fixture uses
by default never records attempts (no control_plane wiring at the
transport layer -- see LocalTransport.send_job's docstring), so a
successful dispatch here still returns [] for a real, honest reason,
not a bug in this route.
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


def test_unknown_job_id_is_404(env):
    client, _ = env
    acme = _signup(client)
    r = client.get("/jobs/does-not-exist/attempts", headers=_h(acme["api_key"]["secret"]))
    assert r.status_code == 404


def test_org_cannot_see_a_different_orgs_job_attempts(env):
    client, coordinator = env
    acme = _signup(client, org_name="Acme", email="a@acme.example")
    globex = _signup(client, org_name="Globex", email="g@globex.example")

    node = GCONAgent(node_id="acme-node")
    node.org_id = acme["organization"]["org_id"]
    coordinator.register_agent(node)
    client.post(
        "/jobs", json={"job_id": "acme-job-1", "command": "echo hi"},
        headers=_h(acme["api_key"]["secret"]),
    )
    assert _wait(lambda: coordinator.jobs["acme-job-1"]["status"] == "completed")

    # Globex's key asking about Acme's job -- must be 404, not 403,
    # same "don't confirm the job_id exists elsewhere" pattern as the
    # existing GET /jobs/{job_id} route and the enrollment-history route.
    r = client.get("/jobs/acme-job-1/attempts", headers=_h(globex["api_key"]["secret"]))
    assert r.status_code == 404


def test_owner_can_query_their_own_jobs_attempt_history(env):
    client, coordinator = env
    acme = _signup(client)
    key = acme["api_key"]["secret"]

    node = GCONAgent(node_id="acme-node")
    node.org_id = acme["organization"]["org_id"]
    coordinator.register_agent(node)
    client.post("/jobs", json={"job_id": "acme-job-2", "command": "echo hi"}, headers=_h(key))
    assert _wait(lambda: coordinator.jobs["acme-job-2"]["status"] == "completed")

    r = client.get("/jobs/acme-job-2/attempts", headers=_h(key))
    assert r.status_code == 200
    # LocalTransport (this fixture's default) never records durable
    # attempts -- see this file's module docstring. An empty list is
    # the correct, honest response here, not a failure of this route;
    # the real gRPC path's non-empty case is proven end-to-end in
    # tests/transport/test_coordinator_receipt_attempt_linking.py.
    assert r.json() == []
