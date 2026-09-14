"""
GET /jobs/{job_id}/cancel and .../retry -- real cross-org
authorization tests. cancel_job had a genuine, currently-shipping
security bug found while building the customer dashboard: the route
called presentation.cancel_job(job_id) with NO org check at all, so
any authenticated key from any org could cancel any job anywhere in
the system just by knowing its job_id. retry_job never existed as a
route before (backend was already built and tested, just never
wired -- same pattern as GET /telemetry/events and
GET /nodes/{node_id}/enrollment-history found in earlier sessions).

Real coordinator + real dispatch + real FastAPI app throughout, same
two_org_setup style as test_node_enrollment_history.py /
test_telemetry_lifecycle.py.
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
def two_org_setup(tmp_path):
    control_plane = ControlPlane(path=str(tmp_path / "cp.db"))
    coordinator = GCONCoordinator(control_plane=control_plane)
    management = ManagementLayer(coordinator=coordinator, db_path=str(tmp_path / "mgmt.db"))

    acme_org = management.create_organization("Acme Corp")
    acme_user = management.create_user(
        "Acme User", "user@acme.example", role="Owner", organization_id=acme_org["org_id"],
    )
    acme_key = management.create_api_key(
        "acme-key", owner_user_id=acme_user["user_id"], scopes=["Submit workflows", "View monitoring"],
    )

    globex_org = management.create_organization("Globex Inc")
    globex_user = management.create_user(
        "Globex User", "user@globex.example", role="Owner", organization_id=globex_org["org_id"],
    )
    globex_key = management.create_api_key(
        "globex-key", owner_user_id=globex_user["user_id"], scopes=["Submit workflows", "View monitoring"],
    )

    acme_node = GCONAgent(node_id="acme-node-1")
    acme_node.org_id = acme_org["org_id"]
    coordinator.register_agent(acme_node)

    presentation = PresentationLayer(coordinator)
    app = create_api_v1_app(management, presentation)
    client = TestClient(app)

    yield client, coordinator, acme_key["secret"], globex_key["secret"]
    coordinator.shutdown()


class TestCancelJobOrgIsolation:
    def test_owner_can_cancel_their_own_running_job(self, two_org_setup):
        client, coordinator, acme_key, _ = two_org_setup
        # A slow command so it's genuinely still "running" when we
        # try to cancel it, not already finished.
        client.post(
            "/jobs", json={"job_id": "job-acme-slow", "command": "sleep 5"},
            headers={"X-API-Key": acme_key},
        )
        deadline = time.time() + 5
        while time.time() < deadline:
            if coordinator.jobs.get("job-acme-slow", {}).get("status") == "running":
                break
            time.sleep(0.05)

        resp = client.post("/jobs/job-acme-slow/cancel", headers={"X-API-Key": acme_key})
        assert resp.status_code == 200

    def test_a_different_org_cannot_cancel_this_orgs_job(self, two_org_setup):
        client, coordinator, acme_key, globex_key = two_org_setup
        client.post(
            "/jobs", json={"job_id": "job-acme-slow2", "command": "sleep 5"},
            headers={"X-API-Key": acme_key},
        )
        deadline = time.time() + 5
        while time.time() < deadline:
            if coordinator.jobs.get("job-acme-slow2", {}).get("status") == "running":
                break
            time.sleep(0.05)

        # THE actual security fix under test: Globex's key must not
        # be able to touch Acme's job at all.
        resp = client.post("/jobs/job-acme-slow2/cancel", headers={"X-API-Key": globex_key})
        assert resp.status_code == 404

        # And the job must genuinely still be running -- the
        # rejected cross-org attempt must not have cancelled it as a
        # side effect before the org check (order of operations
        # matters: check-then-act).
        assert coordinator.jobs["job-acme-slow2"]["status"] == "running"

    def test_unknown_job_id_is_404(self, two_org_setup):
        client, _coordinator, acme_key, _ = two_org_setup
        resp = client.post("/jobs/does-not-exist/cancel", headers={"X-API-Key": acme_key})
        assert resp.status_code == 404


class TestRetryJobOrgIsolation:
    def test_owner_can_retry_their_own_failed_job(self, two_org_setup):
        client, coordinator, acme_key, _ = two_org_setup
        client.post(
            "/jobs", json={"job_id": "job-acme-fail", "command": "exit 1"},
            headers={"X-API-Key": acme_key},
        )
        deadline = time.time() + 5
        while time.time() < deadline:
            if coordinator.jobs.get("job-acme-fail", {}).get("status") == "failed":
                break
            time.sleep(0.05)
        assert coordinator.jobs["job-acme-fail"]["status"] == "failed"

        resp = client.post("/jobs/job-acme-fail/retry", headers={"X-API-Key": acme_key})
        assert resp.status_code == 200
        assert coordinator.jobs["job-acme-fail"]["status"] == "pending"

    def test_a_different_org_cannot_retry_this_orgs_job(self, two_org_setup):
        client, coordinator, acme_key, globex_key = two_org_setup
        client.post(
            "/jobs", json={"job_id": "job-acme-fail2", "command": "exit 1"},
            headers={"X-API-Key": acme_key},
        )
        deadline = time.time() + 5
        while time.time() < deadline:
            if coordinator.jobs.get("job-acme-fail2", {}).get("status") == "failed":
                break
            time.sleep(0.05)

        resp = client.post("/jobs/job-acme-fail2/retry", headers={"X-API-Key": globex_key})
        assert resp.status_code == 404
        assert coordinator.jobs["job-acme-fail2"]["status"] == "failed"

    def test_unknown_job_id_is_404(self, two_org_setup):
        client, _coordinator, acme_key, _ = two_org_setup
        resp = client.post("/jobs/does-not-exist/retry", headers={"X-API-Key": acme_key})
        assert resp.status_code == 404
