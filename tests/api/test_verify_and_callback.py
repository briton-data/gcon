"""
End-to-end HTTP tests for the api_v1.py /jobs endpoint, covering the
two passthrough gaps fixed in this session:

  1. `verify=` (replication) was Python-API only -- not present at all
     in JobSubmitRequest, so no HTTP submitter could ask for it.
  2. `presentation.submit_job()` accepted a `callback_url` parameter
     but never forwarded it to `coordinator.submit_job()` -- any job
     submitted over the API with a callback silently lost it, with no
     error raised anywhere.

Goes through the real FastAPI app + a real API key + a real
authenticated user (not a mocked auth dependency), since the bug
being tested was specifically about data getting lost between two
real layers (api_v1.py -> presentation.py -> coordinator.py) -- a
mocked layer in between would hide exactly this class of bug.
"""

import pytest
from fastapi.testclient import TestClient

from gcon.api.api_v1 import create_api_v1_app
from gcon.cluster.coordinator import GCONCoordinator
from gcon.dashboard.presentation import PresentationLayer
from gcon.execution.agent import GCONAgent
from gcon.management.management_layer import ManagementLayer


@pytest.fixture
def api_setup(tmp_path):
    coordinator = GCONCoordinator()
    coordinator.register_agent(GCONAgent(node_id="node-1"))
    coordinator.register_agent(GCONAgent(node_id="node-2"))

    management = ManagementLayer(coordinator=coordinator, db_path=str(tmp_path / "mgmt.db"))
    org = management.create_organization("Acme Corp")
    user = management.create_user(
        "Test User", "test@acme.example", role="Owner",
        organization_id=org["org_id"],
    )
    key = management.create_api_key(
        "test-key", owner_user_id=user["user_id"], scopes=["Submit workflows", "View monitoring"],
    )

    presentation = PresentationLayer(coordinator)
    app = create_api_v1_app(management, presentation)
    client = TestClient(app)

    yield client, key["secret"], coordinator, org["org_id"]
    coordinator.shutdown()


class TestVerifyAndCallbackPassthrough:
    def test_verify_param_reaches_the_coordinator(self, api_setup):
        client, api_key, coordinator, org_id = api_setup
        resp = client.post(
            "/jobs",
            json={"job_id": "job-verify-1", "command": "echo hi", "verify": {"replicas": 2}},
            headers={"X-API-Key": api_key},
        )
        assert resp.status_code == 200, resp.text
        assert coordinator.jobs["job-verify-1"]["verify"] == {"replicas": 2}

    def test_no_verify_param_is_still_none(self, api_setup):
        client, api_key, coordinator, org_id = api_setup
        resp = client.post(
            "/jobs",
            json={"job_id": "job-plain-1", "command": "echo hi"},
            headers={"X-API-Key": api_key},
        )
        assert resp.status_code == 200, resp.text
        assert coordinator.jobs["job-plain-1"].get("verify") is None

    def test_callback_url_reaches_the_coordinator(self, api_setup):
        client, api_key, coordinator, org_id = api_setup
        resp = client.post(
            "/jobs",
            json={
                "job_id": "job-callback-1",
                "command": "echo hi",
                "callback_url": "https://example.com/gcon-callback",
            },
            headers={"X-API-Key": api_key},
        )
        assert resp.status_code == 200, resp.text
        assert coordinator.jobs["job-callback-1"]["callback_url"] == "https://example.com/gcon-callback"

    def test_org_id_is_derived_from_the_authenticated_user_not_the_request(self, api_setup):
        # A submitter cannot claim a different org_id via the request
        # body -- JobSubmitRequest has no org_id field at all; it's
        # always resolved server-side from auth["owner"].
        client, api_key, coordinator, org_id = api_setup
        resp = client.post(
            "/jobs",
            json={"job_id": "job-org-1", "command": "echo hi"},
            headers={"X-API-Key": api_key},
        )
        assert resp.status_code == 200, resp.text
        assert coordinator.jobs["job-org-1"]["org_id"] == org_id
