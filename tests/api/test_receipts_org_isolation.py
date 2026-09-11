"""
End-to-end HTTP test for the api_v1.py `/receipts` cross-tenant leak
fixed in this session.

`list_receipts` previously called `presentation.get_receipts()` with
no `org_id` at all -- unlike `list_nodes`/`list_jobs`, which already
resolved `org_id = getattr(owner, "organization_id", None)` from the
caller's API key. Any org's API key with "View monitoring" scope
could pull every receipt ever issued to every other org through this
one endpoint, even though the underlying coordinator/DB layer
(`get_receipts(org_id=...)`, `ReceiptRepository.search_paginated`)
already supported org filtering and is exercised by
tests/cluster/test_org_isolation.py at the coordinator level.

Goes through the real FastAPI app + two real orgs + two real API
keys (not a mocked auth dependency), since the bug was specifically
about org_id getting dropped between api_v1.py and presentation.py --
a mocked layer in between would hide exactly this class of bug.
"""

import time

import pytest
from fastapi.testclient import TestClient

from gcon.api.api_v1 import create_api_v1_app
from gcon.cluster.coordinator import GCONCoordinator
from gcon.dashboard.presentation import PresentationLayer
from gcon.execution.agent import GCONAgent
from gcon.management.management_layer import ManagementLayer


@pytest.fixture
def two_org_setup(tmp_path):
    coordinator = GCONCoordinator()

    management = ManagementLayer(coordinator=coordinator, db_path=str(tmp_path / "mgmt.db"))

    acme_org = management.create_organization("Acme Corp")
    acme_user = management.create_user(
        "Acme User", "user@acme.example", role="Owner",
        organization_id=acme_org["org_id"],
    )
    acme_key = management.create_api_key(
        "acme-key", owner_user_id=acme_user["user_id"], scopes=["Submit workflows", "View monitoring"],
    )

    globex_org = management.create_organization("Globex Inc")
    globex_user = management.create_user(
        "Globex User", "user@globex.example", role="Owner",
        organization_id=globex_org["org_id"],
    )
    globex_key = management.create_api_key(
        "globex-key", owner_user_id=globex_user["user_id"], scopes=["Submit workflows", "View monitoring"],
    )

    # Dispatch requires a node whose OWN attested org_id matches the
    # submitting job's org_id exactly (scheduler.py select_node) -- so
    # each org needs its own dedicated node using that org's real
    # org_id, not a placeholder string, or the job never dispatches.
    acme_node = GCONAgent(node_id="acme-node-1")
    acme_node.org_id = acme_org["org_id"]
    coordinator.register_agent(acme_node)

    globex_node = GCONAgent(node_id="globex-node-1")
    globex_node.org_id = globex_org["org_id"]
    coordinator.register_agent(globex_node)

    presentation = PresentationLayer(coordinator)
    app = create_api_v1_app(management, presentation)
    client = TestClient(app)

    yield client, coordinator, acme_key["secret"], globex_key["secret"]
    coordinator.shutdown()


class TestReceiptsOrgIsolation:
    def test_org_only_sees_its_own_receipts_via_rest_api(self, two_org_setup):
        client, coordinator, acme_key, globex_key = two_org_setup

        acme_resp = client.post(
            "/jobs", json={"job_id": "job-acme-1", "command": "echo hi"},
            headers={"X-API-Key": acme_key},
        )
        assert acme_resp.status_code == 200
        globex_resp = client.post(
            "/jobs", json={"job_id": "job-globex-1", "command": "echo hi"},
            headers={"X-API-Key": globex_key},
        )
        assert globex_resp.status_code == 200

        # Give the background dispatch threads a moment to post a
        # receipt for each job (same wait pattern as
        # tests/cluster/test_org_isolation.py).
        for _ in range(50):
            if "job-acme-1" in coordinator.receipts and "job-globex-1" in coordinator.receipts:
                break
            time.sleep(0.05)

        acme_list = client.get("/receipts", headers={"X-API-Key": acme_key}).json()
        acme_job_ids = {r["job_id"] for r in acme_list}
        assert "job-acme-1" in acme_job_ids
        assert "job-globex-1" not in acme_job_ids, (
            "acme's API key could see globex's receipt via GET /receipts"
        )

        globex_list = client.get("/receipts", headers={"X-API-Key": globex_key}).json()
        globex_job_ids = {r["job_id"] for r in globex_list}
        assert "job-globex-1" in globex_job_ids
        assert "job-acme-1" not in globex_job_ids, (
            "globex's API key could see acme's receipt via GET /receipts"
        )
