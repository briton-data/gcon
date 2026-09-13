"""
End-to-end tests for GET /nodes/{node_id}/enrollment-history -- real
coordinator + real control_plane + real FastAPI app, same style as
test_telemetry_lifecycle.py. This route exposes
node_enrollment_audit (migrations/registry.py version 7), the
durable "who/where enrolled this worker" trail that
grpc_transport.py's Enroll() handler now writes instead of only
logging.

Reuses the same two_org_setup fixture pattern as
test_telemetry_lifecycle.py / test_receipts_org_isolation.py.
"""
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

    globex_node = GCONAgent(node_id="globex-node-1")
    globex_node.org_id = globex_org["org_id"]
    coordinator.register_agent(globex_node)

    coordinator.control_plane.node_enrollment_audit.record(
        node_id="acme-node-1", accepted=True, org_id=acme_org["org_id"],
        enroll_token_id="tok-acme", source_ip="203.0.113.5",
    )
    coordinator.control_plane.node_enrollment_audit.record(
        node_id="globex-node-1", accepted=True, org_id=globex_org["org_id"],
        enroll_token_id="tok-globex", source_ip="198.51.100.9",
    )

    presentation = PresentationLayer(coordinator)
    app = create_api_v1_app(management, presentation)
    client = TestClient(app)

    yield client, acme_key["secret"], globex_key["secret"]
    coordinator.shutdown()


class TestNodeEnrollmentHistoryOrgIsolation:
    def test_owner_sees_their_own_nodes_enrollment_history(self, two_org_setup):
        client, acme_key, _ = two_org_setup
        resp = client.get("/nodes/acme-node-1/enrollment-history", headers={"X-API-Key": acme_key})
        assert resp.status_code == 200
        body = resp.json()
        assert len(body) == 1
        assert body[0]["enroll_token_id"] == "tok-acme"
        assert body[0]["source_ip"] == "203.0.113.5"

    def test_org_cannot_see_a_different_orgs_node_enrollment_history(self, two_org_setup):
        client, acme_key, _ = two_org_setup
        # Acme's key asking about Globex's node - must be 404, not 403
        # (same "don't confirm the node_id exists elsewhere" pattern
        # as the existing GET /nodes/{node_id} route).
        resp = client.get("/nodes/globex-node-1/enrollment-history", headers={"X-API-Key": acme_key})
        assert resp.status_code == 404

    def test_unknown_node_id_is_404(self, two_org_setup):
        client, acme_key, _ = two_org_setup
        resp = client.get("/nodes/does-not-exist/enrollment-history", headers={"X-API-Key": acme_key})
        assert resp.status_code == 404
