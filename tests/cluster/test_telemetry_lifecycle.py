"""
End-to-end telemetry tests -- real coordinator + real control_plane +
real FastAPI app, not mocks, since the thing actually worth verifying
is that trace_id survives the real journey (submit_job -> assign_job
-> dispatch -> _run_job/_run_replicated_job -> create_receipt ->
validate_proof) intact, and that the GET /telemetry/events endpoint's
org-scoping actually works through the real SQL join
(TelemetryRepository.query's org_id path), the same class of bug the
receipts cross-tenant leak earlier this project was.
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

    globex_node = GCONAgent(node_id="globex-node-1")
    globex_node.org_id = globex_org["org_id"]
    coordinator.register_agent(globex_node)

    presentation = PresentationLayer(coordinator)
    app = create_api_v1_app(management, presentation)
    client = TestClient(app)

    yield client, coordinator, acme_key["secret"], globex_key["secret"]
    coordinator.shutdown()


def _wait_for_completion(coordinator, job_id, timeout=5):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if coordinator.jobs.get(job_id, {}).get("status") in ("completed", "failed"):
            return
        time.sleep(0.05)


class TestTraceIdLifecycle:
    def test_trace_id_minted_once_and_consistent_across_lifecycle(self, two_org_setup):
        client, coordinator, acme_key, _ = two_org_setup

        resp = client.post(
            "/jobs", json={"job_id": "job-trace-1", "command": "echo hi"},
            headers={"X-API-Key": acme_key},
        )
        assert resp.status_code == 200
        _wait_for_completion(coordinator, "job-trace-1")

        job_trace_id = coordinator.jobs["job-trace-1"]["trace_id"]
        assert job_trace_id  # minted, not None/empty

        # The receipt itself carries the SAME trace_id, not a fresh one.
        receipt = coordinator.receipts["job-trace-1"]
        assert receipt["trace_id"] == job_trace_id

        # Every telemetry event for this job shares that one trace_id.
        events = client.get(
            "/telemetry/events?job_id=job-trace-1", headers={"X-API-Key": acme_key},
        ).json()
        assert len(events) >= 1
        trace_ids = {e["trace_id"] for e in events}
        assert trace_ids == {job_trace_id}, (
            f"expected every event for this job to share trace_id "
            f"{job_trace_id!r}, got {trace_ids}"
        )

        event_types = {e["event_type"] for e in events}
        assert "job_submitted" in event_types

    def test_two_jobs_get_two_different_trace_ids(self, two_org_setup):
        client, coordinator, acme_key, _ = two_org_setup

        client.post("/jobs", json={"job_id": "job-a", "command": "echo a"}, headers={"X-API-Key": acme_key})
        client.post("/jobs", json={"job_id": "job-b", "command": "echo b"}, headers={"X-API-Key": acme_key})
        _wait_for_completion(coordinator, "job-a")
        _wait_for_completion(coordinator, "job-b")

        trace_a = coordinator.jobs["job-a"]["trace_id"]
        trace_b = coordinator.jobs["job-b"]["trace_id"]
        assert trace_a != trace_b


class TestTelemetryEventsAPIOrgIsolation:
    def test_org_only_sees_its_own_telemetry_events(self, two_org_setup):
        client, coordinator, acme_key, globex_key = two_org_setup

        client.post("/jobs", json={"job_id": "job-acme-t", "command": "echo hi"}, headers={"X-API-Key": acme_key})
        client.post("/jobs", json={"job_id": "job-globex-t", "command": "echo hi"}, headers={"X-API-Key": globex_key})
        _wait_for_completion(coordinator, "job-acme-t")
        _wait_for_completion(coordinator, "job-globex-t")

        acme_events = client.get("/telemetry/events", headers={"X-API-Key": acme_key}).json()
        acme_job_ids = {e["job_id"] for e in acme_events}
        assert "job-acme-t" in acme_job_ids
        assert "job-globex-t" not in acme_job_ids, (
            "acme's API key could see globex's telemetry events"
        )

        globex_events = client.get("/telemetry/events", headers={"X-API-Key": globex_key}).json()
        globex_job_ids = {e["job_id"] for e in globex_events}
        assert "job-globex-t" in globex_job_ids
        assert "job-acme-t" not in globex_job_ids

    def test_job_id_query_param_filters_correctly(self, two_org_setup):
        client, coordinator, acme_key, _ = two_org_setup
        client.post("/jobs", json={"job_id": "job-x", "command": "echo x"}, headers={"X-API-Key": acme_key})
        client.post("/jobs", json={"job_id": "job-y", "command": "echo y"}, headers={"X-API-Key": acme_key})
        _wait_for_completion(coordinator, "job-x")
        _wait_for_completion(coordinator, "job-y")

        events = client.get("/telemetry/events?job_id=job-x", headers={"X-API-Key": acme_key}).json()
        assert len(events) >= 1
        assert all(e["job_id"] == "job-x" for e in events)
