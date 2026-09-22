"""
POST /jobs/{job_id}/cancel on a job that is still "pending" (queued, not yet
dispatched to any worker -- e.g. it asked for a GPU and none is available).

Before this, cancel_job() only accepted "running" jobs, so a customer whose
job could never be scheduled had no way to cancel it and no way to submit a
corrected one under the same job_id. The critical property, beyond "it can be
cancelled", is that a job cancelled while still queued must never actually
run later if a matching worker shows up afterwards.
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
    yield client, coordinator, db
    coordinator.shutdown()


def _signup(client, email="a@acme.example"):
    r = client.post("/auth/signup", json={"org_name": "Acme", "name": "Ann", "email": email, "password": "correct-horse-1"})
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


class TestCancelWhileStillQueued:
    def test_a_job_that_cannot_be_scheduled_can_be_cancelled(self, env):
        client, coordinator, _ = env
        acme = _signup(client)
        key = acme["api_key"]["secret"]
        # No workers registered at all -> the job can never leave "pending" on its own.
        r = client.post("/jobs", json={"job_id": "stuck-1", "command": "python x.py", "kind": "resourced", "requires": {"gpu": True}}, headers=_h(key))
        assert r.status_code == 200
        assert client.get("/jobs/stuck-1", headers=_h(key)).json()["status"] == "pending"

        cancel = client.post("/jobs/stuck-1/cancel", headers=_h(key))
        assert cancel.status_code == 200, cancel.text
        assert cancel.json() == {"job_id": "stuck-1", "cancelled": True, "process_killed": False}
        job = client.get("/jobs/stuck-1", headers=_h(key)).json()
        assert job["status"] == "cancelled" and job["completed_at"] is not None

    def test_a_cancelled_queued_job_never_runs_even_if_a_worker_appears_later(self, env):
        client, coordinator, _ = env
        acme = _signup(client)
        key = acme["api_key"]["secret"]
        client.post("/jobs", json={"job_id": "race-1", "command": "python x.py", "kind": "resourced", "requires": {"gpu": True}}, headers=_h(key))
        assert client.get("/jobs/race-1", headers=_h(key)).json()["status"] == "pending"
        assert client.post("/jobs/race-1/cancel", headers=_h(key)).status_code == 200

        # The matching worker shows up only now, after cancellation, and the job
        # is still (deliberately) sitting in the coordinator's dispatch queue.
        # GPU capability is looked up from the durable capabilities table (see
        # scheduler.Scheduler._satisfies), not detected from real hardware, so
        # it's reported directly the same way a real GPU worker's heartbeat
        # would populate it.
        node = GCONAgent(node_id="late-gpu-node")
        node.org_id = acme["organization"]["org_id"]
        coordinator.register_agent(node)
        coordinator.control_plane.node_capabilities.set_capabilities(
            "late-gpu-node", {"gpu": "Fake GPU", "gpu_memory_total_mb": "16384"},
        )

        time.sleep(0.6)  # give the real scheduler loop several ticks to try dispatching it
        job = client.get("/jobs/race-1", headers=_h(key)).json()
        assert job["status"] == "cancelled", f"a cancelled queued job started running: {job}"
        assert job["node_id"] is None

    def test_cancel_is_persisted_and_survives_a_restart(self, env):
        client, coordinator, db = env
        acme = _signup(client)
        key = acme["api_key"]["secret"]
        client.post("/jobs", json={"job_id": "stuck-2", "command": "python x.py", "kind": "resourced", "requires": {"gpu": True}}, headers=_h(key))
        client.post("/jobs/stuck-2/cancel", headers=_h(key))
        coordinator.shutdown()

        again = GCONCoordinator(control_plane=ControlPlane(path=db))
        try:
            row = again.control_plane.jobs.get("stuck-2")
            assert row is not None and row["status"] == "cancelled"
        finally:
            again.shutdown()

    def test_it_can_be_retried_afterwards_like_any_other_non_running_job(self, env):
        # retry_job() already accepts "failed" or "pending" -- confirm "cancelled"
        # (this new terminal state for a never-dispatched job) is refused, same
        # as cancelling a completed job would be: retry is for failed work, not
        # for undoing a cancellation.
        client, coordinator, _ = env
        acme = _signup(client)
        key = acme["api_key"]["secret"]
        client.post("/jobs", json={"job_id": "stuck-3", "command": "echo hi", "kind": "resourced", "requires": {"gpu": True}}, headers=_h(key))
        client.post("/jobs/stuck-3/cancel", headers=_h(key))
        r = client.post("/jobs/stuck-3/retry", headers=_h(key))
        assert r.status_code == 400
        assert "cancelled" in r.json()["detail"]

    def test_organization_isolation_still_applies(self, env):
        client, coordinator, _ = env
        acme = _signup(client, "a@acme.example")
        globex = _signup(client, "g@globex.example")
        client.post("/jobs", json={"job_id": "mine-1", "command": "x", "kind": "resourced", "requires": {"gpu": True}}, headers=_h(acme["api_key"]["secret"]))
        r = client.post("/jobs/mine-1/cancel", headers=_h(globex["api_key"]["secret"]))
        assert r.status_code == 404
        assert client.get("/jobs/mine-1", headers=_h(acme["api_key"]["secret"])).json()["status"] == "pending"

    def test_a_standard_command_job_can_also_be_cancelled_while_queued(self, env):
        # Doesn't need a hardware requirement to stay pending -- any job is
        # pending until a worker exists at all.
        client, coordinator, _ = env
        acme = _signup(client)
        key = acme["api_key"]["secret"]
        client.post("/jobs", json={"job_id": "plain-1", "command": "echo hi"}, headers=_h(key))
        assert client.get("/jobs/plain-1", headers=_h(key)).json()["status"] == "pending"
        assert client.post("/jobs/plain-1/cancel", headers=_h(key)).status_code == 200
        assert client.get("/jobs/plain-1", headers=_h(key)).json()["status"] == "cancelled"


class TestStillRefusesWhatItShould:
    def test_completed_job_cannot_be_cancelled(self, env):
        client, coordinator, _ = env
        acme = _signup(client)
        key = acme["api_key"]["secret"]
        node = GCONAgent(node_id="n1")
        node.org_id = acme["organization"]["org_id"]
        coordinator.register_agent(node)
        client.post("/jobs", json={"job_id": "done-1", "command": "echo hi"}, headers=_h(key))
        assert _wait(lambda: client.get("/jobs/done-1", headers=_h(key)).json()["status"] == "completed")
        r = client.post("/jobs/done-1/cancel", headers=_h(key))
        assert r.status_code == 400 and "not pending or running" in r.json()["detail"]

    def test_already_cancelled_job_cannot_be_cancelled_again(self, env):
        client, coordinator, _ = env
        acme = _signup(client)
        key = acme["api_key"]["secret"]
        client.post("/jobs", json={"job_id": "stuck-4", "command": "x", "kind": "resourced", "requires": {"gpu": True}}, headers=_h(key))
        assert client.post("/jobs/stuck-4/cancel", headers=_h(key)).status_code == 200
        again = client.post("/jobs/stuck-4/cancel", headers=_h(key))
        assert again.status_code == 400 and "not pending or running" in again.json()["detail"]

    def test_a_genuinely_running_job_still_cancels_by_killing_its_process(self, env):
        # Regression check: the original (pre-existing) behaviour for a real
        # running job must be completely unaffected by this change.
        client, coordinator, _ = env
        acme = _signup(client)
        key = acme["api_key"]["secret"]
        node = GCONAgent(node_id="n1")
        node.org_id = acme["organization"]["org_id"]
        coordinator.register_agent(node)
        client.post("/jobs", json={"job_id": "run-1", "command": "sleep 30"}, headers=_h(key))
        assert _wait(lambda: client.get("/jobs/run-1", headers=_h(key)).json()["status"] == "running")
        r = client.post("/jobs/run-1/cancel", headers=_h(key))
        assert r.status_code == 200 and r.json()["job_id"] == "run-1"
        assert _wait(lambda: client.get("/jobs/run-1", headers=_h(key)).json()["status"] == "cancelled")
