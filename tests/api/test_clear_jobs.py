"""
POST /jobs/clear -- real coordinator, real control-plane database, real API app.

What must hold:
  * only the jobs NAMED in the request are touched;
  * only jobs of the caller's own organization (another org's job id looks
    exactly like a job that doesn't exist);
  * only failed or cancelled jobs -- never queued, running or completed;
  * never a job that has a receipt (a receipt is attributed to an org only
    through its job, so deleting the job would strand the evidence);
  * a cleared job stays gone after a restart (it must leave the database, not
    just memory);
  * anything skipped is reported with a reason and left exactly as it was.
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


def _signup(client, org, email):
    r = client.post("/auth/signup", json={"org_name": org, "name": org, "email": email, "password": "correct-horse-1"})
    assert r.status_code == 200, r.text
    return r.json()


def _h(secret):
    return {"Authorization": f"Bearer {secret}"}


def _wait(pred, timeout=8.0):
    end = time.time() + timeout
    while time.time() < end:
        if pred():
            return True
        time.sleep(0.05)
    return False


class World:
    """Acme with one worker and jobs in every state; Globex alongside."""
    def __init__(self, client, coordinator):
        self.client, self.coord = client, coordinator
        acme = _signup(client, "Acme", "a@acme.example")
        globex = _signup(client, "Globex", "g@globex.example")
        self.key, self.gkey = acme["api_key"]["secret"], globex["api_key"]["secret"]
        self.org = acme["organization"]["org_id"]
        self.gorg = globex["organization"]["org_id"]
        for i in range(3):
            n = GCONAgent(node_id=f"acme-{i}")
            n.org_id = self.org
            coordinator.register_agent(n)

    def submit(self, job_id, command="echo hi", key=None, **extra):
        r = self.client.post("/jobs", json={"job_id": job_id, "command": command, **extra}, headers=_h(key or self.key))
        assert r.status_code == 200, r.text

    def status(self, job_id, key=None):
        r = self.client.get(f"/jobs/{job_id}", headers=_h(key or self.key))
        return r.json()["status"] if r.status_code == 200 else None

    def clear(self, ids, key=None):
        return self.client.post("/jobs/clear", json={"job_ids": ids}, headers=_h(key or self.key))

    def make_failed(self, job_id):
        self.submit(job_id, "exit 3")
        assert _wait(lambda: self.status(job_id) == "failed"), self.status(job_id)

    def make_cancelled(self, job_id):
        self.submit(job_id, "sleep 30")
        assert _wait(lambda: self.status(job_id) == "running")
        assert self.client.post(f"/jobs/{job_id}/cancel", headers=_h(self.key)).status_code == 200
        assert _wait(lambda: self.status(job_id) == "cancelled")

    def make_completed(self, job_id):
        self.submit(job_id, "echo done")
        assert _wait(lambda: self.status(job_id) == "completed")
        assert _wait(lambda: any(r["job_id"] == job_id for r in self.client.get("/receipts", headers=_h(self.key)).json()))

    def make_pending(self, job_id):
        # needs a GPU that no worker has, so it stays queued
        self.submit(job_id, "python x.py", kind="resourced", requires={"gpu": True})
        assert self.status(job_id) == "pending"


@pytest.fixture
def w(env):
    client, coordinator, _ = env
    return World(client, coordinator)


class TestClearsWhatItShould:
    def test_clears_a_failed_job(self, w):
        w.make_failed("f1")
        r = w.clear(["f1"])
        assert r.status_code == 200
        assert r.json() == {"cleared": ["f1"], "skipped": []}
        assert w.status("f1") is None
        assert all(j["job_id"] != "f1" for j in w.client.get("/jobs", headers=_h(w.key)).json())

    def test_clears_a_cancelled_job(self, w):
        w.make_cancelled("c1")
        assert w.clear(["c1"]).json()["cleared"] == ["c1"]
        assert w.status("c1") is None

    def test_only_the_named_jobs_are_touched(self, w):
        for j in ("f1", "f2", "f3"):
            w.make_failed(j)
        assert w.clear(["f2"]).json()["cleared"] == ["f2"]
        assert w.status("f1") == "failed" and w.status("f3") == "failed"

    def test_duplicates_in_the_request_are_cleared_once(self, w):
        w.make_failed("f1")
        r = w.clear(["f1", "f1", " f1 "]).json()
        assert r["cleared"] == ["f1"] and r["skipped"] == []


class TestRefusesWhatItMust:
    def test_running_and_queued_jobs_are_never_cleared(self, w):
        w.submit("run1", "sleep 30")
        assert _wait(lambda: w.status("run1") == "running")
        w.make_pending("q1")
        r = w.clear(["run1", "q1"]).json()
        assert r["cleared"] == []
        assert {s["job_id"]: s["reason"] for s in r["skipped"]} == {"run1": "not_clearable", "q1": "not_clearable"}
        assert w.status("run1") == "running" and w.status("q1") == "pending"
        w.client.post("/jobs/run1/cancel", headers=_h(w.key))

    def test_a_completed_job_is_refused_and_its_receipt_survives(self, w):
        w.make_completed("done1")
        receipts_before = w.client.get("/receipts", headers=_h(w.key)).json()
        r = w.clear(["done1"]).json()
        assert r["cleared"] == []
        assert r["skipped"][0]["job_id"] == "done1" and r["skipped"][0]["reason"] == "not_clearable"
        assert w.status("done1") == "completed"
        assert w.client.get("/receipts", headers=_h(w.key)).json() == receipts_before
        rid = receipts_before[0]["receipt_id"]
        assert w.client.get(f"/receipts/{rid}", headers=_h(w.key)).status_code == 200

    def test_a_failed_job_that_somehow_has_a_receipt_is_still_refused(self, w):
        # Belt and braces: the receipt check applies whatever the status says.
        w.make_completed("odd1")
        with w.coord.jobs_lock:
            w.coord.jobs["odd1"]["status"] = "failed"
        r = w.clear(["odd1"]).json()
        assert r["cleared"] == [] and r["skipped"][0]["reason"] == "has_receipt"
        assert "odd1" in w.coord.jobs

    def test_unknown_job_is_skipped_not_an_error(self, w):
        w.make_failed("f1")
        r = w.clear(["nope", "f1"])
        assert r.status_code == 200
        assert r.json()["cleared"] == ["f1"]
        assert r.json()["skipped"] == [{"job_id": "nope", "reason": "not_found", "message": "This job was not found."}]

    def test_skipped_jobs_carry_a_customer_readable_message(self, w):
        w.make_completed("done1")
        sk = w.clear(["done1"]).json()["skipped"][0]
        assert "failed or cancelled" in sk["message"]


class TestOrganizationIsolation:
    def test_another_orgs_job_cannot_be_cleared_and_looks_nonexistent(self, w):
        w.make_failed("f1")
        theirs = w.clear(["f1"], key=w.gkey).json()
        nothing = w.clear(["never-existed"], key=w.gkey).json()
        assert theirs["cleared"] == [] and w.status("f1") == "failed"
        # Same reason and wording as a job that doesn't exist: ids can't be probed across orgs.
        assert theirs["skipped"][0]["reason"] == nothing["skipped"][0]["reason"] == "not_found"
        assert theirs["skipped"][0]["message"] == nothing["skipped"][0]["message"]

    def test_a_mixed_batch_only_clears_own_jobs(self, w):
        w.make_failed("mine")
        gnode = GCONAgent(node_id="glob-0")
        gnode.org_id = w.gorg
        w.coord.register_agent(gnode)
        w.client.post("/jobs", json={"job_id": "theirs", "command": "exit 1"}, headers=_h(w.gkey))
        assert _wait(lambda: w.status("theirs", key=w.gkey) == "failed")
        r = w.clear(["mine", "theirs"]).json()
        assert r["cleared"] == ["mine"]
        assert r["skipped"][0]["job_id"] == "theirs" and r["skipped"][0]["reason"] == "not_found"
        assert w.status("theirs", key=w.gkey) == "failed"


class TestInputAndAuth:
    def test_requires_authentication(self, w):
        assert w.client.post("/jobs/clear", json={"job_ids": ["x"]}).status_code == 401

    def test_empty_selection_is_rejected(self, w):
        assert w.clear([]).status_code == 400
        assert w.clear(["", "  "]).status_code == 400

    def test_there_is_no_clear_everything_form(self, w):
        assert w.client.post("/jobs/clear", json={}, headers=_h(w.key)).status_code == 422
        assert w.client.post("/jobs/clear", json={"status": "failed"}, headers=_h(w.key)).status_code == 422

    def test_too_many_at_once_is_rejected_and_nothing_is_cleared(self, w):
        w.make_failed("f1")
        r = w.clear(["f1"] + [f"x{i}" for i in range(200)])
        assert r.status_code == 400 and "at most 200" in r.json()["detail"]
        assert w.status("f1") == "failed"

    def test_a_cleared_job_cannot_then_be_retried(self, w):
        w.make_failed("f1")
        w.clear(["f1"])
        assert w.client.post("/jobs/f1/retry", headers=_h(w.key)).status_code == 404


class TestDurability:
    def test_a_cleared_job_stays_gone_after_a_restart(self, env):
        client, coordinator, db = env
        w = World(client, coordinator)
        w.make_failed("gone")
        w.make_failed("kept")
        assert w.clear(["gone"]).json()["cleared"] == ["gone"]
        coordinator.shutdown()

        # A fresh coordinator on the same database, as after a restart.
        again = GCONCoordinator(control_plane=ControlPlane(path=db))
        try:
            assert "gone" not in again.jobs
            assert again.control_plane.jobs.get("gone") is None
            assert again.control_plane.jobs.get("kept") is not None      # only what was named was removed
        finally:
            again.shutdown()

    def test_attempts_and_logs_of_a_cleared_job_go_with_it(self, env):
        client, coordinator, db = env
        w = World(client, coordinator)
        w.make_failed("gone")
        w.clear(["gone"])
        rows = coordinator.control_plane.db.execute("SELECT COUNT(*) FROM job_attempts WHERE job_id = ?", ("gone",)).fetchone()
        assert rows[0] == 0
