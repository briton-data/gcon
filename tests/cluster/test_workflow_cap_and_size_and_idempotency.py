"""
Three things about workflows and the organization's limits.

  D1  The concurrent-job cap does not fail a workflow. A job the cap holds back waits (READY) and goes in as the
      organization's jobs finish, including after a restart. Before, it failed the job and everything downstream.
  M1  A workflow has a most-jobs limit, checked before anything is built.
  I1  POST /workflows honours Idempotency-Key like POST /jobs: a retried request returns the first workflow.
"""
import sys
import threading

import pytest
from tests.support.label_client import LabelClient as TestClient

from gcon.api.api_v1 import create_api_v1_app
from gcon.cluster.coordinator import GCONCoordinator
from gcon.dashboard.presentation import PresentationLayer
from gcon.management.management_layer import ManagementLayer
from gcon.persistence.control_plane import ControlPlane
from gcon.workflow.workflow import Workflow, WorkflowJob

PY = sys.executable
OK = f'{PY} -c "print(1)"'


def make_workflow(wid, jobs, deps, **kw):
    wf = Workflow(wid, **kw)
    for job_id, command in jobs.items():
        wf.add_job(WorkflowJob(job_id, command))
    for parent, child in deps:
        wf.add_dependency(parent, child)
    return wf


@pytest.fixture
def coord(tmp_path):
    c = GCONCoordinator(control_plane=ControlPlane(path=str(tmp_path / "cp.db")))
    c.pause_scheduler()                     # nothing runs by itself: the tests finish jobs explicitly
    c._max_concurrent_jobs_per_org = 2
    yield c
    c.shutdown()


def finish(coord, job_id, success=True):
    """What the coordinator does when a job ends."""
    coord.jobs[job_id]["status"] = "completed" if success else "failed"
    coord._advance_workflow(job_id, coord.jobs[job_id], success)


# ------------------------------------------------------------------ D1
class TestTheCapDefersInsteadOfFailing:
    def test_jobs_the_cap_holds_back_wait_and_nothing_fails(self, coord):
        wf = make_workflow("w", {f"j{i}": OK for i in range(5)}, [], org_id="acme")
        state = coord.submit_workflow(wf)
        running = [j for j, s in state.job_states.items() if s == "RUNNING"]
        waiting = [j for j, s in state.job_states.items() if s == "READY"]
        assert len(running) == 2 and len(waiting) == 3
        assert state.status == "RUNNING" and not state.errors and not state.failed_jobs
        assert not any(j in coord.jobs for j in waiting)           # not created until there is room

    def test_a_finishing_job_lets_the_next_one_in_until_all_are_done(self, coord):
        state = coord.submit_workflow(make_workflow("w", {f"j{i}": OK for i in range(5)}, [], org_id="acme"))
        for _ in range(10):
            running = [j for j, s in state.job_states.items() if s == "RUNNING"]
            assert len(running) <= 2
            if not running:
                break
            finish(coord, running[0])
        assert state.status == "COMPLETED"
        assert set(state.job_states.values()) == {"COMPLETED"}

    def test_a_dependent_job_waits_for_room_after_its_parent_is_done(self, coord):
        coord._max_concurrent_jobs_per_org = 1
        coord.submit_job("other-1", OK, org_id="acme")                 # the organization's one slot is in use
        state = coord.submit_workflow(make_workflow("w", {"a": OK, "b": OK}, [("a", "b")], org_id="acme"))
        assert state.job_states["a"] == "READY" and state.job_states["b"] == "PENDING"
        finish(coord, "other-1")                                       # NOT a workflow job: it still frees room
        assert state.job_states["a"] == "RUNNING"
        coord.jobs["a"]["status"] = "completed"                        # a ends, and someone else takes its slot
        coord.submit_job("x", OK, org_id="acme")
        coord._advance_workflow("a", coord.jobs["a"], True)
        assert state.job_states["a"] == "COMPLETED" and state.job_states["b"] == "READY"
        finish(coord, "x")
        assert state.job_states["b"] == "RUNNING"

    def test_another_organizations_jobs_do_not_hold_it_back(self, coord):
        coord.submit_job("g1", OK, org_id="globex")
        coord.submit_job("g2", OK, org_id="globex")
        state = coord.submit_workflow(make_workflow("w", {"a": OK}, [], org_id="acme"))
        assert state.job_states["a"] == "RUNNING"

    def test_a_job_that_breaks_a_rule_still_fails_the_workflow(self, coord):
        """Only a cap waits. A refusal that will never change (here: the command was emptied after admission) is final."""
        coord._max_concurrent_jobs_per_org = 1
        wf = make_workflow("w", {"a": OK, "b": OK, "after": OK}, [("b", "after")], org_id="acme")
        state = coord.submit_workflow(wf)
        assert state.job_states["b"] == "READY"
        wf.jobs["b"].command = "   "                                    # no longer a valid command
        finish(coord, "a")
        assert state.job_states["b"] == "FAILED" and state.job_states["after"] == "BLOCKED"
        assert state.errors["b"]

    def test_waiting_survives_a_restart(self, tmp_path, monkeypatch):
        monkeypatch.setenv("GCON_MAX_CONCURRENT_JOBS_PER_ORG", "1")
        db = str(tmp_path / "cp.db")
        c1 = GCONCoordinator(control_plane=ControlPlane(path=db))
        c1.pause_scheduler()
        c1.submit_workflow(make_workflow("w", {"a": OK, "b": OK}, [], org_id="acme"))
        c1.shutdown()
        c2 = GCONCoordinator(control_plane=ControlPlane(path=db))
        try:
            c2.pause_scheduler()
            state = c2.workflow_engine.states["w"]
            assert sorted(state.job_states.values()) == ["READY", "RUNNING"]
            running = next(j for j, s in state.job_states.items() if s == "RUNNING")
            finish(c2, running)
            assert sorted(state.job_states.values()) == ["COMPLETED", "RUNNING"]
        finally:
            c2.shutdown()

    def test_simultaneous_completions_never_submit_a_job_twice(self, coord):
        coord._max_concurrent_jobs_per_org = 6
        state = coord.submit_workflow(make_workflow("w", {f"j{i}": OK for i in range(6)}, [], org_id="acme"))
        coord._max_concurrent_jobs_per_org = 2
        for j in list(state.job_states):
            coord.jobs[j]["status"] = "completed"                  # all six done at once ...
        more = make_workflow("w2", {f"k{i}": OK for i in range(8)}, [], org_id="acme")
        state2 = coord.submit_workflow(more)
        barrier = threading.Barrier(2)
        errors = []

        def done(job_id):
            barrier.wait()
            try:
                finish(coord, job_id)
            except Exception as exc:
                errors.append(exc)

        running = [j for j, s in state2.job_states.items() if s == "RUNNING"]
        threads = [threading.Thread(target=done, args=(j,)) for j in running]
        [t.start() for t in threads]
        [t.join(10) for t in threads]
        assert not errors and not state2.errors and not state2.failed_jobs
        assert len([j for j in state2.job_states if j in coord.jobs]) == len({j for j in coord.jobs if j.startswith("k")})


# ------------------------------------------------------------------ M1
class TestWorkflowSize:
    def test_a_workflow_over_the_limit_is_refused_whole(self, coord, monkeypatch):
        monkeypatch.setenv("GCON_MAX_WORKFLOW_JOBS", "5")
        with pytest.raises(ValueError, match="at most 5 jobs"):
            coord.submit_workflow(make_workflow("big", {f"j{i}": OK for i in range(6)}, [], org_id="acme"))
        assert "big" not in coord.workflow_engine.states and not coord.jobs

    def test_a_workflow_at_the_limit_is_accepted(self, coord, monkeypatch):
        monkeypatch.setenv("GCON_MAX_WORKFLOW_JOBS", "5")
        coord.submit_workflow(make_workflow("ok", {f"j{i}": OK for i in range(5)}, [], org_id="acme"))
        assert "ok" in coord.workflow_engine.states

    def test_there_is_a_sensible_default(self, coord, monkeypatch):
        monkeypatch.delenv("GCON_MAX_WORKFLOW_JOBS", raising=False)
        from gcon.workflow.workflow_engine import max_workflow_jobs
        assert 50 <= max_workflow_jobs() <= 1000


# ------------------------------------------------------------------ I1 and M1 through the API
@pytest.fixture
def api(tmp_path):
    coordinator = GCONCoordinator(control_plane=ControlPlane(path=str(tmp_path / "cp.db")))
    management = ManagementLayer(coordinator=coordinator, db_path=str(tmp_path / "m.db"))
    coordinator.org_worker_gate_resolver = None
    coordinator.pause_scheduler()
    orgs = {}
    for name in ("acme", "globex"):
        org = management.create_organization(f"{name} Inc")
        user = management.create_user(f"{name} user", f"u@{name}.example", role="Owner", organization_id=org["org_id"])
        orgs[name] = management.create_api_key(f"{name}-k", user["user_id"], scopes=["Submit workflows", "View monitoring"])["secret"]
    yield TestClient(create_api_v1_app(management, PresentationLayer(coordinator))), coordinator, orgs
    coordinator.shutdown()


def _body(*extra):
    return {"name": "n", "jobs": [{"job_id": "a", "command": OK}, {"job_id": "b", "command": OK, "depends_on": ["a"]}, *extra]}


def _h(key, idem=None):
    h = {"X-API-Key": key}
    if idem:
        h["Idempotency-Key"] = idem
    return h


class TestWorkflowIdempotency:
    def test_a_retry_returns_the_first_workflow_and_creates_no_second(self, api):
        client, coordinator, keys = api
        first = client.post("/workflows", json=_body(), headers=_h(keys["acme"], "run-1"))
        again = client.post("/workflows", json=_body(), headers=_h(keys["acme"], "run-1"))
        assert first.status_code == again.status_code == 200
        assert again.json()["workflow_id"] == first.json()["workflow_id"]
        assert again.json()["jobs"] == first.json()["jobs"]
        assert again.headers.get("Idempotent-Replayed") == "true"
        assert "Idempotent-Replayed" not in first.headers
        assert len(coordinator.workflow_engine.states) == 1 and len(coordinator.jobs) == 1

    def test_without_a_key_every_request_is_a_new_workflow(self, api):
        client, coordinator, keys = api
        client.post("/workflows", json=_body(), headers=_h(keys["acme"]))
        client.post("/workflows", json=_body(), headers=_h(keys["acme"]))
        assert len(coordinator.workflow_engine.states) == 2

    def test_the_same_key_for_a_different_workflow_is_refused(self, api):
        client, _, keys = api
        client.post("/workflows", json=_body(), headers=_h(keys["acme"], "run-1"))
        other = client.post("/workflows", json=_body({"job_id": "c", "command": OK}), headers=_h(keys["acme"], "run-1"))
        assert other.status_code == 422

    def test_keys_are_per_organization(self, api):
        client, coordinator, keys = api
        a = client.post("/workflows", json=_body(), headers=_h(keys["acme"], "shared"))
        g = client.post("/workflows", json=_body(), headers=_h(keys["globex"], "shared"))
        assert a.json()["workflow_id"] != g.json()["workflow_id"]

    def test_a_workflow_key_does_not_collide_with_a_job_key(self, api):
        client, _, keys = api
        job = client.post("/jobs", json={"command": OK}, headers=_h(keys["acme"], "same"))
        wf = client.post("/workflows", json=_body(), headers=_h(keys["acme"], "same"))
        assert job.status_code == 200 and wf.status_code == 200 and "workflow_id" in wf.json()

    def test_a_refused_workflow_does_not_use_up_its_key(self, api):
        client, _, keys = api
        bad = client.post("/workflows", json={"name": "n", "jobs": [{"job_id": "a", "command": "   "}]},
                          headers=_h(keys["acme"], "run-1"))
        assert bad.status_code == 400
        good = client.post("/workflows", json=_body(), headers=_h(keys["acme"], "run-1"))
        assert good.status_code == 200

    def test_simultaneous_retries_make_one_workflow(self, api):
        client, coordinator, keys = api
        out = []
        barrier = threading.Barrier(6)

        def go():
            barrier.wait()
            out.append(client.post("/workflows", json=_body(), headers=_h(keys["acme"], "burst")).json()["workflow_id"])

        threads = [threading.Thread(target=go) for _ in range(6)]
        [t.start() for t in threads]
        [t.join(30) for t in threads]
        assert len(set(out)) == 1 and len(coordinator.workflow_engine.states) == 1

    def test_a_bad_key_is_refused(self, api):
        client, _, keys = api
        r = client.post("/workflows", json=_body(), headers=_h(keys["acme"], "x" * 300))
        assert r.status_code == 400


class TestWorkflowSizeThroughTheApi:
    def test_too_many_jobs_is_a_400_that_says_so(self, api, monkeypatch):
        client, coordinator, keys = api
        monkeypatch.setenv("GCON_MAX_WORKFLOW_JOBS", "3")
        body = {"name": "n", "jobs": [{"job_id": f"j{i}", "command": OK} for i in range(4)]}
        r = client.post("/workflows", json=body, headers=_h(keys["acme"]))
        assert r.status_code == 400 and "at most 3 jobs" in r.json()["detail"]
        assert not coordinator.workflow_engine.states
