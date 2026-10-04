"""
Orchestration wiring: a workflow's jobs behave like first-class jobs.

Real GCONCoordinator + real ControlPlane (SQLite) + real in-process agents,
same as test_coordinator_full.py. Covers what used to be broken:
  - workflow jobs lost their organization (so org limits/isolation skipped them)
  - a failed job blocked only its direct children, leaving deeper jobs PENDING
  - a cancelled job left its workflow RUNNING forever
  - retrying a failed job completed the job but never resumed the workflow
  - a duplicate workflow_id silently overwrote the live one; a job-id clash
    half-submitted a workflow
  - workflow state lived only in memory, so a restart orphaned every DAG
"""
import sys
import time

import pytest
from tests.support.label_client import LabelClient as TestClient

from gcon.api.api_v1 import create_api_v1_app
from gcon.cluster.coordinator import GCONCoordinator
from gcon.dashboard.presentation import PresentationLayer
from gcon.execution.agent import GCONAgent
from gcon.management.management_layer import ManagementLayer
from gcon.persistence.control_plane import ControlPlane
from gcon.workflow.workflow import Workflow, WorkflowJob

PY = sys.executable
OK = f'{PY} -c "print(1)"'
BAD = f'{PY} -c "import sys; sys.exit(1)"'


def wait_for(pred, timeout=10, interval=0.05):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if pred():
            return True
        time.sleep(interval)
    return False


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
    c.register_agent(GCONAgent(node_id="n1"))
    yield c
    c.shutdown()


# ------------------------------------------------------------------ org
def test_workflow_jobs_inherit_the_workflow_org(coord):
    # Org-attributed jobs only run on that org's workers (tenant isolation),
    # exactly like jobs submitted directly -- so give acme its own worker.
    node = GCONAgent(node_id="acme-node")
    node.org_id = "acme"
    coord.register_agent(node)
    wf = make_workflow("wf-org", {"a": OK, "b": OK}, [("a", "b")], org_id="acme", created_by="u1")
    state = coord.submit_workflow(wf)
    assert wait_for(lambda: state.status == "COMPLETED")
    assert coord.jobs["a"]["org_id"] == "acme" and coord.jobs["b"]["org_id"] == "acme"
    assert coord.jobs["a"]["node_id"] == "acme-node"             # never ran on the unowned worker
    assert state.summary()["org_id"] == "acme"


def test_get_workflows_is_scoped_by_org(coord):
    coord.submit_workflow(make_workflow("wf-acme", {"a1": OK}, [], org_id="acme"))
    coord.submit_workflow(make_workflow("wf-globex", {"g1": OK}, [], org_id="globex"))
    assert {w["workflow_id"] for w in coord.get_workflows(org_id="acme")} == {"wf-acme"}
    assert {w["workflow_id"] for w in coord.get_workflows()} == {"wf-acme", "wf-globex"}


# ----------------------------------------------------- failure propagation
def test_failure_blocks_every_descendant_but_not_independent_branches(coord):
    #  a(fails) -> b -> c          d (independent, succeeds)
    wf = make_workflow("wf-fail", {"a": BAD, "b": OK, "c": OK, "d": OK}, [("a", "b"), ("b", "c")])
    state = coord.submit_workflow(wf)
    assert wait_for(lambda: state.job_states.get("a") == "FAILED" and state.job_states.get("d") == "COMPLETED")
    assert state.status == "FAILED"
    assert state.job_states["b"] == "BLOCKED" and state.job_states["c"] == "BLOCKED"   # not just the direct child
    assert "b" not in coord.jobs and "c" not in coord.jobs                             # never submitted
    s = state.summary()
    assert s["failed_jobs"] == 1 and s["blocked_jobs"] == 2 and s["pending_jobs"] == 0
    assert wait_for(lambda: state.completed_at is not None)                            # settles once nothing is running


# --------------------------------------------------------------- cancel
def test_cancelling_a_queued_workflow_job_cancels_the_workflow(coord):
    coord.pause_scheduler()
    wf = make_workflow("wf-cancel", {"a": OK, "b": OK}, [("a", "b")])
    state = coord.submit_workflow(wf)
    coord.cancel_job("a")
    assert state.status == "CANCELLED"
    assert state.job_states == {"a": "CANCELLED", "b": "BLOCKED"}
    assert state.summary()["cancelled_jobs"] == 1 and state.completed_at is not None


def test_clear_queue_cancels_workflow_jobs_too(coord):
    coord.pause_scheduler()
    state = coord.submit_workflow(make_workflow("wf-clear", {"a": OK, "b": OK}, [("a", "b")]))
    coord.clear_queue()
    assert state.status == "CANCELLED" and state.job_states["b"] == "BLOCKED"


def test_failure_outranks_cancellation(coord):
    coord.pause_scheduler()
    state = coord.submit_workflow(make_workflow("wf-both", {"a": OK, "x": OK, "b": OK}, [("a", "b")]))
    coord.jobs["x"]["status"] = "failed"
    coord._advance_workflow("x", coord.jobs["x"], success=False)
    coord.cancel_job("a")
    assert state.status == "FAILED"


# ---------------------------------------------------------------- retry
def test_retrying_a_failed_job_resumes_the_whole_workflow(coord):
    wf = make_workflow("wf-retry", {"a": BAD, "b": OK, "c": OK}, [("a", "b"), ("b", "c")])
    state = coord.submit_workflow(wf)
    assert wait_for(lambda: state.status == "FAILED" and coord.jobs["a"]["status"] == "failed")
    assert state.job_states["c"] == "BLOCKED"

    coord.jobs["a"]["command"] = OK                      # "fix" the cause, then retry
    coord.retry_job("a")
    assert state.status == "RUNNING" and state.job_states["a"] == "RUNNING"
    assert state.job_states["b"] == "PENDING" and state.completed_at is None

    assert wait_for(lambda: state.status == "COMPLETED")
    assert state.job_states == {"a": "COMPLETED", "b": "COMPLETED", "c": "COMPLETED"}


def test_retry_does_not_unblock_jobs_with_another_broken_parent(coord):
    #  a(fails) \
    #            -> c        b(fails too)
    #  b(fails) /
    wf = make_workflow("wf-two", {"a": BAD, "b": BAD, "c": OK}, [("a", "c"), ("b", "c")])
    state = coord.submit_workflow(wf)
    assert wait_for(lambda: {"a", "b"} <= state.failed_jobs and coord.jobs["a"]["status"] == "failed"
                    and coord.jobs["b"]["status"] == "failed")
    coord.jobs["a"]["command"] = OK
    coord.retry_job("a")
    assert state.job_states["c"] == "BLOCKED"            # b is still broken
    assert wait_for(lambda: state.job_states["a"] == "COMPLETED")
    assert state.status == "FAILED" and state.job_states["c"] == "BLOCKED"


# ------------------------------------------------- collisions & rejection
def test_duplicate_workflow_id_is_rejected_and_leaves_the_live_one_alone(coord):
    first = coord.submit_workflow(make_workflow("dup", {"a": OK}, []))
    with pytest.raises(ValueError, match="already exists"):
        coord.submit_workflow(make_workflow("dup", {"z": OK}, []))
    assert coord.workflow_engine.states["dup"] is first and "z" not in coord.jobs


def test_job_id_clash_rejects_the_whole_workflow_before_anything_runs(coord):
    coord.submit_job("taken", OK)
    with pytest.raises(ValueError, match="already in use"):
        coord.submit_workflow(make_workflow("wf-clash", {"fresh": OK, "taken": OK}, []))
    assert "wf-clash" not in coord.workflow_engine.states
    assert "fresh" not in coord.jobs                      # no half-submitted workflow

    coord.submit_workflow(make_workflow("wf-one", {"shared": OK}, []))
    with pytest.raises(ValueError, match="already in use"):
        coord.submit_workflow(make_workflow("wf-two", {"shared": OK}, []))   # same id in another workflow


def test_a_job_the_coordinator_refuses_fails_the_workflow_with_the_reason(coord):
    coord._max_concurrent_jobs_per_org = 1                # per-org cap: one job in flight
    coord.pause_scheduler()                               # keep r1 pending so the cap is hit
    wf = make_workflow("wf-limit", {"r1": OK, "r2": OK, "after": OK}, [("r2", "after")], org_id="acme")
    state = coord.submit_workflow(wf)                     # returns normally: no half-built workflow, no 500
    assert "r2" in state.errors and "concurrent-job limit" in state.errors["r2"]
    assert state.job_states["r2"] == "FAILED" and state.job_states["after"] == "BLOCKED"
    assert state.job_states["r1"] == "RUNNING" and state.status == "FAILED"
    assert "r2" not in coord.jobs                          # the refused job was never created
    assert state.summary()["errors"]["r2"] == state.errors["r2"]


# ------------------------------------------------------------- restart
def test_workflows_survive_a_coordinator_restart(tmp_path):
    db = str(tmp_path / "cp.db")
    c1 = GCONCoordinator(control_plane=ControlPlane(path=db))
    c1.pause_scheduler()
    c1.submit_workflow(make_workflow("wf-persist", {"a": OK, "b": OK}, [("a", "b")], org_id="acme", name="nightly"))
    c1.shutdown()

    c2 = GCONCoordinator(control_plane=ControlPlane(path=db))
    try:
        s = c2.workflow_engine.states["wf-persist"]
        assert s.org_id == "acme" and s.name == "nightly"
        assert s.job_states == {"a": "RUNNING", "b": "PENDING"}
        assert [w["workflow_id"] for w in c2.get_workflows(org_id="acme")] == ["wf-persist"]
        assert c2.workflow_engine.dags["wf-persist"].descendants("a") == {"b"}
    finally:
        c2.shutdown()


def test_restart_reconciles_a_job_that_finished_before_the_dag_advanced(tmp_path):
    """The crash window: the job row says completed, the workflow row still
    says RUNNING. On restart the DAG must move on instead of stalling."""
    db = str(tmp_path / "cp.db")
    c1 = GCONCoordinator(control_plane=ControlPlane(path=db))
    c1.pause_scheduler()
    c1.submit_workflow(make_workflow("wf-gap", {"a": OK, "b": OK}, [("a", "b")]))
    c1.control_plane.jobs.set_status("a", "completed", completed=True)
    c1.shutdown()

    c2 = GCONCoordinator(control_plane=ControlPlane(path=db))
    c2.register_agent(GCONAgent(node_id="n1"))
    try:
        s = c2.workflow_engine.states["wf-gap"]
        assert s.job_states["a"] == "COMPLETED"
        assert wait_for(lambda: s.status == "COMPLETED")          # b was dispatched and ran
        assert c2.jobs["b"]["status"] == "completed"
    finally:
        c2.shutdown()


def test_retry_after_restart_still_resumes_the_workflow(tmp_path):
    db = str(tmp_path / "cp.db")
    c1 = GCONCoordinator(control_plane=ControlPlane(path=db))
    c1.register_agent(GCONAgent(node_id="n1"))
    s1 = c1.submit_workflow(make_workflow("wf-r", {"a": BAD, "b": OK}, [("a", "b")]))
    assert wait_for(lambda: s1.status == "FAILED" and c1.jobs["a"]["status"] == "failed")
    c1.shutdown()

    c2 = GCONCoordinator(control_plane=ControlPlane(path=db))
    c2.register_agent(GCONAgent(node_id="n1"))
    try:
        s2 = c2.workflow_engine.states["wf-r"]
        assert s2.status == "FAILED" and s2.job_states["b"] == "BLOCKED"
        c2.jobs["a"]["command"] = OK
        c2.retry_job("a")
        assert wait_for(lambda: s2.status == "COMPLETED")
    finally:
        c2.shutdown()


# ----------------------------------------------------------------- API
@pytest.fixture
def api(tmp_path):
    cp = ControlPlane(path=str(tmp_path / "cp.db"))
    coordinator = GCONCoordinator(control_plane=cp)
    management = ManagementLayer(coordinator=coordinator, db_path=str(tmp_path / "mgmt.db"))
    keys = {}
    for name in ("acme", "globex"):
        org = management.create_organization(f"{name} Inc")
        user = management.create_user(f"{name} user", f"u@{name}.example", role="Owner",
                                      organization_id=org["org_id"])
        keys[name] = (org["org_id"], management.create_api_key(
            f"{name}-key", owner_user_id=user["user_id"],
            scopes=["Submit workflows", "View monitoring"])["secret"])
    coordinator.pause_scheduler()
    client = TestClient(create_api_v1_app(management, PresentationLayer(coordinator)))
    yield client, coordinator, keys
    coordinator.shutdown()


def test_api_attributes_workflows_to_the_callers_org_and_scopes_the_list(api):
    client, coordinator, keys = api
    (acme_org, acme_key), (_, globex_key) = keys["acme"], keys["globex"]
    body = {"workflow_id": "wf-api", "name": "n", "jobs": [
        {"job_id": "api-a", "command": OK}, {"job_id": "api-b", "command": OK, "depends_on": ["api-a"]}]}
    r = client.post("/workflows", json=body, headers={"X-API-Key": acme_key})
    assert r.status_code == 200, r.text
    data = r.json()
    assert coordinator.jobs[data["jobs"]["api-a"]]["org_id"] == acme_org

    mine = client.get("/workflows", headers={"X-API-Key": acme_key}).json()
    theirs = client.get("/workflows", headers={"X-API-Key": globex_key}).json()
    assert [w["workflow_id"] for w in mine] == [data["workflow_id"]]
    assert theirs == []                                   # another customer's key sees nothing

    # GCON owns the ids, so re-sending the same labels is simply another
    # workflow with its own ids -- there is no id of the caller's to collide with.
    again = client.post("/workflows", json=body, headers={"X-API-Key": acme_key})
    assert again.status_code == 200
    assert again.json()["workflow_id"] != data["workflow_id"]
    assert set(again.json()["jobs"].values()).isdisjoint(data["jobs"].values())
