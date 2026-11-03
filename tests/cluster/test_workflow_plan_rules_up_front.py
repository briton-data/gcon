"""
A workflow whose jobs break the organization's plan rules is refused as a whole, up front, and recorded nowhere.

Before, each step was checked only when the engine reached it: the first steps ran (using compute), a later step
that the plan does not allow then failed the workflow, and the response to the submit had already said "submitted".
"""
import sys

import pytest

from gcon.cluster.coordinator import GCONCoordinator
from gcon.execution.agent import GCONAgent
from gcon.persistence.control_plane import ControlPlane
from gcon.workflow.workflow import Workflow, WorkflowJob

PY = sys.executable
OK = f'{PY} -c "print(1)"'
NOT_ALLOWED = "curl http://example.com"


@pytest.fixture
def coord(tmp_path):
    c = GCONCoordinator(control_plane=ControlPlane(path=str(tmp_path / "cp.db")))
    c.pause_scheduler()                                    # nothing is dispatched: we look at what was admitted
    c.org_job_rules_resolver = lambda org_id: {
        "allowed_programs": [PY.split("/")[-1], PY], "max_runtime_seconds": 0, "verification": True}
    yield c
    c.shutdown()


def _workflow(workflow_id, commands, edges, org_id="acme"):
    wf = Workflow(workflow_id=workflow_id, name=workflow_id, org_id=org_id)
    for job_id, command in commands.items():
        wf.add_job(WorkflowJob(job_id=job_id, command=command))
    for parent, child in edges:
        wf.add_dependency(parent, child)
    return wf


class TestWorkflowIsCheckedAsAWhole:
    def test_a_later_step_the_plan_does_not_allow_refuses_the_whole_workflow(self, coord):
        wf = _workflow("wf-late", {"a": OK, "b": NOT_ALLOWED}, [("a", "b")])
        with pytest.raises(ValueError, match="not an allowed program"):
            coord.submit_workflow(wf)
        assert "wf-late" not in coord.workflow_engine.states      # recorded nowhere
        assert "a" not in coord.jobs and "b" not in coord.jobs    # and the first step was never queued

    def test_a_blank_command_in_any_step_refuses_the_whole_workflow(self, coord):
        wf = _workflow("wf-blank", {"a": OK, "b": "   "}, [("a", "b")])
        with pytest.raises(ValueError):
            coord.submit_workflow(wf)
        assert "wf-blank" not in coord.workflow_engine.states and "a" not in coord.jobs

    def test_the_message_says_which_step(self, coord):
        wf = _workflow("wf-which", {"a": OK, "second": NOT_ALLOWED}, [("a", "second")])
        with pytest.raises(ValueError, match="second"):
            coord.submit_workflow(wf)

    def test_a_workflow_that_follows_the_plan_is_admitted_as_before(self, coord):
        state = coord.submit_workflow(_workflow("wf-ok", {"a": OK, "b": OK}, [("a", "b")]))
        assert state.job_states["a"] == "RUNNING"
        assert "wf-ok" in coord.workflow_engine.states

    def test_an_organization_without_program_limits_is_not_restricted(self, coord):
        coord.org_job_rules_resolver = None
        state = coord.submit_workflow(_workflow("wf-free", {"a": OK, "b": NOT_ALLOWED}, [("a", "b")]))
        assert "wf-free" in coord.workflow_engine.states

    def test_staff_workflows_with_no_organization_are_not_restricted(self, coord):
        coord.submit_workflow(_workflow("wf-staff", {"a": OK, "b": NOT_ALLOWED}, [("a", "b")], org_id=None))
        assert "wf-staff" in coord.workflow_engine.states


# ------------------------------------------------------------------ through the public API
@pytest.fixture
def api(tmp_path):
    from tests.support.label_client import LabelClient as TestClient
    from gcon.api.api_v1 import create_api_v1_app
    from gcon.dashboard.presentation import PresentationLayer
    from gcon.management.management_layer import ManagementLayer

    coordinator = GCONCoordinator(control_plane=ControlPlane(path=str(tmp_path / "cp.db")))
    management = ManagementLayer(coordinator=coordinator, db_path=str(tmp_path / "mgmt.db"))
    coordinator.org_worker_gate_resolver = None          # not about worker availability
    org = management.create_organization("Acme Inc")
    user = management.create_user("Acme user", "u@acme.example", role="Owner", organization_id=org["org_id"])
    key = management.create_api_key("k", owner_user_id=user["user_id"],
                                    scopes=["Submit workflows", "View monitoring"])["secret"]
    coordinator.pause_scheduler()
    yield TestClient(create_api_v1_app(management, PresentationLayer(coordinator))), coordinator, key
    coordinator.shutdown()


@pytest.mark.real_plan_defaults
def test_the_api_answers_400_names_the_step_and_records_nothing(api):
    client, coordinator, key = api
    body = {"name": "n", "jobs": [
        {"job_id": "first", "command": 'python3 -c "print(1)"'},
        {"job_id": "junk-step", "command": "nn", "depends_on": ["first"]}]}
    r = client.post("/workflows", json=body, headers={"X-API-Key": key})
    assert r.status_code == 400, r.text
    assert "junk-step" in r.json()["detail"]
    assert client.get("/workflows", headers={"X-API-Key": key}).json() == []
    assert coordinator.jobs == {}
