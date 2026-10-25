"""
Plan rules on a job's command, through a real coordinator and real workers:

  * a blank/oversized/binary command is refused at submission for everyone,
  * an organization's job must start with a program its plan allows,
  * the plan's max runtime is handed to the worker, which really kills the job,
  * a worker fails a job at once when its program is not there.
"""
import os
import sys
import time
from types import SimpleNamespace

import pytest

from gcon.cluster.coordinator import GCONCoordinator
from gcon.execution.agent import GCONAgent
from gcon.management.management_layer import ManagementLayer
from gcon.persistence.control_plane import ControlPlane

pytestmark = pytest.mark.real_plan_defaults

OWNER = SimpleNamespace(name="Olive", role="Owner")
posix_only = pytest.mark.skipif(os.name != "posix", reason="uses POSIX shell behaviour")


@pytest.fixture
def stack(tmp_path, monkeypatch):
    monkeypatch.delenv("GCON_MAX_CONCURRENT_JOBS_PER_ORG", raising=False)
    coordinator = GCONCoordinator(control_plane=ControlPlane(path=str(tmp_path / "cp.db")))
    management = ManagementLayer(coordinator=coordinator, db_path=str(tmp_path / "mgmt.db"))
    coordinator.org_worker_gate_resolver = None   # this file is not about worker availability
    org = management.create_organization("Acme", "Standard")
    yield coordinator, management, org["org_id"]
    coordinator.shutdown()


def wait_until(predicate, timeout=20.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(0.05)
    return False


def terminal(coordinator, job_id):
    return coordinator.jobs[job_id]["status"] in ("completed", "failed", "cancelled")


class TestShapeAtSubmission:
    @pytest.mark.parametrize("bad", ["", "   ", "x" * 10_001, "python\x00x"])
    def test_refused_for_organizations_and_for_internal_jobs_alike(self, stack, bad):
        coordinator, _, org_id = stack
        for org in (org_id, None):
            with pytest.raises(ValueError):
                coordinator.submit_job("bad-1", bad, org_id=org)
        assert coordinator.jobs == {}

    def test_nothing_is_queued_or_persisted_for_a_refused_command(self, stack):
        coordinator, _, org_id = stack
        with pytest.raises(ValueError):
            coordinator.submit_job("bad-2", "  ", org_id=org_id)
        assert coordinator.control_plane.jobs.get("bad-2") is None


class TestAllowedPrograms:
    def test_an_organization_cannot_queue_a_program_its_plan_does_not_allow(self, stack):
        coordinator, _, org_id = stack
        for command in ("nn", "tt tt tt", "curl http://example.com", "python x.py; curl evil"):
            with pytest.raises(ValueError):
                coordinator.submit_job(f"j-{abs(hash(command))}", command, org_id=org_id)
        assert coordinator.jobs == {}

    def test_listed_programs_are_queued(self, stack):
        coordinator, _, org_id = stack
        coordinator.submit_job("ok-1", "python train.py --epochs 3", org_id=org_id)
        coordinator.submit_job("ok-2", "echo hi", org_id=org_id)
        assert set(coordinator.jobs) == {"ok-1", "ok-2"}

    def test_a_job_with_no_organization_is_not_restricted(self, stack):
        coordinator, _, _ = stack
        coordinator.submit_job("internal-1", "anything-at-all | really", org_id=None)
        assert "internal-1" in coordinator.jobs

    def test_the_plan_decides_so_a_higher_plan_runs_more(self, stack):
        coordinator, management, org_id = stack
        with pytest.raises(ValueError):
            coordinator.submit_job("b-1", "bash run.sh", org_id=org_id)             # Standard: no bash
        management.update_organization(org_id, plan="Pro")
        coordinator.submit_job("b-2", "bash run.sh", org_id=org_id)                 # Pro: bash
        with pytest.raises(ValueError):
            coordinator.submit_job("b-3", "nn", org_id=org_id)                      # still not nonsense
        management.update_organization(org_id, plan="Enterprise")
        coordinator.submit_job("b-4", "nn", org_id=org_id)                          # Enterprise: any

    def test_a_staff_edit_applies_to_the_next_submission(self, stack):
        coordinator, management, org_id = stack
        with pytest.raises(ValueError):
            coordinator.submit_job("e-1", "make build", org_id=org_id)
        management.set_plan_limits("Standard", 600, 50, ["make"], None, actor=OWNER)
        coordinator.submit_job("e-2", "make build", org_id=org_id)
        with pytest.raises(ValueError):
            coordinator.submit_job("e-3", "python x.py", org_id=org_id)             # no longer listed

    def test_a_failing_rules_lookup_does_not_block_submissions(self, stack):
        coordinator, _, org_id = stack

        def broken(_org):
            raise RuntimeError("lookup broke")

        coordinator.org_job_rules_resolver = broken
        coordinator.submit_job("f-1", "nn", org_id=org_id)
        assert "f-1" in coordinator.jobs

    def test_workflows_cannot_get_around_it(self, stack):
        # Workflows submit every job through the coordinator, like everything else.
        coordinator, _, org_id = stack
        with pytest.raises(ValueError):
            coordinator.submit_job("wf-step-1", "nn", org_id=org_id, workflow_id="wf-1")


class TestRuntimeLimit:
    def test_the_plans_limit_is_what_the_worker_is_told(self, stack):
        coordinator, management, org_id = stack
        management.set_plan_limits("Standard", 600, 50, None, 123, actor=OWNER)
        coordinator.submit_job("t-1", "python -V", org_id=org_id)
        assert coordinator._job_timeout_seconds(coordinator.jobs["t-1"]) == 123

    def test_no_limit_means_no_timeout_and_no_organization_means_none(self, stack):
        coordinator, management, org_id = stack
        management.set_plan_limits("Standard", 600, 50, None, 0, actor=OWNER)
        coordinator.submit_job("t-2", "python -V", org_id=org_id)
        coordinator.submit_job("t-3", "echo hi", org_id=None)
        assert coordinator._job_timeout_seconds(coordinator.jobs["t-2"]) is None
        assert coordinator._job_timeout_seconds(coordinator.jobs["t-3"]) is None

    def test_a_job_that_runs_past_its_plans_limit_is_really_killed(self, stack):
        coordinator, management, org_id = stack
        management.set_plan_limits("Standard", 600, 50, None, 2, actor=OWNER)
        node = GCONAgent(node_id="acme-node-1")
        node.org_id = org_id
        coordinator.register_agent(node)
        started = time.time()
        coordinator.submit_job("slow-1", 'python -c "import time; time.sleep(60)"', org_id=org_id)
        coordinator.assign_job("slow-1")

        assert wait_until(lambda: terminal(coordinator, "slow-1"), timeout=30)
        elapsed = time.time() - started
        job = coordinator.jobs["slow-1"]
        assert elapsed < 20, f"the 60 s job should have been stopped at about 2 s, took {elapsed:.1f}s"
        assert job["status"] == "failed"
        assert "timeout" in str(job.get("error") or job.get("result") or "").lower()
        # the worker is free again, not stuck with a runaway process
        assert wait_until(lambda: node.status == "idle", timeout=10)

    def test_a_job_inside_its_limit_finishes_normally(self, stack):
        coordinator, management, org_id = stack
        management.set_plan_limits("Standard", 600, 50, None, 30, actor=OWNER)
        node = GCONAgent(node_id="acme-node-2")
        node.org_id = org_id
        coordinator.register_agent(node)
        coordinator.submit_job("fast-1", "echo done", org_id=org_id)
        coordinator.assign_job("fast-1")
        assert wait_until(lambda: terminal(coordinator, "fast-1"), timeout=20)
        assert coordinator.jobs["fast-1"]["status"] == "completed"


@posix_only
class TestWorkerFailsFast:
    def test_a_missing_program_fails_at_once_with_a_clear_message(self, stack):
        coordinator, _, _ = stack
        node = GCONAgent(node_id="trusted-node-1")
        coordinator.register_agent(node)
        coordinator.submit_job("nn-1", "definitely-not-a-program-xyz --flag", org_id=None)
        coordinator.assign_job("nn-1")
        assert wait_until(lambda: terminal(coordinator, "nn-1"), timeout=20)
        job = coordinator.jobs["nn-1"]
        assert job["status"] == "failed"
        assert "command not found" in str(job.get("error") or job.get("result") or "")

    def test_the_check_runs_before_anything_is_spawned(self, tmp_path):
        agent = GCONAgent(node_id="direct-1")
        started = time.time()
        result = agent.execute_job("direct-job", "nn-not-a-program")
        assert result["status"] == "error"
        assert "command not found" in result["error"]
        assert agent.process is None            # no process was ever started
        assert time.time() - started < 5

    def test_a_good_command_still_runs(self):
        agent = GCONAgent(node_id="direct-2")
        result = agent.execute_job("direct-job-2", "echo hello")
        assert result["status"] == "success"
        assert "hello" in result.get("stdout", "")

    def test_a_command_the_shell_can_run_is_not_blocked_by_the_check(self):
        agent = GCONAgent(node_id="direct-3")
        result = agent.execute_job("direct-job-3", "cd /tmp && echo fine")
        assert result["status"] == "success"
        result = agent.execute_job("direct-job-4", "nn-nope || echo fallback")
        assert result["status"] == "success" and "fallback" in result.get("stdout", "")
