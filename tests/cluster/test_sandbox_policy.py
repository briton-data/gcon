"""
GCON_SANDBOX_POLICY: "required" (the default) only dispatches jobs to
workers that run them inside a container; "trusted" is the explicit,
deployment-wide opt-out for operators running only their own workloads.
Also covers the job-environment scrub: a job must not inherit the
worker's own GCON_* configuration (cert/key material in particular).

No Docker daemon is needed. A stub subclass of the real GCONAgent that
declares `sandboxed = True` stands in for a docker-backend worker -- what's
under test is the coordinator/scheduler decision, not the container.
"""
import os
import time

import pytest

from gcon.cluster.coordinator import GCONCoordinator
from gcon.execution.agent import GCONAgent
from gcon.persistence.control_plane import ControlPlane
from gcon.transport.remote_node import RemoteNodeProxy


class SandboxedAgent(GCONAgent):
    """A real GCONAgent that reports itself as running jobs in a container."""
    sandboxed = True


class LegacyAgent(GCONAgent):
    """A node object from before `sandboxed` existed: the attribute is simply absent."""
    @property
    def sandboxed(self):
        raise AttributeError("sandboxed")


def _wait_for(predicate, timeout=15.0, interval=0.05):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


@pytest.fixture
def required(monkeypatch):
    monkeypatch.setenv("GCON_SANDBOX_POLICY", "required")


@pytest.fixture
def make_coordinator(tmp_path):
    made = []

    def _make():
        plane = ControlPlane(path=str(tmp_path / f"cp{len(made)}.db"))
        coord = GCONCoordinator(control_plane=plane)
        made.append((coord, plane))
        return coord, plane

    yield _make
    for coord, plane in made:
        coord.shutdown()
        plane.close()


class TestPolicyValue:
    def test_defaults_to_required_when_unset(self, monkeypatch, make_coordinator):
        monkeypatch.delenv("GCON_SANDBOX_POLICY", raising=False)
        coord, _ = make_coordinator()
        assert coord._sandbox_policy == "required"

    def test_trusted_is_accepted_case_insensitively(self, monkeypatch, make_coordinator):
        monkeypatch.setenv("GCON_SANDBOX_POLICY", " Trusted ")
        coord, _ = make_coordinator()
        assert coord._sandbox_policy == "trusted"

    @pytest.mark.parametrize("bad", ["truste", "off", "false", "0", "none", "sandbox"])
    def test_anything_else_is_rejected_not_guessed(self, monkeypatch, bad):
        # A typo must never quietly downgrade isolation.
        monkeypatch.setenv("GCON_SANDBOX_POLICY", bad)
        with pytest.raises(ValueError, match="GCON_SANDBOX_POLICY"):
            GCONCoordinator()


class TestRequiredPolicy:
    def test_unsandboxed_worker_gets_no_jobs(self, required, make_coordinator):
        coord, plane = make_coordinator()
        coord.register_agent(GCONAgent("plain-node"))
        coord.submit_job("J", "echo hi")
        time.sleep(1.0)

        job = coord.jobs["J"]
        assert job["status"] == "pending"
        assert job.get("node_id") is None
        assert job.get("attempt_number", 0) == 0
        # The stranded state is legible, not silent: one event that names the policy.
        assert plane.telemetry_events.count_by_event_type().get("job_dispatch_failed", 0) == 1

    def test_sandboxed_worker_gets_the_job(self, required, make_coordinator):
        coord, _ = make_coordinator()
        coord.register_agent(SandboxedAgent("box-node"))
        coord.submit_job("J", "echo hi")
        assert _wait_for(lambda: coord.jobs["J"]["status"] == "completed")
        assert coord.jobs["J"]["node_id"] == "box-node"

    def test_sandboxed_worker_wins_even_when_an_unsandboxed_one_is_idle(self, required, make_coordinator):
        coord, _ = make_coordinator()
        coord.register_agent(GCONAgent("plain-node"))
        coord.register_agent(SandboxedAgent("box-node"))
        for i in range(4):
            coord.submit_job(f"J{i}", "echo hi")
        assert _wait_for(lambda: all(coord.jobs[f"J{i}"]["status"] == "completed" for i in range(4)))
        assert {coord.jobs[f"J{i}"]["node_id"] for i in range(4)} == {"box-node"}

    def test_replicated_job_needs_every_replica_sandboxed(self, required, make_coordinator):
        coord, _ = make_coordinator()
        coord.register_agent(SandboxedAgent("box-1"))
        coord.register_agent(GCONAgent("plain-node"))
        coord.submit_job("J", "echo hi", verify={"replicas": 2})
        time.sleep(1.0)
        # Only one sandboxed node exists -> must not fall back to the plain one.
        assert coord.jobs["J"]["status"] == "pending"
        assert coord.jobs["J"].get("attempt_number", 0) == 0

        coord.register_agent(SandboxedAgent("box-2"))
        assert _wait_for(lambda: coord.jobs["J"]["status"] == "completed")
        assert set(coord.jobs["J"]["replica_node_ids"]) == {"box-1", "box-2"}

    def test_node_with_no_sandboxed_attribute_is_treated_as_unsandboxed(self, required, make_coordinator):
        """Fail closed: a node object that never declared it can't be assumed isolated."""
        coord, _ = make_coordinator()
        coord.register_agent(LegacyAgent("legacy-node"))
        coord.submit_job("J", "echo hi")
        time.sleep(0.8)
        assert coord.jobs["J"]["status"] == "pending"


class TestTrustedPolicy:
    def test_unsandboxed_worker_is_used(self, monkeypatch, make_coordinator):
        monkeypatch.setenv("GCON_SANDBOX_POLICY", "trusted")
        coord, _ = make_coordinator()
        coord.register_agent(GCONAgent("plain-node"))
        coord.submit_job("J", "echo hi")
        assert _wait_for(lambda: coord.jobs["J"]["status"] == "completed")
        assert coord.jobs["J"]["node_id"] == "plain-node"


class TestNodeReportsSandboxState:
    def test_agent_is_sandboxed_only_with_the_docker_backend(self, monkeypatch):
        monkeypatch.setenv("GCON_EXECUTION_BACKEND", "subprocess")
        assert GCONAgent("a").sandboxed is False
        monkeypatch.setenv("GCON_EXECUTION_BACKEND", "docker")
        assert GCONAgent("b").sandboxed is True
        monkeypatch.setenv("GCON_EXECUTION_BACKEND", "typo")  # falls back to subprocess
        assert GCONAgent("c").sandboxed is False

    def test_remote_proxy_defaults_to_unsandboxed(self):
        assert RemoteNodeProxy("n", transport=None).sandboxed is False
        assert RemoteNodeProxy("n", transport=None, sandboxed=True).sandboxed is True


class TestJobEnvironmentScrub:
    """With the subprocess backend a job used to inherit the worker's whole
    environment, including GCON_AGENT_KEY_B64 & co. -- enough to impersonate
    the node."""

    def test_job_cannot_see_worker_gcon_variables(self, monkeypatch, tmp_path):
        monkeypatch.setenv("GCON_AGENT_KEY_B64", "SENTINEL-PRIVATE-KEY")
        monkeypatch.setenv("GCON_TLS_CERT_DIR", "/sentinel/certs")
        monkeypatch.setenv("GCON_SOMETHING_ELSE", "sentinel")
        result = GCONAgent("n").execute_job(
            "job-env-1",
            'echo "key=[$GCON_AGENT_KEY_B64] dir=[$GCON_TLS_CERT_DIR] other=[$GCON_SOMETHING_ELSE]"',
            timeout=30,
        )
        out = result["stdout"]
        assert "key=[]" in out and "dir=[]" in out and "other=[]" in out
        assert "SENTINEL" not in out and "sentinel" not in out

    def test_report_path_variables_the_job_needs_still_arrive(self, monkeypatch, tmp_path):
        usage = str(tmp_path / "usage.json")
        stage = str(tmp_path / "stage.json")
        result = GCONAgent("n").execute_job(
            "job-env-2",
            'echo "u=$GCON_USAGE_REPORT_PATH s=$GCON_STAGE_REPORT_PATH"',
            timeout=30, usage_report_path=usage, stage_report_path=stage,
        )
        assert f"u={usage}" in result["stdout"]
        assert f"s={stage}" in result["stdout"]

    def test_ordinary_variables_are_untouched(self, monkeypatch):
        monkeypatch.setenv("MY_JOB_SETTING", "keep-me")
        result = GCONAgent("n").execute_job("job-env-3", 'echo "v=$MY_JOB_SETTING"', timeout=30)
        assert "v=keep-me" in result["stdout"]
        assert os.environ.get("PATH")  # and the job could still find `echo`
