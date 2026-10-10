"""The shared worker pool: operator-run, sandboxed workers with no org of their
own, available to an organization's jobs only as the customer allows.

  * the org's own workers are always tried first; the pool is the fallback
  * a pool worker must be sandboxed -- unsandboxed internal workers never are
  * the pool never reaches a DIFFERENT customer's dedicated worker
  * the customer's mode decides: off / basic (default: no artifacts) / full
  * the placement is recorded on the job (execution_pool)

No Docker daemon is needed: a stub agent that declares `sandboxed = True`
stands in for a container worker, as in test_sandbox_boundary.py.
"""
import time

import pytest

pytestmark = pytest.mark.real_sandbox

from gcon.cluster.coordinator import GCONCoordinator
from gcon.execution.agent import GCONAgent
from gcon.persistence.control_plane import ControlPlane


class SandboxedAgent(GCONAgent):
    sandboxed = True


def _agent(cls, node_id, org_id=None):
    agent = cls(node_id=node_id)
    agent.org_id = org_id
    return agent


def _pool_worker(coord, node_id="pool-1"):
    """An operator-enrolled, sandboxed worker that the operator has put in the pool."""
    coord.control_plane.shared_pool_nodes.add(node_id, "test-operator")
    coord.register_agent(_agent(SandboxedAgent, node_id))


def _wait_for(predicate, timeout=15.0):
    end = time.time() + timeout
    while time.time() < end:
        if predicate():
            return True
        time.sleep(0.05)
    return False


@pytest.fixture
def coord(tmp_path):
    plane = ControlPlane(path=str(tmp_path / "cp.db"))
    c = GCONCoordinator(control_plane=plane)
    yield c
    c.shutdown()
    plane.close()


def _submit(coord, job_id, org_id="acme"):
    coord.submit_job(job_id, "echo hi", org_id=org_id)


def _stays_pending(coord, job_id, seconds=1.5):
    time.sleep(seconds)
    job = coord.jobs[job_id]
    return job["status"] == "pending" and job.get("node_id") is None


def test_an_org_job_with_no_worker_of_its_own_runs_on_the_pool(coord):
    _pool_worker(coord)
    _submit(coord, "j1")
    assert _wait_for(lambda: coord.jobs["j1"]["status"] == "completed")
    assert coord.jobs["j1"]["node_id"] == "pool-1"
    assert coord.jobs["j1"]["execution_pool"] == "shared"
    assert [j for j in coord.get_jobs(org_id="acme") if j["job_id"] == "j1"][0]["execution_pool"] == "shared"


def test_the_orgs_own_worker_is_preferred_over_the_pool(coord):
    _pool_worker(coord)
    coord.register_agent(_agent(SandboxedAgent, "acme-w", "acme"))
    _submit(coord, "j1")
    assert _wait_for(lambda: coord.jobs["j1"]["status"] == "completed")
    assert coord.jobs["j1"]["node_id"] == "acme-w"
    assert coord.jobs["j1"]["execution_pool"] == "dedicated"


def test_mode_off_keeps_the_job_off_the_pool(coord):
    coord.control_plane.org_pool_settings.set_mode("acme", "off")
    _pool_worker(coord)
    _submit(coord, "j1")
    assert _stays_pending(coord, "j1")


def test_the_pool_never_includes_an_unsandboxed_worker(coord):
    coord.register_agent(_agent(GCONAgent, "raw-internal"))   # org-less, NOT sandboxed
    _submit(coord, "j1")
    assert _stays_pending(coord, "j1")


def test_the_pool_does_not_reach_another_customers_dedicated_worker(coord):
    coord.register_agent(_agent(SandboxedAgent, "globex-w", "globex"))
    _submit(coord, "j1", org_id="acme")
    assert _stays_pending(coord, "j1")


def test_a_sandboxed_org_less_worker_is_not_in_the_pool_unless_the_operator_says_so(coord):
    coord.register_agent(_agent(SandboxedAgent, "unlisted"))   # org-less, sandboxed, NOT flagged
    _submit(coord, "j1")
    assert _stays_pending(coord, "j1")


def test_flagging_a_running_worker_takes_effect_without_a_reconnect(coord):
    coord.register_agent(_agent(SandboxedAgent, "late"))
    _submit(coord, "j1")
    assert _stays_pending(coord, "j1", seconds=1.0)
    coord.control_plane.shared_pool_nodes.add("late", "test-operator")
    coord.registry.set_shared_pool("late", True)
    assert _wait_for(lambda: coord.jobs["j1"]["status"] == "completed")
    assert coord.jobs["j1"]["execution_pool"] == "shared"


def test_an_org_bound_worker_can_never_be_in_the_pool(coord):
    coord.control_plane.shared_pool_nodes.add("acme-w", "test-operator")
    coord.register_agent(_agent(SandboxedAgent, "acme-w", "acme"))
    assert coord.registry.get_node_info("acme-w")["shared_pool"] is False
    coord.registry.set_shared_pool("acme-w", True)
    assert coord.registry.get_node_info("acme-w")["shared_pool"] is False


def test_an_org_less_job_is_unaffected(coord):
    _pool_worker(coord)
    coord.submit_job("internal-1", "echo hi")
    assert _wait_for(lambda: coord.jobs["internal-1"]["status"] == "completed")
    assert "execution_pool" not in coord.jobs["internal-1"]


def test_a_job_on_the_pool_is_always_held_to_the_sandbox_check(coord):
    assert coord._must_sandbox({"require_sandbox": False, "execution_pool": "shared"}) is True
    # and without the pool marker, the flag still decides (the trusted-policy case)
    assert coord._must_sandbox({"require_sandbox": False}) == (coord._sandbox_policy == "required")


class TestWhichJobsMayUseThePool:
    def _allowed(self, coord, mode, **job):
        if mode:
            coord.control_plane.org_pool_settings.set_mode("acme", mode)
        return coord._shared_pool_allowed({"org_id": "acme", **job})

    def test_default_is_basic_a_plain_job_may_a_job_with_artifacts_may_not(self, coord):
        assert self._allowed(coord, None) is True
        assert self._allowed(coord, None, artifacts=["a1"]) is False
        assert self._allowed(coord, None, dataset_artifacts=["d1"]) is False

    def test_full_allows_the_customers_artifacts_and_datasets(self, coord):
        assert self._allowed(coord, "full", artifacts=["a1"], dataset_artifacts=["d1"]) is True

    def test_off_allows_nothing(self, coord):
        assert self._allowed(coord, "off") is False

    def test_a_job_with_no_org_is_never_a_pool_job(self, coord):
        assert coord._shared_pool_allowed({"org_id": None}) is False

    def test_it_fails_closed_without_a_control_plane(self):
        bare = GCONCoordinator()
        try:
            assert bare._shared_pool_allowed({"org_id": "acme"}) is False
        finally:
            bare.shutdown()

    def test_it_fails_closed_when_the_setting_cannot_be_read(self, coord, monkeypatch):
        def boom(_org):
            raise RuntimeError("db down")
        monkeypatch.setattr(coord.control_plane.org_pool_settings, "get_mode", boom)
        assert coord._shared_pool_allowed({"org_id": "acme"}) is False


def test_the_choice_is_per_organization_and_validated(coord):
    settings = coord.control_plane.org_pool_settings
    assert settings.get_mode("acme") == "basic" and settings.get_mode("globex") == "basic"
    settings.set_mode("acme", "off")
    settings.set_mode("acme", "full")           # update, not a duplicate-key error
    assert settings.get_mode("acme") == "full" and settings.get_mode("globex") == "basic"
    with pytest.raises(ValueError):
        settings.set_mode("acme", "everything")
    assert settings.get_mode(None) == "off"
