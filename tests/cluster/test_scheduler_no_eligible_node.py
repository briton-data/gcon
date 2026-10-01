"""
A job that has an idle node available but no ELIGIBLE one (wrong org,
unmet `requires`, or a replica count larger than the idle pool) used to
put scheduler_loop into a hot loop: has_available_node() only knows
"some node is idle", so the job passed that gate, failed inside
assign_job(), was requeued, and was retried immediately. Measured at
thousands of assign_job() calls per second, with each failed pass also
counting as a dispatch attempt (burning GCON_MAX_JOB_ATTEMPTS before the
job had ever run) and writing a telemetry row.

Real coordinator + real scheduler thread + LocalTransport agents, no
mocking of the code under test -- the only wrapper counts assign_job()
calls and delegates to the real one.
"""
import os
import time

import pytest

from gcon.cluster.coordinator import GCONCoordinator
from gcon.execution.agent import GCONAgent
from gcon.persistence.control_plane import ControlPlane


def _agent(node_id, org_id=None):
    agent = GCONAgent(node_id)
    agent.org_id = org_id
    return agent


def _wait_for(predicate, timeout=15.0, interval=0.05):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


def _count_assign_calls(coordinator):
    calls = {"n": 0}
    real = coordinator.assign_job

    def counting(job_id):
        calls["n"] += 1
        return real(job_id)

    coordinator.assign_job = counting
    return calls


@pytest.fixture
def cp(tmp_path):
    plane = ControlPlane(path=str(tmp_path / "cp.db"))
    yield plane
    plane.close()


@pytest.fixture
def coordinator(cp):
    coord = GCONCoordinator(control_plane=cp)
    yield coord
    coord.shutdown()


# Before the fix these ran at ~3,000-68,000 calls/s. After it, roughly
# 10/s (one 0.1s wait per full pass). The bound is deliberately loose so
# a slow CI box can't flake it while still being ~100x below the bug.
OBSERVE_SECONDS = 1.5
MAX_CALLS = 60


class TestNoHotLoop:
    def test_idle_node_belonging_to_another_org(self, coordinator):
        coordinator.register_agent(_agent("globex-node", org_id="globex"))
        coordinator.submit_job("J", "echo hi", org_id="acme")
        time.sleep(0.3)
        calls = _count_assign_calls(coordinator)
        time.sleep(OBSERVE_SECONDS)
        assert calls["n"] < MAX_CALLS
        assert coordinator.jobs["J"]["status"] == "pending"

    def test_requires_unmet_by_the_only_idle_node(self, coordinator):
        coordinator.register_agent(_agent("cpu-node"))
        coordinator.submit_job("J", "echo hi", kind="resourced", requires={"gpu": True})
        time.sleep(0.3)
        calls = _count_assign_calls(coordinator)
        time.sleep(OBSERVE_SECONDS)
        assert calls["n"] < MAX_CALLS
        assert coordinator.jobs["J"]["status"] == "pending"

    def test_replica_count_exceeds_idle_nodes(self, coordinator):
        coordinator.register_agent(_agent("only-node"))
        coordinator.submit_job("J", "echo hi", verify={"replicas": 2})
        time.sleep(0.3)
        calls = _count_assign_calls(coordinator)
        time.sleep(OBSERVE_SECONDS)
        assert calls["n"] < MAX_CALLS
        assert coordinator.jobs["J"]["status"] == "pending"

    def test_no_nodes_at_all_is_still_throttled(self, coordinator):
        coordinator.submit_job("J", "echo hi")
        time.sleep(0.3)
        calls = _count_assign_calls(coordinator)
        time.sleep(OBSERVE_SECONDS)
        assert calls["n"] == 0


class TestFailedPassesAreNotAttempts:
    def test_waiting_job_has_no_attempts_and_one_telemetry_event(self, coordinator, cp):
        coordinator.register_agent(_agent("globex-node", org_id="globex"))
        coordinator.submit_job("J", "echo hi", org_id="acme")
        time.sleep(OBSERVE_SECONDS)

        # Never dispatched -> zero attempts (was ~4,000+ per second).
        assert coordinator.jobs["J"].get("attempt_number", 0) == 0
        # Reported once, not once per scheduler pass.
        assert cp.telemetry_events.count_by_event_type().get("job_dispatch_failed", 0) == 1

    def test_first_real_dispatch_is_attempt_one_and_retry_is_still_allowed(self, coordinator):
        coordinator.register_agent(_agent("globex-node", org_id="globex"))
        coordinator.submit_job("J", "exit 1", org_id="acme")
        time.sleep(OBSERVE_SECONDS)  # spin on the ineligible node for a while

        coordinator.register_agent(_agent("acme-node", org_id="acme"))
        assert _wait_for(lambda: coordinator.jobs["J"]["status"] in ("failed", "completed"))

        assert coordinator.jobs["J"]["attempt_number"] == 1
        # The once-per-wait telemetry dedupe resets on a real dispatch, so
        # a later wait for the same job would be reported again.
        assert "J" not in coordinator._dispatch_failure_reported
        # Previously refused with "reached max attempt limit" after one
        # real run, because the waiting had already used up the budget.
        coordinator.retry_job("J")

    def test_replicated_job_waiting_for_nodes_has_no_attempts(self, coordinator):
        coordinator.register_agent(_agent("n1"))
        coordinator.submit_job("J", "echo hi", verify={"replicas": 2})
        time.sleep(OBSERVE_SECONDS)
        assert coordinator.jobs["J"].get("attempt_number", 0) == 0

        coordinator.register_agent(_agent("n2"))
        assert _wait_for(lambda: coordinator.jobs["J"]["status"] == "completed")
        assert coordinator.jobs["J"]["attempt_number"] == 1


class TestOneUnplaceableJobDoesNotBlockOthers:
    def test_placeable_job_behind_an_unplaceable_one_still_runs(self, coordinator):
        coordinator.register_agent(_agent("globex-node", org_id="globex"))
        coordinator.submit_job("STUCK", "echo hi", org_id="acme")      # no acme node exists
        coordinator.submit_job("FINE", "echo hi", org_id="globex")     # queued behind it

        assert _wait_for(lambda: coordinator.jobs["FINE"]["status"] == "completed")
        assert coordinator.jobs["STUCK"]["status"] == "pending"
        assert coordinator.jobs["STUCK"].get("attempt_number", 0) == 0
