"""
A worker that goes away while running a job must cause the job to be
recovered onto another worker -- not to be failed.

Two things fire off the same lost connection: _run_job's error handler
(send_job raises) and on_node_disconnected() -> recover_jobs(). If the
handler won, it marked the job permanently failed, and recovery then found
nothing "running" to requeue. Measured with a real SIGKILLed worker process:
3 of 30 runs on the original code ended with the job failed, 0 of 30 after.

The race itself is timing luck, so these tests take the timing out: an agent
that raises the same NodeUnavailableError the transport raises when a worker
disappears, with the handler necessarily the first (and only) thing to run.
"""
import time

import pytest

from gcon.cluster.coordinator import GCONCoordinator
from gcon.execution.agent import GCONAgent
from gcon.transport.errors import NodeUnavailableError


def _wait_for(predicate, timeout=15.0, interval=0.05):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


class WorkerThatVanishes(GCONAgent):
    """Behaves like a worker whose connection drops mid-job."""

    def execute_job(self, job_id, *args, **kwargs):
        raise NodeUnavailableError(
            f"Node '{self.node_id}' disconnected while job '{job_id}' was running."
        )


class WorkerThatCrashesTheDispatch(GCONAgent):
    """A different kind of failure: not a lost worker, just a broken dispatch."""

    def execute_job(self, job_id, *args, **kwargs):
        raise RuntimeError("boom: bad response from agent")


@pytest.fixture
def coordinator():
    coord = GCONCoordinator()
    events = []
    real = coord._dispatch_webhook
    coord._dispatch_webhook = lambda job_id, job, event: (events.append((job_id, event)), real(job_id, job, event))[1]
    coord.webhook_events = events
    yield coord
    coord.shutdown()


class TestLostWorker:
    def test_job_is_not_failed_and_completes_on_another_worker(self, coordinator):
        coordinator.register_agent(WorkerThatVanishes("gone-node"))
        coordinator.submit_job("J", "echo recovered")

        # Worker lost -> job goes back to waiting (nobody else is registered yet).
        assert _wait_for(lambda: coordinator.jobs["J"]["status"] == "pending"
                         and coordinator.jobs["J"].get("attempt_number") == 1)
        assert coordinator.jobs["J"]["status"] != "failed"
        assert ("J", "JOB_FAILED") not in coordinator.webhook_events

        # A healthy worker appears -> the job runs there, as a second attempt.
        coordinator.register_agent(GCONAgent("healthy-node"))
        assert _wait_for(lambda: coordinator.jobs["J"]["status"] == "completed")
        job = coordinator.jobs["J"]
        assert job["node_id"] == "healthy-node"
        assert job["attempt_number"] == 2
        assert "recovered" in job["result"]["stdout"]
        assert ("J", "JOB_FAILED") not in coordinator.webhook_events

    def test_lost_worker_is_not_handed_back_to_the_scheduler_as_idle(self, coordinator):
        """The vanished worker must not be freed to idle -- it's gone, and
        marking it idle would let the scheduler dispatch to it again."""
        coordinator.register_agent(WorkerThatVanishes("gone-node"))
        coordinator.submit_job("J", "echo hi")
        assert _wait_for(lambda: coordinator.jobs["J"]["status"] == "pending"
                         and coordinator.jobs["J"].get("attempt_number") == 1)
        time.sleep(0.5)
        assert coordinator.jobs["J"].get("attempt_number") == 1  # not retried onto the dead node
        assert coordinator.registry.get_node_info("gone-node")["status"] != "idle"

    def test_recovery_is_still_bounded_by_the_max_attempts_cap(self, coordinator):
        """Losing a worker must not become an unbounded retry loop."""
        coordinator._max_job_attempts = 1
        coordinator.register_agent(WorkerThatVanishes("gone-node"))
        coordinator.submit_job("J", "echo hi")
        assert _wait_for(lambda: coordinator.jobs["J"]["status"] == "failed")
        assert "max" in coordinator.jobs["J"]["result"]["message"].lower()


class TestOtherDispatchFailuresAreUnchanged:
    def test_a_non_worker_loss_error_still_fails_the_job(self, coordinator):
        coordinator.register_agent(WorkerThatCrashesTheDispatch("odd-node"))
        coordinator.submit_job("J", "echo hi")
        assert _wait_for(lambda: coordinator.jobs["J"]["status"] == "failed")
        assert "boom" in coordinator.jobs["J"]["result"]["message"]
        assert ("J", "JOB_FAILED") in coordinator.webhook_events

    def test_a_cancelled_job_whose_worker_vanishes_stays_cancelled(self, coordinator):
        class VanishesAfterCancel(WorkerThatVanishes):
            def execute_job(inner, job_id, *args, **kwargs):
                coordinator.jobs[job_id]["cancel_requested"] = True
                return super().execute_job(job_id, *args, **kwargs)

        coordinator.register_agent(VanishesAfterCancel("gone-node"))
        coordinator.submit_job("J", "echo hi")
        assert _wait_for(lambda: coordinator.jobs["J"]["status"] == "cancelled")
        assert ("J", "JOB_CANCELLED") in coordinator.webhook_events
