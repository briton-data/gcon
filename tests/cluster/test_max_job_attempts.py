"""
Deterministic (LocalTransport, no real network) tests for the
max-attempts cap added to recover_jobs/retry_job/retry_failed_jobs --
GCON_MAX_JOB_ATTEMPTS, defaulting to 3. Before this, none of the
three retry paths had any ceiling at all: a job that kept losing its
worker, or kept failing, would retry forever with nothing durable
recording how many times it had already been tried, and no way to
stop it.

assign_job() increments job["attempt_number"] once per real dispatch
(the single choke point every first dispatch and every retry already
goes through). These tests set coordinator._max_job_attempts directly
to a small number rather than looping real dispatches many times to
reach the default of 3 -- the cap boundary itself is what's under
test here, not how many cycles it takes to reach it. scheduler_loop
is never auto-started by the constructor, so directly calling
assign_job()/manipulating coordinator.jobs[...] between calls is safe
and matches the existing convention in
tests/persistence/test_on_node_disconnected.py.
"""
import pytest

from gcon.cluster.coordinator import GCONCoordinator
from gcon.execution.agent import GCONAgent


def _make_idle_node(coordinator, node_id="node-1"):
    node = GCONAgent(node_id)
    coordinator.register_agent(node)
    return node


class TestRecoverJobsMaxAttempts:
    def test_marks_permanently_failed_once_cap_reached(self):
        coordinator = GCONCoordinator()
        coordinator._max_job_attempts = 1
        _make_idle_node(coordinator)

        coordinator.submit_job("job-cap-1", "echo hi")
        coordinator.assign_job("job-cap-1")
        assert coordinator.jobs["job-cap-1"]["attempt_number"] == 1

        # Worker loss with attempt_number already at the cap -- must
        # fail permanently, not reset to "pending" and reassign.
        coordinator.on_node_disconnected("node-1")

        job = coordinator.jobs["job-cap-1"]
        assert job["status"] == "failed"
        assert job["node_id"] is None
        assert "max attempts" in job["result"]["message"].lower()

        coordinator.shutdown()

    def test_still_reassigns_normally_when_under_the_cap(self):
        coordinator = GCONCoordinator()  # default cap (3)
        _make_idle_node(coordinator, "node-1")
        _make_idle_node(coordinator, "node-2")

        coordinator.submit_job("job-cap-2", "echo hi")
        coordinator.assign_job("job-cap-2")
        assert coordinator.jobs["job-cap-2"]["node_id"] == "node-1"

        coordinator.on_node_disconnected("node-1")

        job = coordinator.jobs["job-cap-2"]
        # Reassigned to the other idle node, not permanently failed --
        # confirms the cap only bites at the boundary, not on every
        # single worker loss.
        assert job["status"] != "failed"
        assert job["node_id"] == "node-2"
        assert job["attempt_number"] == 2

        coordinator.shutdown()

    def test_requeues_instead_of_abandoning_when_no_node_is_idle_yet(self):
        """
        Regression for a real bug found while building this round's
        tests: recover_jobs() used to call assign_job() directly and,
        if that raised RuntimeError ("no available nodes"), just log
        and leave the job at status="pending" without ever putting it
        back on self.job_queue -- so nothing would ever pick it up
        again even after a replacement node became available.
        scheduler_loop's own retry loop always requeues on the same
        RuntimeError; recover_jobs() now does too.
        """
        coordinator = GCONCoordinator()
        _make_idle_node(coordinator, "node-1")

        coordinator.submit_job("job-cap-3", "echo hi")
        # submit_job already queued it (scheduler_loop isn't running
        # to drain that in this test); pop it off before dispatching
        # directly, mirroring what scheduler_loop would have done.
        coordinator.job_queue.get()
        coordinator.assign_job("job-cap-3")

        assert coordinator.job_queue.empty()
        coordinator.on_node_disconnected("node-1")  # no other idle node

        job = coordinator.jobs["job-cap-3"]
        assert job["status"] == "pending"
        # The real fix: it's back on the queue, not just sitting in
        # "pending" limbo with nothing left that will ever look at it.
        assert not coordinator.job_queue.empty()
        assert coordinator.job_queue.get() == "job-cap-3"

        coordinator.shutdown()


class TestRetryJobMaxAttempts:
    def test_raises_once_cap_reached(self):
        coordinator = GCONCoordinator()
        coordinator._max_job_attempts = 1
        coordinator.submit_job("job-cap-4", "echo hi")
        coordinator.jobs["job-cap-4"]["status"] = "failed"
        coordinator.jobs["job-cap-4"]["attempt_number"] = 1

        with pytest.raises(ValueError, match="max attempt"):
            coordinator.retry_job("job-cap-4")

        # Must not have been mutated by the rejected retry attempt.
        assert coordinator.jobs["job-cap-4"]["status"] == "failed"

        coordinator.shutdown()

    def test_still_retries_under_the_cap(self):
        coordinator = GCONCoordinator()  # default cap (3)
        coordinator.submit_job("job-cap-5", "echo hi")
        coordinator.jobs["job-cap-5"]["status"] = "failed"
        coordinator.jobs["job-cap-5"]["attempt_number"] = 1

        coordinator.retry_job("job-cap-5")
        assert coordinator.jobs["job-cap-5"]["status"] == "pending"

        coordinator.shutdown()


class TestRetryFailedJobsMaxAttempts:
    def test_skips_maxed_out_jobs_and_reports_them_separately(self):
        coordinator = GCONCoordinator()
        coordinator._max_job_attempts = 1

        coordinator.submit_job("job-cap-ok", "echo hi")
        coordinator.jobs["job-cap-ok"]["status"] = "failed"
        coordinator.jobs["job-cap-ok"]["attempt_number"] = 0  # under cap (1)

        coordinator.submit_job("job-cap-maxed", "echo hi")
        coordinator.jobs["job-cap-maxed"]["status"] = "failed"
        coordinator.jobs["job-cap-maxed"]["attempt_number"] = 1  # at cap

        retried = coordinator.retry_failed_jobs()

        assert retried == ["job-cap-ok"]
        assert coordinator.jobs["job-cap-ok"]["status"] == "pending"
        # Untouched by the bulk retry -- still failed, not silently
        # retried past its own cap.
        assert coordinator.jobs["job-cap-maxed"]["status"] == "failed"

        coordinator.shutdown()
