"""
Regression test for the replicated-execution (verify=N) counterpart
of the stale-attempt-overwrite bug fixed in _run_job (see its
docstring). _run_replicated_job dispatches a whole replica group as
one attempt (assign_job increments job["attempt_number"] once for the
group, not once per replica) -- if the job is reassigned to a newer
attempt while this group's real subprocess dispatches are still in
flight, the group's own (now-stale) outcome must not overwrite the
job, and must not become job["policy_report"] or self.receipts[job_id]
(the slot every existing get_receipt_detail/dashboard consumer treats
as THIS job's current receipt) -- while each replica's own receipt is
still created and persisted (via self.replica_receipts) so "what
happened on this attempt" stays a real answer.

Uses a real `sleep` subprocess (GCONAgent.execute_job runs real host
subprocesses even under LocalTransport -- see its own module notes)
to get a reliable window to inject the "job already moved to attempt
2" mutation while the group's dispatch threads are still blocked,
mirroring exactly what recover_jobs() does in production.
"""
import time

from gcon.cluster.coordinator import GCONCoordinator
from gcon.execution.agent import GCONAgent


def test_stale_replicated_result_does_not_overwrite_a_newer_attempt():
    coordinator = GCONCoordinator()
    coordinator.register_agent(GCONAgent(node_id="node-r1"))
    coordinator.register_agent(GCONAgent(node_id="node-r2"))

    coordinator.submit_job("job-rep-1", "sleep 2 && echo hi", verify={"replicas": 2})
    coordinator.job_queue.get()  # see test_max_job_attempts.py's note on this
    coordinator.assign_job("job-rep-1")

    assert coordinator.jobs["job-rep-1"]["attempt_number"] == 1
    assert coordinator.jobs["job-rep-1"]["status"] == "running"

    # Simulate recover_jobs() having already reassigned this job to a
    # second attempt elsewhere, while the real "sleep 2" replicas are
    # still mid-flight.
    with coordinator.jobs_lock:
        coordinator.jobs["job-rep-1"]["attempt_number"] = 2
        coordinator.jobs["job-rep-1"]["node_id"] = "node-elsewhere"
        coordinator.jobs["job-rep-1"]["replica_node_ids"] = ["node-elsewhere"]

    # Give the real replica group time to finish its sleep and run its
    # (now-stale) completion logic.
    time.sleep(3.5)

    job = coordinator.jobs["job-rep-1"]
    assert job["attempt_number"] == 2, "attempt_number was overwritten by the stale group"
    assert job["status"] == "running", (
        f"a stale replicated result overwrote the job -- got status={job['status']!r}"
    )
    assert job["node_id"] == "node-elsewhere"

    # The stale group's receipts are still recorded per-replica (the
    # audit trail for that attempt survives)...
    assert len(coordinator.replica_receipts.get("job-rep-1", [])) == 2
    # ...but neither was allowed to become THIS job's "primary"
    # receipt -- that slot must stay exactly as it was before the
    # stale group ever ran (nothing; this job never legitimately
    # completed in this test).
    assert "job-rep-1" not in coordinator.receipts

    coordinator.shutdown()
