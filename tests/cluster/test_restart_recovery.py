"""
restore_from_persistence() used to unconditionally mark every
pending/running job "failed" on coordinator restart, requiring the
customer to resubmit. It now re-dispatches them as a fresh, durably
recorded attempt via the ordinary queue_job -> scheduler_loop ->
assign_job -> _run_job path -- the same mechanism every other retry
already uses, not a special restart-only code path.

Two cases still can't be safely auto-resumed and fall back to the old
failed-needs-resubmission behavior, each with its own specific reason:
a job that was mid-flight as a replicated (verify=N) execution (config
never durably persisted -- silently downgrading it to single-node
would be worse than failing it), and a job already at the
max-attempts cap. See restore_from_persistence's own docstring/inline
comments for the full reasoning, including how a replicated job is
detected without needing its verify config to have been stored at
all (more than one job_attempts row still "dispatched" for the same
job_id is only possible for a replicated dispatch).

These tests build the control_plane's rows directly (control_plane.
jobs.create / control_plane.job_attempts.record_attempt) rather than
running a real dispatch and killing the process, so each scenario
(one open attempt, several already-resolved attempts, two
simultaneously-open attempts) is set up exactly and deterministically
-- restore_from_persistence has no way to tell the difference between
that and a real crash, since it only ever reads durable state either
way.
"""
import time

from gcon.cluster.coordinator import GCONCoordinator
from gcon.execution.agent import GCONAgent
from gcon.persistence.control_plane import ControlPlane


def _seed_in_flight_job(control_plane, job_id, status="running", n_attempts=1,
                          all_open=False, resolved_status="success"):
    """Directly create a job + attempt history in the control plane,
    as if a previous coordinator process had dispatched it and then
    died before finishing. `all_open` simulates a replicated
    dispatch (every attempt still "dispatched", none resolved yet);
    otherwise every attempt but the last is resolved and the last one
    (the one the crash caught) is left "dispatched"."""
    control_plane.jobs.create(job_id, "sleep 0.5 && echo hi")
    control_plane.jobs.set_status(job_id, status)
    for i in range(n_attempts):
        node_id = f"node-prior-{job_id}-{i}"
        # job_attempts.node_id is a real FK into nodes -- a prior
        # attempt's node has to actually be registered, same as
        # record_attempt() always finds true in production (it's only
        # ever called from send_job(), after the scheduler has already
        # picked a real, registered node).
        control_plane.nodes.upsert(node_id, hostname=node_id)
        attempt = control_plane.job_attempts.record_attempt(
            job_id, node_id, f"prior-msg-{job_id}-{i}"
        )
        if not all_open and i < n_attempts - 1:
            control_plane.job_attempts.set_status(
                attempt["attempt_id"], resolved_status, completed=True
            )
    return control_plane.job_attempts.list_for_job(job_id)


def test_resumes_a_job_with_one_open_attempt(tmp_path):
    control_plane = ControlPlane(path=str(tmp_path / "cp.db"))
    _seed_in_flight_job(control_plane, "job-resume-1", n_attempts=1)

    coordinator = GCONCoordinator(control_plane=control_plane)  # restore_from_persistence() runs in __init__

    job = coordinator.jobs["job-resume-1"]
    assert job["status"] == "pending"
    assert job["node_id"] is None
    # Seeded from the one prior (durable) attempt, so the next real
    # dispatch (assign_job increments it) becomes attempt 2, and the
    # max-attempts cap keeps counting from where the pre-crash history
    # left off instead of getting a fresh budget.
    assert job["attempt_number"] == 1
    assert coordinator.job_queue.get() == "job-resume-1"

    # And it's durable too, not just in memory.
    assert control_plane.jobs.get("job-resume-1")["status"] == "pending"

    coordinator.shutdown()


def test_resumed_job_gets_a_real_new_durable_attempt_once_redispatched(tmp_path):
    control_plane = ControlPlane(path=str(tmp_path / "cp.db"))
    _seed_in_flight_job(control_plane, "job-resume-2", n_attempts=1)

    coordinator = GCONCoordinator(control_plane=control_plane)  # restore_from_persistence() runs in __init__
    coordinator.job_queue.get()  # drain, dispatch manually (see test_max_job_attempts.py)

    coordinator.register_agent(GCONAgent(node_id="node-after-restart"))
    coordinator.assign_job("job-resume-2")

    assert coordinator.jobs["job-resume-2"]["attempt_number"] == 2

    end = time.time() + 5
    while time.time() < end and coordinator.jobs["job-resume-2"]["status"] == "running":
        time.sleep(0.05)
    assert coordinator.jobs["job-resume-2"]["status"] == "completed"

    # Not asserting a second durable job_attempts row here: this test
    # dispatches over LocalTransport (a plain GCONAgent registered
    # in-process), which -- same as every other LocalTransport-based
    # test in this suite -- never calls job_attempts.record_attempt()
    # at all (see LocalTransport.send_job's own docstring; only
    # GrpcTransport does, since only it has a real wire-level
    # request_message_id to key a durable attempt row on). The durable
    # side of this -- that a real post-restart redispatch gets its own
    # real job_attempts row, on top of the pre-crash one -- is proven
    # over real gRPC in
    # test_restart_recovery_late_completion.py::
    # test_resumed_attempt_gets_its_own_durable_row_after_a_prior_one.
    # What's under test here is the in-memory side: attempt_number
    # correctly continues counting from the pre-crash history (1, not
    # reset to a fresh 1), and the job completes normally afterward.

    coordinator.shutdown()


def test_does_not_resume_a_job_already_at_the_max_attempts_cap(tmp_path):
    control_plane = ControlPlane(path=str(tmp_path / "cp.db"))
    # Default cap (3) -- seeded with exactly that many prior attempts,
    # set up before construction since restore_from_persistence() (and
    # the _max_job_attempts read it relies on) runs inside __init__.
    _seed_in_flight_job(control_plane, "job-resume-cap", n_attempts=3)

    coordinator = GCONCoordinator(control_plane=control_plane)  # restore_from_persistence() runs in __init__

    job = coordinator.jobs["job-resume-cap"]
    assert job["status"] == "failed"
    assert "max attempt" in job["result"]["error"].lower()
    assert coordinator.job_queue.empty()
    assert control_plane.jobs.get("job-resume-cap")["status"] == "failed"

    coordinator.shutdown()


def test_does_not_resume_a_replicated_job_detected_via_multiple_open_attempts(tmp_path):
    control_plane = ControlPlane(path=str(tmp_path / "cp.db"))
    _seed_in_flight_job(control_plane, "job-resume-rep", n_attempts=2, all_open=True)

    coordinator = GCONCoordinator(control_plane=control_plane)  # restore_from_persistence() runs in __init__

    job = coordinator.jobs["job-resume-rep"]
    assert job["status"] == "failed"
    assert "replicated" in job["result"]["error"].lower()
    assert coordinator.job_queue.empty()

    coordinator.shutdown()


def test_still_resumes_a_pending_job_with_no_prior_attempts(tmp_path):
    """A job that was submitted but never even got as far as its
    first dispatch before the crash (attempt_number starts at 0, not
    seeded from any prior attempt) -- the simplest case, and a
    regression guard that seeding from an empty attempt history
    doesn't itself break resumption."""
    control_plane = ControlPlane(path=str(tmp_path / "cp.db"))
    control_plane.jobs.create("job-resume-fresh", "echo hi")
    control_plane.jobs.set_status("job-resume-fresh", "pending")

    coordinator = GCONCoordinator(control_plane=control_plane)  # restore_from_persistence() runs in __init__

    job = coordinator.jobs["job-resume-fresh"]
    assert job["status"] == "pending"
    assert job["attempt_number"] == 0
    assert coordinator.job_queue.get() == "job-resume-fresh"

    coordinator.shutdown()
