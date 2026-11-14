"""
Three behaviours added together:

1. A job's submission spec (kind, requires, verify, stages, dataset_artifacts, require_sandbox) is stored with the
   job, so a coordinator restart re-queues a replicated job as the replicated job it was, instead of failing it.
   A job stored before this existed (no spec) keeps the old behaviour, covered in test_restart_recovery.py.
2. A pending job says why it has not started (`waiting_reason`).
3. GCON_QUEUE_TIMEOUT_SECONDS (0 = off) fails a job that waited too long for a worker, with a reason.
"""
from datetime import UTC, datetime, timedelta

from gcon.cluster.coordinator import GCONCoordinator
from gcon.persistence.control_plane import ControlPlane
from tests.cluster.test_restart_recovery import _seed_in_flight_job


def test_submit_stores_the_spec(tmp_path):
    cp = ControlPlane(path=str(tmp_path / "cp.db"))
    coord = GCONCoordinator(control_plane=cp)
    coord.submit_job("j-spec", "echo hi", verify={"replicas": 2, "tolerance": 0.02})
    spec = cp.jobs.get_spec("j-spec")
    assert spec["verify"] == {"replicas": 2, "tolerance": 0.02}
    assert spec["kind"] == "command"
    # Never leaks into the job rows handed to callers.
    assert "spec_json" not in cp.jobs.get("j-spec")
    coord.shutdown()


def test_replicated_job_with_a_stored_spec_resumes_as_replicated(tmp_path):
    cp = ControlPlane(path=str(tmp_path / "cp.db"))
    _seed_in_flight_job(cp, "j-rep", n_attempts=2, all_open=True)      # two replicas dispatched, none finished
    cp.jobs.set_spec("j-rep", {"kind": "command", "verify": {"replicas": 2}, "require_sandbox": False})

    coord = GCONCoordinator(control_plane=cp)

    job = coord.jobs["j-rep"]
    assert job["status"] == "pending"
    assert job["verify"] == {"replicas": 2}
    assert job["attempt_number"] == 1          # one dispatch (of two replicas) used, not two
    assert coord.job_queue.get() == "j-rep"
    coord.shutdown()


def test_replicated_job_at_the_attempt_cap_is_still_failed(tmp_path):
    cp = ControlPlane(path=str(tmp_path / "cp.db"))
    _seed_in_flight_job(cp, "j-cap", n_attempts=6, all_open=True)      # 3 dispatches x 2 replicas
    cp.jobs.set_spec("j-cap", {"kind": "command", "verify": {"replicas": 2}})

    coord = GCONCoordinator(control_plane=cp)

    assert coord.jobs["j-cap"]["status"] == "failed"
    assert "max attempt" in coord.jobs["j-cap"]["result"]["error"]
    coord.shutdown()


def test_pending_job_says_why_it_is_waiting(tmp_path):
    cp = ControlPlane(path=str(tmp_path / "cp.db"))
    coord = GCONCoordinator(control_plane=cp)
    coord.pause_scheduler()
    coord.submit_job("j-wait", "echo hi")
    summary = coord.get_job_for_org("j-wait")
    assert summary["status"] == "pending"
    assert "No worker is available" in summary["waiting_reason"]
    coord.jobs["j-wait"]["status"] = "completed"
    assert coord.get_job_for_org("j-wait")["waiting_reason"] is None
    coord.shutdown()


def _backdate(coord, job_id, seconds):
    coord.jobs[job_id]["created_at"] = (datetime.now(UTC) - timedelta(seconds=seconds)).isoformat()


def test_queue_timeout_is_off_by_default(tmp_path):
    cp = ControlPlane(path=str(tmp_path / "cp.db"))
    coord = GCONCoordinator(control_plane=cp)
    coord.pause_scheduler()
    coord.submit_job("j-old", "echo hi")
    _backdate(coord, "j-old", 86400)
    coord._expire_stale_queued_jobs()
    assert coord.jobs["j-old"]["status"] == "pending"
    coord.shutdown()


def test_queue_timeout_fails_only_jobs_that_waited_too_long(tmp_path):
    cp = ControlPlane(path=str(tmp_path / "cp.db"))
    coord = GCONCoordinator(control_plane=cp)
    coord.pause_scheduler()
    coord._queue_timeout_seconds = 60
    coord.submit_job("j-old", "echo hi")
    coord.submit_job("j-new", "echo hi")
    _backdate(coord, "j-old", 120)

    coord._expire_stale_queued_jobs()

    assert coord.jobs["j-old"]["status"] == "failed"
    assert "No worker became available within 60 seconds" in coord.jobs["j-old"]["result"]["error"]
    assert cp.jobs.get("j-old")["status"] == "failed"             # durable, not only in memory
    assert coord.jobs["j-new"]["status"] == "pending"
    coord.shutdown()
