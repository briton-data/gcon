"""
Two related guarantees for a job's completion:

1. Completion is announced only AFTER its receipt exists. Previously the job
   was marked `completed` and the JOB_COMPLETED webhook fired first, and the
   receipt was created afterwards -- with a durable database, 52 of 60 jobs
   were visibly "completed" with no receipt yet (median 1.3ms, worst 26ms), so
   a client that polled the job and then fetched its receipt could get
   "not found".

2. A replicated (verify) job exposes its verdict on the job itself, so an API
   customer doesn't have to open the receipt to learn the replicas disagreed.
   A disputed job still has status "completed" (billing and every existing
   client keep working); `verification` is how it is told apart.
"""
import time

import pytest

from gcon.api.api_v1 import JobOut
from gcon.cluster.coordinator import GCONCoordinator
from gcon.execution.agent import GCONAgent
from gcon.persistence.control_plane import ControlPlane
from gcon.transport.webhooks import dispatch_job_event


def _wait_for(predicate, timeout=20.0, interval=0.05):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


@pytest.fixture
def cp(tmp_path):
    plane = ControlPlane(path=str(tmp_path / "cp.db"))
    yield plane
    plane.close()


@pytest.fixture
def coordinator(cp):
    coord = GCONCoordinator(control_plane=cp)
    coord.register_agent(GCONAgent("n1"))
    coord.register_agent(GCONAgent("n2"))
    # Record, at the instant each webhook is dispatched, what the world looked like.
    coord.webhooks = []
    real = coord._dispatch_webhook

    def recording(job_id, job, event):
        coord.webhooks.append({
            "event": event,
            "receipt_exists": job_id in coord.receipts,
            "status": job["status"],
            "outcome": (coord._job_verification(job) or {}).get("outcome"),
        })
        return real(job_id, job, event)

    coord._dispatch_webhook = recording
    yield coord
    coord.shutdown()


def _run(coord, job_id, command, **kwargs):
    coord.submit_job(job_id, command, **kwargs)
    assert _wait_for(lambda: coord.jobs[job_id]["status"] in ("completed", "failed"))
    assert _wait_for(lambda: any(w["event"].startswith("JOB_") for w in coord.webhooks))


class TestReceiptExistsBeforeCompletionIsAnnounced:
    def test_single_node_job(self, coordinator):
        _run(coordinator, "S", "echo hi")
        completed = [w for w in coordinator.webhooks if w["event"] == "JOB_COMPLETED"]
        assert len(completed) == 1
        assert completed[0]["receipt_exists"] is True

    def test_replicated_job(self, coordinator):
        _run(coordinator, "R", "echo hi", verify={"replicas": 2})
        completed = [w for w in coordinator.webhooks if w["event"] == "JOB_COMPLETED"]
        assert len(completed) == 1
        assert completed[0]["receipt_exists"] is True

    def test_a_job_is_never_completed_without_its_receipt(self, coordinator):
        """Polling from outside, as a client would: once the job reads
        completed, its receipt must already be there."""
        coordinator.submit_job("P", "echo hi")
        seen_completed_without_receipt = False
        deadline = time.time() + 20
        while time.time() < deadline:
            if coordinator.jobs["P"]["status"] == "completed":
                seen_completed_without_receipt = "P" not in coordinator.receipts
                break
            time.sleep(0.0005)
        assert not seen_completed_without_receipt

    def test_a_failed_job_is_still_finalized_without_a_receipt(self, coordinator):
        _run(coordinator, "F", "exit 3")
        assert coordinator.jobs["F"]["status"] == "failed"
        assert "F" not in coordinator.receipts
        assert [w["event"] for w in coordinator.webhooks] == ["JOB_FAILED"]


class TestVerificationOnTheJob:
    def test_agreeing_replicas(self, coordinator):
        _run(coordinator, "A", "echo same", verify={"replicas": 2})
        v = coordinator._job_verification(coordinator.jobs["A"])
        assert v["outcome"] == "agreed" and v["agreement"] is True
        assert v["replicas"] == 2 and sorted(v["witnesses"]) == ["n1", "n2"]
        assert v["mismatches"] == []

    def test_disagreeing_replicas_are_disputed_but_still_completed(self, coordinator):
        _run(coordinator, "D", "echo $$", verify={"replicas": 2})  # each shell prints its own pid
        job = coordinator.jobs["D"]
        v = coordinator._job_verification(job)
        assert job["status"] == "completed"           # existing clients & billing unaffected
        assert v["outcome"] == "disputed" and v["agreement"] is False
        assert [m["field"] for m in v["mismatches"]] == ["output_hash"]

    def test_a_job_that_was_not_replicated_has_no_verification(self, coordinator):
        _run(coordinator, "S", "echo hi")
        assert coordinator._job_verification(coordinator.jobs["S"]) is None

    def test_verdict_is_visible_in_the_api_job_view(self, coordinator):
        _run(coordinator, "D", "echo $$", verify={"replicas": 2})
        view = next(j for j in coordinator.get_jobs() if j["job_id"] == "D")
        assert view["verification"]["outcome"] == "disputed"
        assert JobOut(**view).verification["outcome"] == "disputed"  # survives the response model

    def test_replicas_that_could_not_be_compared_say_so(self, coordinator, monkeypatch):
        import gcon.execution.replication as replication

        def boom(*a, **k):
            raise RuntimeError("comparator exploded")

        monkeypatch.setattr(replication, "compare_results", boom)
        _run(coordinator, "U", "echo hi", verify={"replicas": 2})
        job = coordinator.jobs["U"]
        v = coordinator._job_verification(job)
        assert job["status"] == "completed"           # a comparator bug must not fail the job
        assert v["outcome"] == "unavailable" and v["agreement"] is None
        assert "comparator exploded" in v["error"]


class TestDisputeWebhooks:
    def test_dispute_fires_a_second_webhook_after_completion(self, coordinator):
        _run(coordinator, "D", "echo $$", verify={"replicas": 2})
        assert _wait_for(lambda: any(w["event"] == "EXECUTION_DISPUTED" for w in coordinator.webhooks))
        assert [w["event"] for w in coordinator.webhooks] == ["JOB_COMPLETED", "EXECUTION_DISPUTED"]
        assert all(w["receipt_exists"] for w in coordinator.webhooks)
        assert all(w["outcome"] == "disputed" for w in coordinator.webhooks)

    def test_agreement_fires_no_dispute_webhook(self, coordinator):
        _run(coordinator, "A", "echo same", verify={"replicas": 2})
        time.sleep(0.3)
        assert [w["event"] for w in coordinator.webhooks] == ["JOB_COMPLETED"]


class _FakeWebhooks:
    def __init__(self):
        self.deliveries = []

    def enqueue_delivery(self, **kw):
        self.deliveries.append(kw)

    def list_for_org(self, org_id, active_only=True):
        return []


class _FakeControlPlane:
    def __init__(self):
        self.webhooks = _FakeWebhooks()


class TestWebhookPayload:
    def _payload(self, job):
        plane = _FakeControlPlane()
        dispatch_job_event(plane, dict(job, job_id="J", callback_url="http://example.invalid/hook"), "JOB_COMPLETED")
        assert len(plane.webhooks.deliveries) == 1
        return plane.webhooks.deliveries[0]["payload"]

    def test_verdict_is_in_the_payload_when_present(self):
        verification = {"method": "replication", "outcome": "disputed", "agreement": False}
        payload = self._payload({"status": "completed", "verification": verification})
        assert payload["verification"] == verification

    def test_payload_shape_is_unchanged_for_a_job_without_one(self):
        assert "verification" not in self._payload({"status": "completed", "verification": None})
        assert "verification" not in self._payload({"status": "completed"})


class TestVerdictSurvivesReloadAndRestart:
    def test_reloaded_from_the_durable_store(self, coordinator, cp):
        _run(coordinator, "D", "echo $$", verify={"replicas": 2})
        loaded = coordinator._load_job_from_control_plane("D")
        assert coordinator._job_verification(loaded)["outcome"] == "disputed"

    def test_visible_after_a_coordinator_restart(self, coordinator, cp):
        _run(coordinator, "D", "echo $$", verify={"replicas": 2})
        coordinator.shutdown()

        restarted = GCONCoordinator(control_plane=cp)
        try:
            view = next(j for j in restarted.get_jobs() if j["job_id"] == "D")
            assert view["status"] == "completed"
            assert view["verification"]["outcome"] == "disputed"
        finally:
            restarted.shutdown()

    def test_the_agents_own_result_is_not_altered_by_persisting_the_verdict(self, coordinator):
        _run(coordinator, "D", "echo $$", verify={"replicas": 2})
        assert "verification" not in coordinator.jobs["D"]["result"]
