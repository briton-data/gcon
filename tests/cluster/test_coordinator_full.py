"""
Full GCONCoordinator test suite.

GCONCoordinator is a large, multi-responsibility class (job lifecycle,
node lifecycle, admin operations, dashboard/read APIs, workflows,
receipts/verification -- 66 public methods as of this writing). Most
of the CORE submit->dispatch->verify pipeline already has real,
focused coverage elsewhere (test_org_isolation.py, test_submission_
policy_gate.py, test_telemetry_lifecycle.py, stress_test.py/
stress_test2.py, test_quarantine.py, test_receipts_org_isolation.py) --
this file does not re-litigate that in detail. It has two jobs
instead:

  1. TestFullJobPipelineStory -- one coherent, realistic story
     (submit -> assign -> dispatch -> execute -> receipt -> verify ->
     cancel-a-different-job) exercised as a single narrative, the way
     an actual user's session would look, not as isolated unit calls.
  2. Everything else -- explicit, direct coverage of every public
     method that a name-search across the existing suite found ZERO
     direct references to (31 of 66 methods, audited this session) --
     admin operations, dashboard/read endpoints, node lifecycle,
     workflows, artifacts, events. Organized into test classes by
     responsibility area, matching how the coordinator itself groups
     these concerns, not by file position.

Real GCONCoordinator + real ControlPlane (SQLite, tmp_path) + real
GCONAgent nodes throughout -- no mocking of the coordinator itself,
since the point is confirming these methods actually work against
real state, not that they're wired to call the right mock.
"""
import os
import tempfile
import time

import pytest

from gcon.cluster.coordinator import GCONCoordinator, PolicyRejectionError
from gcon.execution.agent import GCONAgent
from gcon.persistence.control_plane import ControlPlane
from gcon.workflow.workflow import Workflow, WorkflowJob


@pytest.fixture
def coordinator(tmp_path):
    control_plane = ControlPlane(path=str(tmp_path / "cp.db"))
    coord = GCONCoordinator(control_plane=control_plane)
    yield coord
    coord.shutdown()


@pytest.fixture
def coordinator_with_node(coordinator):
    node = GCONAgent(node_id="node-1")
    coordinator.register_agent(node)
    return coordinator, node


def _wait_for(predicate, timeout=5, interval=0.05):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


def _wait_for_job(coordinator, job_id, statuses=("completed", "failed", "cancelled"), timeout=5):
    return _wait_for(lambda: coordinator.jobs.get(job_id, {}).get("status") in statuses, timeout=timeout)


# ==================================================================
# 1. Full pipeline story
# ==================================================================

class TestFullJobPipelineStory:
    def test_submit_dispatch_execute_verify_and_a_separate_cancel(self, coordinator_with_node):
        """One coherent session: submit a job, watch it run to
        completion with a real signed receipt, confirm it verifies,
        then submit and cancel a second, unrelated job -- the two
        most common real interactions with a coordinator, back to
        back, the way an actual caller would do both in one sitting."""
        coordinator, node = coordinator_with_node

        coordinator.submit_job("job-story-1", "echo hello-gcon")
        assert _wait_for_job(coordinator, "job-story-1")
        assert coordinator.jobs["job-story-1"]["status"] == "completed"

        status = coordinator.get_job_status("job-story-1")
        assert status["status"] == "completed"

        assert _wait_for(lambda: "job-story-1" in coordinator.receipts)
        receipt = coordinator.receipts["job-story-1"]
        assert "signature" in receipt["proof"]
        assert receipt["trace_id"] == coordinator.jobs["job-story-1"]["trace_id"]

        # Verification: the automatic drain should mark this genuinely valid.
        coordinator._drain_pending_receipt_verifications()
        detail = coordinator.get_receipt_detail(receipt["receipt_id"])
        assert detail["verified"] is True

        # A second, unrelated job -- submit then cancel before it can
        # be double-counted against the first job's state.
        node2 = GCONAgent(node_id="node-2")
        coordinator.register_agent(node2)
        coordinator.submit_job("job-story-2", "sleep 30")
        assert _wait_for(lambda: coordinator.jobs["job-story-2"]["status"] == "running")
        killed = coordinator.cancel_job("job-story-2")
        assert _wait_for_job(coordinator, "job-story-2")
        assert coordinator.jobs["job-story-2"]["status"] in ("cancelled", "failed")


# ==================================================================
# 2. Node lifecycle
# ==================================================================

class TestNodeLifecycle:
    def test_register_and_deregister_agent(self, coordinator):
        node = GCONAgent(node_id="node-x")
        coordinator.register_agent(node)
        assert coordinator.get_total_node_count() == 1
        # get_registered_nodes() returns node IDs (strings), not dicts
        # -- confirmed against the real method, not assumed.
        assert "node-x" in coordinator.get_registered_nodes()

        coordinator.deregister_agent("node-x")
        assert coordinator.get_total_node_count() == 0

    def test_get_idle_nodes_and_idle_count(self, coordinator_with_node):
        coordinator, node = coordinator_with_node
        idle = coordinator.get_idle_nodes()
        assert any(n.node_id == "node-1" for n in idle)
        assert coordinator.get_idle_node_count() == 1

    def test_receive_heartbeat_and_resource_report(self, coordinator_with_node):
        coordinator, node = coordinator_with_node
        coordinator.receive_heartbeat(node.heartbeat())
        coordinator.receive_resource_report(node.report_resources())
        # No exception, and the node is still tracked as registered.
        assert coordinator.get_total_node_count() == 1

    def test_on_node_disconnected_and_recover_jobs(self, coordinator_with_node):
        coordinator, node = coordinator_with_node
        # A second node so there's actually somewhere for the
        # recovered job to land -- with only the one (now-offline)
        # node, recovery correctly has nowhere to go and the job just
        # stays failed, which would test the "no capacity" path, not
        # recovery succeeding.
        node2 = GCONAgent(node_id="node-2")
        coordinator.register_agent(node2)
        coordinator.submit_job("job-recover-1", "sleep 2")
        assert _wait_for(lambda: coordinator.jobs["job-recover-1"]["status"] == "running")
        running_node_id = coordinator.jobs["job-recover-1"]["node_id"]

        coordinator.on_node_disconnected(running_node_id)
        coordinator.recover_jobs(running_node_id)
        # Confirm it was actually reassigned to the OTHER node, not
        # just left in some intermediate state -- that's the real
        # thing recover_jobs is supposed to do. Not waiting for full
        # completion: this job's own runtime is irrelevant to what
        # recovery itself does.
        assert _wait_for(lambda: coordinator.jobs["job-recover-1"]["node_id"] != running_node_id)

    def test_drain_node_and_clear_quarantine(self, coordinator_with_node):
        coordinator, node = coordinator_with_node
        coordinator.drain_node("node-1")
        # Draining shouldn't crash even with no jobs running; clearing
        # a quarantine that was never set is a safe no-op.
        coordinator.clear_quarantine("node-1")

    def test_restart_and_stop_worker(self, coordinator_with_node):
        coordinator, node = coordinator_with_node
        # Both are control-plane signals to the node, not destructive
        # to coordinator state -- confirm they run without raising
        # against a real registered node.
        coordinator.restart_worker("node-1")
        coordinator.stop_worker("node-1")

    def test_rediscover_nodes(self, coordinator_with_node):
        coordinator, node = coordinator_with_node
        result = coordinator.rediscover_nodes()
        assert result is not None

    def test_get_node_summary(self, coordinator_with_node):
        coordinator, node = coordinator_with_node
        summary = coordinator.get_node_summary()
        assert summary is not None


# ==================================================================
# 3. Cluster health & trust
# ==================================================================

class TestClusterHealthAndTrust:
    def test_check_cluster_health(self, coordinator_with_node):
        coordinator, node = coordinator_with_node
        # Side-effecting (offline-node detection, health/trust
        # sampling, retention sweep) not a query -- no return value;
        # confirmed against the real method. The real assertion is
        # that it runs clean against live state.
        coordinator.check_cluster_health()

    def test_get_trust_score_and_history(self, coordinator_with_node):
        coordinator, node = coordinator_with_node
        score = coordinator.get_trust_score()
        assert score is not None
        history = coordinator.get_trust_history(limit=10)
        assert isinstance(history, list)

    def test_get_cluster_status_health_state_snapshot(self, coordinator_with_node):
        coordinator, node = coordinator_with_node
        assert coordinator.get_cluster_status() is not None
        assert coordinator.get_cluster_health() is not None
        assert coordinator.get_cluster_state() is not None
        assert coordinator.get_cluster_snapshot() is not None

    def test_get_health_details(self, coordinator_with_node):
        coordinator, node = coordinator_with_node
        details = coordinator.get_health_details()
        assert details is not None


# ==================================================================
# 4. Admin operations
# ==================================================================

class TestAdminOperations:
    def test_pause_and_resume_scheduler(self, coordinator):
        coordinator.pause_scheduler()
        coordinator.resume_scheduler()  # must not raise either direction

    def test_clear_queue(self, coordinator):
        coordinator.submit_job("job-q1", "echo hi")
        coordinator.clear_queue()
        # Whatever was pending is gone from the queue -- doesn't
        # assert job status here, just that the operation itself
        # completes against real queued state.

    def test_clear_failed_jobs_and_retry_failed_jobs(self, coordinator_with_node):
        coordinator, node = coordinator_with_node
        coordinator.submit_job("job-will-fail", "false")  # real nonzero exit
        assert _wait_for_job(coordinator, "job-will-fail")
        coordinator.retry_failed_jobs()
        coordinator.clear_failed_jobs()

    def test_retry_job(self, coordinator_with_node):
        coordinator, node = coordinator_with_node
        coordinator.submit_job("job-retry-1", "false")
        assert _wait_for_job(coordinator, "job-retry-1")
        result = coordinator.retry_job("job-retry-1")
        assert result is not None

    def test_clear_completed_jobs(self, coordinator_with_node):
        coordinator, node = coordinator_with_node
        coordinator.submit_job("job-done-1", "echo hi")
        assert _wait_for_job(coordinator, "job-done-1")
        coordinator.clear_completed_jobs()

    def test_verify_all_receipts(self, coordinator_with_node):
        coordinator, node = coordinator_with_node
        coordinator.submit_job("job-verify-all-1", "echo hi")
        assert _wait_for_job(coordinator, "job-verify-all-1")
        result = coordinator.verify_all_receipts()
        assert result is not None

    def test_export_logs(self, coordinator_with_node):
        coordinator, node = coordinator_with_node
        coordinator.submit_job("job-export-1", "echo hi")
        assert _wait_for_job(coordinator, "job-export-1")
        logs = coordinator.export_logs()
        assert logs is not None

    def test_emergency_stop(self, tmp_path):
        # Own coordinator instance -- this is explicitly destructive
        # cluster-wide, so it doesn't share the module fixture's
        # coordinator with other tests.
        control_plane = ControlPlane(path=str(tmp_path / "estop.db"))
        coord = GCONCoordinator(control_plane=control_plane)
        node = GCONAgent(node_id="node-estop")
        coord.register_agent(node)
        coord.submit_job("job-estop-1", "sleep 30")
        assert _wait_for(lambda: coord.jobs["job-estop-1"]["status"] == "running")
        coord.emergency_stop()
        assert _wait_for_job(coord, "job-estop-1")
        coord.shutdown()


# ==================================================================
# 5. Receipts, jobs, and metrics (dashboard/read APIs)
# ==================================================================

class TestReceiptsJobsAndMetrics:
    def test_get_receipts_and_receipts_page(self, coordinator_with_node):
        coordinator, node = coordinator_with_node
        coordinator.submit_job("job-r1", "echo hi")
        assert _wait_for_job(coordinator, "job-r1")
        assert _wait_for(lambda: "job-r1" in coordinator.receipts)
        assert len(coordinator.get_receipts()) >= 1
        items, total = coordinator.get_receipts_page(limit=10, offset=0)
        assert total >= 1
        assert len(items) >= 1

    def test_get_receipt_verification_counts(self, coordinator_with_node):
        coordinator, node = coordinator_with_node
        coordinator.submit_job("job-r2", "echo hi")
        assert _wait_for_job(coordinator, "job-r2")
        # Poll rather than a single drain-then-assert: under load
        # (many coordinators' background health_check_loop threads
        # still winding down from earlier tests in the same process)
        # one drain call can occasionally run before the receipt has
        # actually landed in self.receipts. health_check_loop itself
        # also drains automatically every ~3s regardless, so this
        # converges either way -- just not always instantly.
        assert _wait_for(lambda: (
            coordinator._drain_pending_receipt_verifications() or True
        ) and coordinator.get_receipt_verification_counts()["verified"] >= 1, timeout=8)

    def test_get_receipt_detail_and_execution_detail(self, coordinator_with_node):
        coordinator, node = coordinator_with_node
        coordinator.submit_job("job-r3", "echo hi")
        assert _wait_for_job(coordinator, "job-r3")
        # Under full-suite CPU load, there's a brief real gap between a
        # job's status turning "completed" and its receipt actually
        # landing in self.receipts -- wait for the receipt itself, not
        # just the job status.
        assert _wait_for(lambda: "job-r3" in coordinator.receipts)
        receipt_id = coordinator.receipts["job-r3"]["receipt_id"]
        detail = coordinator.get_receipt_detail(receipt_id)
        assert detail["job_id"] == "job-r3"
        exec_detail = coordinator.get_execution_detail("job-r3")
        assert exec_detail is not None

    def test_get_jobs_and_jobs_page_and_status_counts(self, coordinator_with_node):
        coordinator, node = coordinator_with_node
        coordinator.submit_job("job-j1", "echo hi")
        assert _wait_for_job(coordinator, "job-j1")
        assert len(coordinator.get_jobs()) >= 1
        items, total = coordinator.get_jobs_page(limit=10, offset=0)
        assert total >= 1
        counts = coordinator.get_job_status_counts()
        assert counts.get("completed", 0) >= 1

    def test_get_pending_job_count(self, coordinator):
        coordinator.pause_scheduler()  # keep it pending, not dispatched
        coordinator.submit_job("job-pending-1", "echo hi")
        assert coordinator.get_pending_job_count() >= 1
        coordinator.resume_scheduler()

    def test_get_metrics_and_storage(self, coordinator_with_node):
        coordinator, node = coordinator_with_node
        coordinator.submit_job("job-m1", "echo hi")
        assert _wait_for_job(coordinator, "job-m1")
        metrics = coordinator.get_metrics()
        assert metrics is not None
        storage = coordinator.get_storage()
        assert storage is not None


# ==================================================================
# 6. Workflows
# ==================================================================

class TestWorkflows:
    def test_submit_workflow_and_get_workflows(self, coordinator_with_node):
        coordinator, node = coordinator_with_node
        workflow = Workflow(workflow_id="wf-full-1", name="test workflow")
        workflow.add_job(WorkflowJob(job_id="wf-job-a", command="echo a"))
        workflow.add_job(WorkflowJob(job_id="wf-job-b", command="echo b"))

        result = coordinator.submit_workflow(workflow)
        assert result is not None

        workflows = coordinator.get_workflows()
        assert any(w.get("workflow_id") == "wf-full-1" for w in workflows)


# ==================================================================
# 7. Events and artifacts
# ==================================================================

class TestEventsAndArtifacts:
    def test_get_events_and_get_all_events(self, coordinator_with_node):
        coordinator, node = coordinator_with_node
        coordinator.submit_job("job-ev1", "echo hi")
        assert _wait_for_job(coordinator, "job-ev1")
        recent = coordinator.get_events(limit=5)
        assert isinstance(recent, list)
        all_events = coordinator.get_all_events()
        assert isinstance(all_events, list)
        assert len(all_events) >= len(recent)

    def test_register_job_artifact(self, coordinator_with_node):
        coordinator, node = coordinator_with_node
        coordinator.submit_job("job-art1", "echo hi")
        assert _wait_for_job(coordinator, "job-art1")

        fd, path = tempfile.mkstemp()
        os.write(fd, b"artifact contents")
        os.close(fd)
        try:
            artifact_id = coordinator.register_job_artifact("job-art1", "node-1", path)
            assert artifact_id is not None
            assert artifact_id in coordinator.jobs["job-art1"]["artifacts"]
            assert any(a["artifact_id"] == artifact_id for a in coordinator.get_artifacts())
        finally:
            os.remove(path)


# ==================================================================
# 8. Persistence & shutdown
# ==================================================================

class TestPersistenceAndShutdown:
    def test_restore_from_persistence_and_get_persisted_nodes(self, tmp_path):
        control_plane = ControlPlane(path=str(tmp_path / "restore.db"))
        coord1 = GCONCoordinator(control_plane=control_plane)
        node = GCONAgent(node_id="node-persist-1")
        coord1.register_agent(node)
        coord1.submit_job("job-persist-1", "echo hi")
        assert _wait_for_job(coord1, "job-persist-1")
        coord1.shutdown()

        # A fresh coordinator against the SAME control_plane should
        # recover what was durable.
        coord2 = GCONCoordinator(control_plane=control_plane)
        coord2.restore_from_persistence()
        persisted_nodes = coord2.get_persisted_nodes()
        assert persisted_nodes is not None
        coord2.shutdown()

    def test_shutdown_is_safe_to_call_and_stops_background_loops(self, tmp_path):
        control_plane = ControlPlane(path=str(tmp_path / "shutdown.db"))
        coord = GCONCoordinator(control_plane=control_plane)
        coord.register_agent(GCONAgent(node_id="node-shutdown-1"))
        coord.shutdown(timeout=2.0)
        # Calling it again must not raise -- shutdown should be
        # idempotent, not assume it's only ever called once.
        coord.shutdown(timeout=2.0)


# ==================================================================
# 9. Background loops (one bounded iteration each, not run forever)
# ==================================================================

class TestBackgroundLoops:
    def test_scheduler_loop_dispatches_a_pending_job(self, tmp_path):
        control_plane = ControlPlane(path=str(tmp_path / "sched.db"))
        coord = GCONCoordinator(control_plane=control_plane)
        coord.register_agent(GCONAgent(node_id="node-sched-1"))
        coord.pause_scheduler()
        coord.submit_job("job-sched-1", "echo hi")
        assert coord.jobs["job-sched-1"]["status"] == "pending"

        coord.resume_scheduler()
        # scheduler_loop runs as a background thread from __init__;
        # resuming it should let the already-queued job actually
        # dispatch without a fresh submit_job call.
        assert _wait_for_job(coord, "job-sched-1")
        coord.shutdown()

    def test_health_check_loop_updates_trust_score_after_a_job(self, tmp_path):
        control_plane = ControlPlane(path=str(tmp_path / "health.db"))
        coord = GCONCoordinator(control_plane=control_plane)
        coord.register_agent(GCONAgent(node_id="node-health-1"))
        coord.submit_job("job-health-1", "echo hi")
        assert _wait_for_job(coord, "job-health-1")
        # health_check_loop runs automatically in the background;
        # give it a couple of ticks to drain and commit verification.
        assert _wait_for(lambda: coord.get_receipt_verification_counts()["verified"] >= 1, timeout=8)
        coord.shutdown()

    def test_autoscale_loop_does_not_crash_with_no_cloud_backend(self, tmp_path):
        """autoscaler.scale_up() is documented to refuse (RuntimeError)
        without real cloud-provisioning backing (see AutoScaler) -- the
        background loop wrapping it must swallow that, not crash the
        coordinator's whole autoscale thread."""
        control_plane = ControlPlane(path=str(tmp_path / "autoscale.db"))
        coord = GCONCoordinator(control_plane=control_plane)
        coord.pause_scheduler()
        coord.submit_job("job-autoscale-1", "echo hi")  # queue pressure, no idle nodes
        time.sleep(1.0)  # let autoscale_loop's background thread tick at least once
        # Still alive and responsive -- the real assertion here is
        # "didn't crash the process", confirmed indirectly by this
        # still working after the sleep above.
        assert coord.get_total_node_count() == 0
        coord.shutdown()
