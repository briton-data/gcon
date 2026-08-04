"""
Covers the "rehydrate state on startup" requirement: GCONCoordinator
used to write jobs/nodes/receipts to the control-plane DB (via
GrpcTransport -> JobRepository/NodeRepository/ReceiptRepository) but
never read them back on boot, so a restart wiped the dashboard's
history even though the underlying rows survived. These tests drive
the repositories directly (exactly what GrpcTransport does under a
real agent connection) rather than standing up a real gRPC server, to
isolate the persistence/rehydration behavior itself.
"""

from gcon.cluster.coordinator import GCONCoordinator
from gcon.persistence.control_plane import ControlPlane


def _seed(control_plane, job_id="job-restore-1", node_id="node-restore-1"):
    control_plane.jobs.create(job_id, "echo hello", priority=1)
    control_plane.jobs.set_status(job_id, "completed")

    control_plane.nodes.upsert(
        node_id, hostname="worker-1.internal", status="idle",
        transport_endpoint="worker-1.internal:50051",
    )

    receipt_payload = {
        "job_id": job_id,
        "receipt_id": f"receipt-{job_id}",
        "status": "verified",
        "proof": "deadbeef",
        "issued_at": "2026-01-01T00:00:00+00:00",
    }
    control_plane.receipts.upload(
        job_id=job_id,
        payload=receipt_payload,
        receipt_hash=f"hash-{job_id}",
        node_id=node_id,
    )
    return job_id, node_id


def test_no_control_plane_means_no_restore_and_no_crash():
    # Existing behavior for every local-only coordinator (most tests,
    # tests/stages/*, LocalTransport) must be completely unaffected.
    coordinator = GCONCoordinator()
    assert coordinator.jobs == {}
    assert coordinator.receipts == {}
    assert coordinator.get_persisted_nodes() == {}
    coordinator.shutdown()


def test_fresh_empty_db_restores_cleanly(tmp_path):
    path = str(tmp_path / "cp.db")
    control_plane = ControlPlane(path=path)
    coordinator = GCONCoordinator(control_plane=control_plane)

    assert coordinator.jobs == {}
    assert coordinator.receipts == {}
    assert coordinator.get_persisted_nodes() == {}

    coordinator.shutdown()
    control_plane.close()


def test_job_receipt_and_node_survive_a_coordinator_restart(tmp_path):
    path = str(tmp_path / "cp.db")

    # --- "before restart" ---
    control_plane_1 = ControlPlane(path=path)
    job_id, node_id = _seed(control_plane_1)

    coordinator_1 = GCONCoordinator(control_plane=control_plane_1)
    assert job_id in coordinator_1.jobs
    assert job_id in coordinator_1.receipts
    assert node_id in coordinator_1.get_persisted_nodes()

    coordinator_1.shutdown()
    control_plane_1.close()

    # --- "restart": new process, new ControlPlane, same DB file ---
    control_plane_2 = ControlPlane(path=path)
    coordinator_2 = GCONCoordinator(control_plane=control_plane_2)

    try:
        assert job_id in coordinator_2.jobs
        restored_job = coordinator_2.jobs[job_id]
        assert restored_job["command"] == "echo hello"
        assert restored_job["status"] == "completed"

        assert job_id in coordinator_2.receipts
        assert coordinator_2.receipts[job_id]["receipt_id"] == f"receipt-{job_id}"
        assert coordinator_2.receipts[job_id]["status"] == "verified"

        persisted_nodes = coordinator_2.get_persisted_nodes()
        assert node_id in persisted_nodes
        assert persisted_nodes[node_id]["hostname"] == "worker-1.internal"

        # A restart restores *history*, but does not fabricate a live
        # connection -- the node hasn't reconnected in this test, so
        # it must NOT be schedulable via the live registry.
        assert node_id not in coordinator_2.registry.nodes
    finally:
        coordinator_2.shutdown()
        control_plane_2.close()


def test_multiple_jobs_and_receipts_all_restored(tmp_path):
    path = str(tmp_path / "cp.db")
    control_plane_1 = ControlPlane(path=path)
    ids = [_seed(control_plane_1, job_id=f"job-{i}", node_id=f"node-{i}") for i in range(3)]
    control_plane_1.close()

    control_plane_2 = ControlPlane(path=path)
    coordinator = GCONCoordinator(control_plane=control_plane_2)
    try:
        for job_id, node_id in ids:
            assert job_id in coordinator.jobs
            assert job_id in coordinator.receipts
            assert node_id in coordinator.get_persisted_nodes()
    finally:
        coordinator.shutdown()
        control_plane_2.close()


def test_restore_never_raises_on_a_corrupt_control_plane(tmp_path, monkeypatch):
    path = str(tmp_path / "cp.db")
    control_plane = ControlPlane(path=path)
    _seed(control_plane)

    def _boom():
        raise RuntimeError("simulated corrupt row")

    monkeypatch.setattr(control_plane.jobs, "list_all", _boom)

    # Must degrade to "empty job history", not prevent the coordinator
    # from starting at all.
    coordinator = GCONCoordinator(control_plane=control_plane)
    assert coordinator.jobs == {}
    coordinator.shutdown()
    control_plane.close()