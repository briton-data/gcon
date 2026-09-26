"""
Two real-gRPC regression tests for restart recovery:

1. GrpcTransport.send_job's is_latest_attempt guard (see its own
   comment in grpc_transport.py): once a newer job_attempts row exists
   for a job_id -- exactly what restore_from_persistence's redispatch
   creates the moment it actually dispatches -- an older attempt's
   real, correct completion arriving late must not be allowed to
   overwrite control_plane.jobs. This is the durable-DB-level half of
   the same guarantee _run_job's dispatch_attempt_number fencing gives
   the in-memory side (see its docstring); it has to live in
   send_job() itself because that write happens unconditionally,
   before _run_job ever gets a chance to check anything.

   Tested directly and deterministically: a real job dispatched to a
   real agent, a second (newer) job_attempts row inserted directly
   while the first is still genuinely in flight (simulating exactly
   what a restart's redispatch would have created by the time it
   happens), then the real agent's real completion is allowed to
   arrive. Same approach as
   test_coordinator_receipt_attempt_linking.py's staleness test --
   precise and deterministic rather than orchestrating two full live
   coordinator processes.

2. A resumed job gets its own new, durable job_attempts row (on top
   of the pre-crash one) once actually redispatched -- the real-gRPC
   counterpart of test_restart_recovery.py's LocalTransport-based
   attempt_number check, which can't observe this (LocalTransport
   never records durable attempts at all).
"""
import datetime
import time

import pytest

from gcon.cluster.communication import CommunicationManager
from gcon.cluster.coordinator import GCONCoordinator
from gcon.transport.config import TransportConfig
from gcon.transport.grpc_transport import GrpcTransport
from gcon.transport.remote_node import RemoteNodeProxy

from tests.transport.conftest import free_tcp_port, wait_until
from tests.transport.test_grpc_transport import _start_agent


@pytest.fixture
def coordinator_over_grpc(control_plane, cert_dir):
    port = free_tcp_port()
    control_plane.settings.set("grpc_port", str(port))
    control_plane.settings.set("tls_cert_dir", cert_dir)
    control_plane.settings.set("heartbeat_interval_seconds", "1")
    config = TransportConfig.load(control_plane)

    coordinator = GCONCoordinator(transport=None, control_plane=control_plane)

    transport_holder = {}

    def on_node_registered(node_id, capabilities, org_id=None, address=None):
        proxy = RemoteNodeProxy(node_id, transport_holder["transport"], org_id=org_id, address=address)
        coordinator.register_agent(proxy)

    def on_heartbeat(node_id, payload):
        coordinator.receive_heartbeat({
            "node_id": node_id,
            "status": payload["status"],
            "timestamp": datetime.datetime.now(datetime.UTC),
        })

    def on_node_disconnected(node_id):
        coordinator.on_node_disconnected(node_id)

    transport = GrpcTransport(
        control_plane=control_plane,
        config=config,
        on_heartbeat=on_heartbeat,
        on_node_registered=on_node_registered,
        on_node_disconnected=on_node_disconnected,
    )
    transport_holder["transport"] = transport
    coordinator.communication = CommunicationManager(transport=transport)
    transport.start()

    yield coordinator, transport, f"localhost:{port}", cert_dir

    transport.shutdown(grace_period=3)
    coordinator.shutdown()


def test_late_completion_from_a_superseded_attempt_does_not_overwrite_job_status(
    coordinator_over_grpc, tmp_path
):
    coordinator, transport, address, cert_dir = coordinator_over_grpc
    daemon = _start_agent("node-restart-late", address, cert_dir, tmp_path)
    try:
        assert wait_until(
            lambda: (
                "node-restart-late" in [n.node_id for n in coordinator.registry.available_nodes()]
                and "node-restart-late" in transport.list_node_ids()
            )
        )

        coordinator.submit_job("job-restart-late", "sleep 2 && echo original-result")
        assert wait_until(
            lambda: coordinator.jobs["job-restart-late"]["status"] == "running", timeout=5
        )

        attempts_before = coordinator.control_plane.job_attempts.list_for_job("job-restart-late")
        assert len(attempts_before) == 1

        # Simulate exactly what restore_from_persistence's redispatch
        # creates the moment it actually dispatches the resumed job --
        # a newer job_attempts row -- without needing a second real
        # coordinator process. In a real restart this row and the new
        # process's in-memory attempt_number always move together (a
        # fresh process has no old _run_job thread for the pre-crash
        # attempt to race in the first place); reproducing that
        # consistently here means bumping both, not just the durable
        # row, to accurately simulate "this job has already moved to a
        # new attempt" rather than a same-process state this specific
        # combination can't actually arise in. Also set the durable job
        # status to something the *real* (about-to-arrive) attempt-1
        # result would never produce, so we can tell whether it got
        # written.
        coordinator.control_plane.nodes.upsert("node-restart-sim", hostname="node-restart-sim")
        coordinator.control_plane.job_attempts.record_attempt(
            "job-restart-late", "node-restart-sim", "restart-sim-msg-1"
        )
        coordinator.control_plane.jobs.set_status("job-restart-late", "running")
        with coordinator.jobs_lock:
            coordinator.jobs["job-restart-late"]["attempt_number"] = 2

        # Let the real attempt-1 dispatch (sleep 2) actually finish and
        # report its real, correct result back over the still-open
        # connection.
        time.sleep(3)

        job_row = coordinator.control_plane.jobs.get("job-restart-late")
        assert job_row["status"] == "running", (
            "a late completion from a superseded attempt overwrote the "
            f"durable job status -- got {job_row['status']!r}"
        )

        # The superseded attempt's own row is still correctly marked
        # with its real outcome -- the guard only blocks the
        # jobs-table write, not the per-attempt one.
        attempts_after = coordinator.control_plane.job_attempts.list_for_job("job-restart-late")
        assert len(attempts_after) == 2
        assert attempts_after[0]["attempt_id"] == attempts_before[0]["attempt_id"]
        assert attempts_after[0]["status"] == "success"
    finally:
        daemon.stop()


def test_resumed_attempt_gets_its_own_durable_row_after_a_prior_one(coordinator_over_grpc, tmp_path):
    coordinator, transport, address, cert_dir = coordinator_over_grpc
    daemon = _start_agent("node-restart-resume", address, cert_dir, tmp_path)
    try:
        assert wait_until(
            lambda: (
                "node-restart-resume" in [n.node_id for n in coordinator.registry.available_nodes()]
                and "node-restart-resume" in transport.list_node_ids()
            )
        )

        # Seed a pre-crash attempt directly (as if a now-dead prior
        # coordinator process had dispatched it), then let restart
        # recovery pick it up on a coordinator built against the same
        # control_plane -- the actual restore_from_persistence path,
        # not a hand-rolled substitute.
        coordinator.control_plane.jobs.create("job-restart-resume", "echo hi")
        coordinator.control_plane.jobs.set_status("job-restart-resume", "running")
        coordinator.control_plane.nodes.upsert("node-prior-dead", hostname="node-prior-dead")
        prior_attempt = coordinator.control_plane.job_attempts.record_attempt(
            "job-restart-resume", "node-prior-dead", "prior-msg-1"
        )

        coordinator.restore_from_persistence()
        assert coordinator.jobs["job-restart-resume"]["status"] == "pending"
        coordinator.job_queue.get()
        coordinator.assign_job("job-restart-resume")

        assert wait_until(
            lambda: coordinator.jobs["job-restart-resume"]["status"] == "completed", timeout=10
        )

        attempts = coordinator.control_plane.job_attempts.list_for_job("job-restart-resume")
        assert [a["attempt_id"] for a in attempts][0] == prior_attempt["attempt_id"]
        assert len(attempts) == 2
        assert attempts[1]["status"] == "success"
        assert attempts[1]["node_id"] == "node-restart-resume"

        receipts = coordinator.control_plane.receipts.list_for_job("job-restart-resume")
        assert len(receipts) == 1
        assert receipts[0]["attempt_id"] == attempts[1]["attempt_id"]
    finally:
        daemon.stop()
