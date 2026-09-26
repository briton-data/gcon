"""
End-to-end: a real GCONCoordinator, wired to a real GrpcTransport the
same way scripts/run_coordinator.py wires it in production
(on_node_registered/on_heartbeat/on_node_disconnected callbacks
calling back into the coordinator), dispatching a job to a real
AgentDaemon-backed agent process over real mTLS.

This is the layer test_grpc_transport.py's existing attempt tests
don't reach: those confirm the *transport* creates and completes a
job_attempts row correctly. They say nothing about whether the
*coordinator*, one layer up, actually threads that attempt_id through
to the receipt it creates in _run_job. Before this round,
receipts.attempt_id was a real schema column (and a real FK target
for other tables) that was always written as None -- _run_job never
had the attempt_id available to pass to receipts.upload(). Confirmed
here against the real production wiring, not a stub.
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


def test_receipt_attempt_id_matches_the_real_job_attempts_row(coordinator_over_grpc, tmp_path):
    coordinator, transport, address, cert_dir = coordinator_over_grpc
    daemon = _start_agent("node-link-1", address, cert_dir, tmp_path)
    try:
        # coordinator.registry knowing about the node (via the
        # Register() RPC's on_node_registered callback) is NOT the
        # same moment as the transport actually being able to dispatch
        # to it -- Register() is a one-shot RPC, separate from and
        # earlier than the agent's long-lived Control() stream that
        # send_job() needs. Waiting only on the registry here is a
        # real, empirically-confirmed race: dispatching in that window
        # fails immediately with "Node '<id>' is not connected." and
        # burns an attempt for nothing. Waiting on both is the correct
        # readiness check (and arguably a real gap worth its own fix
        # later -- see the round's summary).
        assert wait_until(
            lambda: (
                "node-link-1" in [n.node_id for n in coordinator.registry.available_nodes()]
                and "node-link-1" in transport.list_node_ids()
            )
        )

        coordinator.submit_job("job-link-1", "echo attempt-link-test")
        assert wait_until(
            lambda: coordinator.jobs["job-link-1"]["status"] == "completed", timeout=10
        )

        attempts = coordinator.control_plane.job_attempts.list_for_job("job-link-1")
        assert len(attempts) == 1
        assert attempts[0]["status"] == "success"

        receipts = coordinator.control_plane.receipts.list_for_job("job-link-1")
        assert len(receipts) == 1
        # This is the actual regression this test exists for: before
        # this round attempts[0]["attempt_id"] was real and durable,
        # but receipts[0]["attempt_id"] was unconditionally None --
        # the FK column existed and was simply never populated.
        assert receipts[0]["attempt_id"] == attempts[0]["attempt_id"]
        assert receipts[0]["attempt_id"] is not None
    finally:
        daemon.stop()


def test_stale_attempt_failure_does_not_overwrite_a_newer_attempt(coordinator_over_grpc, tmp_path):
    """
    Regression test for a real bug: a dispatch's _run_job thread is
    blocked on a real, long-running network call (sleep 5, well past
    when we're about to kill its node). We simulate exactly what
    recover_jobs() does in production -- bump job["attempt_number"]
    and move the job off this node, as if it had already been
    reassigned to a second attempt -- *before* the real node dies and
    unblocks this thread's failure handler. The point under test is
    narrow and precise: does that stale failure, arriving after the
    job has moved on, get correctly discarded instead of overwriting
    the job with attempt 1's error? (A full second real dispatch is
    exercised separately, in the simpler single-attempt test above and
    in tests/cluster/test_max_job_attempts.py's LocalTransport-based
    recover_jobs tests -- this test isolates the fencing logic itself
    from needing two real gRPC agents to both complete cleanly in
    sequence, which is a second, unrelated source of flakiness.)
    """
    coordinator, transport, address, cert_dir = coordinator_over_grpc
    daemon = _start_agent("node-link-2a", address, cert_dir, tmp_path)
    try:
        assert wait_until(
            lambda: (
                "node-link-2a" in [n.node_id for n in coordinator.registry.available_nodes()]
                and "node-link-2a" in transport.list_node_ids()
            )
        )
        coordinator.submit_job("job-link-2", "sleep 5 && echo done")
        assert wait_until(
            lambda: coordinator.jobs["job-link-2"]["status"] == "running", timeout=5
        )
        assert coordinator.jobs["job-link-2"]["attempt_number"] == 1

        # Simulate recover_jobs() having already reassigned this job
        # to a second attempt elsewhere, moments before node-link-2a
        # actually dies -- the exact ordering that clobbered a real
        # job before this round's fix.
        with coordinator.jobs_lock:
            coordinator.jobs["job-link-2"]["attempt_number"] = 2
            coordinator.jobs["job-link-2"]["status"] = "running"
            coordinator.jobs["job-link-2"]["node_id"] = "node-link-2b"
    finally:
        # Kill node-link-2a -- its _run_job thread (still blocked
        # inside communication.send_job for the ORIGINAL attempt 1)
        # will shortly get unblocked with a failure by the disconnect.
        daemon.stop()

    # Give the stale thread's failure handler time to run and (before
    # this round's fix) clobber the job.
    time.sleep(2)

    job = coordinator.jobs["job-link-2"]
    assert job["status"] == "running", (
        "a stale attempt-1 failure overwrote the job even though it "
        f"had already moved to attempt 2 -- got status={job['status']!r}"
    )
    assert job["node_id"] == "node-link-2b"
    assert job["attempt_number"] == 2
