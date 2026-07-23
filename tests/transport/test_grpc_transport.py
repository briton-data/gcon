"""
Integration tests for GrpcTransport + AgentDaemon.

Every test here spins up a real `grpc.Server` bound to a real TCP
port on localhost with real mTLS certificates (via `gcon.transport.tls`)
and drives it with either a real `AgentDaemon` or a raw generated gRPC
stub. No networking is mocked or simulated.
"""

import os
import time

import grpc
import pytest

from gcon.cluster.communication import CommunicationManager
from gcon.execution.agent import GCONAgent
from gcon.transport import tls
from gcon.transport.agent_daemon import AgentDaemon
from gcon.transport.errors import JobDispatchTimeoutError, NodeUnavailableError
from gcon.transport.proto import gcon_transport_pb2 as pb
from gcon.transport.proto import gcon_transport_pb2_grpc as pb_grpc

from tests.transport.conftest import wait_until


def _start_agent(node_id, address, cert_dir, tmp_path, capabilities=None):
    keys_dir = tmp_path / f"keys-{node_id}"
    keys_dir.mkdir(exist_ok=True)
    old_cwd = os.getcwd()
    os.chdir(str(keys_dir))
    try:
        agent = GCONAgent(node_id=node_id)
        daemon = AgentDaemon(
            node_id=node_id,
            coordinator_address=address,
            cert_dir=cert_dir,
            agent=agent,
            capabilities=capabilities or {},
        )
        daemon.start()
        return daemon
    finally:
        os.chdir(old_cwd)


# --------------------------------------------------------------- registration
def test_agent_registers_and_appears_in_control_plane(running_transport, tmp_path):
    transport, address = running_transport
    cert_dir = transport.config.tls_cert_dir

    daemon = _start_agent("node-reg-1", address, cert_dir, tmp_path, capabilities={"gpu": "A100"})
    try:
        assert wait_until(lambda: "node-reg-1" in transport.list_node_ids())

        node_row = transport.control_plane.nodes.get("node-reg-1")
        assert node_row is not None
        assert node_row["status"] == "idle"

        caps = transport.control_plane.node_capabilities.get_capabilities("node-reg-1")
        assert caps == {"gpu": "A100"}

        events = [e["event_type"] for e in transport.control_plane.cluster_events.recent()]
        assert "NODE_REGISTERED" in events
    finally:
        daemon.stop()


def test_reregistration_is_idempotent_in_control_plane(running_transport, tmp_path):
    transport, address = running_transport
    cert_dir = transport.config.tls_cert_dir

    daemon = _start_agent("node-reg-2", address, cert_dir, tmp_path)
    try:
        assert wait_until(lambda: "node-reg-2" in transport.list_node_ids())
    finally:
        daemon.stop()

    assert wait_until(lambda: "node-reg-2" not in transport.list_node_ids())

    daemon2 = _start_agent("node-reg-2", address, cert_dir, tmp_path)
    try:
        assert wait_until(lambda: "node-reg-2" in transport.list_node_ids())
        # still exactly one durable row for this node_id
        assert len(
            [n for n in transport.control_plane.nodes.list_all() if n["node_id"] == "node-reg-2"]
        ) == 1
    finally:
        daemon2.stop()


# ------------------------------------------------------------- mutual auth
def test_register_rejected_when_claimed_node_id_does_not_match_certificate(
    running_transport, tmp_path
):
    transport, address = running_transport
    cert_dir = transport.config.tls_cert_dir

    # Certificate genuinely issued for 'node-honest', but the Register
    # request claims a *different* node_id -- mutual auth must reject
    # this: the TLS-verified certificate identity is authoritative,
    # not whatever the message body claims.
    credentials = tls.load_agent_channel_credentials(cert_dir, "node-honest")
    channel = grpc.secure_channel(address, credentials)
    try:
        stub = pb_grpc.AgentControlStub(channel)
        with pytest.raises(grpc.RpcError) as excinfo:
            stub.Register(
                pb.RegisterRequest(node_id="node-impersonated", hostname="h"), timeout=10
            )
        assert excinfo.value.code() == grpc.StatusCode.PERMISSION_DENIED
    finally:
        channel.close()


def test_connection_without_client_certificate_is_rejected(running_transport):
    transport, address = running_transport
    cert_dir = transport.config.tls_cert_dir

    ca_paths = tls.ensure_ca(cert_dir)
    with open(ca_paths.ca_cert_path, "rb") as f:
        root_ca = f.read()

    # Server-only TLS credentials -- no client certificate presented,
    # which the coordinator's require_client_auth=True must refuse.
    credentials = grpc.ssl_channel_credentials(root_certificates=root_ca)
    channel = grpc.secure_channel(address, credentials)
    try:
        stub = pb_grpc.AgentControlStub(channel)
        with pytest.raises(grpc.RpcError):
            stub.Register(pb.RegisterRequest(node_id="node-x", hostname="h"), timeout=10)
    finally:
        channel.close()


# ----------------------------------------------------------------- heartbeats
def test_heartbeats_are_recorded_and_sequenced(running_transport, tmp_path):
    transport, address = running_transport
    cert_dir = transport.config.tls_cert_dir

    daemon = _start_agent("node-hb-1", address, cert_dir, tmp_path)
    try:
        assert wait_until(
            lambda: transport.control_plane.heartbeats.last_sequence("node-hb-1") >= 2,
            timeout=6,
        )
        recent = transport.control_plane.heartbeats.recent_for_node("node-hb-1", limit=10)
        sequences = [h["sequence"] for h in recent]
        assert sequences == sorted(sequences, reverse=True)
    finally:
        daemon.stop()


# -------------------------------------------------------------- job dispatch
def test_job_dispatch_through_communication_manager(running_transport, tmp_path):
    transport, address = running_transport
    cert_dir = transport.config.tls_cert_dir

    daemon = _start_agent("node-job-1", address, cert_dir, tmp_path)
    try:
        assert wait_until(lambda: "node-job-1" in transport.list_node_ids())

        manager = CommunicationManager(transport=transport)
        response = manager.send_job("node-job-1", "job-abc", "echo hello-transport", timeout=15)

        assert response["status"] == "success"
        assert response["result"]["status"] == "success"
        assert "hello-transport" in response["result"]["stdout"]

        job_row = transport.control_plane.jobs.get("job-abc")
        assert job_row["status"] == "success"

        attempts = transport.control_plane.job_attempts.list_for_job("job-abc")
        assert len(attempts) == 1
        assert attempts[0]["status"] == "success"
    finally:
        daemon.stop()


def test_job_dispatch_to_disconnected_node_raises(running_transport):
    transport, address = running_transport
    with pytest.raises(NodeUnavailableError):
        transport.send_job("no-such-node", "job-x", "echo hi")


def test_repeated_dispatch_creates_incrementing_attempts(running_transport, tmp_path):
    transport, address = running_transport
    cert_dir = transport.config.tls_cert_dir

    daemon = _start_agent("node-job-2", address, cert_dir, tmp_path)
    try:
        assert wait_until(lambda: "node-job-2" in transport.list_node_ids())
        transport.send_job("node-job-2", "job-retry", "echo one", timeout=10)
        transport.send_job("node-job-2", "job-retry", "echo two", timeout=10)

        attempts = transport.control_plane.job_attempts.list_for_job("job-retry")
        assert [a["attempt_number"] for a in attempts] == [1, 2]
    finally:
        daemon.stop()


# -------------------------------------------------------------- cancellation
def test_job_cancellation_kills_running_process(running_transport, tmp_path):
    transport, address = running_transport
    cert_dir = transport.config.tls_cert_dir

    daemon = _start_agent("node-cancel-1", address, cert_dir, tmp_path)
    try:
        assert wait_until(lambda: "node-cancel-1" in transport.list_node_ids())

        result_holder = {}

        def dispatch():
            result_holder["response"] = transport.send_job(
                "node-cancel-1", "job-long", "sleep 30", timeout=60
            )

        import threading

        t = threading.Thread(target=dispatch, daemon=True)
        t.start()

        # give the job a moment to actually start running on the agent
        assert wait_until(lambda: "node-cancel-1" in transport.list_node_ids(), timeout=3)
        time.sleep(0.5)

        cancelled = transport.cancel_job("node-cancel-1", "job-long")
        assert cancelled is True

        t.join(timeout=15)
        assert "response" in result_holder
        # killed process: non-zero/negative return code, not a clean success
        assert result_holder["response"]["result"]["status"] != "success"
    finally:
        daemon.stop()


def test_cancel_on_disconnected_node_returns_false(running_transport):
    transport, address = running_transport
    assert transport.cancel_job("nobody-connected", "job-x") is False


# ------------------------------------------------------------------ logging
def test_job_output_is_streamed_and_persisted(running_transport, tmp_path):
    transport, address = running_transport
    cert_dir = transport.config.tls_cert_dir

    daemon = _start_agent("node-log-1", address, cert_dir, tmp_path)
    try:
        assert wait_until(lambda: "node-log-1" in transport.list_node_ids())
        transport.send_job("node-log-1", "job-log-1", "echo streamed-line", timeout=15)

        assert wait_until(
            lambda: len(transport.control_plane.execution_logs.for_job("job-log-1")) > 0,
            timeout=5,
        )
        logs = transport.control_plane.execution_logs.for_job("job-log-1")
        assert any("streamed-line" in row["content"] for row in logs)
        # every persisted line is attributed to the real job_attempts row,
        # which is what makes (attempt_id, stream, sequence) dedup work
        assert all(row["attempt_id"] for row in logs)
    finally:
        daemon.stop()


# ----------------------------------------------------------------- receipts
def test_receipt_uploaded_and_persisted(running_transport, tmp_path):
    transport, address = running_transport
    cert_dir = transport.config.tls_cert_dir

    daemon = _start_agent("node-receipt-1", address, cert_dir, tmp_path)
    try:
        assert wait_until(lambda: "node-receipt-1" in transport.list_node_ids())
        transport.send_job("node-receipt-1", "job-receipt-1", "echo receipt-test", timeout=15)

        assert wait_until(
            lambda: len(transport.control_plane.receipts.list_for_job("job-receipt-1")) > 0,
            timeout=5,
        )
        receipts = transport.control_plane.receipts.list_for_job("job-receipt-1")
        assert len(receipts) == 1
        assert receipts[0]["payload"]["job_id"] == "job-receipt-1"
        assert receipts[0]["receipt_hash"]
    finally:
        daemon.stop()


# ---------------------------------------------------------------- reconnect
def test_agent_reconnects_after_coordinator_restart(control_plane, cert_dir, tmp_path):
    from tests.transport.conftest import free_tcp_port
    from gcon.transport.config import TransportConfig
    from gcon.transport.grpc_transport import GrpcTransport

    port = free_tcp_port()
    control_plane.settings.set("grpc_port", str(port))
    control_plane.settings.set("tls_cert_dir", cert_dir)
    control_plane.settings.set("heartbeat_interval_seconds", "1")
    control_plane.settings.set("reconnect_initial_backoff_seconds", "0.2")
    control_plane.settings.set("reconnect_max_backoff_seconds", "0.5")
    address = f"localhost:{port}"

    config = TransportConfig.load(control_plane)
    transport1 = GrpcTransport(control_plane=control_plane, config=config)
    transport1.start()

    daemon = _start_agent("node-reconnect-1", address, cert_dir, tmp_path)
    try:
        assert wait_until(lambda: "node-reconnect-1" in transport1.list_node_ids())

        # Simulate the coordinator process going away.
        transport1.shutdown(grace_period=1)
        assert wait_until(lambda: "node-reconnect-1" not in transport1.list_node_ids(), timeout=3)

        # Bring a new coordinator server up on the same address; the
        # already-running AgentDaemon's reconnect loop (automatic,
        # exponential backoff) should find it without any restart of
        # the daemon itself.
        transport2 = GrpcTransport(control_plane=control_plane, config=config)
        transport2.start()
        try:
            assert wait_until(
                lambda: "node-reconnect-1" in transport2.list_node_ids(), timeout=10
            )
            response = transport2.send_job(
                "node-reconnect-1", "job-after-reconnect", "echo back-online", timeout=15
            )
            assert response["result"]["status"] == "success"
        finally:
            transport2.shutdown(grace_period=2)
    finally:
        daemon.stop()


# ------------------------------------------------------------- graceful stop
def test_graceful_daemon_shutdown_notifies_coordinator(running_transport, tmp_path):
    transport, address = running_transport
    cert_dir = transport.config.tls_cert_dir

    daemon = _start_agent("node-shutdown-1", address, cert_dir, tmp_path)
    assert wait_until(lambda: "node-shutdown-1" in transport.list_node_ids())

    daemon.stop(reason="test teardown")

    assert wait_until(lambda: "node-shutdown-1" not in transport.list_node_ids(), timeout=5)
    node_row = transport.control_plane.nodes.get("node-shutdown-1")
    assert node_row["status"] == "offline"

    events = [e["event_type"] for e in transport.control_plane.cluster_events.recent()]
    assert "NODE_DISCONNECTED" in events
