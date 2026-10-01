"""
End-to-end: a real AgentDaemon-backed agent signs its own job result
with its own Ed25519 key before ever sending it, over real mTLS; the
coordinator verifies that signature against the node's registered
public key and embeds it into the receipt it creates. This is the
layer tests/execution/test_worker_identity.py's unit tests don't
reach: those prove the crypto primitive is correct in isolation, not
that it's actually wired through registration -> JobAssign ->
signing -> JobResult -> verification -> receipt for real.
"""
import datetime
import json
import os

import pytest

from gcon.cluster.communication import CommunicationManager
from gcon.cluster.coordinator import GCONCoordinator
from gcon.execution.worker_identity import verify_attestation
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

    def on_node_registered(node_id, capabilities, org_id=None, address=None):
        proxy = RemoteNodeProxy(node_id, transport, org_id=org_id, address=address)
        coordinator.register_agent(proxy)

    def on_heartbeat(node_id, payload):
        coordinator.receive_heartbeat({
            "node_id": node_id, "status": payload["status"],
            "timestamp": datetime.datetime.now(datetime.UTC),
        })

    def on_node_disconnected(node_id):
        coordinator.on_node_disconnected(node_id)

    transport = GrpcTransport(
        control_plane=control_plane, config=config,
        on_heartbeat=on_heartbeat, on_node_registered=on_node_registered,
        on_node_disconnected=on_node_disconnected,
    )
    coordinator.communication = CommunicationManager(transport=transport)
    transport.start()

    yield coordinator, transport, f"localhost:{port}", cert_dir

    transport.shutdown(grace_period=3)
    coordinator.shutdown()


def test_receipt_carries_a_valid_independently_checkable_worker_attestation(
    coordinator_over_grpc, tmp_path
):
    coordinator, transport, address, cert_dir = coordinator_over_grpc
    daemon = _start_agent("node-attest-1", address, cert_dir, tmp_path)
    try:
        assert wait_until(
            lambda: (
                "node-attest-1" in [n.node_id for n in coordinator.registry.available_nodes()]
                and "node-attest-1" in transport.list_node_ids()
            )
        )

        coordinator.submit_job("job-attest-1", "echo attest-me")
        assert wait_until(
            lambda: coordinator.jobs["job-attest-1"]["status"] == "completed", timeout=10
        )

        # Registered during Register() -- proves the public key round-
        # tripped through the wire and NodeRepository.upsert, not just
        # that ensure_node_keypair works in isolation.
        node_row = coordinator.control_plane.nodes.get("node-attest-1")
        assert node_row["ed25519_public_key"]
        assert "BEGIN PUBLIC KEY" in node_row["ed25519_public_key"]

        receipts = coordinator.control_plane.receipts.list_for_job("job-attest-1")
        assert len(receipts) == 1
        attestation = receipts[0]["payload"]["worker_attestation"]
        assert attestation is not None
        assert attestation["payload"]["job_id"] == "job-attest-1"
        assert attestation["payload"]["node_id"] == "node-attest-1"
        assert attestation["public_key_pem"] == node_row["ed25519_public_key"]

        # The actual claim: independently checkable with JUST the
        # public key embedded in the receipt -- no coordinator, no
        # HMAC secret, nothing else needed.
        assert verify_attestation(
            attestation["public_key_pem"], attestation["payload"], attestation["signature"]
        ) is True

        # And through the coordinator's own on-demand re-check (see
        # validate_worker_attestation's docstring for why this is
        # always a fresh check, never a stored bool). get_receipt_detail
        # keys off the hash-based id create_receipt embeds INSIDE the
        # payload, not the DB row's own separately-generated uuid4
        # primary key (see _load_receipt_from_control_plane) -- two
        # different, both-legitimate id namespaces for the same
        # receipt, pre-existing and unrelated to this round's work.
        detail = coordinator.get_receipt_detail(receipts[0]["payload"]["receipt_id"])
        assert detail["worker_attestation"]["verified"] is True
        assert detail["worker_attestation"]["node_id"] == "node-attest-1"
    finally:
        daemon.stop()


def test_altered_stdout_after_signing_is_caught_by_the_attestation(coordinator_over_grpc, tmp_path):
    """
    A receipt with its worker_attestation payload's output_hash
    tampered with after the fact (simulating someone editing the
    stored receipt to claim a different result than the node actually
    signed) must fail re-verification -- proving the signature is
    actually binding, not decorative.
    """
    coordinator, transport, address, cert_dir = coordinator_over_grpc
    daemon = _start_agent("node-attest-2", address, cert_dir, tmp_path)
    try:
        assert wait_until(
            lambda: (
                "node-attest-2" in [n.node_id for n in coordinator.registry.available_nodes()]
                and "node-attest-2" in transport.list_node_ids()
            )
        )
        coordinator.submit_job("job-attest-2", "echo original-output")
        assert wait_until(
            lambda: coordinator.jobs["job-attest-2"]["status"] == "completed", timeout=10
        )

        receipts = coordinator.control_plane.receipts.list_for_job("job-attest-2")
        receipt = receipts[0]["payload"]
        assert coordinator.verifier.validate_worker_attestation(receipt)[0] is True

        tampered = dict(receipt)
        tampered["worker_attestation"] = dict(receipt["worker_attestation"])
        tampered["worker_attestation"]["payload"] = dict(receipt["worker_attestation"]["payload"])
        tampered["worker_attestation"]["payload"]["output_hash"] = "forged-hash"

        is_valid, message = coordinator.verifier.validate_worker_attestation(tampered)
        assert is_valid is False
        assert "invalid" in message.lower()
    finally:
        daemon.stop()


def test_local_transport_jobs_have_no_worker_attestation(coordinator_over_grpc):
    """LocalTransport (in-process dev/test nodes) has no per-node
    identity at all -- a job dispatched that way must not fabricate a
    worker_attestation block, just omit it. Uses a plain LocalTransport
    coordinator, not the real-gRPC fixture, since that's the point
    being tested."""
    from gcon.cluster.coordinator import GCONCoordinator as _Coord
    from gcon.execution.agent import GCONAgent

    coordinator = _Coord()
    coordinator.register_agent(GCONAgent(node_id="local-node-1"))
    coordinator.submit_job("job-local-1", "echo hi")
    coordinator.assign_job("job-local-1")

    assert wait_until(lambda: coordinator.jobs["job-local-1"]["status"] == "completed", timeout=5)
    assert coordinator.receipts["job-local-1"].get("worker_attestation") is None
    coordinator.shutdown()


# ------------------------------------------------ execution_backend is attested
def _install_stand_in_docker(bin_dir):
    """A `docker` executable that runs the trailing `sh -c <script>` of
    `docker run ... <image> sh -c <script>` directly, and no-ops the rest --
    enough to drive the agent's real docker code path without a Docker daemon."""
    bin_dir.mkdir()
    script = bin_dir / "docker"
    script.write_text(
        "#!/usr/bin/env python3\n"
        "import subprocess, sys\n"
        "if len(sys.argv) > 1 and sys.argv[1] == 'run':\n"
        "    sys.exit(subprocess.run(sys.argv[-3:]).returncode)\n"
        "sys.exit(0)\n"
    )
    script.chmod(0o755)


def _run_one_job(coordinator, transport, address, cert_dir, tmp_path, node_id, job_id):
    daemon = _start_agent(node_id, address, cert_dir, tmp_path)
    try:
        assert wait_until(
            lambda: (
                node_id in [n.node_id for n in coordinator.registry.available_nodes()]
                and node_id in transport.list_node_ids()
            )
        )
        coordinator.submit_job(job_id, "echo backend-check")
        assert wait_until(lambda: coordinator.jobs[job_id]["status"] == "completed", timeout=15)
        assert wait_until(lambda: coordinator.control_plane.receipts.list_for_job(job_id), timeout=10)
        return coordinator.control_plane.receipts.list_for_job(job_id)[0]["payload"]
    finally:
        daemon.stop()


def test_receipt_attests_a_subprocess_backend(coordinator_over_grpc, tmp_path, monkeypatch):
    coordinator, transport, address, cert_dir = coordinator_over_grpc
    monkeypatch.delenv("GCON_EXECUTION_BACKEND", raising=False)
    receipt = _run_one_job(coordinator, transport, address, cert_dir, tmp_path, "node-be-1", "job-be-1")

    attestation = receipt["worker_attestation"]
    assert attestation["payload"]["execution_backend"] == "subprocess"
    assert verify_attestation(
        attestation["public_key_pem"], attestation["payload"], attestation["signature"]
    ) is True
    detail = coordinator.get_receipt_detail(receipt["receipt_id"])
    assert detail["worker_attestation"]["execution_backend"] == "subprocess"


def test_receipt_attests_a_docker_backend(coordinator_over_grpc, tmp_path, monkeypatch):
    coordinator, transport, address, cert_dir = coordinator_over_grpc
    _install_stand_in_docker(tmp_path / "bin")
    monkeypatch.setenv("PATH", f"{tmp_path / 'bin'}:{os.environ['PATH']}")
    monkeypatch.setenv("GCON_EXECUTION_BACKEND", "docker")
    receipt = _run_one_job(coordinator, transport, address, cert_dir, tmp_path, "node-be-2", "job-be-2")

    attestation = receipt["worker_attestation"]
    assert attestation["payload"]["execution_backend"] == "docker"
    assert verify_attestation(
        attestation["public_key_pem"], attestation["payload"], attestation["signature"]
    ) is True
    detail = coordinator.get_receipt_detail(receipt["receipt_id"])
    assert detail["worker_attestation"]["verified"] is True
    assert detail["worker_attestation"]["execution_backend"] == "docker"


def test_rewriting_the_attested_backend_breaks_the_receipt(coordinator_over_grpc, tmp_path, monkeypatch):
    """The coordinator (or anyone holding the receipt) can't relabel how the job
    ran: the node signed it, so a changed value fails validation, and that
    surfaces through assurance as an attestation mismatch."""
    from gcon.execution.verifier import ExecutionVerifier

    coordinator, transport, address, cert_dir = coordinator_over_grpc
    monkeypatch.delenv("GCON_EXECUTION_BACKEND", raising=False)
    receipt = _run_one_job(coordinator, transport, address, cert_dir, tmp_path, "node-be-3", "job-be-3")

    assert ExecutionVerifier.validate_worker_attestation(receipt)[0] is True

    relabelled = json.loads(json.dumps(receipt))
    relabelled["worker_attestation"]["payload"]["execution_backend"] = "docker"
    valid, message = ExecutionVerifier.validate_worker_attestation(relabelled)
    assert valid is False and "invalid" in message.lower()
