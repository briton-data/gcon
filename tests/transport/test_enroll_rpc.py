"""
Real, end-to-end tests for the Enroll RPC -- the one gap flagged
after the node_enrollment_audit work: that feature's tests (see
tests/persistence/test_node_enrollment_audit.py and
tests/api/test_node_enrollment_history.py) only ever exercised the
repository directly and a REST route reading data inserted straight
into it. The actual code that was changed -- grpc_transport.py's
Enroll() handler itself -- had never been driven by a real RPC call
at all.

Same engineering rule as the rest of tests/transport/: a real
`grpc.Server` on a real TCP port, real CSR generation, no mocked
context. Enroll is deliberately reachable over a *plaintext* port
(see GrpcTransport.start()'s "Second, separate server" comment) --
there is no TLS/client-cert layer to set up here, unlike Register.
"""

import os

import grpc
import pytest

from gcon.persistence.control_plane import ControlPlane
from gcon.transport import tls
from gcon.transport.config import TransportConfig
from gcon.transport.grpc_transport import GrpcTransport
from gcon.transport.proto import gcon_transport_pb2 as pb
from gcon.transport.proto import gcon_transport_pb2_grpc as pb_grpc

from tests.transport.conftest import free_tcp_port


@pytest.fixture
def enroll_setup(tmp_path, monkeypatch):
    # Must be set before GrpcTransport.start() runs -- but thanks to
    # the _enroll_token() live-read fix (same class of fix as
    # management_layer.py's BOOTSTRAP_OWNER_EMAIL), it no longer
    # matters whether grpc_transport was already imported earlier in
    # this pytest process by some other test module; this is no
    # longer an import-order-dependent test-isolation trap.
    monkeypatch.setenv("GCON_ENROLL_TOKEN", "shared-dev-token-123")

    control_plane = ControlPlane(path=str(tmp_path / "cp.db"))
    cert_dir = str(tmp_path / "certs")
    os.makedirs(cert_dir, exist_ok=True)

    port = free_tcp_port()
    control_plane.settings.set("grpc_port", str(port))
    control_plane.settings.set("tls_cert_dir", cert_dir)
    config = TransportConfig.load(control_plane)
    transport = GrpcTransport(control_plane=control_plane, config=config)
    transport.start()

    # grpc_enroll_port isn't a real TransportConfig field -- start()
    # always derives it as grpc_port + 1 (see its own comment). Same
    # assumption the production code itself makes.
    enroll_address = f"localhost:{port + 1}"

    channel = grpc.insecure_channel(enroll_address)
    stub = pb_grpc.AgentControlStub(channel)

    yield control_plane, stub

    channel.close()
    transport.shutdown(grace_period=3)
    control_plane.close()


def _csr(node_id: str) -> bytes:
    _key_pem, csr_pem = tls.generate_agent_csr(node_id)
    return csr_pem


class TestEnrollRpcRealServer:
    def test_full_enrollment_lifecycle(self, enroll_setup):
        """
        One real server, one test, walking through every real
        Enroll() code path in sequence so it's easy to read top to
        bottom and easy to see nothing else in this file needs to
        change if any one section here does:

          1. Legacy shared-token enrollment is accepted, returns a
             real signed cert, and is persisted to
             node_enrollment_audit with the real loopback source IP
             and no org_id/enroll_token_id (legacy path has no
             per-org token row to point to).
          2. A bad/unknown token is rejected and persisted with a
             reason (not silently dropped).
          3. A real per-org enroll_token (enroll_tokens.create_token)
             is accepted and persisted with its correct org_id and
             enroll_token_id.
          4. That same org token, once revoked, is rejected -- not
             silently falling through as valid via the legacy
             shared-token path -- and the rejection is persisted too.
        """
        control_plane, stub = enroll_setup

        # 1. Legacy shared-token path: accepted.
        resp = stub.Enroll(
            pb.EnrollRequest(
                node_id="worker-legacy", enroll_token="shared-dev-token-123",
                csr_pem=_csr("worker-legacy"),
            ),
            timeout=10,
        )
        assert resp.accepted is True
        assert resp.cert_pem
        assert resp.ca_cert_pem

        latest = control_plane.node_enrollment_audit.get_latest_for_node("worker-legacy")
        assert latest is not None
        assert latest["accepted"] == 1
        # See test_node_enrollment_audit.py::test_legacy_shared_token_path_has_no_token_id
        assert latest["enroll_token_id"] is None
        assert latest["org_id"] is None
        assert latest["source_ip"] in ("127.0.0.1", "::1")

        # 2. Bad token: rejected, but still recorded (real security
        # signal on its own - e.g. someone probing with a wrong
        # token - previously invisible outside a log line).
        resp = stub.Enroll(
            pb.EnrollRequest(
                node_id="worker-rejected", enroll_token="totally-wrong-token",
                csr_pem=_csr("worker-rejected"),
            ),
            timeout=10,
        )
        assert resp.accepted is False
        assert resp.reason

        history = control_plane.node_enrollment_audit.list_for_node("worker-rejected")
        assert len(history) == 1
        assert history[0]["accepted"] == 0
        assert history[0]["reason"]

        # 3. Real per-org token: accepted, and correctly attributed
        # to its org/token_id (this is the whole point of the
        # traceability feature - previously only the legacy path
        # existed to test against).
        org_token = control_plane.enroll_tokens.create_token(org_id="acme", label="ci-fleet")
        token_row = control_plane.enroll_tokens.get_by_token(org_token)

        stub.Enroll(
            pb.EnrollRequest(
                node_id="worker-acme-1", enroll_token=org_token,
                csr_pem=_csr("worker-acme-1"),
            ),
            timeout=10,
        )
        latest = control_plane.node_enrollment_audit.get_latest_for_node("worker-acme-1")
        assert latest["accepted"] == 1
        assert latest["org_id"] == "acme"
        assert latest["enroll_token_id"] == token_row["token_id"]

        # 4. Revoke that same token, then try to enroll a second node
        # with it: must be a clean reject, not a silent fall-through
        # to "valid" via the unrelated legacy shared-token check.
        control_plane.enroll_tokens.revoke(token_row["token_id"])

        resp = stub.Enroll(
            pb.EnrollRequest(
                node_id="worker-acme-2", enroll_token=org_token,
                csr_pem=_csr("worker-acme-2"),
            ),
            timeout=10,
        )
        assert resp.accepted is False
        history = control_plane.node_enrollment_audit.list_for_node("worker-acme-2")
        assert history[0]["accepted"] == 0
