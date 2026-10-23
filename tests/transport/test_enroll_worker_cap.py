"""A plan's worker cap is enforced when a worker enrolls: a new worker past the cap is refused, a worker
enrolling again under its own name is not a new worker, and archiving a worker frees its place."""
import os

import grpc
import pytest

from gcon.persistence import ControlPlane
from gcon.transport import tls
from gcon.transport.config import TransportConfig
from gcon.transport.grpc_transport import GrpcTransport
from gcon.transport.proto import gcon_transport_pb2 as pb
from gcon.transport.proto import gcon_transport_pb2_grpc as pb_grpc
from tests.transport.conftest import free_tcp_port


@pytest.fixture
def capped(tmp_path, monkeypatch):
    monkeypatch.setenv("GCON_ENROLL_TOKEN", "shared-dev-token-123")
    monkeypatch.setenv("GCON_ENROLL_INSECURE", "1")
    control_plane = ControlPlane(path=str(tmp_path / "cp.db"))
    cert_dir = str(tmp_path / "certs")
    os.makedirs(cert_dir, exist_ok=True)
    port = free_tcp_port()
    control_plane.settings.set("grpc_port", str(port))
    control_plane.settings.set("tls_cert_dir", cert_dir)
    transport = GrpcTransport(control_plane=control_plane, config=TransportConfig.load(control_plane))
    transport.start()
    channel = grpc.insecure_channel(f"localhost:{port + 1}")
    yield control_plane, transport, pb_grpc.AgentControlStub(channel)
    channel.close()
    transport.shutdown(grace_period=3)
    control_plane.close()


def _enroll(stub, token, node_id):
    _key, csr = tls.generate_agent_csr(node_id)
    return stub.Enroll(pb.EnrollRequest(enroll_token=token, node_id=node_id, csr_pem=csr))


def _registered(control_plane, node_id, org_id):
    control_plane.nodes.upsert(node_id=node_id, hostname=node_id, status="idle", org_id=org_id)


def test_a_new_worker_past_the_cap_is_refused_and_a_returning_one_is_not(capped):
    control_plane, transport, stub = capped
    transport.servicer.org_worker_cap_resolver = lambda org_id: 2
    token = control_plane.enroll_tokens.create_token("org-a", "a")

    for name in ("w1", "w2"):
        assert _enroll(stub, token, name).accepted
        _registered(control_plane, name, "org-a")

    third = _enroll(stub, token, "w3")
    assert not third.accepted and "plan allows 2" in third.reason
    assert _enroll(stub, token, "w1").accepted           # w1 enrolling again is not a new worker

    audit = control_plane.node_enrollment_audit.list_for_node("w3")
    assert audit and not audit[0]["accepted"]


def test_archiving_a_worker_frees_its_place(capped):
    control_plane, transport, stub = capped
    transport.servicer.org_worker_cap_resolver = lambda org_id: 1
    token = control_plane.enroll_tokens.create_token("org-a", "a")
    assert _enroll(stub, token, "w1").accepted
    _registered(control_plane, "w1", "org-a")
    assert not _enroll(stub, token, "w2").accepted

    control_plane.nodes.archive("w1")
    assert _enroll(stub, token, "w2").accepted


def test_a_cap_of_zero_means_no_limit_and_another_org_is_not_counted(capped):
    control_plane, transport, stub = capped
    caps = {"org-a": 0, "org-b": 1}
    transport.servicer.org_worker_cap_resolver = lambda org_id: caps[org_id]
    token_a = control_plane.enroll_tokens.create_token("org-a", "a")
    token_b = control_plane.enroll_tokens.create_token("org-b", "b")
    for n in range(4):
        assert _enroll(stub, token_a, f"a{n}").accepted
        _registered(control_plane, f"a{n}", "org-a")
    assert _enroll(stub, token_b, "b1").accepted      # org-a's four workers do not count against org-b


def test_a_reconnecting_worker_is_no_longer_archived(capped):
    control_plane, _transport, _stub = capped
    _registered(control_plane, "w1", "org-a")
    control_plane.nodes.archive("w1")
    assert control_plane.nodes.count_active_for_org("org-a") == 0
    _registered(control_plane, "w1", "org-a")
    assert control_plane.nodes.count_active_for_org("org-a") == 1
