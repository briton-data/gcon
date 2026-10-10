"""Enrollment is verified TLS by default.

Over a plaintext enroll port the token is readable on the network, and whoever
answers can hand the worker a CA of their own. The worker now enrolls over TLS
against the CA certificate it was given with its join command, and refuses to
enroll without one. Real gRPC servers on real ports, no mocked channels.
"""
import logging
import os
import shutil

import grpc
import pytest

from gcon.persistence.control_plane import ControlPlane
from gcon.transport import tls
from gcon.transport.agent_daemon import AgentDaemon, _looks_like_tls_trust_failure
from gcon.transport.config import TransportConfig
from gcon.transport.grpc_transport import GrpcTransport
from gcon.transport.proto import gcon_transport_pb2 as pb
from gcon.transport.proto import gcon_transport_pb2_grpc as pb_grpc

from tests.transport.conftest import free_tcp_port


def _start(tmp_path, monkeypatch, control_plane=None):
    monkeypatch.setenv("GCON_ENROLL_TOKEN", "legacy-token")
    monkeypatch.delenv("GCON_ENROLL_INSECURE", raising=False)
    control_plane = control_plane or ControlPlane(path=str(tmp_path / "cp.db"))
    cert_dir = str(tmp_path / "coord-certs")
    os.makedirs(cert_dir, exist_ok=True)
    port = free_tcp_port()
    control_plane.settings.set("grpc_port", str(port))
    control_plane.settings.set("tls_cert_dir", cert_dir)
    transport = GrpcTransport(control_plane=control_plane, config=TransportConfig.load(control_plane))
    transport.start()
    return control_plane, transport, cert_dir, f"localhost:{port + 1}"


@pytest.fixture
def server(tmp_path, monkeypatch):
    control_plane, transport, cert_dir, enroll_address = _start(tmp_path, monkeypatch)
    token = control_plane.enroll_tokens.create_token("acme", "test")
    yield control_plane, cert_dir, enroll_address, token
    transport.shutdown(grace_period=3)
    control_plane.close()


def _enroll(address, token, node_id, root_cert=None):
    creds = grpc.ssl_channel_credentials(root_certificates=root_cert) if root_cert else None
    channel = grpc.secure_channel(address, creds) if creds else grpc.insecure_channel(address)
    try:
        _key, csr = tls.generate_agent_csr(node_id)
        return pb_grpc.AgentControlStub(channel).Enroll(
            pb.EnrollRequest(node_id=node_id, enroll_token=token, csr_pem=csr), timeout=5)
    finally:
        channel.close()


def _ca(cert_dir):
    with open(os.path.join(cert_dir, tls.CA_CERT_FILE), "rb") as f:
        return f.read()


def test_enrollment_over_tls_verified_against_the_coordinators_ca(server):
    _cp, cert_dir, address, token = server
    response = _enroll(address, token, "w1", root_cert=_ca(cert_dir))
    assert response.accepted and response.cert_pem


def test_plaintext_client_cannot_use_the_default_enroll_port(server):
    _cp, _dir, address, token = server
    with pytest.raises(grpc.RpcError):
        _enroll(address, token, "w1")


def test_a_client_trusting_a_different_ca_refuses_to_enroll(server, tmp_path):
    _cp, _dir, address, token = server
    attacker_dir = str(tmp_path / "attacker")
    os.makedirs(attacker_dir)
    tls.ensure_ca(attacker_dir)
    with pytest.raises(grpc.RpcError):
        _enroll(address, token, "w1", root_cert=_ca(attacker_dir))


def test_worker_enrolls_with_only_the_ca_and_a_token(server, tmp_path):
    _cp, cert_dir, address, token = server
    worker_dir = str(tmp_path / "worker")
    os.makedirs(worker_dir)
    shutil.copy(os.path.join(cert_dir, tls.CA_CERT_FILE), worker_dir)
    daemon = AgentDaemon("w1", "localhost:1", worker_dir, enroll_token=token, enroll_address=address)
    daemon._ensure_enrolled()
    assert os.path.exists(os.path.join(worker_dir, "agent-w1.cert.pem"))
    assert os.path.exists(os.path.join(worker_dir, "agent-w1.key.pem"))
    daemon._ensure_enrolled()  # second call is a no-op


def test_worker_without_a_ca_refuses_instead_of_trusting_whoever_answers(server, tmp_path):
    _cp, _dir, address, token = server
    worker_dir = str(tmp_path / "worker")
    os.makedirs(worker_dir)
    daemon = AgentDaemon("w1", "localhost:1", worker_dir, enroll_token=token, enroll_address=address)
    with pytest.raises(RuntimeError, match="CA certificate"):
        daemon._ensure_enrolled()
    assert not os.path.exists(os.path.join(worker_dir, "agent-w1.cert.pem"))


def test_worker_holding_the_wrong_ca_does_not_enroll(server, tmp_path):
    _cp, _dir, address, token = server
    wrong = str(tmp_path / "wrong-ca")
    os.makedirs(wrong)
    tls.ensure_ca(wrong)
    worker_dir = str(tmp_path / "worker")
    os.makedirs(worker_dir)
    shutil.copy(os.path.join(wrong, tls.CA_CERT_FILE), worker_dir)
    daemon = AgentDaemon("w1", "localhost:1", worker_dir, enroll_token=token, enroll_address=address)
    with pytest.raises(grpc.RpcError):
        daemon._ensure_enrolled()
    assert not os.path.exists(os.path.join(worker_dir, "agent-w1.cert.pem"))


def test_a_regenerated_ca_with_enrolled_workers_is_reported(tmp_path, monkeypatch, caplog):
    control_plane = ControlPlane(path=str(tmp_path / "cp.db"))
    control_plane.nodes.upsert("old-worker", "host", status="offline", auth_fingerprint="abc123")
    with caplog.at_level(logging.WARNING, logger="gcon.transport.grpc_transport"):
        _cp, transport, cert_dir, _addr = _start(tmp_path, monkeypatch, control_plane)
    try:
        assert any("NEW certificate authority" in r.getMessage() for r in caplog.records)
        caplog.clear()
    finally:
        transport.shutdown(grace_period=3)
    # Restarting with the same cert dir reuses the CA: no warning this time.
    with caplog.at_level(logging.WARNING, logger="gcon.transport.grpc_transport"):
        control_plane.settings.set("tls_cert_dir", cert_dir)
        control_plane.settings.set("grpc_port", str(free_tcp_port()))
        t2 = GrpcTransport(control_plane=control_plane, config=TransportConfig.load(control_plane))
        t2.start()
    try:
        assert not any("NEW certificate authority" in r.getMessage() for r in caplog.records)
    finally:
        t2.shutdown(grace_period=3)
        control_plane.close()


def test_a_fresh_coordinator_with_no_workers_does_not_warn(tmp_path, monkeypatch, caplog):
    with caplog.at_level(logging.WARNING, logger="gcon.transport.grpc_transport"):
        control_plane, transport, _dir, _addr = _start(tmp_path, monkeypatch)
    try:
        assert not any("NEW certificate authority" in r.getMessage() for r in caplog.records)
    finally:
        transport.shutdown(grace_period=3)
        control_plane.close()


@pytest.mark.parametrize("text,expected", [
    ("failed to connect: certificate verify failed", True),
    ("SSL handshake failed", True),
    ("Connection refused", False),
    ("deadline exceeded", False),
])
def test_tls_trust_failure_detection(text, expected):
    assert _looks_like_tls_trust_failure(RuntimeError(text)) is expected


# ---- the CA arrives inside the join command; the customer never handles a file
def test_install_ca_writes_the_ca_and_returns_its_fingerprint(server, tmp_path):
    _cp, cert_dir, _addr, _token = server
    info = tls.read_ca(cert_dir)
    worker_dir = str(tmp_path / "w")
    fp = tls.install_ca(worker_dir, info["ca_cert_b64"], info["sha256_fingerprint"])
    assert fp == info["sha256_fingerprint"] == tls.cert_fingerprint(os.path.join(cert_dir, tls.CA_CERT_FILE))
    assert open(os.path.join(worker_dir, tls.CA_CERT_FILE), "rb").read() == info["ca_cert_pem"].encode()


def test_install_ca_refuses_a_ca_that_does_not_match_the_fingerprint(server, tmp_path):
    _cp, cert_dir, _addr, _token = server
    worker_dir = str(tmp_path / "w")
    with pytest.raises(ValueError, match="fingerprint"):
        tls.install_ca(worker_dir, tls.read_ca(cert_dir)["ca_cert_b64"], "ab" * 32)
    assert not os.path.exists(os.path.join(worker_dir, tls.CA_CERT_FILE))   # nothing trusted


def test_install_ca_refuses_garbage_and_accepts_a_colon_separated_fingerprint(server, tmp_path):
    _cp, cert_dir, _addr, _token = server
    with pytest.raises(ValueError):
        tls.install_ca(str(tmp_path / "w"), "not base64 !!")
    with pytest.raises(ValueError):
        tls.install_ca(str(tmp_path / "w"), "aGVsbG8=")      # valid base64, not a certificate
    info = tls.read_ca(cert_dir)
    colon = ":".join(info["sha256_fingerprint"][i:i + 2] for i in range(0, 64, 2)).upper()
    tls.install_ca(str(tmp_path / "w2"), info["ca_cert_b64"], colon)


def test_a_fingerprint_alone_verifies_the_ca_already_on_disk(server, tmp_path):
    _cp, cert_dir, _addr, _token = server
    worker_dir = str(tmp_path / "w")
    os.makedirs(worker_dir)
    shutil.copy(os.path.join(cert_dir, tls.CA_CERT_FILE), worker_dir)
    fp = tls.read_ca(cert_dir)["sha256_fingerprint"]
    assert tls.install_ca(worker_dir, "", fp) == fp
    with pytest.raises(ValueError, match="fingerprint"):
        tls.install_ca(worker_dir, "", "cd" * 32)
    with pytest.raises(ValueError, match="none at"):
        tls.install_ca(str(tmp_path / "empty"), "", fp)


def test_a_worker_enrolls_end_to_end_from_just_the_join_command_values(server, tmp_path):
    """token + base64 CA + fingerprint -- exactly what a generated command carries."""
    _cp, cert_dir, address, token = server
    info = tls.read_ca(cert_dir)
    worker_dir = str(tmp_path / "w")
    tls.install_ca(worker_dir, info["ca_cert_b64"], info["sha256_fingerprint"])
    daemon = AgentDaemon("w1", "localhost:1", worker_dir, enroll_token=token, enroll_address=address)
    daemon._ensure_enrolled()
    assert os.path.exists(os.path.join(worker_dir, "agent-w1.cert.pem"))


def test_a_closed_enroll_port_is_explained_at_startup(tmp_path, monkeypatch, caplog):
    monkeypatch.delenv("GCON_ENROLL_TOKEN", raising=False)
    control_plane = ControlPlane(path=str(tmp_path / "cp.db"))
    control_plane.settings.set("grpc_port", str(free_tcp_port()))
    control_plane.settings.set("tls_cert_dir", str(tmp_path / "certs"))
    os.makedirs(str(tmp_path / "certs"))
    with caplog.at_level(logging.WARNING, logger="gcon.transport.grpc_transport"):
        transport = GrpcTransport(control_plane=control_plane, config=TransportConfig.load(control_plane))
        transport.start()
    try:
        text = " ".join(r.getMessage() for r in caplog.records)
        assert "self-enrollment is DISABLED" in text
        assert "GCON_ENROLL_TOKEN" in text and "per-organization" in text
    finally:
        transport.shutdown(grace_period=3)
        control_plane.close()
