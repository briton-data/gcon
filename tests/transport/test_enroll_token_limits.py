"""An enroll token can carry an expiry and a use limit. Both are optional: a
token minted without them behaves exactly as before. No figure is chosen here;
whoever mints the token decides. Real gRPC servers, as in test_enroll_tls."""
from datetime import UTC, datetime, timedelta

import pytest

from tests.transport.test_enroll_tls import _ca, _enroll, _start


def _enroll_mismatched_csr(address, cert_dir, token):
    """A CSR for one node name sent as another: refused AFTER the token checks."""
    import grpc
    from gcon.transport import tls
    from gcon.transport.proto import gcon_transport_pb2 as pb
    from gcon.transport.proto import gcon_transport_pb2_grpc as pb_grpc
    channel = grpc.secure_channel(address, grpc.ssl_channel_credentials(root_certificates=_ca(cert_dir)))
    try:
        _key, csr = tls.generate_agent_csr("csr-for-this-name")
        return pb_grpc.AgentControlStub(channel).Enroll(
            pb.EnrollRequest(node_id="claims-another-name", enroll_token=token, csr_pem=csr), timeout=5)
    finally:
        channel.close()


@pytest.fixture
def server(tmp_path, monkeypatch):
    control_plane, transport, cert_dir, address = _start(tmp_path, monkeypatch)
    yield control_plane, cert_dir, address
    transport.shutdown(grace_period=3)
    control_plane.close()


def _enroll_tls(address, cert_dir, token, node_id):
    return _enroll(address, token, node_id, root_cert=_ca(cert_dir))


def test_a_token_with_no_limits_keeps_working(server):
    cp, cert_dir, address = server
    token = cp.enroll_tokens.create_token("acme", "open")
    assert _enroll_tls(address, cert_dir, token, "n1").accepted
    assert _enroll_tls(address, cert_dir, token, "n2").accepted


def test_a_token_stops_after_its_use_limit(server):
    cp, cert_dir, address = server
    token = cp.enroll_tokens.create_token("acme", "two", max_uses=2)
    assert _enroll_tls(address, cert_dir, token, "n1").accepted
    assert _enroll_tls(address, cert_dir, token, "n2").accepted
    third = _enroll_tls(address, cert_dir, token, "n3")
    assert not third.accepted and "already been used" in third.reason
    row = cp.enroll_tokens.get_by_token(token)
    assert row["uses"] == 2


def test_an_expired_token_is_refused(server):
    cp, cert_dir, address = server
    past = (datetime.now(UTC) - timedelta(minutes=1)).isoformat()
    token = cp.enroll_tokens.create_token("acme", "old", expires_at=past)
    reply = _enroll_tls(address, cert_dir, token, "n1")
    assert not reply.accepted and "expired" in reply.reason


def test_an_unexpired_token_enrolls(server):
    cp, cert_dir, address = server
    future = (datetime.now(UTC) + timedelta(hours=1)).isoformat()
    token = cp.enroll_tokens.create_token("acme", "fresh", expires_at=future)
    assert _enroll_tls(address, cert_dir, token, "n1").accepted


def test_a_rejected_enrollment_does_not_spend_a_use(server):
    cp, cert_dir, address = server
    token = cp.enroll_tokens.create_token("acme", "one", max_uses=1)
    assert not _enroll_mismatched_csr(address, cert_dir, token).accepted
    assert cp.enroll_tokens.get_by_token(token)["uses"] == 0
    assert _enroll_tls(address, cert_dir, token, "n1").accepted


def test_a_refused_token_is_audited(server):
    cp, cert_dir, address = server
    token = cp.enroll_tokens.create_token("acme", "one", max_uses=1)
    _enroll_tls(address, cert_dir, token, "n1")
    _enroll_tls(address, cert_dir, token, "n2")
    reasons = [r.get("reason") for r in cp.node_enrollment_audit.list_for_node("n2")]
    assert any("already been used" in (x or "") for x in reasons)


@pytest.mark.parametrize("bad", [0, -1, True, 1.5])
def test_nonsense_use_limits_are_rejected_when_minting(server, bad):
    cp, _, _ = server
    with pytest.raises(ValueError):
        cp.enroll_tokens.create_token("acme", max_uses=bad)


def test_a_malformed_expiry_is_rejected_when_minting(server):
    cp, _, _ = server
    with pytest.raises(ValueError):
        cp.enroll_tokens.create_token("acme", expires_at="tomorrow-ish")
