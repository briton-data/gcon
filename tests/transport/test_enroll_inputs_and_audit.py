"""
What the no-certificate enroll port accepts and keeps.

  N1  node_id has a fixed shape. It becomes a certificate's Common Name and DNS name, a file name, a log line and an
      audit row, so what a stranger can put there is limited before any of that happens.
  A1  Rejected enrollment attempts (which anyone who can reach the port can generate) are kept only up to a limit;
      accepted ones, the record of who brought a node online, are never removed by it.
  S1  source_ip is a real IP address or nothing: never a half-parsed string.
"""
import os

import grpc
import pytest

from gcon.persistence.control_plane import ControlPlane
from gcon.transport import tls
from gcon.transport.grpc_transport import _peer_address
from gcon.transport.proto import gcon_transport_pb2 as pb
from tests.transport.test_enroll_rpc import enroll_setup  # noqa: F401  (fixture)

BAD_IDS = ["../evil", "a b", "x\ny", "node;rm -rf", "a/b", "-leading", ".leading", "nöde", "(unknown)", "x" * 65, "a\x00b"]


def _enroll(stub, node_id, token="shared-dev-token-123"):
    _, csr = tls.generate_agent_csr(node_id)
    return stub.Enroll(pb.EnrollRequest(node_id=node_id, enroll_token=token, csr_pem=csr), timeout=10)


class TestNodeIdShape:
    @pytest.mark.parametrize("node_id", ["node-1", "BRITON-PC", "worker_01.example", "a", "n" * 64, "7zip"])
    def test_ordinary_ids_are_accepted(self, node_id):
        from gcon.transport.tls import validate_node_id
        assert validate_node_id(node_id) == node_id

    @pytest.mark.parametrize("node_id", BAD_IDS + [""])
    def test_everything_else_is_refused(self, node_id):
        from gcon.transport.tls import validate_node_id
        with pytest.raises(ValueError):
            validate_node_id(node_id)

    @pytest.mark.parametrize("node_id", BAD_IDS)
    def test_enroll_signs_no_certificate_for_it(self, enroll_setup, node_id):
        control_plane, stub = enroll_setup
        try:
            resp = _enroll(stub, node_id)
        except Exception:
            return                                      # a CSR that cannot even be built is also a refusal
        assert not resp.accepted and not resp.certificate_pem if hasattr(resp, "certificate_pem") else not resp.accepted
        assert "node_id" in resp.reason.lower()

    def test_the_audit_never_holds_what_the_stranger_sent(self, enroll_setup):
        control_plane, stub = enroll_setup
        _enroll(stub, "x\nFORGED LOG LINE ../..")
        rows = control_plane.db.query("SELECT node_id, accepted FROM node_enrollment_audit")
        assert rows and all(r["node_id"] in ("(invalid)",) and not r["accepted"] for r in rows)

    def test_issuing_a_certificate_cannot_write_outside_its_directory(self, tmp_path):
        cert_dir = tmp_path / "certs"
        cert_dir.mkdir()
        with pytest.raises(ValueError):
            tls.issue_agent_cert(str(cert_dir), "../escaped")
        assert not list(tmp_path.glob("agent-*")) and not list(cert_dir.glob("agent-*"))

    def test_signing_a_csr_refuses_it_too(self, tmp_path):
        os.makedirs(tmp_path / "c", exist_ok=True)
        _, csr = tls.generate_agent_csr("bad id")
        with pytest.raises(ValueError):
            tls.sign_agent_csr(str(tmp_path / "c"), csr, "bad id")

    def test_a_normal_enrollment_still_works(self, enroll_setup):
        _, stub = enroll_setup
        assert _enroll(stub, "good-worker-1").accepted


class TestAuditRetention:
    def _repo(self, tmp_path, monkeypatch, keep="10"):
        monkeypatch.setenv("GCON_ENROLL_AUDIT_REJECTED_KEEP", keep)
        cp = ControlPlane(path=str(tmp_path / "cp.db"))
        repo = cp.node_enrollment_audit
        repo._prune_every = 5
        return cp, repo

    def _count(self, cp, accepted):
        return cp.db.query("SELECT COUNT(*) AS c FROM node_enrollment_audit WHERE accepted = ?", (accepted,))[0]["c"]

    def test_rejected_attempts_stay_bounded(self, tmp_path, monkeypatch):
        cp, repo = self._repo(tmp_path, monkeypatch)
        for i in range(200):
            repo.record(f"n{i}", accepted=False, source_ip="203.0.113.1", reason="invalid token")
        assert self._count(cp, 0) <= 10 + repo._prune_every

    def test_the_newest_rejections_are_the_ones_kept(self, tmp_path, monkeypatch):
        cp, repo = self._repo(tmp_path, monkeypatch)
        for i in range(100):
            repo.record(f"n{i}", accepted=False)
        kept = {r["node_id"] for r in cp.db.query("SELECT node_id FROM node_enrollment_audit")}
        assert "n99" in kept and "n0" not in kept

    def test_accepted_enrollments_are_never_removed(self, tmp_path, monkeypatch):
        cp, repo = self._repo(tmp_path, monkeypatch)
        for i in range(3):
            repo.record(f"real-{i}", accepted=True, org_id="acme", source_ip="203.0.113.9")
        for i in range(300):
            repo.record(f"junk-{i}", accepted=False)
        assert self._count(cp, 1) == 3
        assert repo.get_latest_for_node("real-0")["org_id"] == "acme"

    def test_zero_keeps_everything(self, tmp_path, monkeypatch):
        cp, repo = self._repo(tmp_path, monkeypatch, keep="0")
        for i in range(60):
            repo.record(f"n{i}", accepted=False)
        assert self._count(cp, 0) == 60


class _Ctx:
    def __init__(self, peer):
        self._peer = peer

    def peer(self):
        return self._peer


class TestSourceIp:
    @pytest.mark.parametrize("peer,expected", [
        ("ipv4:203.0.113.5:54321", "203.0.113.5"),
        ("ipv4:203.0.113.5", "203.0.113.5"),
        ("ipv6:[2001:db8::1]:54321", "2001:db8::1"),
        ("ipv6:%5B::1%5D:10250", "::1"),
        ("ipv6:[::ffff:203.0.113.5]:7", "::ffff:203.0.113.5"),
    ])
    def test_a_real_address_is_returned_bare(self, peer, expected):
        assert _peer_address(_Ctx(peer)) == expected

    @pytest.mark.parametrize("peer", [
        "ipv4:garbage:1", "ipv4:999.1.1.1:5", "ipv4:", "ipv6:", "ipv6:[not-an-ip]:5", "ipv6:%5B::1", "unix:/tmp/x.sock",
        "dns:example.com:443", "", None, "ipv4:203.0.113.5\nFORGED:1",
    ])
    def test_anything_else_is_none(self, peer):
        assert _peer_address(_Ctx(peer)) is None

    def test_a_real_enroll_records_an_actual_ip(self, enroll_setup):
        import ipaddress
        control_plane, stub = enroll_setup
        _enroll(stub, "ip-check-worker")
        row = control_plane.db.query("SELECT source_ip FROM node_enrollment_audit")[0]
        ipaddress.ip_address(row["source_ip"])
