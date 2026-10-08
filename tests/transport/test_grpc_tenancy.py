"""
Tenancy over gRPC:

  G3  A certificate with no organization never lets the node name its own org.
  T1  A node name that belongs to one organization cannot be enrolled by another.
  G6  A node can only write logs / receipts for jobs that were dispatched to it.
"""
import grpc
import pytest

from gcon.transport.proto import gcon_transport_pb2 as pb
from gcon.transport import tls
from tests.transport.conftest import wait_until
from tests.transport.test_enroll_rpc import enroll_setup, _csr  # noqa: F401  (fixture)
from tests.transport.test_grpc_transport import _start_agent


class TestEnrollRefusesAnotherOrgsNodeName:
    def test_enrollment_is_per_organization(self, enroll_setup):
        control_plane, stub = enroll_setup
        token_a = control_plane.enroll_tokens.create_token("org-a", "a")
        token_b = control_plane.enroll_tokens.create_token("org-b", "b")
        control_plane.nodes.upsert(node_id="shared-name", hostname="h", status="idle", org_id="org-a")

        def enroll(token, node_id="shared-name"):
            return stub.Enroll(pb.EnrollRequest(enroll_token=token, node_id=node_id, csr_pem=_csr(node_id)))

        taken = enroll(token_b)
        assert not taken.accepted and "different organization" in taken.reason
        legacy = enroll("shared-dev-token-123")
        assert not legacy.accepted and "different organization" in legacy.reason
        assert enroll(token_a).accepted                         # the owner may re-enroll (renewal)
        assert enroll(token_b, "brand-new-name").accepted       # a free name is fine

        audit = control_plane.node_enrollment_audit
        rows = audit.list_for_node("shared-name") if hasattr(audit, "list_for_node") else []
        assert not rows or any(not r["accepted"] for r in rows)


class TestSelfReportedOrgIsNeverBelieved:
    def test_a_certificate_without_an_organization_cannot_claim_one(self, running_transport, tmp_path):
        transport, address = running_transport
        cert_dir = transport.config.tls_cert_dir
        daemon = _start_agent("sneaky", address, cert_dir, tmp_path, capabilities={"org_id": "victim-org"})
        try:
            assert wait_until(lambda: transport.control_plane.nodes.get("sneaky") is not None)
            assert transport.control_plane.nodes.get("sneaky")["org_id"] is None
        finally:
            daemon.stop()


class TestNodesOnlyWriteAboutTheirOwnJobs:
    @pytest.fixture
    def servicer(self, running_transport):
        transport, _ = running_transport
        svc = transport._servicer if hasattr(transport, "_servicer") else transport.servicer
        return transport, svc

    class _Ctx:
        def abort(self, code, details):
            raise PermissionError((code, details))

    def _session(self, svc, node_id):
        from gcon.transport.grpc_transport import NodeSession
        svc._sessions[node_id] = NodeSession(node_id, "tok")

    def test_logs_for_a_job_the_node_never_ran_are_refused(self, servicer):
        transport, svc = servicer
        cp = transport.control_plane
        for n in ("node-a", "node-b"):
            cp.nodes.upsert(node_id=n, hostname=n, status="idle")
            self._session(svc, n)
        cp.jobs.ensure_exists("job-1", "echo hi")
        cp.job_attempts.record_attempt("job-1", "node-a", "req-1")

        chunk = lambda node: pb.LogChunk(node_id=node, session_token="tok", job_id="job-1",
                                         stream="stdout", sequence=1, content="x")
        svc.StreamLogs(iter([chunk("node-a")]), self._Ctx())          # its own job: accepted
        with pytest.raises(PermissionError) as err:
            svc.StreamLogs(iter([chunk("node-b")]), self._Ctx())      # someone else's: refused
        assert err.value.args[0][0] == grpc.StatusCode.PERMISSION_DENIED

    def test_a_receipt_for_a_job_the_node_never_ran_is_refused(self, servicer):
        transport, svc = servicer
        cp = transport.control_plane
        for n in ("node-a", "node-b"):
            cp.nodes.upsert(node_id=n, hostname=n, status="idle")
            self._session(svc, n)
        cp.jobs.ensure_exists("job-2", "echo hi")
        cp.job_attempts.record_attempt("job-2", "node-a", "req-2")

        upload = lambda node: pb.ReceiptUpload(node_id=node, session_token="tok", job_id="job-2",
                                               receipt_hash="h-" + node, payload_json="{}")
        with pytest.raises(PermissionError):
            svc.UploadReceipt(upload("node-b"), self._Ctx())
        assert svc.UploadReceipt(upload("node-a"), self._Ctx()).accepted
