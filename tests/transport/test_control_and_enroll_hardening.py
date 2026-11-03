"""
Control-stream spoofing and enroll-port flooding, driven over a real
gRPC server with real mTLS channels.

  S1  An authenticated node cannot send heartbeats / job results /
      shutdown notices on behalf of another node over its own stream.
  S2  A session token alone is not enough to open a Control stream: the
      TLS-verified certificate must belong to the node the session is for.
  E1  The no-certificate enroll port throttles a flood from one source and
      stops writing an audit row for every refused attempt.
"""
import queue
import time

import grpc
import pytest

from gcon.transport import tls
from gcon.transport.proto import gcon_transport_pb2 as pb
from gcon.transport.proto import gcon_transport_pb2_grpc as pb_grpc
from tests.transport.conftest import wait_until
from tests.transport.test_enroll_rpc import enroll_setup, _csr  # noqa: F401  (fixture)


def _mtls_stub(cert_dir, node_id, address):
    tls.issue_agent_cert(cert_dir, node_id)
    channel = grpc.secure_channel(
        address, tls.load_agent_channel_credentials(cert_dir, node_id),
        options=[("grpc.ssl_target_name_override", "localhost")],
    )
    return channel, pb_grpc.AgentControlStub(channel)


def _register(stub, node_id):
    resp = stub.Register(pb.RegisterRequest(node_id=node_id, hostname=node_id), timeout=10)
    assert resp.accepted
    return resp.session_token


class _Outbox:
    """Request iterator the test can push envelopes into."""
    def __init__(self):
        self.q = queue.Queue()

    def __iter__(self):
        while True:
            item = self.q.get()
            if item is None:
                return
            yield item

    def put(self, item):
        self.q.put(item)

    def close(self):
        self.q.put(None)


def _heartbeat(node_id, token, seq=1, status="busy"):
    return pb.AgentEnvelope(
        node_id=node_id, session_token=token,
        heartbeat=pb.Heartbeat(sequence=seq, status=status),
    )


@pytest.fixture
def two_nodes(running_transport):
    transport, address = running_transport
    cert_dir = transport.config.tls_cert_dir
    ch_a, stub_a = _mtls_stub(cert_dir, "node-a", address)
    ch_b, stub_b = _mtls_stub(cert_dir, "node-b", address)
    tok_a = _register(stub_a, "node-a")
    tok_b = _register(stub_b, "node-b")
    yield transport, (stub_a, tok_a), (stub_b, tok_b)
    ch_a.close()
    ch_b.close()


class TestControlStreamCannotSpeakForAnotherNode:
    def test_heartbeat_for_another_node_is_not_applied(self, two_nodes):
        transport, (stub_a, tok_a), (stub_b, tok_b) = two_nodes
        cp = transport.control_plane
        out = _Outbox()
        responses = stub_a.Control(iter(out))
        out.put(_heartbeat("node-a", tok_a, seq=1, status="idle"))   # opens the stream legitimately
        assert wait_until(lambda: cp.heartbeats.latest_for_node("node-a") is not None)

        # Node A now claims to be node B on its own authenticated stream.
        out.put(_heartbeat("node-b", tok_b, seq=1, status="offline-spoofed"))
        out.put(_heartbeat("node-b", "anything", seq=2, status="offline-spoofed"))
        time.sleep(0.8)

        assert cp.heartbeats.latest_for_node("node-b") is None
        node_b = cp.nodes.get("node-b")
        assert node_b["status"] != "offline-spoofed"
        out.close()
        try:
            list(responses)
        except grpc.RpcError:
            pass

    def test_shutdown_notice_for_another_node_is_not_recorded_against_it(self, two_nodes):
        transport, (stub_a, tok_a), (stub_b, tok_b) = two_nodes
        cp = transport.control_plane
        out = _Outbox()
        responses = stub_a.Control(iter(out))
        out.put(_heartbeat("node-a", tok_a))
        assert wait_until(lambda: cp.heartbeats.latest_for_node("node-a") is not None)
        out.put(pb.AgentEnvelope(node_id="node-b", session_token=tok_a,
                                 shutdown_notice=pb.ShutdownNotice(reason="spoofed")))
        time.sleep(0.8)
        events = [e for e in cp.cluster_events.for_node("node-b", 200)
                  if "SHUTDOWN" in str(e)]
        assert events == []
        out.close()
        try:
            list(responses)
        except grpc.RpcError:
            pass

    def test_job_result_cannot_complete_another_nodes_attempt(self, two_nodes):
        transport, (stub_a, tok_a), (stub_b, tok_b) = two_nodes
        cp = transport.control_plane
        cp.jobs.ensure_exists("job-b", "echo hi")
        attempt = cp.job_attempts.record_attempt("job-b", "node-b", "req-b-1")
        out = _Outbox()
        responses = stub_a.Control(iter(out))
        out.put(_heartbeat("node-a", tok_a))
        assert wait_until(lambda: cp.heartbeats.latest_for_node("node-a") is not None)
        out.put(pb.AgentEnvelope(
            node_id="node-a", session_token=tok_a,
            job_result=pb.JobResult(job_id="job-b", request_message_id="req-b-1",
                                    status="success", return_code=0),
        ))
        time.sleep(0.8)
        after = cp.job_attempts.get_by_request_id("req-b-1")
        assert after["status"] == attempt["status"]
        assert not after.get("completed_at")
        out.close()
        try:
            list(responses)
        except grpc.RpcError:
            pass


class TestSessionTokenIsBoundToTheCertificate:
    def test_stolen_token_cannot_open_a_stream_with_another_nodes_cert(self, two_nodes):
        transport, (stub_a, tok_a), (stub_b, tok_b) = two_nodes
        cp = transport.control_plane
        out = _Outbox()
        # node B's certificate, node A's (leaked) session token.
        responses = stub_b.Control(iter(out))
        out.put(_heartbeat("node-a", tok_a, seq=1, status="hijacked"))
        time.sleep(0.8)
        assert cp.heartbeats.latest_for_node("node-a") is None
        out.close()
        try:
            list(responses)
        except grpc.RpcError:
            pass


class TestEnrollPortFlood:
    def test_flood_from_one_source_is_throttled_and_not_audited_row_per_row(self, enroll_setup, monkeypatch):
        control_plane, stub = enroll_setup
        count_rows = lambda: control_plane.db.query(
            "SELECT COUNT(*) AS c FROM node_enrollment_audit")[0]["c"]
        refused_before = count_rows()
        throttled = 0
        total = 400
        for i in range(total):
            try:
                resp = stub.Enroll(
                    pb.EnrollRequest(node_id=f"flood-{i}", enroll_token="wrong", csr_pem=b"x"),
                    timeout=10,
                )
                if "too many" in (resp.reason or "").lower():
                    throttled += 1
            except grpc.RpcError as e:
                if e.code() == grpc.StatusCode.RESOURCE_EXHAUSTED:
                    throttled += 1
        assert throttled > 0, "a 400-request flood from one address was never throttled"
        assert count_rows() - refused_before < total

    def test_a_legitimate_enrollment_still_works_after_a_few_bad_attempts(self, enroll_setup):
        control_plane, stub = enroll_setup
        for i in range(3):
            stub.Enroll(pb.EnrollRequest(node_id=f"typo-{i}", enroll_token="wrong", csr_pem=b"x"), timeout=10)
        resp = stub.Enroll(
            pb.EnrollRequest(node_id="good-worker", enroll_token="shared-dev-token-123",
                             csr_pem=_csr("good-worker")),
            timeout=10,
        )
        assert resp.accepted
