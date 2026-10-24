"""
Hosted-fleet enrollment and the clean-up round trip, over real gRPC.

* A fleet token enrolls a worker with NO organization and records it in the fleet; the marker a fleet token is stored
  under must never reach a certificate.
* The name of a hosted worker cannot be taken over with any other kind of token.
* A clean-up request goes down the worker's stream, the real worker answers, and the answer reaches the coordinator
  tagged with who it is from. A worker that cannot verify it is clean says so.
"""
import os
import threading

import pytest
from cryptography import x509

from gcon.execution.agent import GCONAgent
from gcon.cluster.registry import FLEET_MIN_WORKER_VERSION, version_below
from gcon.persistence.repositories.enroll_tokens import FLEET_TOKEN_MARKER
from gcon.transport import tls
from gcon.transport.agent_daemon import AGENT_VERSION, AgentDaemon
from tests.transport.conftest import wait_until
from tests.transport.test_enroll_worker_cap import _enroll, _registered, capped  # noqa: F401  (fixture)

SHARED = "shared-dev-token-123"            # what test_enroll_worker_cap's fixture sets as GCON_ENROLL_TOKEN


def _subject_text(cert_pem):
    cert = x509.load_pem_x509_certificate(cert_pem.encode() if isinstance(cert_pem, str) else cert_pem)
    return cert.subject.rfc4514_string()


class TestFleetEnroll:
    def test_a_fleet_token_makes_an_organizationless_hosted_worker(self, capped):  # noqa: F811
        cp, _transport, stub = capped
        token = cp.enroll_tokens.create_fleet_token("fleet-a")
        response = _enroll(stub, token, "hosted-1")
        assert response.accepted, response.reason
        assert cp.fleet_nodes.is_member("hosted-1")
        subject = _subject_text(response.cert_pem)
        assert FLEET_TOKEN_MARKER not in subject and "OU=" not in subject      # no organization, ever
        audit = cp.node_enrollment_audit.list_for_node("hosted-1")[0]
        assert audit["accepted"] and audit["org_id"] is None

    def test_a_fleet_token_is_never_an_organizations_token(self, capped):  # noqa: F811
        cp, _transport, _stub = capped
        token = cp.enroll_tokens.create_fleet_token()
        assert cp.enroll_tokens.lookup_org_id(token) is None

    def test_a_fleet_token_has_a_use_count_like_any_other(self, capped):  # noqa: F811
        cp, _transport, stub = capped
        token = cp.enroll_tokens.create_fleet_token(max_uses=1)
        assert _enroll(stub, token, "h1").accepted
        second = _enroll(stub, token, "h2")
        assert not second.accepted and "allowed number of times" in second.reason
        assert not cp.fleet_nodes.is_member("h2")

    def test_nobody_else_can_enroll_under_a_hosted_workers_name(self, capped):  # noqa: F811
        cp, _transport, stub = capped
        assert _enroll(stub, cp.enroll_tokens.create_fleet_token(), "hosted-1").accepted
        for token in (cp.enroll_tokens.create_token("org-a", "a"), SHARED):
            refused = _enroll(stub, token, "hosted-1")
            assert not refused.accepted and "hosted fleet" in refused.reason

    def test_a_customers_worker_cannot_be_turned_into_a_hosted_one_by_name(self, capped):  # noqa: F811
        cp, _transport, stub = capped
        assert _enroll(stub, cp.enroll_tokens.create_token("org-a", "a"), "mine").accepted
        _registered(cp, "mine", "org-a")
        refused = _enroll(stub, cp.enroll_tokens.create_fleet_token(), "mine")
        assert not refused.accepted and not cp.fleet_nodes.is_member("mine")

    def test_a_hosted_worker_does_not_count_against_any_customers_worker_cap(self, capped):  # noqa: F811
        cp, transport, stub = capped
        transport.servicer.org_worker_cap_resolver = lambda org_id: 1
        for name in ("h1", "h2", "h3"):
            assert _enroll(stub, cp.enroll_tokens.create_fleet_token(), name).accepted
            _registered(cp, name, None)
        assert _enroll(stub, cp.enroll_tokens.create_token("org-a", "a"), "mine").accepted


def test_the_version_a_worker_reports_is_new_enough_to_be_hosted():
    assert not version_below(AGENT_VERSION, FLEET_MIN_WORKER_VERSION)
    assert version_below("1.0.0", FLEET_MIN_WORKER_VERSION)


# ---------------------------------------------------------------- the clean-up round trip
def _start(node_id, address, cert_dir, tmp_path):
    keys = tmp_path / f"keys-{node_id}"
    keys.mkdir(exist_ok=True)
    old = os.getcwd()
    os.chdir(str(keys))
    try:
        tls.issue_agent_cert(cert_dir, node_id)
        daemon = AgentDaemon(node_id=node_id, coordinator_address=address, cert_dir=cert_dir,
                             agent=GCONAgent(node_id=node_id), capabilities={})
        daemon.start()
        return daemon
    finally:
        os.chdir(old)


class TestSanitizeRoundTrip:
    def test_the_worker_answers_and_the_answer_says_who_it_is_from(self, running_transport, tmp_path, monkeypatch):
        transport, address = running_transport
        answers, got = [], threading.Event()
        transport.servicer.on_sanitize_result = lambda node, ref, ok, detail: (answers.append((node, ref, ok, detail)), got.set())
        monkeypatch.setattr("gcon.execution.sanitize.sanitize_host", lambda **kw: (True, "verified"))
        daemon = _start("clean-1", address, transport.config.tls_cert_dir, tmp_path)
        try:
            assert wait_until(lambda: "clean-1" in transport.list_node_ids())
            assert transport.sanitize_node("clean-1", "lease_abc")
            assert got.wait(10)
            assert answers == [("clean-1", "lease_abc", True, "verified")]
        finally:
            daemon.stop()

    def test_a_worker_that_cannot_verify_it_is_clean_says_so(self, running_transport, tmp_path, monkeypatch):
        transport, address = running_transport
        answers, got = [], threading.Event()
        transport.servicer.on_sanitize_result = lambda node, ref, ok, detail: (answers.append((ok, detail)), got.set())
        # A test machine has no Docker: the worker must report that, never "clean".
        monkeypatch.setattr("gcon.execution.sanitize.list_job_containers",
                            lambda: (_ for _ in ()).throw(RuntimeError("could not ask Docker")))
        daemon = _start("clean-2", address, transport.config.tls_cert_dir, tmp_path)
        try:
            assert wait_until(lambda: "clean-2" in transport.list_node_ids())
            assert transport.sanitize_node("clean-2", "ref")
            assert got.wait(10)
            assert answers[0][0] is False and "Docker" in answers[0][1]
        finally:
            daemon.stop()

    def test_asking_a_worker_that_is_not_connected_returns_false(self, running_transport):
        transport, _address = running_transport
        assert transport.sanitize_node("nobody", "ref") is False
