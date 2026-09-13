"""
NodeEnrollmentAuditRepository -- durable "who/where enrolled this
node" trail (migrations/registry.py version 7). See
persistence/repositories/node_enrollment_audit.py's module docstring
for why this exists: grpc_transport.py's Enroll() handler previously
only logged the presented token/source IP, never persisted them.
"""

import pytest

from gcon.persistence.control_plane import ControlPlane


@pytest.fixture
def cp():
    plane = ControlPlane(path=":memory:")
    yield plane
    plane.close()


def test_accepted_enrollment_is_recorded(cp):
    cp.node_enrollment_audit.record(
        node_id="worker-1", accepted=True, org_id="acme",
        enroll_token_id="tok-1", source_ip="203.0.113.5",
    )
    latest = cp.node_enrollment_audit.get_latest_for_node("worker-1")
    assert latest["node_id"] == "worker-1"
    assert latest["org_id"] == "acme"
    assert latest["enroll_token_id"] == "tok-1"
    assert latest["source_ip"] == "203.0.113.5"
    assert latest["accepted"] == 1


def test_rejected_enrollment_is_also_recorded_but_not_latest_accepted(cp):
    cp.node_enrollment_audit.record(
        node_id="worker-2", accepted=False, source_ip="198.51.100.9",
        reason="invalid or missing enroll token",
    )
    # A rejected attempt is real audit signal (e.g. someone probing
    # with a bad token) but must never surface as this node's
    # "current" enrollment -- get_latest_for_node only looks at
    # accepted=1 rows.
    assert cp.node_enrollment_audit.get_latest_for_node("worker-2") is None
    history = cp.node_enrollment_audit.list_for_node("worker-2")
    assert len(history) == 1
    assert history[0]["accepted"] == 0
    assert history[0]["reason"] == "invalid or missing enroll token"


def test_get_latest_for_node_returns_none_when_no_history(cp):
    assert cp.node_enrollment_audit.get_latest_for_node("never-enrolled") is None
    assert cp.node_enrollment_audit.list_for_node("never-enrolled") == []


def test_re_enrollment_keeps_full_history_not_just_latest(cp):
    cp.node_enrollment_audit.record(
        node_id="worker-3", accepted=True, org_id="acme",
        enroll_token_id="tok-old", source_ip="203.0.113.5",
    )
    cp.node_enrollment_audit.record(
        node_id="worker-3", accepted=True, org_id="acme",
        enroll_token_id="tok-new", source_ip="203.0.113.99",
    )
    history = cp.node_enrollment_audit.list_for_node("worker-3")
    assert len(history) == 2
    # Most recent first.
    assert history[0]["enroll_token_id"] == "tok-new"
    assert history[1]["enroll_token_id"] == "tok-old"
    latest = cp.node_enrollment_audit.get_latest_for_node("worker-3")
    assert latest["enroll_token_id"] == "tok-new"


def test_legacy_shared_token_path_has_no_token_id(cp):
    # The dev/fallback GCON_ENROLL_TOKEN path has no per-org token
    # row to point to -- enroll_token_id must stay nullable, not
    # forced to some placeholder.
    cp.node_enrollment_audit.record(
        node_id="dev-worker", accepted=True, org_id=None,
        enroll_token_id=None, source_ip="127.0.0.1",
    )
    latest = cp.node_enrollment_audit.get_latest_for_node("dev-worker")
    assert latest["enroll_token_id"] is None
    assert latest["org_id"] is None
