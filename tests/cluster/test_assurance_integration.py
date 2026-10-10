"""
Integration coverage for the "assurance" field get_receipt_detail()
now returns (see gcon.execution.assurance -- unit-tested standalone in
tests/execution/test_assurance.py). This file confirms the real
wiring: a real coordinator, real receipt, real policy file on disk.
"""
import json
import time

import pytest

from gcon.cluster.coordinator import GCONCoordinator
from gcon.execution.agent import GCONAgent


def _wait_for(predicate, timeout=5, interval=0.05):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


def _load_independent_policy(coordinator, tmp_path):
    """
    Give a coordinator a policy whose verdict does not depend on how busy the
    machine is. The repo-root policy.json caps HOST-wide cpu/memory percent at
    90/95, and PolicyEngine fills any key a policy file omits with those
    defaults, so on a small machine under load (right after a heavy test run,
    or two replicas starting at once) an ordinary `echo hi` could be flagged as
    a violation and these tests failed intermittently. Suspected cause, not
    proven: the assertions below print the assurance details so a recurrence
    shows which check tripped.
    """
    policy_path = tmp_path / "policy.json"
    policy_path.write_text(json.dumps({
        "version": "1.0", "max_runtime": 30.0,
        "max_cpu_percent": 1_000_000.0, "max_memory_percent": 1_000_000.0,
    }))
    coordinator.policy_engine = coordinator.policy_engine.__class__(policy_file=str(policy_path))


def test_ordinary_completed_job_is_verified_and_assured(tmp_path):
    coordinator = GCONCoordinator()
    _load_independent_policy(coordinator, tmp_path)
    coordinator.register_agent(GCONAgent(node_id="node-assure-1"))

    coordinator.submit_job("job-assure-1", "echo hello")
    assert _wait_for(lambda: coordinator.jobs["job-assure-1"]["status"] == "completed")
    assert _wait_for(lambda: "job-assure-1" in coordinator.receipts)

    receipt = coordinator.receipts["job-assure-1"]
    detail = coordinator.get_receipt_detail(receipt["receipt_id"])

    assert detail["assurance"]["level"] == "verified", detail["assurance"]
    assert detail["assurance"]["assured"] is True
    assert detail["assurance"]["signals"]["crypto"]["passed"] is True
    # LocalTransport nodes have no per-node identity, so no
    # attestation block exists for this receipt -- absence, not
    # failure, so it must not have blocked "verified".
    assert detail["assurance"]["signals"]["worker_attestation"]["applicable"] is False

    coordinator.shutdown()


def test_policy_violating_job_downgrades_assurance_but_stays_crypto_valid(tmp_path):
    policy_path = tmp_path / "policy.json"
    policy_path.write_text(json.dumps({"version": "1.0", "max_runtime": 0.01}))

    coordinator = GCONCoordinator()
    coordinator.policy_engine = coordinator.policy_engine.__class__(policy_file=str(policy_path))
    coordinator.register_agent(GCONAgent(node_id="node-assure-2"))

    coordinator.submit_job("job-assure-2", "sleep 0.3 && echo done")
    assert _wait_for(lambda: coordinator.jobs["job-assure-2"]["status"] == "completed", timeout=10)
    assert _wait_for(lambda: "job-assure-2" in coordinator.receipts)

    receipt = coordinator.receipts["job-assure-2"]
    detail = coordinator.get_receipt_detail(receipt["receipt_id"])

    # The signature itself is still genuinely valid -- only the policy
    # check should have failed.
    assert detail["assurance"]["signals"]["crypto"]["passed"] is True
    assert detail["policy"]["trusted"] is False
    assert detail["assurance"]["level"] == "policy_violation"
    assert detail["assurance"]["assured"] is False
    assert any("Runtime" in r or "runtime" in r for r in detail["assurance"]["reasons"])

    coordinator.shutdown()


def test_replicated_job_assurance_is_self_consistent_with_its_execution_proof(tmp_path):
    """Two real nodes run an identical command. Whichever way real
    replication.compare_results() comes out, get_receipt_detail()'s
    "assurance" field must accurately reflect it -- that's what #7
    built. This checks the two are self-consistent in both directions;
    the outcome itself is pinned down by the two strict tests below.
    """
    coordinator = GCONCoordinator()
    _load_independent_policy(coordinator, tmp_path)
    coordinator.register_agent(GCONAgent(node_id="node-assure-3"))
    coordinator.register_agent(GCONAgent(node_id="node-assure-4"))

    coordinator.submit_job("job-assure-3", "echo hi", verify={"replicas": 2})
    assert _wait_for(lambda: coordinator.jobs["job-assure-3"]["status"] == "completed", timeout=10)
    assert _wait_for(lambda: "job-assure-3" in coordinator.receipts)

    receipt = coordinator.receipts["job-assure-3"]
    detail = coordinator.get_receipt_detail(receipt["receipt_id"])

    assert detail["execution_proof"] is not None
    replication_signal = detail["assurance"]["signals"]["replication"]
    assert replication_signal["applicable"] is True

    if detail["execution_proof"]["agreement"] is True:
        assert replication_signal["passed"] is True
        assert detail["assurance"]["level"] == "verified", detail["assurance"]
        assert detail["assurance"]["assured"] is True
    else:
        assert replication_signal["passed"] is False
        assert detail["assurance"]["level"] == "disputed"
        assert detail["assurance"]["assured"] is False

    coordinator.shutdown()


@pytest.mark.parametrize("valid", [True])
def test_assurance_precedence_matches_module_ordering(valid):
    """Sanity check that LEVELS in gcon.execution.assurance actually
    matches the order synthesize_assurance's own if/elif chain
    implements -- guards against the two silently drifting apart."""
    from gcon.execution.assurance import LEVELS
    assert LEVELS == (
        "invalid",
        "disputed",
        "attestation_mismatch",
        "policy_violation",
        "verified",
    )


def test_honest_identical_replicas_are_verified(tmp_path):
    """Deterministic output on two healthy nodes must come out verified.
    Previously ~90% of these were "disputed" on runtime jitter alone."""
    coordinator = GCONCoordinator()
    _load_independent_policy(coordinator, tmp_path)
    coordinator.register_agent(GCONAgent(node_id="node-assure-5"))
    coordinator.register_agent(GCONAgent(node_id="node-assure-6"))
    try:
        for i in range(5):
            job_id = f"job-assure-honest-{i}"
            coordinator.submit_job(job_id, "echo hi", verify={"replicas": 2})
            assert _wait_for(lambda: coordinator.jobs[job_id]["status"] == "completed", timeout=10)
            assert _wait_for(lambda: job_id in coordinator.receipts)
            detail = coordinator.get_receipt_detail(coordinator.receipts[job_id]["receipt_id"])
            assert detail["execution_proof"]["agreement"] is True, detail["execution_proof"]
            assert detail["assurance"]["level"] == "verified", detail["assurance"]
    finally:
        coordinator.shutdown()


def test_replicas_that_really_produce_different_output_are_disputed():
    """The fix must not have made comparison lenient: output that truly
    differs between replicas (each shell prints its own pid) is still a dispute."""
    coordinator = GCONCoordinator()
    coordinator.register_agent(GCONAgent(node_id="node-assure-7"))
    coordinator.register_agent(GCONAgent(node_id="node-assure-8"))
    try:
        coordinator.submit_job("job-assure-differs", "echo $$", verify={"replicas": 2})
        assert _wait_for(lambda: coordinator.jobs["job-assure-differs"]["status"] == "completed", timeout=10)
        assert _wait_for(lambda: "job-assure-differs" in coordinator.receipts)
        detail = coordinator.get_receipt_detail(coordinator.receipts["job-assure-differs"]["receipt_id"])
        assert detail["execution_proof"]["agreement"] is False
        assert [m["field"] for m in detail["execution_proof"]["mismatches"]] == ["output_hash"]
        assert detail["assurance"]["level"] == "disputed"
    finally:
        coordinator.shutdown()
