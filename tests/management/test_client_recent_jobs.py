"""
ManagementLayer.get_client_recent_jobs -- the stage-derivation logic
backing the client detail drawer's "current jobs" list (see
get_client_recent_jobs's own docstring in management_layer.py for
the full orchestration/execution/verification/assurance/proof
mapping).

Real GCONCoordinator + real ManagementLayer + real GCONAgent nodes +
real ExecutionVerifier throughout -- "verified" here is always a
genuine HMAC signature check, never faked. Even "assurance" and
"verification failed" are reachable through real dispatch in
principle (there's a genuine brief window between a receipt existing
and its policy_report being attached -- see
test_completed_job_reaches_proof_via_real_dispatch's own comment),
but pinning the assertions to that narrow a race would make this
file flaky. Those two states are instead exercised by constructing a
receipt directly via coordinator.verifier.create_receipt() (a real,
valid signature) and either leaving off policy_report (assurance) or
corrupting the stored signature afterward (verification failed) --
the receipt itself is never fake, only how it lands in
coordinator.receipts is shortcut, the same way test_coordinator_full.py
and others already read coordinator.receipts[...] directly rather
than only ever going through the full RPC/dispatch path.
"""
import time

import pytest

from gcon.cluster.coordinator import GCONCoordinator
from gcon.execution.agent import GCONAgent
from gcon.management.management_layer import ManagementLayer
from gcon.persistence.control_plane import ControlPlane


@pytest.fixture
def setup(tmp_path):
    control_plane = ControlPlane(path=str(tmp_path / "cp.db"))
    coordinator = GCONCoordinator(control_plane=control_plane)
    management = ManagementLayer(coordinator=coordinator, db_path=str(tmp_path / "mgmt.db"))
    org = management.create_organization("Acme Corp")
    yield coordinator, management, org["org_id"]
    coordinator.shutdown()


def _wait_for(predicate, timeout=5, interval=0.05):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


class TestClientRecentJobsStages:
    def test_pending_job_before_any_node_registered(self, setup):
        coordinator, management, org_id = setup
        coordinator.submit_job("job-pending", "echo hi", org_id=org_id)

        jobs = management.get_client_recent_jobs(org_id)
        assert len(jobs) == 1
        assert jobs[0]["stage"] == "orchestration"
        assert jobs[0]["status"] == "pending"

    def test_completed_job_reaches_proof_via_real_dispatch(self, setup):
        # policy_report is attached via in-place mutation on the JOB
        # dict for the single-node path (_run_job) -- never the
        # receipt (only the replicated path attaches it to both; see
        # get_client_recent_jobs's fallback and its own comment for
        # why). Wait for the real attachment point, not the receipt.
        coordinator, management, org_id = setup
        node = GCONAgent(node_id="acme-node-1")
        node.org_id = org_id
        coordinator.register_agent(node)
        coordinator.submit_job("job-proof", "echo hi", org_id=org_id)
        coordinator.assign_job("job-proof")

        assert _wait_for(
            lambda: coordinator.jobs.get("job-proof", {}).get("policy_report") is not None
        )
        jobs = management.get_client_recent_jobs(org_id)
        job = next(j for j in jobs if j["job_id"] == "job-proof")
        assert job["status"] == "completed"
        assert job["stage"] == "proof"

    def test_failed_job_reports_its_own_status_not_a_pipeline_stage(self, setup):
        coordinator, management, org_id = setup
        node = GCONAgent(node_id="acme-node-1")
        node.org_id = org_id
        coordinator.register_agent(node)
        coordinator.submit_job("job-failed", "exit 1", org_id=org_id)
        coordinator.assign_job("job-failed")

        assert _wait_for(
            lambda: coordinator.jobs.get("job-failed", {}).get("status") == "failed"
        )
        jobs = management.get_client_recent_jobs(org_id)
        job = next(j for j in jobs if j["job_id"] == "job-failed")
        assert job["stage"] == "failed"

    def test_completed_no_receipt_yet_is_verification(self, setup):
        # A real, brief, already-documented gap in the coordinator's
        # own code: status flips to "completed" slightly before the
        # receipt is persisted (see coordinator.py's own comments on
        # this exact ordering). Simulated directly here rather than
        # trying to win that race, since it's about this method's
        # handling of that gap, not re-proving the gap exists.
        coordinator, management, org_id = setup
        coordinator.jobs["job-gap"] = {
            "job_id": "job-gap", "status": "completed", "org_id": org_id,
            "created_at": "2026-01-01T00:00:00", "completed_at": "2026-01-01T00:00:01",
        }
        jobs = management.get_client_recent_jobs(org_id)
        job = next(j for j in jobs if j["job_id"] == "job-gap")
        assert job["stage"] == "verification"

    def test_verified_receipt_with_no_policy_report_is_assurance(self, setup):
        coordinator, management, org_id = setup
        coordinator.jobs["job-assurance"] = {
            "job_id": "job-assurance", "status": "completed", "org_id": org_id,
            "created_at": "2026-01-01T00:00:00", "completed_at": "2026-01-01T00:00:01",
        }
        # A real, validly-signed receipt (genuine HMAC signature) --
        # just never had PolicyEngine.evaluate() attach a
        # policy_report to it, which is a real, if usually brief,
        # window for a job that completed but hasn't been policy-
        # evaluated yet.
        receipt = coordinator.verifier.create_receipt(
            job_id="job-assurance", agent_id="acme-node-1",
            execution_result={"status": "completed"},
            input_hash="in", output_hash="out",
        )
        coordinator.receipts["job-assurance"] = receipt

        jobs = management.get_client_recent_jobs(org_id)
        job = next(j for j in jobs if j["job_id"] == "job-assurance")
        assert job["stage"] == "assurance"

    def test_verified_receipt_with_policy_report_is_proof(self, setup):
        coordinator, management, org_id = setup
        coordinator.jobs["job-proof-direct"] = {
            "job_id": "job-proof-direct", "status": "completed", "org_id": org_id,
            "created_at": "2026-01-01T00:00:00", "completed_at": "2026-01-01T00:00:01",
        }
        receipt = coordinator.verifier.create_receipt(
            job_id="job-proof-direct", agent_id="acme-node-1",
            execution_result={"status": "completed"},
            input_hash="in", output_hash="out",
        )
        receipt["policy_report"] = {"trusted": True, "checks": []}
        coordinator.receipts["job-proof-direct"] = receipt

        jobs = management.get_client_recent_jobs(org_id)
        job = next(j for j in jobs if j["job_id"] == "job-proof-direct")
        assert job["stage"] == "proof"

    def test_tampered_receipt_signature_is_verification_failed(self, setup):
        coordinator, management, org_id = setup
        coordinator.jobs["job-tampered"] = {
            "job_id": "job-tampered", "status": "completed", "org_id": org_id,
            "created_at": "2026-01-01T00:00:00", "completed_at": "2026-01-01T00:00:01",
        }
        # Real signature, then genuinely corrupted after the fact --
        # this is what a real tampering/corruption detection looks
        # like, not a fabricated "invalid" flag.
        receipt = coordinator.verifier.create_receipt(
            job_id="job-tampered", agent_id="acme-node-1",
            execution_result={"status": "completed"},
            input_hash="in", output_hash="out",
        )
        receipt["proof"]["signature"] = "0" * len(receipt["proof"]["signature"])
        coordinator.receipts["job-tampered"] = receipt

        jobs = management.get_client_recent_jobs(org_id)
        job = next(j for j in jobs if j["job_id"] == "job-tampered")
        assert job["stage"] == "verification failed"

    def test_other_orgs_jobs_are_excluded(self, setup):
        coordinator, management, org_id = setup
        other_org = management.create_organization("Globex Inc")
        coordinator.submit_job("job-globex", "echo hi", org_id=other_org["org_id"])
        coordinator.submit_job("job-acme", "echo hi", org_id=org_id)

        jobs = management.get_client_recent_jobs(org_id)
        assert {j["job_id"] for j in jobs} == {"job-acme"}

    def test_no_coordinator_returns_empty_list_not_a_crash(self):
        management = ManagementLayer(coordinator=None, db_path=":memory:")
        assert management.get_client_recent_jobs("some-org") == []
