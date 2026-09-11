"""
End-to-end test for the real, pre-dispatch assurance gate: a
submission that violates policy.json's submission-time settings
(max_replicas / max_requires / require_org_id) must be rejected by
GCONCoordinator.submit_job() itself -- not accepted and merely
flagged after the fact -- with the job never created, queued, or
persisted at all.

Goes through the real coordinator (not a mocked PolicyEngine), since
the thing actually being verified is that submit_job() calls
policy_engine.check_submission() at the right point (before job
creation) and translates a rejection into a real, catchable
PolicyRejectionError -- not just that check_submission() itself
works correctly (already covered in isolation by
TestPolicyEngineSubmissionGate in stress_test2.py).
"""
import json

import pytest

from gcon.cluster.coordinator import GCONCoordinator, PolicyRejectionError


@pytest.fixture
def coordinator_with_policy(tmp_path, monkeypatch):
    policy_file = tmp_path / "policy.json"
    policy_file.write_text(json.dumps({
        "version": "1.0",
        "max_runtime": 30.0,
        "max_cpu_percent": 90.0,
        "max_memory_percent": 95.0,
        "require_gpu": False,
        "max_replicas": 2,
        "max_requires": {"min_vram_gb": 40},
        "require_org_id": True,
    }))
    # GCONCoordinator.__init__ has no policy_file param of its own --
    # it reads GCON_POLICY_FILE (see coordinator.py's PolicyEngine
    # construction), same mechanism a real deployment would use.
    monkeypatch.setenv("GCON_POLICY_FILE", str(policy_file))
    coordinator = GCONCoordinator()
    yield coordinator
    coordinator.shutdown()


class TestSubmissionPolicyGateEndToEnd:
    def test_over_max_replicas_submission_is_rejected_before_job_exists(self, coordinator_with_policy):
        coordinator = coordinator_with_policy
        with pytest.raises(PolicyRejectionError, match="max_replicas"):
            coordinator.submit_job(
                "job-too-many-replicas", "echo hi",
                org_id="acme", verify={"replicas": 5},
            )
        assert "job-too-many-replicas" not in coordinator.jobs

    def test_over_max_requires_submission_is_rejected_before_job_exists(self, coordinator_with_policy):
        coordinator = coordinator_with_policy
        with pytest.raises(PolicyRejectionError, match="min_vram_gb"):
            coordinator.submit_job(
                "job-too-much-vram", "echo hi", org_id="acme",
                kind="resourced", requires={"min_vram_gb": 80},
            )
        assert "job-too-much-vram" not in coordinator.jobs

    def test_missing_org_id_is_rejected_when_require_org_id_is_set(self, coordinator_with_policy):
        coordinator = coordinator_with_policy
        with pytest.raises(PolicyRejectionError, match="org_id"):
            coordinator.submit_job("job-no-org", "echo hi", org_id=None)
        assert "job-no-org" not in coordinator.jobs

    def test_compliant_submission_still_succeeds_normally(self, coordinator_with_policy):
        coordinator = coordinator_with_policy
        coordinator.submit_job(
            "job-compliant", "echo hi", org_id="acme",
            kind="resourced", requires={"min_vram_gb": 10}, verify={"replicas": 2},
        )
        assert "job-compliant" in coordinator.jobs
        assert coordinator.jobs["job-compliant"]["status"] == "pending"

    def test_default_policy_file_at_repo_root_still_places_no_submission_restrictions(self):
        """
        Regression guard: the real policy.json shipped at the repo
        root predates this feature and sets none of the new keys.
        This confirms the load-time merge (see policy_engine.py) means
        that file continues to place zero submission restrictions,
        exactly as before this session -- nothing regresses for a
        deployment still running the original file.
        """
        coordinator = GCONCoordinator()  # default policy_file="policy.json"
        try:
            coordinator.submit_job(
                "job-under-default-policy", "echo hi",
                kind="resourced", requires={"min_vram_gb": 999},
                verify={"replicas": 50}, org_id=None,
            )
            assert "job-under-default-policy" in coordinator.jobs
        finally:
            coordinator.shutdown()
