"""
Multi-tenancy isolation tests for the shared coordinator.

These exist to answer one question concretely, not by inspection:
can org A's job land on org B's node, or see org B's nodes/jobs/
receipts, through any of the read or dispatch paths a real caller
would use? Each test sets up two orgs on ONE shared coordinator
(the actual deployment shape -- see [[gcon-strategy]]) and asserts
the boundary holds.

Covers:
  - dispatch: Scheduler.select_node() never crosses org_id
  - reads: get_nodes / get_jobs / get_receipts / get_receipts_page
    all correctly exclude the other org's rows when given org_id
  - enrollment: EnrollTokenRepository resolves a token to exactly
    the org it was minted for, and nothing else

Does NOT cover the dashboard's HTTP routes (web_server.py) -- those
still take org_id as a caller-supplied query parameter rather than
deriving it from the logged-in session, which is a separate,
explicitly-flagged gap (see the accompanying chat response), not an
oversight in this test file.
"""

import time

import pytest

from gcon.cluster.coordinator import GCONCoordinator
from gcon.execution.agent import GCONAgent
from gcon.persistence.control_plane import ControlPlane


@pytest.fixture
def coordinator(tmp_path):
    cp = ControlPlane(path=str(tmp_path / "cp.db"))
    coord = GCONCoordinator(control_plane=cp)
    yield coord
    coord.shutdown()
    cp.close()


def _agent(node_id, org_id=None):
    agent = GCONAgent(node_id)
    agent.org_id = org_id
    return agent


class TestDispatchIsolation:
    def test_job_never_lands_on_a_different_orgs_node(self, coordinator):
        acme_node = _agent("acme-node-1", org_id="acme")
        globex_node = _agent("globex-node-1", org_id="globex")
        coordinator.register_agent(acme_node)
        coordinator.register_agent(globex_node)

        coordinator.submit_job("job-acme-1", "echo hi", org_id="acme")
        coordinator.assign_job("job-acme-1")

        job = coordinator.jobs["job-acme-1"]
        assert job["node_id"] == "acme-node-1", (
            "an acme job was dispatched to a non-acme node"
        )

    def test_job_with_no_org_never_lands_on_a_dedicated_org_node(self, coordinator):
        # Only a dedicated (org-owned) node is available -- a
        # no-org job must NOT be allowed to opportunistically use
        # it just because it's idle.
        coordinator.register_agent(_agent("acme-node-1", org_id="acme"))
        coordinator.submit_job("job-shared-1", "echo hi", org_id=None)
        with pytest.raises(RuntimeError):
            coordinator.assign_job("job-shared-1")

    def test_shared_node_still_serves_no_org_jobs(self, coordinator):
        # The common/default case (dev, or a shared-pool node with
        # no org attached) must keep working exactly as before.
        coordinator.register_agent(_agent("shared-node-1", org_id=None))
        coordinator.submit_job("job-shared-2", "echo hi", org_id=None)
        coordinator.assign_job("job-shared-2")
        assert coordinator.jobs["job-shared-2"]["node_id"] == "shared-node-1"

    def test_replicated_job_only_claims_matching_org_nodes(self, coordinator):
        coordinator.register_agent(_agent("acme-node-1", org_id="acme"))
        coordinator.register_agent(_agent("acme-node-2", org_id="acme"))
        coordinator.register_agent(_agent("globex-node-1", org_id="globex"))

        coordinator.submit_job(
            "job-acme-verify", "echo hi", org_id="acme", verify={"replicas": 2},
        )
        coordinator.assign_job("job-acme-verify")

        # Both replicas must be acme nodes; globex's node must be
        # untouched (still idle).
        globex_info = coordinator.registry.get_node_info("globex-node-1")
        assert globex_info["status"] == "idle"


class TestReadPathIsolation:
    def _seed_two_orgs(self, coordinator):
        coordinator.register_agent(_agent("acme-node-1", org_id="acme"))
        coordinator.register_agent(_agent("globex-node-1", org_id="globex"))
        coordinator.submit_job("job-acme-1", "echo hi", org_id="acme")
        coordinator.submit_job("job-globex-1", "echo hi", org_id="globex")

    def test_get_nodes_excludes_other_org(self, coordinator):
        self._seed_two_orgs(coordinator)
        acme_nodes = coordinator.get_nodes(org_id="acme")
        assert [n["node_id"] for n in acme_nodes] == ["acme-node-1"]

    def test_get_jobs_excludes_other_org(self, coordinator):
        self._seed_two_orgs(coordinator)
        acme_jobs = coordinator.get_jobs(org_id="acme")
        job_ids = {j["job_id"] for j in acme_jobs}
        assert "job-acme-1" in job_ids
        assert "job-globex-1" not in job_ids

    def test_get_receipts_excludes_other_orgs_receipt(self, coordinator):
        self._seed_two_orgs(coordinator)
        coordinator.assign_job("job-acme-1")
        coordinator.assign_job("job-globex-1")
        # Give the background _run_job threads a moment to post a
        # receipt (GCONAgent executes synchronously/fast for a plain
        # "echo" command, but assign_job dispatches via a thread).
        for _ in range(50):
            if "job-acme-1" in coordinator.receipts and "job-globex-1" in coordinator.receipts:
                break
            time.sleep(0.05)

        acme_receipts = coordinator.get_receipts(org_id="acme")
        receipt_job_ids = {r["job_id"] for r in acme_receipts}
        assert "job-globex-1" not in receipt_job_ids

    def test_get_receipts_page_excludes_other_org_via_db(self, coordinator):
        self._seed_two_orgs(coordinator)
        coordinator.assign_job("job-acme-1")
        coordinator.assign_job("job-globex-1")
        for _ in range(50):
            if "job-acme-1" in coordinator.receipts and "job-globex-1" in coordinator.receipts:
                break
            time.sleep(0.05)

        items, total = coordinator.get_receipts_page(org_id="acme", limit=50, offset=0)
        job_ids = {i["job_id"] for i in items}
        assert "job-globex-1" not in job_ids


class TestEnrollTokenIsolation:
    def test_token_resolves_only_to_its_own_org(self, coordinator):
        acme_token = coordinator.control_plane.enroll_tokens.create_token("acme")
        globex_token = coordinator.control_plane.enroll_tokens.create_token("globex")

        assert coordinator.control_plane.enroll_tokens.lookup_org_id(acme_token) == "acme"
        assert coordinator.control_plane.enroll_tokens.lookup_org_id(globex_token) == "globex"

    def test_revoked_token_resolves_to_nothing(self, coordinator):
        token = coordinator.control_plane.enroll_tokens.create_token("acme")
        row = coordinator.control_plane.enroll_tokens.get_by_token(token)
        coordinator.control_plane.enroll_tokens.revoke(row["token_id"])
        assert coordinator.control_plane.enroll_tokens.lookup_org_id(token) is None

    def test_unknown_token_resolves_to_nothing(self, coordinator):
        assert coordinator.control_plane.enroll_tokens.lookup_org_id("not-a-real-token") is None


class TestPerOrgResourceLimit:
    def test_org_is_capped_once_limit_is_reached(self, coordinator, monkeypatch):
        monkeypatch.setattr(coordinator, "_max_concurrent_jobs_per_org", 2)
        coordinator.register_agent(_agent("acme-node-1", org_id="acme"))

        coordinator.submit_job("job-1", "sleep 1", org_id="acme")
        coordinator.submit_job("job-2", "sleep 1", org_id="acme")
        with pytest.raises(RuntimeError):
            coordinator.submit_job("job-3", "sleep 1", org_id="acme")

    def test_other_org_is_unaffected_by_a_full_org(self, coordinator, monkeypatch):
        monkeypatch.setattr(coordinator, "_max_concurrent_jobs_per_org", 1)
        coordinator.submit_job("job-acme-1", "sleep 1", org_id="acme")
        with pytest.raises(RuntimeError):
            coordinator.submit_job("job-acme-2", "sleep 1", org_id="acme")
        # globex is a separate org -- its own counter, unaffected
        coordinator.submit_job("job-globex-1", "sleep 1", org_id="globex")

    def test_no_org_jobs_are_never_capped(self, coordinator, monkeypatch):
        monkeypatch.setattr(coordinator, "_max_concurrent_jobs_per_org", 1)
        coordinator.submit_job("job-shared-1", "echo hi", org_id=None)
        coordinator.submit_job("job-shared-2", "echo hi", org_id=None)
        coordinator.submit_job("job-shared-3", "echo hi", org_id=None)

    def test_limit_is_off_by_default(self, coordinator):
        assert coordinator._max_concurrent_jobs_per_org == 0
        for i in range(5):
            coordinator.submit_job(f"job-{i}", "echo hi", org_id="acme")
