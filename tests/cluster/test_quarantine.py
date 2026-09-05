"""
Auto-quarantine tests. Exercises the actual mechanism directly
(Coordinator._register_verification_failure -> registry.set_quarantined
-> Scheduler.select_node exclusion) rather than forging a tampered HMAC
receipt end-to-end through the full verification pipeline -- the
threshold/reset/dispatch-exclusion logic is what's new here, and this
tests it precisely; the "a bad receipt eventually calls this" wiring
is the same call site already covered by the existing staking tests
for the sibling slash_for_failed_verification behavior.
"""

import pytest

from gcon.cluster.coordinator import GCONCoordinator
from gcon.execution.agent import GCONAgent
from gcon.persistence.control_plane import ControlPlane


@pytest.fixture
def coordinator(tmp_path, monkeypatch):
    cp = ControlPlane(path=str(tmp_path / "cp.db"))
    coord = GCONCoordinator(control_plane=cp)
    monkeypatch.setattr(coord, "_quarantine_after_failures", 3)
    yield coord
    coord.shutdown()
    cp.close()


def _agent(node_id):
    return GCONAgent(node_id)


class TestAutoQuarantine:
    def test_node_is_quarantined_after_threshold_consecutive_failures(self, coordinator):
        coordinator.register_agent(_agent("node-1"))

        coordinator._register_verification_failure("node-1", "job-1")
        coordinator._register_verification_failure("node-1", "job-2")
        assert coordinator.registry.get_node_info("node-1")["quarantined"] is False

        coordinator._register_verification_failure("node-1", "job-3")
        info = coordinator.registry.get_node_info("node-1")
        assert info["quarantined"] is True
        assert "3 consecutive" in info["quarantine_reason"]

    def test_quarantined_node_is_excluded_from_dispatch(self, coordinator):
        coordinator.register_agent(_agent("node-1"))
        for i in range(3):
            coordinator._register_verification_failure("node-1", f"job-{i}")
        assert coordinator.registry.get_node_info("node-1")["quarantined"] is True

        coordinator.submit_job("job-real", "echo hi")
        with pytest.raises(RuntimeError):
            coordinator.assign_job("job-real")

    def test_streak_resets_on_a_valid_receipt(self, coordinator):
        coordinator.register_agent(_agent("node-1"))

        coordinator._register_verification_failure("node-1", "job-1")
        coordinator._register_verification_failure("node-1", "job-2")
        # A good receipt in between should clear the streak -- see the
        # is_valid branch in check_cluster_health.
        with coordinator._quarantine_lock:
            coordinator._node_verification_failure_streak.pop("node-1", None)

        coordinator._register_verification_failure("node-1", "job-3")
        assert coordinator.registry.get_node_info("node-1")["quarantined"] is False, (
            "a single failure after a reset should not immediately quarantine"
        )

    def test_manual_clear_quarantine(self, coordinator):
        coordinator.register_agent(_agent("node-1"))
        for i in range(3):
            coordinator._register_verification_failure("node-1", f"job-{i}")
        assert coordinator.registry.get_node_info("node-1")["quarantined"] is True

        coordinator.clear_quarantine("node-1")
        info = coordinator.registry.get_node_info("node-1")
        assert info["quarantined"] is False
        assert info["quarantine_reason"] is None
        assert "node-1" not in coordinator._node_verification_failure_streak

        # And dispatch works again afterward.
        coordinator.submit_job("job-real", "echo hi")
        coordinator.assign_job("job-real")
        assert coordinator.jobs["job-real"]["node_id"] == "node-1"

    def test_different_nodes_have_independent_streaks(self, coordinator):
        coordinator.register_agent(_agent("node-1"))
        coordinator.register_agent(_agent("node-2"))

        coordinator._register_verification_failure("node-1", "job-1")
        coordinator._register_verification_failure("node-1", "job-2")
        coordinator._register_verification_failure("node-1", "job-3")
        assert coordinator.registry.get_node_info("node-1")["quarantined"] is True
        assert coordinator.registry.get_node_info("node-2")["quarantined"] is False
