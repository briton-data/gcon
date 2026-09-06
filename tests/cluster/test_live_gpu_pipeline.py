"""
Tests that live GPU data actually flows: agent.detect_gpu() ->
ResourceMonitor.collect() -> Coordinator.receive_resource_report() ->
registry.update_node_resources() -> Coordinator.get_nodes(). Before
this, the pipeline had no GPU fields anywhere -- only cpu/memory
existed structurally, regardless of what the agent measured.
"""

import pytest

from gcon.cluster.coordinator import GCONCoordinator
from gcon.execution.agent import GCONAgent


@pytest.fixture
def coordinator():
    coord = GCONCoordinator()
    yield coord
    coord.shutdown()


class TestLiveGpuPipeline:
    def test_resource_monitor_collect_includes_gpu_fields(self):
        agent = GCONAgent("node-1")
        snapshot = agent.monitor.collect()
        assert "gpu_name" in snapshot
        assert "gpu_memory_total" in snapshot
        assert "gpu_memory_used" in snapshot
        assert "gpu_utilization_percent" in snapshot

    def test_registry_stores_gpu_fields_after_a_resource_report(self, coordinator):
        agent = GCONAgent("node-1")
        coordinator.register_agent(agent)

        coordinator.receive_resource_report({
            "node_id": "node-1", "cpu": 10.0, "memory": 20.0,
            "running_jobs": 0, "status": "idle",
            "timestamp": "2026-01-01T00:00:00",
            "gpu_name": "Fake GPU", "gpu_memory_total": 16000,
            "gpu_memory_used": 4000, "gpu_utilization_percent": 55.0,
        })

        info = coordinator.registry.get_node_info("node-1")
        assert info["gpu_name"] == "Fake GPU"
        assert info["gpu_memory_used"] == 4000
        assert info["gpu_utilization_percent"] == 55.0

    def test_missing_gpu_fields_preserve_previous_reading_not_overwrite_with_zero(self, coordinator):
        agent = GCONAgent("node-1")
        coordinator.register_agent(agent)

        coordinator.receive_resource_report({
            "node_id": "node-1", "cpu": 10.0, "memory": 20.0,
            "running_jobs": 0, "status": "idle",
            "timestamp": "2026-01-01T00:00:00",
            "gpu_name": "Fake GPU", "gpu_memory_used": 4000,
            "gpu_memory_total": 16000, "gpu_utilization_percent": 55.0,
        })
        # A second report with no GPU keys at all (e.g. from a
        # RemoteNodeProxy, see remote_node.py's report_resources)
        # must not wipe the last real reading with fabricated zeros.
        coordinator.receive_resource_report({
            "node_id": "node-1", "cpu": 11.0, "memory": 21.0,
            "running_jobs": 0, "status": "idle",
            "timestamp": "2026-01-01T00:00:01",
        })

        info = coordinator.registry.get_node_info("node-1")
        assert info["gpu_name"] == "Fake GPU"
        assert info["gpu_memory_used"] == 4000
        assert info["cpu"] == 11.0  # non-GPU fields still update normally

    def test_get_nodes_exposes_gpu_and_quarantine_fields(self, coordinator):
        agent = GCONAgent("node-1")
        coordinator.register_agent(agent)
        coordinator.receive_resource_report({
            "node_id": "node-1", "cpu": 10.0, "memory": 20.0,
            "running_jobs": 0, "status": "idle",
            "timestamp": "2026-01-01T00:00:00",
            "gpu_name": "Fake GPU", "gpu_memory_total": 16000,
            "gpu_memory_used": 4000, "gpu_utilization_percent": 55.0,
        })

        [node] = coordinator.get_nodes()
        assert node["gpu_name"] == "Fake GPU"
        assert node["gpu_memory_used"] == 4000
        assert node["gpu_utilization_percent"] == 55.0
        assert node["quarantined"] is False
        assert node["quarantine_reason"] is None

    def test_new_node_has_sane_gpu_defaults_before_any_report(self, coordinator):
        agent = GCONAgent("node-1")
        coordinator.register_agent(agent)
        [node] = coordinator.get_nodes()
        assert node["gpu_name"] is None
        assert node["gpu_memory_used"] == 0
        assert node["gpu_utilization_percent"] == 0.0
