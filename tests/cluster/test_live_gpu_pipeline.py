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

    def test_remote_proxy_report_resources_never_stomps_real_heartbeat_cpu_memory(self, coordinator):
        """
        Regression test for a real bug found while wiring GPU data
        into the heartbeat pipeline: RemoteNodeProxy.report_resources()
        used to return cpu=0.0/memory=0.0 literally (required keys at
        the time), and GCONCoordinator._run_job() calls
        node.report_resources() unconditionally right after every job
        completes -- so a remote node's real, live heartbeat-reported
        cpu/memory got silently reset to zero after every single job.
        report_resources() now omits cpu/memory entirely, and
        update_node_resources() must treat their absence as "keep the
        last real reading," not implicitly coerce to zero.
        """
        from gcon.transport.remote_node import RemoteNodeProxy

        proxy = RemoteNodeProxy("remote-1", transport=None)
        coordinator.register_agent(proxy)

        # Real data arrives via the heartbeat path (see
        # run_coordinator.py's on_heartbeat -> receive_resource_report).
        coordinator.receive_resource_report({
            "node_id": "remote-1", "cpu": 42.0, "memory": 33.0,
            "running_jobs": 0, "status": "idle",
            "timestamp": "2026-01-01T00:00:00",
        })
        assert coordinator.registry.get_node_info("remote-1")["cpu"] == 42.0

        # report_resources() must not carry cpu/memory keys at all --
        # confirms the fix at its source, not just its downstream effect.
        snapshot = proxy.report_resources()
        assert "cpu" not in snapshot
        assert "memory" not in snapshot

        # Feeding that snapshot through the same path a real dispatch
        # completion would use must NOT wipe the real heartbeat value.
        coordinator.receive_resource_report(snapshot)
        assert coordinator.registry.get_node_info("remote-1")["cpu"] == 42.0
        assert coordinator.registry.get_node_info("remote-1")["memory"] == 33.0

    def test_registry_update_tolerates_missing_cpu_memory(self, coordinator):
        agent = GCONAgent("node-1")
        coordinator.register_agent(agent)
        coordinator.receive_resource_report({
            "node_id": "node-1", "cpu": 10.0, "memory": 20.0,
            "running_jobs": 0, "status": "idle",
            "timestamp": "2026-01-01T00:00:00",
        })
        # A report with no cpu/memory keys (running_jobs/status/
        # timestamp still required, same as before) must not raise
        # and must not zero out the prior reading.
        coordinator.receive_resource_report({
            "node_id": "node-1", "running_jobs": 1, "status": "busy",
            "timestamp": "2026-01-01T00:00:01",
        })
        info = coordinator.registry.get_node_info("node-1")
        assert info["cpu"] == 10.0
        assert info["memory"] == 20.0
        assert info["status"] == "busy"  # non-cpu/memory fields still update
