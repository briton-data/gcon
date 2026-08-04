"""
Covers "fix on_node_disconnected": it used to only log. It must
immediately mark the node offline (removing it from
registry.available_nodes(), so the scheduler can't dispatch to it)
and recover any job it was running -- mirroring the heartbeat-timeout
path in check_cluster_health/registry.check_node_health, but without
waiting out the full heartbeat window.
"""

from gcon.cluster.coordinator import GCONCoordinator
from gcon.execution.agent import GCONAgent


def _make_idle_node(coordinator, node_id="node-1"):
    node = GCONAgent(node_id)
    coordinator.register_agent(node)
    return node


def test_on_node_disconnected_marks_node_offline_immediately():
    coordinator = GCONCoordinator()
    _make_idle_node(coordinator)

    assert coordinator.registry.get_node_info("node-1")["status"] != "offline"
    coordinator.on_node_disconnected("node-1")
    assert coordinator.registry.get_node_info("node-1")["status"] == "offline"

    coordinator.shutdown()


def test_on_node_disconnected_removes_node_from_available_pool():
    coordinator = GCONCoordinator()
    _make_idle_node(coordinator)

    assert "node-1" in [n.node_id for n in coordinator.registry.available_nodes()]
    coordinator.on_node_disconnected("node-1")
    assert "node-1" not in [n.node_id for n in coordinator.registry.available_nodes()]

    coordinator.shutdown()


def test_on_node_disconnected_recovers_the_nodes_running_job():
    coordinator = GCONCoordinator()
    _make_idle_node(coordinator)

    coordinator.submit_job("job-1", "echo hi")
    coordinator.assign_job("job-1")
    assert coordinator.jobs["job-1"]["status"] == "running"
    assert coordinator.jobs["job-1"]["node_id"] == "node-1"

    coordinator.on_node_disconnected("node-1")

    # recover_jobs() must have run: the job is no longer stuck
    # "running" against a node that is already known to be gone.
    assert coordinator.jobs["job-1"]["status"] != "running"

    coordinator.shutdown()


def test_on_node_disconnected_is_a_noop_for_an_unknown_node():
    coordinator = GCONCoordinator()
    # Must not raise for a node this coordinator's scheduler never
    # registered (e.g. only known at the transport layer).
    coordinator.on_node_disconnected("never-registered")
    coordinator.shutdown()


def test_on_node_disconnected_is_idempotent():
    coordinator = GCONCoordinator()
    _make_idle_node(coordinator)

    coordinator.on_node_disconnected("node-1")
    coordinator.on_node_disconnected("node-1")  # must not raise / double-recover
    assert coordinator.registry.get_node_info("node-1")["status"] == "offline"

    coordinator.shutdown()


def test_on_node_disconnected_does_not_wait_for_heartbeat_timeout():
    """
    The whole point: a node whose disconnect is already known must be
    excluded from scheduling right away, not just after
    check_cluster_health's next heartbeat-timeout sweep.
    """
    coordinator = GCONCoordinator()
    _make_idle_node(coordinator)
    coordinator.on_node_disconnected("node-1")

    # No time has passed and check_cluster_health() has not run, yet
    # the node must already be unschedulable.
    available_ids = [n.node_id for n in coordinator.registry.available_nodes()]
    assert "node-1" not in available_ids

    coordinator.shutdown()