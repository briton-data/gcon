"""
A node's authenticated fingerprint, real hostname, endpoint and agent version
are recorded when it registers over gRPC. The coordinator's own register_agent
runs afterwards for the same node and used to overwrite all four with NULL /
the bare node id, silently switching off every check that depends on them.
"""
from gcon.cluster.coordinator import GCONCoordinator
from gcon.execution.agent import GCONAgent
from gcon.persistence.control_plane import ControlPlane


def test_register_agent_does_not_wipe_what_grpc_registration_recorded(tmp_path):
    plane = ControlPlane(path=str(tmp_path / "cp.db"))
    plane.nodes.upsert(
        node_id="n1", hostname="gpu-box-7", status="idle", transport_endpoint="10.0.0.7:5000",
        agent_version="1.4.2", auth_fingerprint="ab:cd:ef", org_id="org-a",
    )
    coord = GCONCoordinator(control_plane=plane)
    try:
        coord.register_agent(GCONAgent(node_id="n1"))
        row = plane.nodes.get("n1")
        assert row["hostname"] == "gpu-box-7"
        assert row["transport_endpoint"] == "10.0.0.7:5000"
        assert row["agent_version"] == "1.4.2"
        assert row["auth_fingerprint"] == "ab:cd:ef"
        assert row["org_id"] == "org-a"
    finally:
        coord.shutdown()
        plane.close()


def test_a_reconnect_that_reports_new_values_still_updates_them(tmp_path):
    plane = ControlPlane(path=str(tmp_path / "cp.db"))
    plane.nodes.upsert(node_id="n1", hostname="old", status="idle", agent_version="1.0", auth_fingerprint="aa")
    plane.nodes.upsert(node_id="n1", hostname="new", status="idle", agent_version="2.0", auth_fingerprint="bb")
    row = plane.nodes.get("n1")
    assert (row["hostname"], row["agent_version"], row["auth_fingerprint"]) == ("new", "2.0", "bb")
    plane.close()
