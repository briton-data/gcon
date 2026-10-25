"""The shared pool has a master switch on the dashboard: OFF by default, stored, Owner/Administrator only. Hosted
workers can never be flagged into the pool."""
from types import SimpleNamespace

from gcon.cluster.coordinator import GCONCoordinator
from tests.management.test_shared_pool_admin import env  # noqa: F401  (fixture)


def _worker(coordinator, node_id, org_id=None):
    coordinator.control_plane.nodes.upsert(node_id=node_id, hostname=node_id, status="offline", org_id=org_id)


def test_it_is_off_until_staff_switch_it_on(env):  # noqa: F811
    clients, _server, coordinator, _org = env
    shown = clients["Owner"].get("/management/shared-pool").json()
    assert shown["enabled"] is False and coordinator.shared_pool_enabled is False
    assert shown["modes"] == ["off", "basic", "full"]


def test_an_owner_switches_it_and_the_choice_is_stored_and_audited(env):  # noqa: F811
    clients, server, coordinator, _org = env
    r = clients["Owner"].put("/management/shared-pool", json={"enabled": True})
    assert r.status_code == 200 and r.json()["enabled"] is True
    assert coordinator.shared_pool_enabled is True
    assert coordinator.control_plane.settings.get("shared_pool_enabled") == "on"
    assert any("switched the shared worker pool ON" in e["action"] for e in server.management.audit_logger.list_entries(50))
    assert clients["Owner"].put("/management/shared-pool", json={"enabled": False}).json()["enabled"] is False
    assert coordinator.shared_pool_enabled is False


def test_the_choice_survives_a_restart(env):  # noqa: F811
    clients, _server, coordinator, _org = env
    clients["Owner"].put("/management/shared-pool", json={"enabled": True})
    restarted = GCONCoordinator(control_plane=coordinator.control_plane)
    try:
        assert restarted.shared_pool_enabled is True
    finally:
        restarted.shutdown()


def test_only_owner_or_administrator_and_only_a_real_boolean(env):  # noqa: F811
    clients, _server, coordinator, _org = env
    assert clients["Operator"].put("/management/shared-pool", json={"enabled": True}).status_code in (401, 403)
    assert coordinator.shared_pool_enabled is False
    for bad in ("on", 1, None):
        assert clients["Owner"].put("/management/shared-pool", json={"enabled": bad}).status_code == 400


def test_the_switch_really_gates_placement(env):  # noqa: F811
    clients, _server, coordinator, org_id = env
    job = {"org_id": org_id}
    assert coordinator._shared_pool_allowed(job) is False                       # off: even in the default mode
    clients["Owner"].put("/management/shared-pool", json={"enabled": True})
    assert coordinator._shared_pool_allowed(job) is True                        # on, and the org's mode allows it
    coordinator.control_plane.org_pool_settings.set_mode(org_id, "off")
    assert coordinator._shared_pool_allowed(job) is False                       # on, but this org opted out


def test_the_overview_lists_only_workers_that_could_be_in_the_pool(env):  # noqa: F811
    clients, _server, coordinator, org_id = env
    _worker(coordinator, "loose-1")
    _worker(coordinator, "loose-2")
    _worker(coordinator, "customers-own", org_id)
    _worker(coordinator, "hosted-1")
    coordinator.control_plane.fleet_nodes.add("hosted-1", None)
    _worker(coordinator, "gone")
    coordinator.control_plane.nodes.archive("gone")
    clients["Owner"].put("/management/nodes/loose-2/shared-pool", json={"enabled": True})
    shown = clients["Owner"].get("/management/shared-pool").json()
    assert {w["node_id"]: w["in_pool"] for w in shown["workers"]} == {"loose-1": False, "loose-2": True}
    assert {o["org_id"]: o["mode"] for o in shown["organizations"]}[org_id] == "basic"


def test_a_hosted_worker_can_never_be_flagged_into_the_pool(env):  # noqa: F811
    clients, _server, coordinator, _org = env
    _worker(coordinator, "hosted-1")
    coordinator.control_plane.fleet_nodes.add("hosted-1", None)
    r = clients["Owner"].put("/management/nodes/hosted-1/shared-pool", json={"enabled": True})
    assert r.status_code == 400 and "never joins the shared pool" in r.json()["detail"]
    assert not coordinator.control_plane.shared_pool_nodes.is_member("hosted-1")
