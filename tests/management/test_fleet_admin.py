"""Staff view of the GCON-hosted fleet: what each worker is doing, minting fleet tokens, force-releasing, cleaning."""
from types import SimpleNamespace

import pytest

from tests.management.test_shared_pool_admin import env  # noqa: F401  (fixture)


def _hosted(coordinator, *names, connected=True):
    for n in names:
        coordinator.control_plane.fleet_nodes.add(n, None)
        coordinator.registry.register(SimpleNamespace(
            node_id=n, org_id=None, sandboxed=True, agent_version="1.1.0", status="idle",
            is_connected=lambda c=connected: c))


def _states(client):
    return {w["node_id"]: w for w in client.get("/management/fleet").json()["workers"]}


def test_the_fleet_view_says_what_each_worker_is_doing(env):  # noqa: F811
    clients, _server, coordinator, org_id = env
    coordinator.org_leased_worker_cap_resolver = lambda org: 5
    _hosted(coordinator, "free", "leased", "dirty", "away", connected=True)
    coordinator.registry.register(SimpleNamespace(
        node_id="offline-one", org_id=None, sandboxed=True, agent_version="1.1.0", status="idle",
        is_connected=lambda: False))
    coordinator.control_plane.fleet_nodes.add("offline-one", None)
    coordinator.control_plane.fleet_nodes.hold("dirty", "clean-up failed: 2 item(s) remain")
    coordinator.registry.nodes["away"]["status"] = "offline"
    [lease] = coordinator.worker_leases.request(org_id, 1, "alice")
    # whichever worker the lease picked is the "leased" one; the rest keep their own states
    fleet = clients["Owner"].get("/management/fleet").json()
    states = _states(clients["Owner"])
    assert states[lease["node_id"]]["state"] == "leased" and states[lease["node_id"]]["leased_to"] == "Acme"
    assert states["dirty"]["state"] == "held" and "remain" in states["dirty"]["held_reason"]
    assert states["offline-one"]["state"] == "unavailable" and states["offline-one"]["why_unavailable"]
    assert fleet["counts"]["total"] == 5 and len(fleet["open_leases"]) == 1
    assert fleet["open_leases"][0]["org_name"] == "Acme"


def test_a_fleet_token_is_owner_or_admin_only_has_figures_and_is_listed_as_the_fleets(env):  # noqa: F811
    clients, _server, coordinator, _org = env
    made = clients["Owner"].post("/management/fleet/tokens", json={"label": "rack-1"})
    assert made.status_code == 200 and made.json()["max_uses"] == 5 and made.json()["expires_at"]
    assert clients["Operator"].post("/management/fleet/tokens", json={}).status_code in (401, 403)
    assert clients["Owner"].post("/management/fleet/tokens", json={"max_uses": 0}).status_code == 400
    listed = clients["Owner"].get("/management/enroll-tokens").json()
    assert listed[0]["kind"] == "fleet" and listed[0]["org_name"] == "GCON hosted fleet" and listed[0]["org_id"] is None
    # the fleet token is never mistaken for an organization's
    assert coordinator.control_plane.enroll_tokens.lookup_org_id(made.json()["token"]) is None


def test_staff_can_force_release_a_lease(env):  # noqa: F811
    clients, _server, coordinator, org_id = env
    coordinator.org_leased_worker_cap_resolver = lambda org: 5
    _hosted(coordinator, "h1")
    [lease] = coordinator.worker_leases.request(org_id, 1, "alice")
    r = clients["Owner"].post(f"/management/leases/{lease['lease_id']}/release")
    assert r.status_code == 200
    assert coordinator.control_plane.worker_leases.get(lease["lease_id"])["end_reason"] == "staff_release"
    assert clients["Owner"].post("/management/leases/lease_nope/release").status_code == 404
    assert clients["Operator"].post(f"/management/leases/{lease['lease_id']}/release").status_code in (401, 403)


def test_cleaning_a_held_worker_needs_it_connected_and_idle(env):  # noqa: F811
    clients, _server, coordinator, _org = env
    _hosted(coordinator, "stuck", connected=False)
    coordinator.control_plane.fleet_nodes.hold("stuck", "lost while leased")
    assert clients["Owner"].post("/management/fleet/stuck/clean").status_code == 400
    assert clients["Owner"].post("/management/fleet/not-hosted/clean").status_code == 404


def test_a_leased_worker_cannot_be_archived_from_under_its_customer(env):  # noqa: F811
    clients, _server, coordinator, org_id = env
    coordinator.org_leased_worker_cap_resolver = lambda org: 5
    _hosted(coordinator, "h1")
    [lease] = coordinator.worker_leases.request(org_id, 1, "alice")
    coordinator.registry.nodes["h1"]["status"] = "offline"
    r = clients["Owner"].post("/management/nodes/h1/archive")
    assert r.status_code == 400 and "lease" in r.json()["detail"]


def test_a_workers_full_lease_history_names_every_customer_that_held_it(env):  # noqa: F811
    clients, _server, coordinator, org_id = env
    coordinator.org_leased_worker_cap_resolver = lambda org: 5
    coordinator.worker_leases.send_sanitize = lambda node_id, ref: True      # a worker that receives the request
    _hosted(coordinator, "h1")
    [first] = coordinator.worker_leases.request(org_id, 1, "alice")
    coordinator.worker_leases.release(first["lease_id"], by="ops", reason="staff_release")
    coordinator.worker_leases.on_sanitize_result("h1", first["lease_id"], True, "")
    [second] = coordinator.worker_leases.request(org_id, 1, "bob")
    history = clients["Owner"].get("/management/fleet/h1/leases").json()
    assert [h["lease_id"] for h in history] == [first["lease_id"], second["lease_id"]]
    assert history[0]["org_name"] == "Acme" and history[0]["sanitize_result"] == "clean" and history[1]["ended_at"] is None
    assert clients["Owner"].get("/management/fleet/not-hosted/leases").status_code == 404
    assert clients["Operator"].get("/management/fleet/h1/leases").status_code in (401, 403)
