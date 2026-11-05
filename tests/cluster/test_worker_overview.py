"""
One honest answer to "what are my workers doing?" for the staff dashboard.

Every registered worker is exactly one KIND (GCON hosted fleet, customer-hosted, shared pool, unassigned) and exactly
one STATE (quarantined, offline, working, idle, other), so the numbers always add up to the total. Quarantined comes
first because it is the platform's own sign that a worker may be compromised (its signed results failed to verify, or
its sandbox evidence did not match) and must never be hidden inside "idle". A hosted worker that is held out of the
pool (a clean-up that did not finish) is listed beside them.
"""
import time
from types import SimpleNamespace

import pytest

from gcon.cluster.coordinator import GCONCoordinator
from gcon.persistence.control_plane import ControlPlane


@pytest.fixture
def coord(tmp_path):
    c = GCONCoordinator(control_plane=ControlPlane(path=str(tmp_path / "cp.db")))
    c.pause_scheduler()
    yield c
    c.shutdown()


def add(coord, node_id, *, org=None, fleet=False, shared=False, status="idle", quarantined=None,
        lease_org=None, held=None):
    """Register a worker of the given kind and put it in the given state."""
    if fleet:
        coord.control_plane.fleet_nodes.add(node_id, None)
    if shared:
        coord.registry.shared_pool_lookup = (lambda ids: (lambda nid: nid in ids))(
            getattr(coord, "_shared_ids", set()) | {node_id})
        coord._shared_ids = getattr(coord, "_shared_ids", set()) | {node_id}
    node = SimpleNamespace(node_id=node_id, org_id=org, sandboxed=True, agent_version="1.1.0", status=status,
                           is_connected=lambda: status != "offline")
    coord.registry.register(node)
    if lease_org:
        coord.registry.set_lease(node_id, lease_org, "active")
    coord.registry.nodes[node_id]["status"] = status
    if status == "busy":
        coord.registry.nodes[node_id]["running_jobs"] = 1
    if quarantined:
        coord.registry.set_quarantined(node_id, True, reason=quarantined)
    if held:
        coord.control_plane.fleet_nodes.hold(node_id, held)
    return node


def populate(coord):
    add(coord, "fleet-1", fleet=True, lease_org="acme", status="busy")      # GCON hosted, leased to acme, working
    add(coord, "fleet-2", fleet=True)                                       # GCON hosted, free, idle
    add(coord, "fleet-3", fleet=True, status="offline")                     # GCON hosted, free, offline
    add(coord, "acme-1", org="acme", status="busy")
    add(coord, "acme-2", org="acme")
    add(coord, "acme-3", org="acme", status="offline")
    add(coord, "globex-1", org="globex", quarantined="3 receipts failed verification in a row")
    add(coord, "pool-1", shared=True)
    add(coord, "nobody-1")


class TestEveryWorkerIsCountedOnce:
    def test_an_empty_cluster_is_all_zeros(self, coord):
        o = coord.get_worker_overview()
        assert o["total"] == 0 and o["by_customer"] == [] and o["attention"] == []
        assert all(v == 0 for v in o["states"].values())

    def test_the_kinds_split_correctly(self, coord):
        populate(coord)
        k = coord.get_worker_overview()["kinds"]
        assert {n: k[n]["total"] for n in k} == {"gcon_fleet": 3, "customer_hosted": 4, "shared_pool": 1, "unassigned": 1}

    def test_the_states_add_up_to_the_total_and_to_the_kinds(self, coord):
        populate(coord)
        o = coord.get_worker_overview()
        assert o["total"] == 9 == sum(o["states"].values()) == sum(k["total"] for k in o["kinds"].values())
        for kind in o["kinds"].values():
            assert kind["total"] == sum(kind[s] for s in ("working", "idle", "offline", "quarantined", "other"))

    def test_each_state_is_what_the_registry_says(self, coord):
        populate(coord)
        s = coord.get_worker_overview()["states"]
        assert s == {"working": 2, "idle": 4, "offline": 2, "quarantined": 1, "other": 0}

    def test_a_quarantined_worker_counts_as_quarantined_whatever_else_it_is(self, coord):
        add(coord, "q-busy", org="acme", status="busy", quarantined="sandbox mismatch")
        add(coord, "q-off", org="acme", status="offline", quarantined="sandbox mismatch")
        s = coord.get_worker_overview()["states"]
        assert s["quarantined"] == 2 and s["working"] == 0 and s["offline"] == 0

    def test_an_unknown_status_is_other_not_lost(self, coord):
        add(coord, "weird", org="acme", status="unknown")
        o = coord.get_worker_overview()
        assert o["states"]["other"] == 1 and o["total"] == 1

    def test_the_hosted_fleet_says_how_many_are_leased_free_and_held(self, coord):
        populate(coord)
        add(coord, "fleet-4", fleet=True, held="clean-up did not finish")
        fleet = coord.get_worker_overview()["kinds"]["gcon_fleet"]
        assert (fleet["leased"], fleet["held"], fleet["available"]) == (1, 1, 2)
        assert fleet["total"] == 4 == fleet["leased"] + fleet["held"] + fleet["available"]


class TestPerCustomer:
    def _rows(self, coord):
        return {r["org_id"]: r for r in coord.get_worker_overview()["by_customer"]}

    def test_a_customers_own_workers_and_leased_gcon_workers_are_separate(self, coord):
        populate(coord)
        acme = self._rows(coord)["acme"]
        assert (acme["own"], acme["leased"]) == (3, 1)

    def test_its_states_cover_both(self, coord):
        populate(coord)
        acme = self._rows(coord)["acme"]
        assert (acme["working"], acme["idle"], acme["offline"], acme["quarantined"]) == (2, 1, 1, 0)

    def test_a_customer_with_a_quarantined_worker_shows_it(self, coord):
        populate(coord)
        assert self._rows(coord)["globex"]["quarantined"] == 1

    def test_workers_with_no_customer_are_not_in_the_per_customer_table(self, coord):
        populate(coord)
        assert set(self._rows(coord)) == {"acme", "globex"}

    def test_the_ones_needing_attention_come_first(self, coord):
        add(coord, "big-1", org="big"); add(coord, "big-2", org="big"); add(coord, "big-3", org="big")
        add(coord, "off-1", org="offline-co", status="offline")
        add(coord, "bad-1", org="bad-co", quarantined="x")
        order = [r["org_id"] for r in coord.get_worker_overview()["by_customer"]]
        assert order == ["bad-co", "offline-co", "big"]

    def test_the_table_is_capped_but_says_how_many_customers_there_are(self, coord):
        for i in range(30):
            add(coord, f"w{i}", org=f"org{i:02d}")
        o = coord.get_worker_overview(limit=10)
        assert len(o["by_customer"]) == 10 and o["customers_with_workers"] == 30


class TestNeedsAttention:
    def test_a_quarantined_worker_is_listed_with_its_reason_and_where_it_belongs(self, coord):
        populate(coord)
        item = coord.get_worker_overview()["attention"][0]
        assert item["node_id"] == "globex-1" and item["state"] == "quarantined"
        assert item["org_id"] == "globex" and item["kind"] == "customer_hosted"
        assert "failed verification" in item["reason"]

    def test_a_held_hosted_worker_is_listed_with_why(self, coord):
        add(coord, "fleet-h", fleet=True, held="clean-up did not finish")
        item = coord.get_worker_overview()["attention"][0]
        assert (item["node_id"], item["state"], item["kind"]) == ("fleet-h", "held", "gcon_fleet")
        assert "clean-up" in item["reason"]

    def test_a_healthy_cluster_has_nothing_to_attend_to(self, coord):
        add(coord, "ok-1", org="acme"); add(coord, "ok-2", fleet=True)
        assert coord.get_worker_overview()["attention"] == []

    def test_the_list_is_capped_and_the_real_count_is_kept(self, coord):
        for i in range(30):
            add(coord, f"q{i}", org="acme", quarantined="bad")
        o = coord.get_worker_overview()
        assert len(o["attention"]) == 20 and o["attention_total"] == 30


class TestItIsCheap:
    def test_two_thousand_workers_take_a_fraction_of_a_second(self, coord):
        for i in range(2000):
            add(coord, f"n{i}", org=f"org{i % 50}", status=("busy", "idle", "offline")[i % 3])
        start = time.perf_counter()
        o = coord.get_worker_overview()
        assert time.perf_counter() - start < 1.0
        assert o["total"] == 2000


class TestForTheDashboard:
    @pytest.fixture
    def mgmt(self, tmp_path, monkeypatch):
        from gcon.dashboard.presentation import PresentationLayer
        from gcon.dashboard.web_server import WebServer
        monkeypatch.setenv("GCON_DB_PATH", str(tmp_path / "gcon.db"))
        monkeypatch.setenv("GCON_OWNER_PASSWORD", "owner-pw-123")
        c = GCONCoordinator(control_plane=ControlPlane(path=str(tmp_path / "cp.db")))
        c.pause_scheduler()
        server = WebServer(PresentationLayer(c))
        yield server.management, c
        c.shutdown()

    def test_customers_are_named_not_just_numbered(self, mgmt):
        management, c = mgmt
        org = management.create_organization("Acme Inc")
        add(c, "acme-1", org=org["org_id"], status="busy")
        row = management.get_worker_overview()["by_customer"][0]
        assert row["name"] == "Acme Inc" and row["org_id"] == org["org_id"]

    def test_a_customer_that_no_longer_exists_shows_its_id_not_an_error(self, mgmt):
        management, c = mgmt
        add(c, "ghost-1", org="org_gone", quarantined="bad")
        o = management.get_worker_overview()
        assert o["by_customer"][0]["name"] == "org_gone"
        assert o["attention"][0]["customer"] == "org_gone"
