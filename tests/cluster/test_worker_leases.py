"""
Hosted-worker leases: one customer at a time, cleaned between customers, never returned to the pool on trust.
"""
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from gcon.cluster.registry import NodeRegistry
from gcon.cluster.scheduler import Scheduler
from gcon.cluster.worker_leases import (
    BadRequest, LeaseNotFound, NotAHostedWorker, NotEnoughWorkers, PlanDoesNotAllow, WorkerLeaseManager,
)
from gcon.persistence import ControlPlane


class Clock:
    def __init__(self):
        # Starts at the real time: the repositories stamp rows with the real clock, and the manager compares against
        # this one, so they must start together.
        self.now = datetime.now(UTC)

    def __call__(self):
        return self.now

    def advance(self, **kw):
        self.now += timedelta(**kw)


def _node(node_id, *, org_id=None, sandboxed=True, connected=True, version="1.1.0"):
    return SimpleNamespace(node_id=node_id, org_id=org_id, sandboxed=sandboxed, agent_version=version,
                           status="idle", is_connected=lambda: connected)


class World:
    """A control plane, a registry and a lease manager wired the way the coordinator wires them."""

    def __init__(self, tmp_path, caps=None):
        self.cp = ControlPlane(path=str(tmp_path / "cp.db"))
        self.clock = Clock()
        self.caps = caps if caps is not None else {"acme": 3, "globex": 3}
        self.sent = []                      # (node_id, ref) clean-up requests sent to workers
        self.events = []
        self.deliverable = True
        self.start()

    def start(self):
        """(Re)create the registry and manager, as a coordinator restart would."""
        self.registry = NodeRegistry()
        self.manager = WorkerLeaseManager(
            self.cp, self.registry, cap_resolver=lambda org: self.caps.get(org, 0),
            send_sanitize=self._send, publish=lambda e, p: self.events.append((e, p)), clock=self.clock,
            idle_seconds=600, sanitize_timeout_seconds=300, lost_grace_seconds=120)
        self.registry.fleet_lookup = self.cp.fleet_nodes.is_member
        self.registry.lease_lookup = self.manager.lookup

    def _send(self, node_id, ref):
        self.sent.append((node_id, ref))
        return self.deliverable

    def fleet(self, *names, **kw):
        for n in names:
            self.cp.fleet_nodes.add(n, None)
            self.registry.register(_node(n, **kw))

    def busy(self, node_id, running=1):
        self.registry.nodes[node_id]["status"] = "busy" if running else "idle"
        self.registry.nodes[node_id]["running_jobs"] = running

    def state(self, lease_id):
        return self.cp.worker_leases.get(lease_id)["state"]

    def close(self):
        self.cp.close()


@pytest.fixture
def w(tmp_path):
    world = World(tmp_path)
    yield world
    world.close()


def _answer(w, lease, ok=True, detail=""):
    w.manager.on_sanitize_result(lease["node_id"], lease["lease_id"], ok, detail)


# ---------------------------------------------------------------- who can be leased
class TestAvailability:
    def test_only_clean_connected_sandboxed_current_hosted_workers_are_offered(self, w):
        w.fleet("ok")
        w.fleet("no-sandbox", sandboxed=False)
        w.fleet("old", version="1.0.0")
        w.fleet("down", connected=False)
        w.fleet("held")
        w.cp.fleet_nodes.hold("held", "lost while leased")
        w.registry.register(_node("byo", org_id="acme"))             # a customer's own worker: never leased
        w.registry.register(_node("stray"))                          # no organization, but not in the fleet
        assert w.manager.available_nodes() == ["ok"]

    def test_a_worker_with_an_organization_is_never_hosted_even_if_the_table_says_so(self, w):
        w.cp.fleet_nodes.add("odd", None)
        w.registry.register(_node("odd", org_id="acme"))
        assert w.manager.available_nodes() == []


# ---------------------------------------------------------------- requesting
class TestRequest:
    def test_it_is_all_or_none(self, w):
        w.fleet("a", "b")
        with pytest.raises(NotEnoughWorkers) as e:
            w.manager.request("acme", 3, "alice")
        assert e.value.available == 2
        assert w.cp.worker_leases.list_open() == [] and w.manager.available_nodes() == ["a", "b"]

    def test_the_plan_decides_how_many(self, w):
        w.fleet("a", "b", "c", "d")
        with pytest.raises(PlanDoesNotAllow):
            w.manager.request("nobody-plan", 1, "x")                 # cap 0: hosted workers are not in the plan
        w.manager.request("acme", 2, "alice")
        with pytest.raises(PlanDoesNotAllow):
            w.manager.request("acme", 2, "alice")                    # 2 held + 2 more would pass the cap of 3
        assert len(w.manager.request("acme", 1, "alice")) == 1

    @pytest.mark.parametrize("bad", [0, -1, 51, "2", None, True])
    def test_a_silly_count_is_refused(self, w, bad):
        w.fleet("a")
        with pytest.raises(BadRequest):
            w.manager.request("acme", bad, "alice")

    def test_a_request_records_who_and_when_and_marks_the_worker_leased(self, w):
        w.fleet("a")
        [lease] = w.manager.request("acme", 1, "alice")
        assert lease["requested_by"] == "alice" and lease["state"] == "active" and lease["org_id"] == "acme"
        info = w.registry.snapshot()["a"]
        assert info["org_id"] == "acme" and info["lease_state"] == "active"
        assert w.manager.available_nodes() == []

    def test_the_database_itself_refuses_a_second_open_lease_on_one_worker(self, w):
        w.fleet("a")
        w.manager.request("acme", 1, "alice")
        with pytest.raises(w.cp.db.IntegrityError):
            w.cp.worker_leases.create_many([{"node_id": "a", "org_id": "globex", "requested_by": "mallory"}])


# ---------------------------------------------------------------- isolation (the point of the whole design)
class TestIsolation:
    def test_a_leased_worker_runs_only_its_customers_jobs(self, w):
        w.fleet("a")
        w.manager.request("acme", 1, "alice")
        scheduler = Scheduler(w.registry, w.cp)
        assert scheduler.select_node(org_id="globex", require_sandbox=True, shared_pool=True) is None
        assert scheduler.select_node(org_id=None, require_sandbox=True) is None
        assert scheduler.select_node(org_id="acme", require_sandbox=True).node_id == "a"

    def test_an_unleased_hosted_worker_takes_no_job_at_all(self, w):
        w.fleet("a")
        scheduler = Scheduler(w.registry, w.cp)
        for org in ("acme", None):
            assert scheduler.select_node(org_id=org, require_sandbox=True, shared_pool=True) is None

    def test_the_shared_pool_fallback_never_reaches_the_hosted_fleet(self, w):
        w.fleet("a")
        w.registry.shared_pool_lookup = lambda node_id: True         # even if someone flagged it shared
        assert w.registry.snapshot()["a"].get("shared_pool") is not True
        assert Scheduler(w.registry, w.cp).select_node(org_id="acme", shared_pool=True) is None

    def test_an_ending_lease_takes_no_new_job(self, w):
        w.fleet("a")
        [lease] = w.manager.request("acme", 1, "alice")
        w.busy("a")                                                  # still running its last job
        w.manager.release(lease["lease_id"], org_id="acme", by="alice")
        w.busy("a", 0)
        assert w.state(lease["lease_id"]) == "ending"
        assert Scheduler(w.registry, w.cp).select_node(org_id="acme", require_sandbox=True) is None

    def test_one_customer_cannot_release_anothers_lease_and_cannot_tell_it_exists(self, w):
        w.fleet("a")
        [lease] = w.manager.request("acme", 1, "alice")
        with pytest.raises(LeaseNotFound):
            w.manager.release(lease["lease_id"], org_id="globex", by="mallory")
        with pytest.raises(LeaseNotFound):
            w.manager.release("lease_does_not_exist", org_id="globex", by="mallory")
        assert w.state(lease["lease_id"]) == "active"


# ---------------------------------------------------------------- ending a lease
class TestLifecycle:
    def test_release_waits_for_the_running_job_then_cleans_then_returns_the_worker(self, w):
        w.fleet("a")
        [lease] = w.manager.request("acme", 1, "alice")
        w.busy("a")
        w.manager.release(lease["lease_id"], org_id="acme", by="alice")
        assert w.state(lease["lease_id"]) == "ending" and w.sent == []        # not cleaned under a running job
        w.busy("a", 0)
        w.manager.tick()
        assert w.state(lease["lease_id"]) == "sanitizing" and w.sent == [("a", lease["lease_id"])]
        assert w.manager.available_nodes() == []                              # not back yet: nobody has said it is clean
        _answer(w, lease, ok=True)
        done = w.cp.worker_leases.get(lease["lease_id"])
        assert done["state"] == "ended" and done["sanitize_result"] == "clean" and done["end_reason"] == "customer_release"
        assert w.registry.snapshot()["a"]["lease_state"] is None
        assert w.manager.available_nodes() == ["a"]

    def test_a_failed_clean_up_keeps_the_worker_out_of_the_pool(self, w):
        w.fleet("a")
        [lease] = w.manager.request("acme", 1, "alice")
        w.manager.release(lease["lease_id"], org_id="acme", by="alice")
        _answer(w, lease, ok=False, detail="1 item(s) remain in the job directory")
        assert w.manager.available_nodes() == []
        assert "remain" in w.cp.fleet_nodes.get("a")["held_reason"]
        assert w.cp.worker_leases.get(lease["lease_id"])["state"] == "ended"

    def test_a_silent_worker_is_held_never_trusted(self, w):
        w.fleet("a")
        [lease] = w.manager.request("acme", 1, "alice")
        w.manager.release(lease["lease_id"], org_id="acme", by="alice")
        w.clock.advance(seconds=301)
        w.manager.tick()
        assert w.state(lease["lease_id"]) == "ended"
        assert w.cp.fleet_nodes.get("a")["held_reason"] and w.manager.available_nodes() == []

    def test_a_clean_up_that_cannot_even_be_sent_holds_the_worker(self, w):
        w.fleet("a")
        [lease] = w.manager.request("acme", 1, "alice")
        w.deliverable = False
        w.manager.release(lease["lease_id"], org_id="acme", by="alice")
        assert w.state(lease["lease_id"]) == "ended" and w.cp.fleet_nodes.get("a")["held_reason"]

    def test_a_lost_worker_is_held_and_the_lease_ends(self, w):
        w.fleet("a")
        [lease] = w.manager.request("acme", 1, "alice")
        w.registry.nodes["a"]["status"] = "offline"
        w.manager.tick()
        assert w.state(lease["lease_id"]) == "active"                         # a brief drop is forgiven
        w.clock.advance(seconds=121)
        w.manager.tick()
        done = w.cp.worker_leases.get(lease["lease_id"])
        assert done["state"] == "ended" and done["end_reason"] == "worker_lost"
        assert w.cp.fleet_nodes.get("a")["held_reason"]

    def test_an_idle_lease_is_released_but_a_busy_one_is_not(self, w):
        w.fleet("a", "b")
        la, lb = w.manager.request("acme", 2, "alice")
        w.clock.advance(seconds=500)
        w.busy(lb["node_id"])
        w.manager.tick()                                                      # touches the busy one
        w.clock.advance(seconds=500)
        w.manager.tick()
        states = {l["node_id"]: w.state(l["lease_id"]) for l in (la, lb)}
        assert states[la["node_id"]] in ("ending", "sanitizing") and states[lb["node_id"]] == "active"
        assert w.cp.worker_leases.get(la["lease_id"])["end_reason"] == "idle"

    def test_releasing_twice_changes_nothing(self, w):
        w.fleet("a")
        [lease] = w.manager.request("acme", 1, "alice")
        w.busy("a")
        w.manager.release(lease["lease_id"], org_id="acme", by="alice")
        first = w.cp.worker_leases.get(lease["lease_id"])
        w.manager.release(lease["lease_id"], org_id="acme", by="someone-else")
        assert w.cp.worker_leases.get(lease["lease_id"]) == first

    def test_a_stray_answer_is_ignored(self, w):
        w.fleet("a")
        [lease] = w.manager.request("acme", 1, "alice")
        w.manager.on_sanitize_result("a", lease["lease_id"], True, "")        # not asked yet
        w.manager.on_sanitize_result("a", "made-up-ref", True, "")
        assert w.state(lease["lease_id"]) == "active" and w.manager.available_nodes() == []


# ---------------------------------------------------------------- staff and restarts
class TestHeldWorkersAndRestarts:
    def test_staff_can_have_a_held_worker_cleaned_and_it_returns_only_when_it_says_clean(self, w):
        w.fleet("a")
        w.cp.fleet_nodes.hold("a", "lost while leased")
        w.manager.sanitize_held_node("a", "ops")
        assert w.sent[-1][0] == "a" and w.manager.available_nodes() == []
        ref = w.sent[-1][1]
        w.manager.on_sanitize_result("a", ref, False, "still dirty")
        assert w.cp.fleet_nodes.get("a")["held_reason"] and w.manager.available_nodes() == []
        w.manager.sanitize_held_node("a", "ops")
        w.manager.on_sanitize_result("a", w.sent[-1][1], True, "")
        assert w.cp.fleet_nodes.get("a")["held_reason"] is None and w.manager.available_nodes() == ["a"]

    def test_cleaning_a_worker_that_is_leased_or_not_hosted_is_refused(self, w):
        w.fleet("a")
        w.manager.request("acme", 1, "alice")
        with pytest.raises(BadRequest):
            w.manager.sanitize_held_node("a", "ops")
        with pytest.raises(NotAHostedWorker):
            w.manager.sanitize_held_node("not-in-fleet", "ops")

    def test_a_lease_survives_a_coordinator_restart(self, w):
        w.fleet("a")
        [lease] = w.manager.request("acme", 1, "alice")
        w.start()                                                             # new registry, same database
        w.registry.register(_node("a"))                                       # the worker reconnects
        info = w.registry.snapshot()["a"]
        assert info["org_id"] == "acme" and info["lease_state"] == "active"
        assert Scheduler(w.registry, w.cp).select_node(org_id="globex", require_sandbox=True) is None
        assert Scheduler(w.registry, w.cp).select_node(org_id="acme", require_sandbox=True).node_id == "a"

    def test_a_hold_survives_a_restart_and_a_fresh_enrollment_cannot_clear_it(self, w):
        w.fleet("a")
        w.cp.fleet_nodes.hold("a", "lost while leased")
        w.cp.fleet_nodes.add("a", "tok_2")                                    # enrolling again
        w.start()
        w.registry.register(_node("a"))
        assert w.cp.fleet_nodes.get("a")["held_reason"] and w.manager.available_nodes() == []
