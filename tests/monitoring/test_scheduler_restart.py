"""The scheduler loop is restarted when it crashes, counted, and surfaced; a loop
that keeps crashing is given up on so health reports it dead."""
import time

import pytest

from gcon.cluster.coordinator import GCONCoordinator
from gcon.execution.agent import GCONAgent
from gcon.persistence import ControlPlane


def wait_for(pred, timeout=15):
    end = time.time() + timeout
    while time.time() < end:
        if pred():
            return True
        time.sleep(0.05)
    return False


@pytest.fixture
def coord(tmp_path):
    c = GCONCoordinator(control_plane=ControlPlane(path=str(tmp_path / "cp.db")))
    yield c
    c.shutdown()


def _crash_once(coord):
    original = coord.scheduler.select_node

    def boom(*a, **k):
        coord.scheduler.select_node = original
        raise TypeError("injected, once")

    coord.scheduler.select_node = boom


def test_one_crash_is_survived_counted_and_the_job_still_runs(coord):
    coord.register_agent(GCONAgent(node_id="n1"))
    _crash_once(coord)
    coord.submit_job("after-crash", "echo hi")
    assert wait_for(lambda: coord.jobs["after-crash"]["status"] == "completed")
    assert coord.scheduler_thread.is_alive()
    s = coord.observability.scheduler()
    assert s["restarts"]["total"] == 1 and "TypeError" in s["restarts"]["last_message"]
    assert s["state"] != "dead"
    kinds = [e.event_type for e in coord.get_all_events()]
    assert "SCHEDULER_RESTARTED" in kinds


def test_a_recent_restart_raises_an_incident_rule(coord):
    coord.register_agent(GCONAgent(node_id="n1"))
    _crash_once(coord)
    coord.submit_job("j", "echo hi")
    assert wait_for(lambda: coord.scheduler_stats.restarts_total == 1)
    firing = coord.observability.evaluate_rules(coord.observability.snapshot())
    assert any(f["rule"] == "scheduler_restarted" for f in firing)


def test_a_crash_that_keeps_recurring_is_given_up_on_and_reported_dead(coord, monkeypatch):
    coord.shutdown()                       # replace the fixture's coordinator thread settings
    monkeypatch.setenv("GCON_SCHEDULER_MAX_RESTARTS", "1")
    c = GCONCoordinator()
    try:
        c.register_agent(GCONAgent(node_id="n1"))
        c.scheduler.select_node = lambda *a, **k: (_ for _ in ()).throw(TypeError("always"))
        c.submit_job("never", "echo hi")
        assert wait_for(lambda: not c.scheduler_thread.is_alive())
        assert c.observability.scheduler()["state"] == "dead"
        assert c.get_cluster_health()["checks"]["coordinator"]["healthy"] is False
    finally:
        c.shutdown()
