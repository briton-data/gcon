"""
Scheduler page backend: the live counters the scheduler loop keeps and the
aggregate /management/scheduler serves. Real coordinator and real dispatch.
"""
import time
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from gcon.cluster.coordinator import GCONCoordinator
from gcon.dashboard.presentation import PresentationLayer
from gcon.dashboard.web_server import WebServer
from gcon.execution.agent import GCONAgent
from gcon.persistence import ControlPlane


def wait_for(pred, timeout=8):
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


def test_state_running_then_paused_then_resumed(coord):
    assert wait_for(lambda: coord.observability.scheduler()["state"] == "running")
    coord.pause_scheduler()
    assert wait_for(lambda: coord.observability.scheduler()["state"] == "paused")
    assert wait_for(lambda: coord.observability.scheduler()["dispatch"]["loop_state"] == "paused")
    coord.resume_scheduler()
    assert wait_for(lambda: coord.observability.scheduler()["state"] == "running")
    kinds = [e["event_type"] for e in coord.observability.scheduler()["recent_control"]]
    assert kinds[:2] == ["SCHEDULER_RESUMED", "SCHEDULER_PAUSED"]        # newest first


def test_standby_coordinator_is_reported_as_standby(coord, monkeypatch):
    monkeypatch.setattr(coord, "leader_elector", SimpleNamespace(is_leader=False, stop=lambda: None))
    s = coord.observability.scheduler()
    assert s["state"] == "standby" and s["leader"] is False


def test_a_loop_that_stopped_ticking_is_reported_stalled(coord, monkeypatch):
    assert wait_for(lambda: coord.observability.scheduler()["state"] == "running")
    monkeypatch.setattr(coord.scheduler_stats, "tick", lambda state: None)    # loop "hangs"
    time.sleep(0.3)
    coord.scheduler_stats.last_loop_monotonic = time.monotonic() - 60
    assert coord.observability.scheduler()["state"] == "stalled"


def test_dead_scheduler_thread_is_reported_dead(coord):
    coord.shutdown()
    assert coord.observability.scheduler()["state"] == "dead"


def test_dispatch_is_counted_once_per_real_dispatch(coord):
    coord.register_agent(GCONAgent(node_id="n1"))
    for i in range(3):
        coord.submit_job(f"d{i}", 'echo hi')
    assert wait_for(lambda: all(coord.jobs[f"d{i}"]["status"] == "completed" for i in range(3)))
    d = coord.observability.scheduler()["dispatch"]
    assert d["total"] == 3 and d["last_at"] is not None
    assert d["rate_per_minute"]["1m"] > 0 and d["rate_per_minute"]["15m"] > 0


def test_unplaceable_job_shows_as_failed_passes_and_blocked(coord):
    coord.register_agent(GCONAgent(node_id="n1"))
    coord.submit_job("needs-gpu", "echo hi", requires={"gpu": True})
    coord.submit_job("plain", "echo hi")
    assert wait_for(lambda: coord.jobs["plain"]["status"] == "completed")
    assert wait_for(lambda: coord.observability.scheduler()["failures"]["failed_passes_total"] > 0)
    s = coord.observability.scheduler()
    assert s["failures"]["by_kind"].keys() == {"awaiting_matching_worker"}
    assert s["failures"]["last_kind"] == "awaiting_matching_worker" and "needs-gpu" in s["failures"]["last_message"]
    assert s["queue"]["blocked_now"] == 1                    # needs-gpu; "plain" was placed fine
    coord.cancel_job("needs-gpu")
    assert coord.observability.scheduler()["queue"]["blocked_now"] == 0   # cancelled jobs are not "blocked"


def test_retry_pressure_counts_retried_and_capped_jobs(coord):
    coord.pause_scheduler()
    for jid, status, attempts in (("r1", "completed", 2), ("r2", "failed", coord._max_job_attempts), ("r3", "failed", 1)):
        coord.submit_job(jid, "echo hi")
        coord.jobs[jid]["status"], coord.jobs[jid]["attempt_number"] = status, attempts
    r = coord.observability.scheduler()["retry"]
    assert r["max_attempts"] == coord._max_job_attempts
    assert r["jobs_retried"] == 2 and r["failed_at_attempt_cap"] == 1
    assert r["backoff"] == "none"


def test_queue_section_reuses_waiting_reasons(coord):
    coord.pause_scheduler()
    coord.submit_job("q1", "echo hi")
    q = coord.observability.scheduler()["queue"]
    assert q["pending_jobs"] == 1 and q["depth"] >= 1
    assert "scheduler_paused" in q["waiting"]["by_reason"]
    assert q["oldest_pending_seconds"] is not None


def test_scheduler_endpoint_needs_auth_and_returns_the_aggregate(tmp_path, monkeypatch):
    monkeypatch.setenv("GCON_DB_PATH", str(tmp_path / "gcon.db"))
    monkeypatch.setenv("GCON_OWNER_PASSWORD", "owner-pw-123")
    coordinator = GCONCoordinator(control_plane=ControlPlane(path=str(tmp_path / "cp.db")))
    server = WebServer(PresentationLayer(coordinator))
    try:
        with TestClient(server.app) as c:
            assert c.get("/management/scheduler").status_code in (401, 403)
            from tests.integration.smoke_management_gaps import OWNER_EMAIL
            assert c.post("/auth/login", json={"email": OWNER_EMAIL, "password": "owner-pw-123"}).status_code == 200
            body = c.get("/management/scheduler").json()
            assert {"state", "queue", "dispatch", "failures", "retry", "recent_control", "counting_since"} <= body.keys()
            assert c.post("/cluster/scheduler/pause").status_code == 200
            assert wait_for(lambda: c.get("/management/scheduler").json()["state"] == "paused")
            assert c.post("/cluster/scheduler/resume").status_code == 200
    finally:
        coordinator.shutdown()


def test_queue_age_works_for_jobs_submitted_live_not_just_restored_ones(coord):
    """Regression: live job dicts carry created_at, not submitted_at, so queue
    age was blank for every job submitted since startup (earlier tests only
    passed because they wrote submitted_at by hand)."""
    coord.pause_scheduler()
    coord.submit_job("age1", "echo hi")
    time.sleep(0.3)
    s = coord.observability.scheduler()
    assert s["queue"]["oldest_pending_seconds"] is not None and s["queue"]["oldest_pending_seconds"] >= 0.3
    assert coord.observability.summary()["customers"][0]["oldest_pending_age_seconds"] >= 0.3
