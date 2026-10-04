"""
Observability layer: why work waits, SLIs, per-customer aggregates, sampled
history, incidents, and the three management endpoints.

Job state is injected directly (statuses/timestamps set on coordinator.jobs)
so each rule is tested against a precisely known situation rather than a
timing-dependent live run.
"""
import os
from datetime import datetime, timedelta, UTC

import pytest
from fastapi.testclient import TestClient

from gcon.cluster.coordinator import GCONCoordinator
from gcon.dashboard.presentation import PresentationLayer
from gcon.dashboard.web_server import WebServer
from gcon.persistence import ControlPlane


class Standby:
    """Stand-in for a LeaderElector that does not hold the lease."""
    is_leader = False

    def stop(self):
        pass


@pytest.fixture
def coord(tmp_path):
    cp = ControlPlane(path=str(tmp_path / "t.db"))
    c = GCONCoordinator(control_plane=cp)
    yield c
    c.shutdown()


def add_job(c, job_id, status, org="acme", age=0, **extra):
    c.submit_job(job_id, "echo", org_id=org)
    j = c.jobs[job_id]
    j["status"] = status
    j["submitted_at"] = (datetime.now(UTC) - timedelta(seconds=age)).isoformat()
    j.update(extra)
    return j


def set_workers(c, monkeypatch, *, active, idle):
    nodes = {f"n{i}": {"status": "idle" if i < idle else "busy"} for i in range(active)}
    monkeypatch.setattr(c.registry, "nodes", nodes)
    monkeypatch.setattr(c.registry, "available_nodes", lambda: [f"n{i}" for i in range(idle)])


# ------------------------------------------------------------ waiting reasons
def test_waiting_reason_every_case(coord, monkeypatch):
    add_job(coord, "plain", "pending", age=100)
    reasons = lambda: {k: v["count"] for k, v in coord.observability.waiting_reasons()["by_reason"].items()}

    set_workers(coord, monkeypatch, active=0, idle=0)
    assert reasons() == {"no_workers": 1}

    set_workers(coord, monkeypatch, active=2, idle=0)
    assert reasons() == {"all_workers_busy": 1}

    set_workers(coord, monkeypatch, active=2, idle=1)
    assert reasons() == {"awaiting_dispatch": 1}

    coord.jobs["plain"]["requires"] = {"gpu": True}
    assert reasons() == {"awaiting_matching_worker": 1}

    coord.jobs["plain"].pop("requires")
    coord.jobs["plain"]["verify"] = {"replicas": 3}
    assert reasons() == {"insufficient_replica_workers": 1}

    coord.jobs["plain"].pop("verify")
    coord.scheduler_paused = True
    assert reasons() == {"scheduler_paused": 1}
    coord.scheduler_paused = False

    coord.leader_elector = Standby()
    assert reasons() == {"standby_coordinator": 1}


def test_waiting_ignores_non_pending_and_reports_oldest(coord, monkeypatch):
    set_workers(coord, monkeypatch, active=0, idle=0)
    add_job(coord, "old", "pending", age=500)
    add_job(coord, "new", "pending", age=20)
    add_job(coord, "run", "running", age=900)
    w = coord.observability.waiting_reasons()["by_reason"]["no_workers"]
    assert w["count"] == 2
    assert 499 < w["oldest_age_seconds"] < 520


# ------------------------------------------------------------------- SLIs
def test_sli_window_percentiles_and_failure_rate(coord):
    now = datetime.now(UTC)
    for i, wait in enumerate([10, 20, 30, 40]):
        j = add_job(coord, f"ok{i}", "completed", age=200)
        j["completed_at"] = (now - timedelta(seconds=5)).isoformat()
        j["first_dispatched_at"] = (now - timedelta(seconds=200) + timedelta(seconds=wait)).isoformat()
    f = add_job(coord, "bad", "failed", age=200)
    f["completed_at"] = (now - timedelta(seconds=5)).isoformat()
    # finished long before the window: must not count
    old = add_job(coord, "ancient", "completed", age=99999)
    old["completed_at"] = (now - timedelta(seconds=90000)).isoformat()

    s = coord.observability.sli()
    assert (s["completed"], s["failed"]) == (4, 1)
    assert s["failure_pct"] == pytest.approx(20.0)
    assert s["jobs_per_hour"] == pytest.approx(5.0)          # 5 jobs in a 1h window
    assert s["completion_p50_seconds"] == pytest.approx(195, abs=2)
    assert s["queue_wait_p50_seconds"] == pytest.approx(25, abs=1)   # median of 10,20,30,40
    assert s["queue_wait_p95_seconds"] == pytest.approx(38.5, abs=1)
    assert s["queue_wait_samples"] == 4


def test_sli_empty_is_none_not_zero(coord):
    s = coord.observability.sli()
    assert s["failure_pct"] is None and s["completion_p95_seconds"] is None


def test_first_dispatched_at_is_set_once_so_retries_dont_reset_queue_wait(coord):
    add_job(coord, "j", "pending")
    coord._count_dispatch_attempt("j", coord.jobs["j"])
    first = coord.jobs["j"]["first_dispatched_at"]
    assert coord.jobs["j"]["attempt_number"] == 1
    coord._count_dispatch_attempt("j", coord.jobs["j"])               # a retry
    assert coord.jobs["j"]["first_dispatched_at"] == first
    assert coord.jobs["j"]["attempt_number"] == 2


# ------------------------------------------------------- per customer / nodes
def test_per_org_counts_and_ranking(coord):
    add_job(coord, "a1", "running", org="acme"); add_job(coord, "a2", "pending", org="acme", age=60)
    add_job(coord, "b1", "failed", org="beta"); add_job(coord, "b2", "completed", org="beta")
    orgs = {o["org_id"]: o for o in coord.observability.per_org()}
    assert orgs["acme"]["running"] == 1 and orgs["acme"]["pending"] == 1
    assert orgs["acme"]["oldest_pending_age_seconds"] > 55
    assert orgs["beta"]["failure_pct"] == pytest.approx(50.0)
    assert coord.observability.per_org()[0]["org_id"] == "acme"   # busiest first


def test_node_trust_reports_quarantine_and_streak(coord, monkeypatch):
    monkeypatch.setattr(coord.registry, "nodes", {
        "n1": {"status": "idle", "quarantined": True, "quarantine_reason": "3 failures"},
        "n2": {"status": "busy"},
    })
    coord._node_verification_failure_streak["n1"] = 3
    nodes = {n["node_id"]: n for n in coord.observability.node_trust()}
    assert nodes["n1"]["quarantined"] and nodes["n1"]["verification_failure_streak"] == 3
    assert not nodes["n2"]["quarantined"]
    assert coord.observability.node_trust()[0]["node_id"] == "n1"     # quarantined first


# ------------------------------------------------- history + incident lifecycle
def test_tick_samples_when_due_only_and_not_on_standby(coord, monkeypatch):
    assert coord.observability.maybe_tick() is True
    assert coord.observability.maybe_tick() is False                 # not due yet
    assert len(coord.observability.history(5)) == 1

    coord.leader_elector = Standby()
    coord.observability._last_tick = 0
    assert coord.observability.maybe_tick() is False                 # standby never samples


def test_incident_opens_refreshes_once_and_resolves(coord, monkeypatch):
    set_workers(coord, monkeypatch, active=0, idle=0)
    add_job(coord, "waiting", "pending", age=10)
    inc = coord.control_plane.incidents

    coord.observability._last_tick = 0
    coord.observability.maybe_tick()
    first = {i["rule"]: i for i in inc.open_incidents()}
    assert first["no_workers"]["severity"] == "critical"

    coord.observability._last_tick = 0
    coord.observability.maybe_tick()
    again = [i for i in inc.open_incidents() if i["rule"] == "no_workers"]
    assert len(again) == 1                                            # one problem, one row
    assert again[0]["first_seen"] == first["no_workers"]["first_seen"]
    assert again[0]["last_seen"] >= first["no_workers"]["last_seen"]

    coord.jobs["waiting"]["status"] = "cancelled"                     # queue cleared
    coord.observability._last_tick = 0
    coord.observability.maybe_tick()
    assert "no_workers" not in {i["rule"] for i in inc.open_incidents()}
    assert "no_workers" in {i["rule"] for i in inc.recent_resolved()}


def test_queue_age_rule_uses_env_threshold(coord, monkeypatch):
    set_workers(coord, monkeypatch, active=2, idle=0)
    add_job(coord, "slow", "pending", age=120)
    snap = coord.observability.snapshot()
    monkeypatch.setenv("GCON_ALERT_QUEUE_AGE_SECONDS", "300")
    assert "queue_age" not in {f["rule"] for f in coord.observability.evaluate_rules(snap)}
    monkeypatch.setenv("GCON_ALERT_QUEUE_AGE_SECONDS", "60")
    assert "queue_age" in {f["rule"] for f in coord.observability.evaluate_rules(snap)}


def test_webhook_failure_rule_only_counts_recent_failures(coord):
    cp = coord.control_plane
    now = datetime.now(UTC)

    def insert(delivery_id, last_attempt):
        with cp.db.transaction() as conn:
            conn.execute(
                "INSERT INTO webhook_deliveries (delivery_id, url, secret, event_type, payload_json, "
                "status, attempt_count, created_at, last_attempt_at) "
                "VALUES (?, 'http://x', 's', 'job.completed', '{}', 'failed', 5, ?, ?)",
                (delivery_id, last_attempt.isoformat(), last_attempt.isoformat()),
            )
    insert("old", now - timedelta(days=3))
    rules = lambda: {f["rule"] for f in coord.observability.evaluate_rules(coord.observability.snapshot())}
    assert "webhook_failures" not in rules()                          # old terminal failure: not an incident
    insert("new", now - timedelta(minutes=2))
    assert "webhook_failures" in rules()


def test_history_prune_and_retries_summary(coord):
    cp = coord.control_plane
    cp.metric_snapshots.record((datetime.now(UTC) - timedelta(days=30)).isoformat(), {"pending": 1})
    cp.metric_snapshots.record(datetime.now(UTC).isoformat(), {"pending": 2})
    cp.metric_snapshots.prune((datetime.now(UTC) - timedelta(days=7)).isoformat())
    assert [s["pending"] for s in cp.metric_snapshots.since("2000-01-01T00:00:00+00:00")] == [2]
    assert cp.obs_queries.retry_summary()["total_attempts"] == 0


# --------------------------------------------------------------- endpoints
@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("GCON_DB_PATH", str(tmp_path / "gcon.db"))
    monkeypatch.setenv("GCON_OWNER_PASSWORD", "owner-pw-123")
    coordinator = GCONCoordinator(control_plane=ControlPlane(path=str(tmp_path / "cp.db")))
    server = WebServer(PresentationLayer(coordinator))
    with TestClient(server.app) as c:
        yield c, coordinator
    coordinator.shutdown()


def test_endpoints_require_login_and_return_expected_shapes(client):
    c, coordinator = client
    for path in ("/management/observability/summary", "/management/observability/history", "/management/incidents"):
        assert c.get(path).status_code in (401, 403)

    from tests.integration.smoke_management_gaps import OWNER_EMAIL
    r = c.post("/auth/login", json={"email": OWNER_EMAIL, "password": "owner-pw-123"})
    assert r.status_code == 200, r.text

    s = c.get("/management/observability/summary").json()
    assert set(s) >= {"waiting", "sli", "customers", "nodes", "database", "retries", "webhooks", "events"}
    assert s["database"]["available"] and s["database"]["schema_version"] >= 10

    coordinator.observability._last_tick = 0
    coordinator.observability.maybe_tick()
    h = c.get("/management/observability/history?minutes=5").json()
    assert h and "pending" in h[-1] and "taken_at" in h[-1] and "_waiting" not in h[-1]

    inc = c.get("/management/incidents").json()
    assert set(inc) == {"open", "resolved"}
