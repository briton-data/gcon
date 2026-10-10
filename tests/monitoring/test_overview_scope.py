"""
Backend support for the Overview redesign: incident ownership, incident
impact text, webhook delivery latency, grouped events, and the region/
environment scope. Same approach as test_observability.py -- state is set
directly so each case is a precisely known situation.
"""
from datetime import datetime, timedelta, UTC

import pytest
from fastapi.testclient import TestClient

from gcon.cluster.coordinator import GCONCoordinator
from gcon.dashboard.presentation import PresentationLayer
from gcon.dashboard.web_server import WebServer
from gcon.events.event import Event
from gcon.persistence import ControlPlane


@pytest.fixture
def coord(tmp_path):
    c = GCONCoordinator(control_plane=ControlPlane(path=str(tmp_path / "t.db")))
    yield c
    c.shutdown()


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("GCON_DB_PATH", str(tmp_path / "gcon.db"))
    monkeypatch.setenv("GCON_OWNER_PASSWORD", "owner-pw-123")
    coordinator = GCONCoordinator(control_plane=ControlPlane(path=str(tmp_path / "cp.db")))
    server = WebServer(PresentationLayer(coordinator))
    with TestClient(server.app) as c:
        yield c, coordinator
    coordinator.shutdown()


def _login(c):
    from tests.integration.smoke_management_gaps import OWNER_EMAIL
    r = c.post("/auth/login", json={"email": OWNER_EMAIL, "password": "owner-pw-123"})
    assert r.status_code == 200, r.text


def _open_no_workers_incident(coordinator, monkeypatch):
    monkeypatch.setattr(coordinator.registry, "nodes", {})
    coordinator.submit_job("waiting", "echo", org_id="acme")
    coordinator.jobs["waiting"]["status"] = "pending"
    coordinator.observability._last_tick = 0
    coordinator.observability.maybe_tick()
    return next(i for i in coordinator.control_plane.incidents.open_incidents() if i["rule"] == "no_workers")


# ------------------------------------------------------------------ ownership
def test_incident_starts_unowned_and_has_impact(coord, monkeypatch):
    inc = _open_no_workers_incident(coord, monkeypatch)
    assert inc["owner"] is None and inc["owner_at"] is None
    assert inc["detail"]["impact"] == "1 job(s) cannot start"


def test_owner_survives_sampling_ticks_and_release_clears_it(coord, monkeypatch):
    inc = _open_no_workers_incident(coord, monkeypatch)
    repo = coord.control_plane.incidents
    assert repo.set_owner(inc["incident_id"], "ops@example.com", datetime.now(UTC).isoformat()) is True

    coord.observability._last_tick = 0
    coord.observability.maybe_tick()                        # refreshes the same row
    again = next(i for i in repo.open_incidents() if i["rule"] == "no_workers")
    assert again["incident_id"] == inc["incident_id"]
    assert again["owner"] == "ops@example.com" and again["owner_at"]

    assert repo.set_owner(inc["incident_id"], None, datetime.now(UTC).isoformat()) is True
    cleared = next(i for i in repo.open_incidents() if i["rule"] == "no_workers")
    assert cleared["owner"] is None and cleared["owner_at"] is None


def test_cannot_own_a_resolved_or_unknown_incident(coord, monkeypatch):
    inc = _open_no_workers_incident(coord, monkeypatch)
    coord.jobs["waiting"]["status"] = "cancelled"
    coord.observability._last_tick = 0
    coord.observability.maybe_tick()                        # resolves it
    repo = coord.control_plane.incidents
    assert repo.set_owner(inc["incident_id"], "ops@example.com", "x") is False
    assert repo.set_owner("does-not-exist", "ops@example.com", "x") is False


def test_claim_release_endpoints(client, monkeypatch):
    c, coordinator = client
    inc = _open_no_workers_incident(coordinator, monkeypatch)
    url = f"/management/incidents/{inc['incident_id']}"

    assert c.post(url + "/claim").status_code in (401, 403)          # login required
    _login(c)
    assert c.post(url + "/claim").status_code == 200
    open_now = c.get("/management/incidents").json()["open"]
    mine = next(i for i in open_now if i["incident_id"] == inc["incident_id"])
    assert mine["owner"] == "GCON Owner"        # the bootstrap owner has no username; never the email

    assert c.post(url + "/release").status_code == 200
    open_now = c.get("/management/incidents").json()["open"]
    assert next(i for i in open_now if i["incident_id"] == inc["incident_id"])["owner"] is None
    assert c.post("/management/incidents/nope/claim").status_code == 404


# ----------------------------------------------------------- delivery latency
def _insert_delivery(cp, delivery_id, status, created, last_attempt):
    with cp.db.transaction() as conn:
        conn.execute(
            "INSERT INTO webhook_deliveries (delivery_id, url, secret, event_type, payload_json, "
            "status, attempt_count, created_at, last_attempt_at) "
            "VALUES (?, 'http://x', 's', 'job.completed', '{}', ?, 1, ?, ?)",
            (delivery_id, status, created.isoformat(), last_attempt.isoformat()),
        )


def test_webhook_latency_is_queue_to_successful_attempt(coord):
    now = datetime.now(UTC)
    for i, secs in enumerate((2, 4, 6, 8)):
        _insert_delivery(coord.control_plane, f"ok{i}", "success",
                         now - timedelta(minutes=5, seconds=secs), now - timedelta(minutes=5))
    _insert_delivery(coord.control_plane, "bad", "failed", now - timedelta(minutes=9), now - timedelta(minutes=5))
    _insert_delivery(coord.control_plane, "old", "success", now - timedelta(days=2, seconds=500), now - timedelta(days=2))

    w = coord.observability.summary(window_seconds=3600)["webhooks"]
    assert w["delivered_since"] == 4                        # failed + out-of-window rows excluded
    assert w["latency_p50_seconds"] == pytest.approx(5.0, abs=0.01)
    assert w["latency_p95_seconds"] == pytest.approx(7.7, abs=0.01)

    # A wider window pulls in the old delivery too.
    wide = coord.observability.summary(window_seconds=3 * 86400)["webhooks"]
    assert wide["delivered_since"] == 5


def test_webhook_latency_is_none_not_zero_when_nothing_delivered(coord):
    w = coord.observability.summary()["webhooks"]
    assert w["delivered_since"] == 0
    assert w["latency_p50_seconds"] is None and w["latency_p95_seconds"] is None


# --------------------------------------------------------------- event groups
def test_event_groups_roll_up_by_type_and_source_within_window(coord):
    for _ in range(5):
        coord.event_bus.publish(Event("HEALTH_DEGRADED", "HealthService", {}))
    for _ in range(3):
        coord.event_bus.publish(Event("HEALTH_CRITICAL", "HealthService", {}))
    coord.event_bus.publish(Event("NODE_ONLINE", "registry", {"node_id": "n1"}))
    stale = Event("HEALTH_DEGRADED", "HealthService", {})
    stale.timestamp = datetime.now(UTC) - timedelta(hours=3)
    coord.event_bus.publish(stale)

    g = coord.observability.event_groups(minutes=60)
    assert g["scanned_all"] is True
    by = {(x["event_type"], x["source"]): x["count"] for x in g["groups"]}
    assert by[("HEALTH_DEGRADED", "HealthService")] == 5      # the 3h-old one is outside the window
    assert by[("HEALTH_CRITICAL", "HealthService")] == 3
    assert g["total_events"] == 9 and g["group_count"] == 3
    assert g["groups"][0]["count"] == 5                       # busiest first

    assert len(coord.observability.event_groups(minutes=60, limit=1)["groups"]) == 1


# --------------------------------------------------------------- region scope
def test_region_scope_filters_customers_by_where_the_job_ran(coord, monkeypatch):
    # Submit first (no workers yet, so nothing is dispatched), then expose the workers.
    for job_id, org, node in (("j1", "acme", "eu1"), ("j2", "acme", "us1"), ("j3", "globex", "us1"), ("j4", "globex", None)):
        coord.submit_job(job_id, "echo", org_id=org)
        coord.jobs[job_id]["status"] = "completed" if node else "pending"
        coord.jobs[job_id]["node_id"] = node
    monkeypatch.setattr(coord.registry, "nodes", {"eu1": {"status": "busy"}, "us1": {"status": "busy"}, "bare": {"status": "busy"}})
    cp = coord.control_plane
    for node_id in ("eu1", "us1", "bare"):
        cp.nodes.upsert(node_id, node_id, status="idle")
    cp.node_capabilities.set_capabilities("eu1", {"region": "eu-west"})
    cp.node_capabilities.set_capabilities("us1", {"region": "us-east"})

    opts = coord.observability.scope_options()
    assert opts["regions"] == ["eu-west", "us-east"] and opts["unlabelled_workers"] == 1

    everyone = {o["org_id"]: o["completed"] + o["pending"] for o in coord.observability.summary()["customers"]}
    assert everyone == {"acme": 2, "globex": 2}

    eu = {o["org_id"]: o["completed"] for o in coord.observability.summary(region="eu-west")["customers"]}
    assert eu == {"acme": 1}                                  # globex never ran in eu-west; waiting jobs have no region
    us = {o["org_id"]: o["completed"] for o in coord.observability.summary(region="us-east")["customers"]}
    assert us == {"acme": 1, "globex": 1}


def test_environment_label_comes_from_config_only(coord, monkeypatch):
    monkeypatch.delenv("GCON_ENVIRONMENT", raising=False)
    assert coord.observability.scope_options()["environment"] is None
    monkeypatch.setenv("GCON_ENVIRONMENT", "staging")
    assert coord.observability.scope_options()["environment"] == "staging"


def test_summary_and_event_group_endpoints(client):
    c, coordinator = client
    for path in ("/management/event-groups", "/management/observability/summary?region=x"):
        assert c.get(path).status_code in (401, 403)
    _login(c)
    coordinator.event_bus.publish(Event("HEALTH_DEGRADED", "HealthService", {}))
    g = c.get("/management/event-groups?minutes=30").json()
    assert g["groups"][0]["event_type"] == "HEALTH_DEGRADED"
    s = c.get("/management/observability/summary?minutes=30&region=none").json()
    assert s["customers"] == [] and "scope" in s


def test_history_long_window_keeps_the_recent_end(coord):
    cp = coord.control_plane
    base = datetime.now(UTC) - timedelta(hours=9)
    with cp.db.transaction() as conn:
        for i in range(3000):                                  # more rows than the default limit
            ts = (base + timedelta(seconds=i * 10)).isoformat()
            conn.execute("INSERT INTO metric_snapshots (snapshot_id, taken_at, data_json) VALUES (?, ?, ?)",
                         (f"s{i}", ts, '{"pending": %d}' % i))
    h = coord.observability.history(since_minutes=600)
    assert len(h) == 2000
    assert h[0]["pending"] == 0 and h[-1]["pending"] == 2999   # both ends of the window survive
    assert [p["pending"] for p in h] == sorted(p["pending"] for p in h)
    assert len(coord.observability.history(since_minutes=600, limit=5000)) == 3000   # under the limit: untouched
