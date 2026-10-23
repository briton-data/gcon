"""Staff can clear finished noise off the dashboard: read notifications, resolved incidents, the live event
feed, and (Owner only) old audit entries. Nothing that is still open or unread is ever removed."""
from datetime import datetime, timedelta, UTC

from tests.management.test_shared_pool_admin import env  # noqa: F401  (fixture)


def _incident(cp, rule, subject, resolved):
    now = datetime.now(UTC).isoformat()
    cp.incidents.open_incident(rule, "warning", subject, f"{rule} on {subject}", {}, now)
    if resolved:
        cp.incidents.resolve_missing(set(), now)


def test_clear_resolved_incidents_keeps_open_ones(env):  # noqa: F811
    clients, server, coordinator, _org = env
    cp = coordinator.control_plane
    _incident(cp, "r1", "a", resolved=True)
    _incident(cp, "r2", "b", resolved=True)
    _incident(cp, "r3", "c", resolved=False)
    assert len(clients["Owner"].get("/management/incidents").json()["resolved"]) == 2
    r = clients["Owner"].post("/management/incidents/clear-resolved")
    assert r.status_code == 200 and r.json()["cleared"] == 2
    shown = clients["Owner"].get("/management/incidents").json()
    assert shown["resolved"] == [] and [i["rule"] for i in shown["open"]] == ["r3"]


def test_dismiss_one_resolved_incident_but_never_an_open_one(env):  # noqa: F811
    clients, _server, coordinator, _org = env
    cp = coordinator.control_plane
    _incident(cp, "r1", "a", resolved=True)
    _incident(cp, "r2", "b", resolved=False)
    resolved_id = cp.incidents.recent_resolved()[0]["incident_id"]
    open_id = cp.incidents.open_incidents()[0]["incident_id"]
    assert clients["Owner"].post(f"/management/incidents/{open_id}/dismiss").status_code == 400
    assert clients["Owner"].post(f"/management/incidents/{resolved_id}/dismiss").status_code == 200
    assert cp.incidents.recent_resolved() == [] and len(cp.incidents.open_incidents()) == 1


def test_clear_read_notifications_keeps_unread(env):  # noqa: F811
    clients, server, _c, _org = env
    center = server.management.notification_center
    keep = center.notify("job_failed", "unread one")
    gone = center.notify("job_failed", "read one")
    center.mark_read(gone["notification_id"])
    assert clients["Owner"].post("/management/notifications/clear-read").json()["cleared"] == 1
    messages = [e["message"] for e in center.list_entries()]
    assert "unread one" in messages and "read one" not in messages
    assert clients["Owner"].post(f"/management/notifications/{keep['notification_id']}/dismiss").status_code == 200
    assert "unread one" not in [e["message"] for e in center.list_entries()]
    assert clients["Owner"].post("/management/notifications/notif_nope/dismiss").status_code == 404


def test_clear_events_hides_the_feed_but_keeps_the_history(env):  # noqa: F811
    clients, server, coordinator, _org = env
    coordinator.registry.register(__import__("types").SimpleNamespace(
        node_id="n1", org_id=None, sandboxed=True, agent_version=None, status="idle"))
    from gcon.events.event import Event
    from gcon.events.event_types import EventType
    coordinator.event_bus.publish(Event(timestamp=datetime.now(UTC) - timedelta(seconds=5),
                                        event_type=EventType.NODE_DEREGISTERED, source="t", payload={"node_id": "x"}))
    assert clients["Owner"].get("/events").json()
    assert clients["Owner"].post("/management/events/clear").json()["cleared"] >= 1
    assert clients["Owner"].get("/events").json() == []
    assert coordinator.get_all_events()                      # still there for the code that reads the history


def test_only_an_owner_can_clear_audit_entries_and_it_is_recorded(env):  # noqa: F811
    clients, server, _c, _org = env
    logger = server.management.audit_logger
    old = (datetime.now(UTC) - timedelta(days=90)).isoformat()
    with logger.db.transaction() as conn:
        conn.execute("INSERT INTO audit_log (entry_id, actor, action, target, timestamp, seq) "
                     "VALUES ('audit_old', 'x', 'ancient action', NULL, ?, 0)", (old,))
    logger.entries.insert(0, {"entry_id": "audit_old", "actor": "x", "action": "ancient action",
                              "target": None, "timestamp": old})
    assert clients["Administrator"].post("/management/audit-logs/clear", json={"older_than_days": 30}).status_code == 403
    assert clients["Owner"].post("/management/audit-logs/clear", json={"older_than_days": -1}).status_code == 400
    r = clients["Owner"].post("/management/audit-logs/clear", json={"older_than_days": 30})
    assert r.status_code == 200 and r.json()["cleared"] == 1
    actions = [e["action"] for e in logger.list_entries(500)]
    assert "ancient action" not in actions
    assert any("cleared 1 audit log entries older than 30 day" in a for a in actions)


def test_an_operator_cannot_clear_anything(env):  # noqa: F811
    clients, _server, _c, _org = env
    for path in ("/management/incidents/clear-resolved", "/management/notifications/clear-read",
                 "/management/events/clear"):
        assert clients["Operator"].post(path).status_code in (401, 403)
