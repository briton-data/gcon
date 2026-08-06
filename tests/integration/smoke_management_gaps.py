"""
Live end-to-end smoke test for the Management module gap-closing
work (admin password reset, real active-session count, admin session
controls, team member management, admin unlock).
"""

import pytest
from fastapi.testclient import TestClient

from gcon.cluster.coordinator import GCONCoordinator
from gcon.dashboard.presentation import PresentationLayer
from gcon.dashboard.web_server import WebServer
from gcon.management.rate_limit import MAX_ATTEMPTS


OWNER_EMAIL = "nyongesabriton620@gmail.com"  # BOOTSTRAP_OWNER_EMAIL default (module-level, resolved at import)


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("GCON_DB_PATH", str(tmp_path / "gcon.db"))
    monkeypatch.setenv("GCON_OWNER_PASSWORD", "owner-pw-123")
    coordinator = GCONCoordinator()
    presentation = PresentationLayer(coordinator)
    server = WebServer(presentation)
    with TestClient(server.app) as c:
        yield c
    coordinator.shutdown()


def login(client, email=OWNER_EMAIL, password="owner-pw-123"):
    resp = client.post("/auth/login", json={"email": email, "password": password})
    assert resp.status_code == 200, resp.text
    return resp


def test_issue1_admin_password_reset_end_to_end(client):
    login(client)

    created = client.post("/management/users", json={
        "name": "Reset Target", "email": "reset@x.test", "role": "Developer",
    })
    assert created.status_code == 200, created.text
    user_id = created.json()["user_id"]

    # No password was set at creation, so login correctly fails.
    target_client = TestClient(client.app)
    assert target_client.post("/auth/login", json={
        "email": "reset@x.test", "password": "whatever",
    }).status_code == 401

    resp = client.post(f"/management/users/{user_id}/reset-password",
                        json={"password": "new-pw-456"})
    assert resp.status_code == 200, resp.text

    # New password now works.
    fresh_login = TestClient(client.app)
    ok = fresh_login.post("/auth/login", json={
        "email": "reset@x.test", "password": "new-pw-456",
    })
    assert ok.status_code == 200, ok.text

    # Audit-logged.
    logs = client.get("/management/audit-logs").json()
    assert any(e["action"] == "set password for" and e["target"] == "Reset Target" for e in logs)


def test_issue2_real_active_session_count(client):
    login(client)
    before = client.get("/management/dashboard-cards").json()["active_sessions"]
    assert before >= 1  # the owner session just created

    client.post("/management/users", json={
        "name": "Second", "email": "second@x.test", "role": "Viewer", "password": "pw-789012",
    })
    other_client = TestClient(client.app)
    login(other_client, "second@x.test", "pw-789012")

    after = client.get("/management/dashboard-cards").json()["active_sessions"]
    assert after == before + 1


def test_issue3_admin_session_controls(client):
    login(client)

    created = client.post("/management/users", json={
        "name": "Sessioned", "email": "sess@x.test", "role": "Developer", "password": "pw-abcdef",
    })
    user_id = created.json()["user_id"]

    target_client = TestClient(client.app)
    login(target_client, "sess@x.test", "pw-abcdef")

    sessions = client.get(f"/management/users/{user_id}/sessions").json()
    assert len(sessions) == 1
    assert "session_id" in sessions[0] and "created_at" in sessions[0]
    assert "token" not in str(sessions[0])  # no raw token leaked

    resp = client.post(f"/management/users/{user_id}/force-logout")
    assert resp.status_code == 200

    # Target's existing session is now dead.
    me = target_client.get("/auth/me")
    assert me.status_code == 401

    sessions_after = client.get(f"/management/users/{user_id}/sessions").json()
    assert sessions_after == []


def test_issue4_team_member_management_end_to_end(client):
    login(client)

    org = client.post("/management/organizations", json={"name": "Acme"}).json()
    u1 = client.post("/management/users", json={
        "name": "Alice", "email": "alice@x.test", "role": "Developer",
        "organization_id": org["org_id"],
    }).json()
    u2 = client.post("/management/users", json={
        "name": "Bob", "email": "bob@x.test", "role": "Developer",
        "organization_id": org["org_id"],
    }).json()
    team = client.post("/management/teams", json={
        "org_id": org["org_id"], "name": "Platform",
    }).json()

    add1 = client.post(f"/management/teams/{team['team_id']}/members", json={"user_id": u1["user_id"]})
    assert add1.status_code == 200, add1.text
    add2 = client.post(f"/management/teams/{team['team_id']}/members", json={"user_id": u2["user_id"]})
    assert add2.status_code == 200, add2.text

    fetched = client.get(f"/management/teams/{team['team_id']}").json()
    assert set(fetched["member_ids"]) == {u1["user_id"], u2["user_id"]}
    assert fetched["member_count"] == 2

    rm = client.delete(f"/management/teams/{team['team_id']}/members/{u1['user_id']}")
    assert rm.status_code == 200, rm.text
    fetched2 = client.get(f"/management/teams/{team['team_id']}").json()
    assert fetched2["member_ids"] == [u2["user_id"]]
    assert fetched2["member_count"] == 1

    # Deleting a user reflects in team membership.
    client.delete(f"/management/users/{u2['user_id']}")
    fetched3 = client.get(f"/management/teams/{team['team_id']}").json()
    assert fetched3["member_count"] == 0

    # Org member count reflects reality (u1, u2 both still org members except deleted one).
    org_after = client.get(f"/management/organizations/{org['org_id']}").json()
    assert org_after["member_count"] == 1  # only Alice remains


def test_issue4_org_assignment_then_team_member_management_end_to_end(client):
    """
    Reproduces the real-world gap: users created without picking an
    org at creation time (or via bootstrap) had no way to be
    assigned to one afterward, which meant they could never show up
    as team "add member" candidates. The Settings tab's new
    Organization field (PUT /management/users/{id} with
    organization_id) has to fix that end-to-end.
    """
    login(client)

    org = client.post("/management/organizations", json={"name": "Acme"}).json()

    # Created with NO organization_id -- this is the state that broke things.
    u1 = client.post("/management/users", json={
        "name": "Alice", "email": "alice2@x.test", "role": "Developer",
    }).json()
    assert u1["organization_id"] is None

    team = client.post("/management/teams", json={
        "org_id": org["org_id"], "name": "Platform 2",
    }).json()

    # Before assignment: Alice has no org, so she's correctly NOT a
    # valid candidate for this org's team yet.
    fetched_user = client.get(f"/management/users/{u1['user_id']}").json()
    assert fetched_user["organization_id"] != org["org_id"]

    # Assign her to the org via the same endpoint the drawer's Save
    # Changes button now calls.
    assign = client.put(f"/management/users/{u1['user_id']}", json={
        "status": "Active", "role": "Developer", "organization_id": org["org_id"],
    })
    assert assign.status_code == 200, assign.text
    assert assign.json()["organization_id"] == org["org_id"]

    # Now she can be added to the team.
    add = client.post(f"/management/teams/{team['team_id']}/members", json={"user_id": u1["user_id"]})
    assert add.status_code == 200, add.text
    fetched_team = client.get(f"/management/teams/{team['team_id']}").json()
    assert u1["user_id"] in fetched_team["member_ids"]

    # Clearing the org back to "" (the drawer's "No organization"
    # sentinel) actually clears it, rather than being silently
    # skipped like a real None would be.
    clear = client.put(f"/management/users/{u1['user_id']}", json={
        "status": "Active", "role": "Developer", "organization_id": "",
    })
    assert clear.status_code == 200, clear.text
    assert clear.json()["organization_id"] == ""


def test_issue2b_user_metric_cards_reflect_reality(client):
    """
    Reproduces the reported bug: the Users tab's Total/Active/
    Pending/Suspended cards stayed at 0 forever because loadUsersTab
    wrote to element ids (uc-total-users, etc.) that don't exist
    anywhere in dashboard.html -- the real ids are users-metric-*.
    /management/user-counts is the endpoint the (now-fixed) frontend
    reads from, so this locks in that its numbers are correct and
    split into the four buckets the template actually renders.
    """
    login(client)

    client.post("/management/users", json={
        "name": "Active One", "email": "active-one@x.test", "role": "Viewer", "password": "pw-111111",
    })
    pending_user = client.post("/management/users", json={
        "name": "Pending One", "email": "pending-one@x.test", "role": "Viewer",
    }).json()
    client.put(f"/management/users/{pending_user['user_id']}", json={"status": "Pending"})
    suspended_user = client.post("/management/users", json={
        "name": "Suspended One", "email": "suspended-one@x.test", "role": "Viewer", "password": "pw-222222",
    }).json()
    client.put(f"/management/users/{suspended_user['user_id']}", json={"status": "Suspended"})

    counts = client.get("/management/user-counts").json()
    # owner + Active One = 2 active; Pending One = 1 pending; Suspended One = 1 inactive
    assert counts["active"] == 2
    assert counts["pending"] == 1
    assert counts["inactive"] == 1
    assert counts["total"] == counts["active"] + counts["pending"] + counts["inactive"]


def test_issue1b_no_password_users_are_visible_and_fixable(client):
    """
    Reproduces the reported "Active user can't log in" case: a user
    created without a password (has_password=False) looks perfectly
    normal (status Active) but can never log in until an admin resets
    it -- which is exactly what the Reset Password action is for.
    """
    login(client)

    created = client.post("/management/users", json={
        "name": "No Password Yet", "email": "nopass@x.test", "role": "Operator",
    })
    assert created.status_code == 200, created.text
    user = created.json()
    assert user["has_password"] is False
    assert user["status"] == "Active"

    # Confirmed unable to log in with any password while unset.
    locked_out_login = TestClient(client.app).post("/auth/login", json={
        "email": "nopass@x.test", "password": "anything",
    })
    assert locked_out_login.status_code == 401

    # Admin resets it via the same action now wired in the Users tab.
    reset = client.post(f"/management/users/{user['user_id']}/reset-password",
                         json={"password": "now-set-123"})
    assert reset.status_code == 200, reset.text

    now_has_password = client.get(f"/management/users/{user['user_id']}").json()
    assert now_has_password["has_password"] is True

    can_login = TestClient(client.app).post("/auth/login", json={
        "email": "nopass@x.test", "password": "now-set-123",
    })
    assert can_login.status_code == 200, can_login.text


def test_issue5_admin_unlock_end_to_end(client):
    login(client)

    client.post("/management/users", json={
        "name": "Lockout Target", "email": "locked@x.test", "role": "Viewer", "password": "correct-pw-1",
    })

    locked_client = TestClient(client.app)
    for _ in range(MAX_ATTEMPTS):
        locked_client.post("/auth/login", json={"email": "locked@x.test", "password": "wrong"})

    # Now locked out even with the correct password.
    still_locked = locked_client.post("/auth/login", json={
        "email": "locked@x.test", "password": "correct-pw-1",
    })
    assert still_locked.status_code == 429

    locked_ids = client.get("/management/locked-users").json()
    users = {u["email"]: u["user_id"] for u in client.get("/management/users").json()}
    target_id = users["locked@x.test"]
    assert target_id in locked_ids

    unlock = client.post(f"/management/users/{target_id}/unlock")
    assert unlock.status_code == 200, unlock.text

    locked_ids_after = client.get("/management/locked-users").json()
    assert target_id not in locked_ids_after

    ok = locked_client.post("/auth/login", json={
        "email": "locked@x.test", "password": "correct-pw-1",
    })
    assert ok.status_code == 200, ok.text


def test_role_dashboard_permissions_end_to_end(client):
    """
    Reproduces the reported gap: every role saw every sidebar tab
    regardless of what they're actually allowed to do. /auth/me now
    carries the real permission list for the user's role (single
    source of truth, from rbac.ROLE_PERMISSIONS), which is what the
    now-gated sidebar reads to decide what to show. This locks in
    that the permission list is correct and that the backend
    actually rejects an Operator hitting Administration/Users-only
    routes it shouldn't be able to reach in the first place --
    hiding the tab is a UX improvement on top of a real server-side
    gate, not a replacement for one.
    """
    login(client)

    operator = client.post("/management/users", json={
        "name": "Op", "email": "op@x.test", "role": "Operator", "password": "pw-333333",
    }).json()

    op_client = TestClient(client.app)
    login(op_client, "op@x.test", "pw-333333")

    me = op_client.get("/auth/me").json()
    assert set(me["permissions"]) == {"Submit workflows", "View monitoring", "Access analytics"}
    assert "Manage cluster" not in me["permissions"]
    assert "Manage users" not in me["permissions"]

    # Operator has no "Manage cluster" -> Administration's routes reject it.
    assert op_client.get("/admin/config").status_code == 403
    assert op_client.post("/cluster/scheduler/pause").status_code == 403

    # Operator has no "Manage users" -> Management-group routes reject it.
    assert op_client.get("/management/users").status_code == 403

    # Operator DOES have "Access analytics" -> that tab's route works.
    assert op_client.get("/analytics").status_code == 200

    developer = client.post("/management/users", json={
        "name": "Dev", "email": "dev@x.test", "role": "Developer", "password": "pw-444444",
    }).json()
    dev_client = TestClient(client.app)
    login(dev_client, "dev@x.test", "pw-444444")

    # Developer lacks "Access analytics" -- previously ungated, now enforced.
    assert dev_client.get("/analytics").status_code == 403
