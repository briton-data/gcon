"""Only GCON's Owner or Administrator can change the shared-pool settings, over
the logged-in staff API, and every change is audit-logged. An API key -- which
can leak -- can do none of it."""
import pytest
from fastapi.testclient import TestClient

from gcon.cluster.coordinator import GCONCoordinator
from gcon.dashboard.presentation import PresentationLayer
from gcon.dashboard.web_server import WebServer
from gcon.persistence import ControlPlane

OWNER_PW = "owner-pw-123"


@pytest.fixture
def env(tmp_path, monkeypatch):
    monkeypatch.setenv("GCON_DB_PATH", str(tmp_path / "gcon.db"))
    monkeypatch.setenv("GCON_OWNER_PASSWORD", OWNER_PW)
    coordinator = GCONCoordinator(control_plane=ControlPlane(path=str(tmp_path / "cp.db")))
    server = WebServer(PresentationLayer(coordinator))
    from tests.integration.smoke_management_gaps import OWNER_EMAIL
    clients = {}
    with TestClient(server.app) as owner:
        assert owner.post("/auth/login", json={"email": OWNER_EMAIL, "password": OWNER_PW}).status_code == 200
        clients["Owner"] = owner
        for role in ("Administrator", "Operator", "Developer", "Viewer"):
            email = f"{role.lower()}@gcon.example"
            made = owner.post("/management/users", json={
                "name": role, "email": email, "role": role, "password": "role-pw-12345"})
            assert made.status_code == 200, made.text
            c = TestClient(server.app)
            assert c.post("/auth/login", json={"email": email, "password": "role-pw-12345"}).status_code == 200
            clients[role] = c
        org = owner.post("/management/organizations", json={"name": "Acme"}).json()
        yield clients, server, coordinator, org["org_id"]
    for role, c in clients.items():
        if role != "Owner":
            c.close()
    coordinator.shutdown()


@pytest.mark.parametrize("role", ["Owner", "Administrator"])
def test_owner_and_administrator_can_set_an_orgs_pool_mode(env, role):
    clients, server, coordinator, org_id = env
    r = clients[role].put(f"/management/organizations/{org_id}/shared-pool", json={"mode": "full"})
    assert r.status_code == 200, r.text
    assert r.json()["mode"] == "full" and r.json()["previous_mode"] == "basic"
    assert coordinator.control_plane.org_pool_settings.get_mode(org_id) == "full"


@pytest.mark.parametrize("role", ["Operator", "Developer", "Viewer"])
def test_other_staff_roles_cannot_change_the_pool_mode(env, role):
    clients, _server, coordinator, org_id = env
    r = clients[role].put(f"/management/organizations/{org_id}/shared-pool", json={"mode": "full"})
    assert r.status_code == 403
    assert coordinator.control_plane.org_pool_settings.get_mode(org_id) == "basic"


def test_an_api_key_cannot_reach_the_staff_route(env):
    _clients, server, coordinator, org_id = env
    anonymous = TestClient(server.app)
    r = anonymous.put(f"/management/organizations/{org_id}/shared-pool", json={"mode": "off"},
                      headers={"Authorization": "Bearer not-a-session"})
    assert r.status_code in (401, 403)
    assert coordinator.control_plane.org_pool_settings.get_mode(org_id) == "basic"


def test_the_role_is_enforced_in_the_management_layer_itself(env):
    clients, server, _c, org_id = env
    operator = server.management.user_registry.list_users()
    operator = next(u for u in operator if u.role == "Operator")
    with pytest.raises(PermissionError):
        server.management.set_org_shared_pool_mode(org_id, "off", actor=operator)
    with pytest.raises(PermissionError):
        server.management.set_org_shared_pool_mode(org_id, "off", actor=None)


def test_a_change_is_written_to_the_audit_log_with_who_and_what(env):
    clients, server, _c, org_id = env
    clients["Administrator"].put(f"/management/organizations/{org_id}/shared-pool", json={"mode": "off"})
    entries = [e for e in server.management.get_audit_logs() if "shared-pool" in e["action"]]
    assert entries, "no audit entry written"
    assert entries[0]["actor"] == "Administrator"
    assert "basic -> off" in entries[0]["action"] and entries[0]["target"] == "Acme"


def test_invalid_mode_and_unknown_org_are_rejected(env):
    clients, _s, coordinator, org_id = env
    assert clients["Owner"].put(f"/management/organizations/{org_id}/shared-pool", json={"mode": "yes"}).status_code == 400
    assert clients["Owner"].put("/management/organizations/org_nope/shared-pool", json={"mode": "off"}).status_code == 400
    assert coordinator.control_plane.org_pool_settings.get_mode(org_id) == "basic"


def test_putting_a_worker_in_the_pool_is_owner_admin_only_and_audited(env):
    clients, server, coordinator, _org = env
    coordinator.control_plane.nodes.upsert("w-pool", "host", status="offline")
    assert clients["Operator"].put("/management/nodes/w-pool/shared-pool", json={"enabled": True}).status_code == 403
    assert not coordinator.control_plane.shared_pool_nodes.is_member("w-pool")
    r = clients["Administrator"].put("/management/nodes/w-pool/shared-pool", json={"enabled": True})
    assert r.status_code == 200 and r.json() == {"node_id": "w-pool", "shared_pool": True}
    assert coordinator.control_plane.shared_pool_nodes.is_member("w-pool")
    assert any(e["target"] == "w-pool" and "added worker" in e["action"] for e in server.management.get_audit_logs())
    assert clients["Owner"].put("/management/nodes/w-pool/shared-pool", json={"enabled": False}).status_code == 200
    assert not coordinator.control_plane.shared_pool_nodes.is_member("w-pool")


def test_an_org_bound_or_unknown_worker_cannot_be_put_in_the_pool(env):
    clients, _s, coordinator, _org = env
    coordinator.control_plane.nodes.upsert("acme-w", "host", status="offline", org_id="org_acme")
    assert clients["Owner"].put("/management/nodes/acme-w/shared-pool", json={"enabled": True}).status_code == 400
    assert clients["Owner"].put("/management/nodes/ghost/shared-pool", json={"enabled": True}).status_code == 400
    assert not coordinator.control_plane.shared_pool_nodes.is_member("acme-w")
