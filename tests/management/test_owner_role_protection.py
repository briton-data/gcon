"""An Administrator has "Manage users" but must not be able to grant the Owner
role or take over an Owner account. Exercised over HTTP, the way a real
Administrator session would hit it."""
import pytest
from fastapi.testclient import TestClient

from gcon.cluster.coordinator import GCONCoordinator
from gcon.dashboard.presentation import PresentationLayer
from gcon.dashboard.web_server import WebServer
from gcon.persistence import ControlPlane

ADMIN_EMAIL, ADMIN_PW = "admin@gcon.example", "admin-pw-12345"


@pytest.fixture
def env(tmp_path, monkeypatch):
    monkeypatch.setenv("GCON_DB_PATH", str(tmp_path / "gcon.db"))
    monkeypatch.setenv("GCON_OWNER_PASSWORD", "owner-pw-123")
    coordinator = GCONCoordinator(control_plane=ControlPlane(path=str(tmp_path / "cp.db")))
    server = WebServer(PresentationLayer(coordinator))
    with TestClient(server.app) as owner, TestClient(server.app) as admin:
        from tests.integration.smoke_management_gaps import OWNER_EMAIL
        assert owner.post("/auth/login", json={"email": OWNER_EMAIL, "password": "owner-pw-123"}).status_code == 200
        created = owner.post("/management/users", json={
            "name": "Adm", "email": ADMIN_EMAIL, "role": "Administrator", "password": ADMIN_PW})
        assert created.status_code == 200, created.text
        assert admin.post("/auth/login", json={"email": ADMIN_EMAIL, "password": ADMIN_PW}).status_code == 200
        owner_id = next(u["user_id"] for u in owner.get("/management/users").json() if u["role"] == "Owner")
        yield owner, admin, created.json()["user_id"], owner_id, server
    coordinator.shutdown()


def test_admin_cannot_promote_self_to_owner(env):
    _, admin, admin_id, _, server = env
    r = admin.put(f"/management/users/{admin_id}", json={"role": "Owner"})
    assert r.status_code == 403
    assert server.management.user_registry.get_user(admin_id).role == "Administrator"


def test_admin_cannot_create_an_owner(env):
    _, admin, *_ = env
    r = admin.post("/management/users", json={"name": "X", "email": "x@gcon.example", "role": "Owner"})
    assert r.status_code == 403


def test_admin_cannot_touch_an_existing_owner(env):
    _, admin, _, owner_id, server = env
    pw_before = server.management.user_registry.get_user(owner_id).password_hash
    assert admin.post(f"/management/users/{owner_id}/reset-password", json={"password": "pwned-pw-1234"}).status_code == 403
    assert admin.put(f"/management/users/{owner_id}", json={"role": "Viewer"}).status_code == 403
    assert admin.post(f"/management/users/{owner_id}/status", json={"status": "Disabled"}).status_code == 403
    assert admin.post(f"/management/users/{owner_id}/force-logout").status_code == 403
    assert admin.delete(f"/management/users/{owner_id}").status_code == 403
    owner = server.management.user_registry.get_user(owner_id)
    assert owner.role == "Owner" and owner.status == "Active" and owner.password_hash == pw_before


def test_admin_can_still_manage_non_owners(env):
    _, admin, *_ = env
    made = admin.post("/management/users", json={"name": "Dev", "email": "dev@gcon.example", "role": "Developer"})
    assert made.status_code == 200
    uid = made.json()["user_id"]
    assert admin.put(f"/management/users/{uid}", json={"role": "Operator"}).json()["role"] == "Operator"
    assert admin.post(f"/management/users/{uid}/status", json={"status": "Suspended"}).status_code == 200


def test_owner_can_still_grant_owner(env):
    owner, _, admin_id, _, server = env
    assert owner.put(f"/management/users/{admin_id}", json={"role": "Owner"}).status_code == 200
    assert server.management.user_registry.get_user(admin_id).role == "Owner"


def test_public_signup_cannot_request_owner(env):
    _, _, _, _, server = env
    with pytest.raises(ValueError, match="Owner"):
        server.management.signup("Eve", "eve@x.example", "Owner", "pw-123456789")
