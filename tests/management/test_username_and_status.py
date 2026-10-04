"""
Username (display handle) + the honest service statuses behind the System
Services panel. Real ManagementLayer / WebServer / HealthService, no mocks.
"""
import pytest
from fastapi.testclient import TestClient

from gcon.cluster.coordinator import GCONCoordinator
from gcon.dashboard.presentation import PresentationLayer
from gcon.dashboard.web_server import WebServer
from gcon.persistence import ControlPlane


@pytest.fixture
def env(tmp_path, monkeypatch):
    monkeypatch.setenv("GCON_DB_PATH", str(tmp_path / "gcon.db"))
    monkeypatch.setenv("GCON_OWNER_PASSWORD", "owner-pw-123")
    coordinator = GCONCoordinator(control_plane=ControlPlane(path=str(tmp_path / "cp.db")))
    server = WebServer(PresentationLayer(coordinator))
    with TestClient(server.app) as c:
        from tests.integration.smoke_management_gaps import OWNER_EMAIL
        assert c.post("/auth/login", json={"email": OWNER_EMAIL, "password": "owner-pw-123"}).status_code == 200
        yield c, server, coordinator
    coordinator.shutdown()


# ------------------------------------------------------------- username
def test_username_is_none_until_chosen_then_persists_across_reload(env, tmp_path):
    c, server, _ = env
    assert c.get("/auth/me").json()["username"] is None
    r = c.post("/auth/profile", json={"username": "croc"})
    assert r.status_code == 200 and r.json()["username"] == "croc"
    assert c.get("/auth/me").json()["username"] == "croc"

    # A fresh registry over the same database still has it (real persistence).
    from gcon.management.users import UserRegistry
    reloaded = UserRegistry(server.management.user_registry.db)
    me = c.get("/auth/me").json()["user_id"]
    assert reloaded.get_user(me).username == "croc"


def test_username_can_be_cleared(env):
    c, _, _ = env
    c.post("/auth/profile", json={"username": "croc"})
    assert c.post("/auth/profile", json={"username": "  "}).json()["username"] is None


@pytest.mark.parametrize("bad", ["a", "has space", "x" * 25, "-lead", "emoji\U0001f40a", "Briton Nyonges"])
def test_invalid_usernames_are_rejected(env, bad):
    c, _, _ = env
    r = c.post("/auth/profile", json={"username": bad})
    assert r.status_code == 400 and "Username must be" in r.json()["detail"]


def test_username_must_be_unique_case_insensitively(env):
    c, server, _ = env
    server.management.create_user("Someone Else", "else@example.com", "Viewer", username="Croc")
    r = c.post("/auth/profile", json={"username": "croc"})
    assert r.status_code == 400 and "already taken" in r.json()["detail"]
    assert c.post("/auth/profile", json={"username": "croc2"}).status_code == 200


def test_admin_can_set_a_username_when_creating_a_user(env):
    c, _, _ = env
    r = c.post("/management/users", json={"name": "New Person", "email": "np@example.com", "role": "Viewer", "username": "np"})
    assert r.status_code == 200 and r.json()["username"] == "np"
    dup = c.post("/management/users", json={"name": "Other", "email": "o@example.com", "role": "Viewer", "username": "NP"})
    assert dup.status_code == 400


def test_profile_endpoint_requires_login(env):
    _, server, _ = env
    with TestClient(server.app) as anon:
        assert anon.post("/auth/profile", json={"username": "x1"}).status_code in (401, 403)


# --------------------------------------------------- honest service status
def test_global_status_reports_the_real_scheduler_state(env):
    _, server, coordinator = env
    g = lambda: server.presentation.get_global_status()
    assert g()["scheduler_state"] == "running" and g()["scheduler_running"] is True
    coordinator.pause_scheduler()
    assert g()["scheduler_state"] == "paused" and g()["scheduler_running"] is False   # alive, but not running
    coordinator.resume_scheduler()
    coordinator.shutdown()
    assert g()["scheduler_state"] == "dead" and g()["scheduler_running"] is False


def test_receipts_service_goes_red_when_the_durable_store_fails(env, monkeypatch):
    _, server, coordinator = env
    health = coordinator.health_service if hasattr(coordinator, "health_service") else None
    from gcon.monitoring.health_service import HealthService
    hs = health or HealthService(coordinator)
    assert hs.check_receipt_service().healthy is True

    def boom():
        raise RuntimeError("database is locked")
    monkeypatch.setattr(coordinator.control_plane.receipts, "count_all", boom)
    check = hs.check_receipt_service()
    assert check.healthy is False and "Durable receipt store" in check.detail
    assert server.presentation.get_global_status()["receipt_engine_online"] is False
