"""
A login ends when it should (logout, suspension), the live websocket notices,
and the audit log never keeps a raw typed email. Real WebServer, no mocks.
"""
import pytest
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from gcon.cluster.coordinator import GCONCoordinator
from gcon.dashboard.presentation import PresentationLayer
from gcon.dashboard.web_server import WebServer
from gcon.persistence import ControlPlane

PW = "owner-pw-123"


@pytest.fixture
def env(tmp_path, monkeypatch):
    monkeypatch.setenv("GCON_DB_PATH", str(tmp_path / "gcon.db"))
    monkeypatch.setenv("GCON_OWNER_PASSWORD", PW)
    coordinator = GCONCoordinator(control_plane=ControlPlane(path=str(tmp_path / "cp.db")))
    server = WebServer(PresentationLayer(coordinator))
    with TestClient(server.app) as c:
        from tests.integration.smoke_management_gaps import OWNER_EMAIL
        assert c.post("/auth/login", json={"email": OWNER_EMAIL, "password": PW}).status_code == 200
        yield c, server, coordinator
    coordinator.shutdown()


def test_failed_login_audit_never_stores_the_typed_email(env):
    c, server, _ = env
    with TestClient(server.app) as anon:
        anon.post("/auth/login", json={"email": "typo.person@gmail.com", "password": "nope"})
    entries = server.management.get_audit_logs()
    failed = [e for e in entries if "failed login" in str(e)]
    assert failed
    assert not any("typo.person" in str(e) for e in entries)


def test_suspending_a_user_ends_their_existing_session(env):
    _, server, _ = env
    mgmt = server.management
    user = mgmt.create_user("Sue", "sue@gcon.example", role="Viewer", password="Sue-pw-12345")
    with TestClient(server.app) as sue:
        assert sue.post("/auth/login", json={"email": "sue@gcon.example", "password": "Sue-pw-12345"}).status_code == 200
        assert sue.get("/management/observability/summary").status_code == 200
        mgmt.update_user(user["user_id"], status="Suspended")
        assert sue.get("/management/observability/summary").status_code == 401


def test_live_socket_stops_after_logout(env):
    c, _, _ = env
    with c.websocket_connect("/ws") as ws:
        ws.receive_json()                      # streaming while logged in
        assert c.post("/auth/logout").status_code == 200
        with pytest.raises(WebSocketDisconnect) as stopped:
            ws.receive_json()                  # next cycle re-checks the login
        assert stopped.value.code == 4401


def test_live_socket_refuses_a_visitor_with_no_login(env):
    _, server, _ = env
    with TestClient(server.app) as anon:
        with pytest.raises(WebSocketDisconnect):
            with anon.websocket_connect("/ws"):
                pass
