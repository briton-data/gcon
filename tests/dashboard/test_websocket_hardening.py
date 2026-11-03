"""
The live dashboard socket (/ws).

  O1  A page on another origin cannot open it with the visitor's login cookie.
  C1  One login cannot hold an unlimited number of sockets open; a closed socket frees its slot.
  L1  Building the payload (health, jobs, receipts, ...) does not run on the event loop, where it
      would stall every other request for as long as it takes.
  E1  A failure while building one update ends that socket cleanly and does not leak its slot.
"""
import asyncio

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
    monkeypatch.setenv("GCON_WS_MAX_PER_USER", "3")
    coordinator = GCONCoordinator(control_plane=ControlPlane(path=str(tmp_path / "cp.db")))
    server = WebServer(PresentationLayer(coordinator))
    with TestClient(server.app) as c:
        from tests.integration.smoke_management_gaps import OWNER_EMAIL
        assert c.post("/auth/login", json={"email": OWNER_EMAIL, "password": PW}).status_code == 200
        yield c, server
    coordinator.shutdown()


def _refused(client, headers=None):
    try:
        with client.websocket_connect("/ws", headers=headers or {}) as ws:
            ws.receive_json()
        return None
    except WebSocketDisconnect as exc:
        return exc.code


class TestOrigin:
    def test_a_page_on_another_origin_is_refused(self, env):
        c, _ = env
        assert _refused(c, {"origin": "https://evil.example"}) == 4403

    def test_the_dashboards_own_origin_is_accepted(self, env):
        c, _ = env
        assert _refused(c, {"origin": "http://testserver"}) is None

    def test_a_client_that_sends_no_origin_is_accepted(self, env):
        c, _ = env
        assert _refused(c) is None

    def test_an_origin_listed_for_the_api_is_accepted(self, env, monkeypatch):
        c, server = env
        monkeypatch.setenv("GCON_API_CORS_ORIGINS", "https://app.example.com")
        assert _refused(c, {"origin": "https://app.example.com"}) is None


class TestConnectionCap:
    def test_the_socket_over_the_cap_is_refused(self, env):
        c, _ = env
        a = c.websocket_connect("/ws"); wa = a.__enter__(); wa.receive_json()
        b = c.websocket_connect("/ws"); wb = b.__enter__(); wb.receive_json()
        d = c.websocket_connect("/ws"); wd = d.__enter__(); wd.receive_json()
        try:
            assert _refused(c) == 4429
        finally:
            for x in (d, b, a):
                x.__exit__(None, None, None)

    def test_closing_a_socket_frees_its_slot(self, env):
        c, _ = env
        for _ in range(10):                    # far more connects than the cap, one after another
            assert _refused(c) is None


class TestPayloadIsBuiltOffTheEventLoop:
    def test_health_is_computed_in_a_worker_thread(self, env, monkeypatch):
        c, server = env
        seen = []
        real = server.presentation.get_cluster_health

        def spy():
            try:
                asyncio.get_running_loop()
                seen.append("on the event loop")
            except RuntimeError:
                seen.append("worker thread")
            return real()

        monkeypatch.setattr(server.presentation, "get_cluster_health", spy)
        assert _refused(c) is None
        assert seen and set(seen) == {"worker thread"}


class TestAFailedUpdate:
    def test_ends_the_socket_and_frees_the_slot(self, env, monkeypatch):
        c, server = env
        real = server.presentation.get_cluster_health
        monkeypatch.setattr(server.presentation, "get_cluster_health",
                            lambda: (_ for _ in ()).throw(RuntimeError("boom")))
        for _ in range(6):                      # more failures than the cap: none may leak a slot
            try:
                with c.websocket_connect("/ws") as ws:
                    ws.receive_json()
            except Exception:
                pass
        monkeypatch.setattr(server.presentation, "get_cluster_health", real)
        assert _refused(c) is None
