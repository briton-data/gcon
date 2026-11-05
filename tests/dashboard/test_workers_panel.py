"""
The Workers panel on the staff dashboard's Cluster > Infra tab: it is in the page, its data is in the first payload and in every live
update, and it shows what the registry holds.
"""
import json
import re
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

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
    coordinator.pause_scheduler()
    server = WebServer(PresentationLayer(coordinator))
    with TestClient(server.app) as c:
        from tests.integration.smoke_management_gaps import OWNER_EMAIL
        assert c.post("/auth/login", json={"email": OWNER_EMAIL, "password": PW}).status_code == 200
        yield c, server, coordinator
    coordinator.shutdown()


def _worker(coordinator, node_id, org=None, status="idle", quarantined=None):
    node = SimpleNamespace(node_id=node_id, org_id=org, sandboxed=True, agent_version="1.1.0", status=status,
                           is_connected=lambda: True)
    coordinator.registry.register(node)
    coordinator.registry.nodes[node_id]["status"] = status
    if quarantined:
        coordinator.registry.set_quarantined(node_id, True, reason=quarantined)


def test_the_panel_is_in_the_page(env):
    c, _, _ = env
    html = c.get("/").text
    for element_id in ("workers-panel", "workers-headline", "workers-attention", "workers-kinds-body",
                       "workers-customers-body"):
        assert f'id="{element_id}"' in html


def test_the_first_payload_carries_the_worker_overview(env):
    c, server, coordinator = env
    org = server.management.create_organization("Acme Inc")
    _worker(coordinator, "acme-1", org=org["org_id"], status="busy")
    _worker(coordinator, "acme-2", org=org["org_id"], quarantined="3 receipts failed verification")
    html = c.get("/").text
    boot = json.loads(re.search(r'id="dashboard-bootstrap-data">(.*?)</script>', html, re.S).group(1))
    wo = boot["worker_overview"]
    assert wo["total"] == 2 and wo["states"]["working"] == 1 and wo["states"]["quarantined"] == 1
    assert wo["by_customer"][0]["name"] == "Acme Inc"
    assert wo["attention"][0]["node_id"] == "acme-2"


def test_every_live_update_carries_it_and_follows_the_registry(env):
    c, server, coordinator = env
    with c.websocket_connect("/ws") as ws:
        assert ws.receive_json()["worker_overview"]["total"] == 0
        _worker(coordinator, "w-1", status="busy")
        # the next tick (2s) reflects the new worker
        for _ in range(3):
            update = ws.receive_json()
            if update["worker_overview"]["total"] == 1:
                break
        assert update["worker_overview"]["total"] == 1 and update["worker_overview"]["states"]["working"] == 1
