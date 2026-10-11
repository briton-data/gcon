"""
A browser calling /api/v1 from another origin (the customer site) can only read
a response header from JavaScript if CORS exposes it. The 429 from the request
limit carries Retry-After, so the CORS setup in WebServer must expose it.
Built through the real WebServer so the real middleware stack is tested.
"""
from fastapi.testclient import TestClient

from gcon.cluster.coordinator import GCONCoordinator
from gcon.dashboard.presentation import PresentationLayer
from gcon.dashboard.web_server import WebServer
from gcon.persistence import ControlPlane

ORIGIN = "https://customer-site.example"


def test_retry_after_is_exposed_to_the_allowed_browser_origin(tmp_path, monkeypatch):
    monkeypatch.setenv("GCON_DB_PATH", str(tmp_path / "gcon.db"))
    monkeypatch.setenv("GCON_OWNER_PASSWORD", "owner-pw-123")
    monkeypatch.setenv("GCON_API_CORS_ORIGINS", ORIGIN)
    monkeypatch.setenv("GCON_API_RATE_LIMIT_PER_MINUTE", "2")
    coordinator = GCONCoordinator(control_plane=ControlPlane(path=str(tmp_path / "cp.db")))
    try:
        server = WebServer(PresentationLayer(coordinator))
        with TestClient(server.app) as client:
            signup = client.post(
                "/api/v1/auth/signup",
                json={"org_name": "Acme", "name": "Ann", "email": "a@acme.example",
                      "password": "correct-horse-1"},
            )
            assert signup.status_code == 200, signup.text
            headers = {
                "Authorization": f"Bearer {signup.json()['api_key']['secret']}",
                "Origin": ORIGIN,
            }
            assert client.get("/api/v1/jobs", headers=headers).status_code == 200
            assert client.get("/api/v1/jobs", headers=headers).status_code == 200
            limited = client.get("/api/v1/jobs", headers=headers)

            assert limited.status_code == 429
            assert int(limited.headers["retry-after"]) >= 1
            assert limited.headers["access-control-allow-origin"] == ORIGIN
            assert "retry-after" in limited.headers["access-control-expose-headers"].lower()
    finally:
        coordinator.shutdown()
