"""GET /org/shared-pool -- a customer can SEE how their jobs may use GCON's shared
worker pool, but not change it. An API key (which can leak) must not be able to
move an organization's jobs onto shared machines; GCON's Owner/Administrator
sets the mode (tests/management/test_shared_pool_admin.py)."""
import pytest
from tests.support.label_client import LabelClient as TestClient

from gcon.api.api_v1 import create_api_v1_app
from gcon.cluster.coordinator import GCONCoordinator
from gcon.dashboard.presentation import PresentationLayer
from gcon.management.management_layer import ManagementLayer
from gcon.persistence.control_plane import ControlPlane


@pytest.fixture
def env(tmp_path):
    coordinator = GCONCoordinator(control_plane=ControlPlane(path=str(tmp_path / "cp.db")))
    management = ManagementLayer(coordinator=coordinator, db_path=str(tmp_path / "mgmt.db"))
    client = TestClient(create_api_v1_app(management, PresentationLayer(coordinator)))
    yield client, coordinator
    coordinator.shutdown()


def _signup(client, org_name, email):
    r = client.post("/auth/signup", json={
        "org_name": org_name, "name": "Ann", "email": email, "password": "correct-horse-1"})
    assert r.status_code == 200, r.text
    return {"Authorization": f"Bearer {r.json()['api_key']['secret']}"}, r.json()


def test_default_is_basic(env):
    client, _ = env
    h, _ = _signup(client, "Acme", "a@acme.example")
    r = client.get("/org/shared-pool", headers=h)
    assert r.status_code == 200 and r.json()["mode"] == "basic"
    assert set(r.json()["modes"]) == {"off", "basic", "full"}


def test_an_api_key_cannot_change_the_mode(env):
    client, _ = env
    h, _ = _signup(client, "Acme", "a@acme.example")
    for method in ("put", "post", "patch"):
        r = getattr(client, method)("/org/shared-pool", json={"mode": "full"}, headers=h)
        assert r.status_code in (404, 405), (method, r.status_code)
    assert client.get("/org/shared-pool", headers=h).json()["mode"] == "basic"


def test_it_shows_what_the_operator_set_per_organization(env):
    client, coordinator = env
    acme, a = _signup(client, "Acme", "a@acme.example")
    globex, _ = _signup(client, "Globex", "g@globex.example")
    org_id = a["organization"]["org_id"]
    coordinator.control_plane.org_pool_settings.set_mode(org_id, "off")
    assert client.get("/org/shared-pool", headers=acme).json()["mode"] == "off"
    assert client.get("/org/shared-pool", headers=globex).json()["mode"] == "basic"


def test_it_requires_authentication(env):
    client, _ = env
    assert client.get("/org/shared-pool").status_code == 401
