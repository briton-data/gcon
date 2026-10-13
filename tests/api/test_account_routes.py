"""
POST /auth/change-password and POST /auth/delete-account on /api/v1 -- real
coordinator, real management layer, real FastAPI app, no mocks.
"""
import pytest
from tests.support.label_client import LabelClient as TestClient

from gcon.api.api_v1 import create_api_v1_app
from gcon.cluster.coordinator import GCONCoordinator
from gcon.dashboard.presentation import PresentationLayer
from gcon.management.management_layer import ManagementLayer
from gcon.persistence.control_plane import ControlPlane

PW = "correct-horse-1"


@pytest.fixture
def env(tmp_path):
    control_plane = ControlPlane(path=str(tmp_path / "cp.db"))
    coordinator = GCONCoordinator(control_plane=control_plane)
    management = ManagementLayer(coordinator=coordinator, db_path=str(tmp_path / "mgmt.db"))
    presentation = PresentationLayer(coordinator)
    client = TestClient(create_api_v1_app(management, presentation))
    yield client, coordinator, management
    coordinator.shutdown()


def _signup(client, org, email):
    r = client.post("/auth/signup", json={"org_name": org, "name": f"{org} User", "email": email, "password": PW})
    assert r.status_code == 200, r.text
    return r.json()


def _login(client, email, password=PW):
    return client.post("/auth/login", json={"email": email, "password": password})


def _h(secret):
    return {"Authorization": f"Bearer {secret}"}


class TestChangePassword:
    def test_changes_password_and_signs_out_other_browsers(self, env):
        client, _, _ = env
        _signup(client, "Acme", "a@acme.test")
        here = _login(client, "a@acme.test").json()["api_key"]["secret"]
        other = _login(client, "a@acme.test").json()["api_key"]["secret"]
        by_hand = client.post("/auth/api-keys", json={"name": "my script"}, headers=_h(here)).json()["secret"]

        r = client.post("/auth/change-password", headers=_h(here),
                        json={"current_password": PW, "new_password": "brand-new-pass-2"})
        assert r.status_code == 200 and r.json() == {"changed": True}

        assert _login(client, "a@acme.test", PW).status_code == 401
        assert _login(client, "a@acme.test", "brand-new-pass-2").status_code == 200
        assert client.get("/jobs", headers=_h(here)).status_code == 200        # this tab stays signed in
        assert client.get("/jobs", headers=_h(by_hand)).status_code == 200     # a key made for code stays
        assert client.get("/jobs", headers=_h(other)).status_code == 401       # other browser signed out

    def test_refusals_leave_password_unchanged(self, env):
        client, _, _ = env
        _signup(client, "Acme", "a@acme.test")
        key = _login(client, "a@acme.test").json()["api_key"]["secret"]
        for body, text in [
            ({"current_password": "wrong-password", "new_password": "brand-new-pass-2"}, "incorrect"),
            ({"current_password": PW, "new_password": "short"}, "at least"),
            ({"current_password": PW, "new_password": PW}, "different"),
        ]:
            r = client.post("/auth/change-password", headers=_h(key), json=body)
            assert r.status_code == 400 and text in r.json()["detail"], r.text
        assert _login(client, "a@acme.test", PW).status_code == 200

    def test_needs_a_key(self, env):
        client, _, _ = env
        r = client.post("/auth/change-password", json={"current_password": PW, "new_password": "brand-new-pass-2"})
        assert r.status_code == 401

    def test_wrong_current_password_is_rate_limited(self, env):
        client, _, _ = env
        _signup(client, "Acme", "a@acme.test")
        key = _login(client, "a@acme.test").json()["api_key"]["secret"]
        codes = [client.post("/auth/change-password", headers=_h(key),
                             json={"current_password": "nope-nope-1", "new_password": "brand-new-pass-2"}).status_code
                 for _ in range(12)]
        assert 429 in codes


class TestDeleteAccount:
    def test_deletes_workspace_and_nothing_else(self, env):
        client, _, management = env
        _signup(client, "Acme", "a@acme.test")
        other = _signup(client, "Other", "o@other.test")
        key = _login(client, "a@acme.test").json()["api_key"]["secret"]
        org_id = management.customer_registry.get_user_by_email("a@acme.test").org_id

        r = client.post("/auth/delete-account", headers=_h(key), json={"password": PW, "org_name": "Acme"})
        assert r.status_code == 200 and r.json() == {"deleted": True}

        assert client.get("/jobs", headers=_h(key)).status_code == 401
        assert _login(client, "a@acme.test").status_code == 401
        assert management.customer_registry.get_user_by_email("a@acme.test") is None
        with pytest.raises(ValueError):
            management.org_registry.get_organization(org_id)
        # the other customer is untouched
        assert client.get("/jobs", headers=_h(other["api_key"]["secret"])).status_code == 200
        # and the email can sign up again
        _signup(client, "Acme again", "a@acme.test")

    def test_refusals_delete_nothing(self, env):
        client, _, _ = env
        _signup(client, "Acme", "a@acme.test")
        key = _login(client, "a@acme.test").json()["api_key"]["secret"]
        for body, text in [
            ({"password": "wrong-password", "org_name": "Acme"}, "Password is incorrect"),
            ({"password": PW, "org_name": "acme"}, "doesn't match"),
        ]:
            r = client.post("/auth/delete-account", headers=_h(key), json=body)
            assert r.status_code == 400 and text in r.json()["detail"], r.text
        assert client.get("/jobs", headers=_h(key)).status_code == 200

    def test_refused_while_a_job_is_queued_then_allowed(self, env):
        client, _, _ = env
        _signup(client, "Acme", "a@acme.test")
        key = _login(client, "a@acme.test").json()["api_key"]["secret"]
        job = client.post("/jobs", headers=_h(key), json={"command": "echo hi"}).json()["job_id"]

        r = client.post("/auth/delete-account", headers=_h(key), json={"password": PW, "org_name": "Acme"})
        assert r.status_code == 400 and "still queued or running" in r.json()["detail"]
        assert client.get("/jobs", headers=_h(key)).status_code == 200

        assert client.post(f"/jobs/{job}/cancel", headers=_h(key)).status_code == 200
        r = client.post("/auth/delete-account", headers=_h(key), json={"password": PW, "org_name": "Acme"})
        assert r.status_code == 200, r.text
