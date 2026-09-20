"""
Customer auth flow on /api/v1 -- real coordinator, real management
layer, real FastAPI app, no mocks (same style as the other tests/api
files).

Covers the four changes that make a separate customer frontend
actually usable:

  (a) POST /auth/login now returns a session API key + the
      organization, and that key can be revoked (logout) via
      DELETE /auth/api-keys/{key_id}.
  (b) /auth/login is rate-limited.
  (c) POST /auth/forgot-password no longer hands the reset token to
      whoever asks (that was an account-takeover path once login
      issues a key); it only does so under GCON_EXPOSE_RESET_TOKEN.
  (d) GET /jobs returns kind/requires/verify/error/return_code.
"""
import time

import pytest
from fastapi.testclient import TestClient

from gcon.api.api_v1 import create_api_v1_app
from gcon.cluster.coordinator import GCONCoordinator
from gcon.dashboard.presentation import PresentationLayer
from gcon.execution.agent import GCONAgent
from gcon.management.management_layer import ManagementLayer
from gcon.persistence.control_plane import ControlPlane


@pytest.fixture
def env(tmp_path, monkeypatch):
    monkeypatch.delenv("GCON_EXPOSE_RESET_TOKEN", raising=False)
    control_plane = ControlPlane(path=str(tmp_path / "cp.db"))
    coordinator = GCONCoordinator(control_plane=control_plane)
    management = ManagementLayer(coordinator=coordinator, db_path=str(tmp_path / "mgmt.db"))
    presentation = PresentationLayer(coordinator)
    client = TestClient(create_api_v1_app(management, presentation))
    yield client, coordinator, management
    coordinator.shutdown()


def _signup(client, org, email, password="correct-horse-1"):
    r = client.post("/auth/signup", json={
        "org_name": org, "name": f"{org} User", "email": email, "password": password,
    })
    assert r.status_code == 200, r.text
    return r.json()


def _bearer(secret):
    return {"Authorization": f"Bearer {secret}"}


def _wait_for(predicate, timeout=8.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(0.05)
    return False


# ---------------------------------------------------------------
# (a) login issues a session key; logout revokes it
# ---------------------------------------------------------------

class TestLoginIssuesSessionKey:
    def test_signup_returns_org_and_working_key(self, env):
        client, _, _ = env
        data = _signup(client, "Acme Corp", "a@acme.example")
        assert data["organization"]["name"] == "Acme Corp"
        who = client.get("/whoami", headers=_bearer(data["api_key"]["secret"]))
        assert who.status_code == 200

    def test_login_returns_org_and_a_new_working_key(self, env):
        client, _, _ = env
        signup = _signup(client, "Acme Corp", "a@acme.example")

        r = client.post("/auth/login", json={"email": "a@acme.example", "password": "correct-horse-1"})
        assert r.status_code == 200, r.text
        data = r.json()
        assert data["organization"]["name"] == "Acme Corp"
        assert data["customer_user"]["email"] == "a@acme.example"

        secret = data["api_key"]["secret"]
        assert secret and secret != signup["api_key"]["secret"]
        assert data["api_key"]["name"].startswith("Web session ")
        assert client.get("/jobs", headers=_bearer(secret)).status_code == 200

    def test_each_login_gets_its_own_key(self, env):
        client, _, _ = env
        _signup(client, "Acme Corp", "a@acme.example")
        creds = {"email": "a@acme.example", "password": "correct-horse-1"}
        k1 = client.post("/auth/login", json=creds).json()["api_key"]
        k2 = client.post("/auth/login", json=creds).json()["api_key"]
        assert k1["secret"] != k2["secret"]
        assert k1["key_id"] != k2["key_id"]

    def test_logout_revokes_only_that_session_key(self, env):
        client, _, _ = env
        _signup(client, "Acme Corp", "a@acme.example")
        creds = {"email": "a@acme.example", "password": "correct-horse-1"}
        s1 = client.post("/auth/login", json=creds).json()["api_key"]["secret"]
        s2 = client.post("/auth/login", json=creds).json()["api_key"]["secret"]

        # This is exactly what the frontend does on logout: whoami to
        # learn the key id, then DELETE it.
        key_id = client.get("/whoami", headers=_bearer(s1)).json()["key_id"]
        assert client.delete(f"/auth/api-keys/{key_id}", headers=_bearer(s1)).status_code == 200

        assert client.get("/whoami", headers=_bearer(s1)).status_code == 401
        # Another live session (another tab/device) is unaffected.
        assert client.get("/whoami", headers=_bearer(s2)).status_code == 200

    def test_a_login_key_cannot_touch_another_orgs_keys(self, env):
        client, _, _ = env
        acme = _signup(client, "Acme Corp", "a@acme.example")
        globex = _signup(client, "Globex Inc", "g@globex.example")
        acme_key_id = client.get("/whoami", headers=_bearer(acme["api_key"]["secret"])).json()["key_id"]
        r = client.delete(f"/auth/api-keys/{acme_key_id}", headers=_bearer(globex["api_key"]["secret"]))
        assert r.status_code == 404
        assert client.get("/whoami", headers=_bearer(acme["api_key"]["secret"])).status_code == 200

    def test_wrong_password_is_401_and_issues_no_key(self, env):
        client, _, management = env
        _signup(client, "Acme Corp", "a@acme.example")
        before = len(management.api_key_manager.list_keys())
        r = client.post("/auth/login", json={"email": "a@acme.example", "password": "nope"})
        assert r.status_code == 401
        assert "api_key" not in r.json() or r.json().get("api_key") is None
        assert len(management.api_key_manager.list_keys()) == before

    def test_unknown_email_is_the_same_401(self, env):
        client, _, _ = env
        r = client.post("/auth/login", json={"email": "ghost@nowhere.example", "password": "x"})
        assert r.status_code == 401
        assert r.json()["detail"] == "Invalid email or password."


# ---------------------------------------------------------------
# (b) rate limiting
# ---------------------------------------------------------------

class TestLoginRateLimit:
    def test_repeated_failures_lock_the_account_out_even_for_the_right_password(self, env):
        client, _, _ = env
        _signup(client, "Acme Corp", "a@acme.example")
        bad = {"email": "a@acme.example", "password": "wrong"}
        statuses = [client.post("/auth/login", json=bad).status_code for _ in range(5)]
        assert statuses == [401] * 5

        locked = client.post("/auth/login", json=bad)
        assert locked.status_code == 429

        good = client.post("/auth/login", json={"email": "a@acme.example", "password": "correct-horse-1"})
        assert good.status_code == 429  # lockout holds even for the real password

    def test_lockout_is_per_email(self, env):
        client, _, _ = env
        _signup(client, "Acme Corp", "a@acme.example")
        _signup(client, "Globex Inc", "g@globex.example")
        for _ in range(6):
            client.post("/auth/login", json={"email": "a@acme.example", "password": "wrong"})
        ok = client.post("/auth/login", json={"email": "g@globex.example", "password": "correct-horse-1"})
        assert ok.status_code == 200

    def test_success_clears_earlier_failures(self, env):
        client, _, _ = env
        _signup(client, "Acme Corp", "a@acme.example")
        for _ in range(3):
            client.post("/auth/login", json={"email": "a@acme.example", "password": "wrong"})
        assert client.post("/auth/login", json={"email": "a@acme.example", "password": "correct-horse-1"}).status_code == 200
        for _ in range(4):
            assert client.post("/auth/login", json={"email": "a@acme.example", "password": "wrong"}).status_code == 401


# ---------------------------------------------------------------
# (c) password reset no longer leaks the token
# ---------------------------------------------------------------

class TestForgotPassword:
    def test_default_issues_no_token_and_is_identical_for_unknown_emails(self, env):
        client, _, management = env
        _signup(client, "Acme Corp", "a@acme.example")

        known = client.post("/auth/forgot-password", json={"email": "a@acme.example"})
        unknown = client.post("/auth/forgot-password", json={"email": "ghost@nowhere.example"})
        assert known.status_code == unknown.status_code == 200
        assert known.json() == unknown.json() == {"token": None, "delivery": "unavailable"}

    def test_default_creates_no_usable_token_at_all(self, env):
        client, _, management = env
        _signup(client, "Acme Corp", "a@acme.example")
        client.post("/auth/forgot-password", json={"email": "a@acme.example"})
        # Nothing an attacker could guess/replay was minted either.
        r = client.post("/auth/reset-password", json={"token": "anything", "new_password": "new-password-9"})
        assert r.status_code == 400
        # And the password did not change.
        assert client.post("/auth/login", json={"email": "a@acme.example", "password": "correct-horse-1"}).status_code == 200

    def test_dev_flag_restores_token_and_full_reset_works(self, env, monkeypatch):
        client, _, _ = env
        monkeypatch.setenv("GCON_EXPOSE_RESET_TOKEN", "1")
        _signup(client, "Acme Corp", "a@acme.example")

        r = client.post("/auth/forgot-password", json={"email": "a@acme.example"})
        body = r.json()
        assert body["delivery"] == "dev_token" and body["token"]

        unknown = client.post("/auth/forgot-password", json={"email": "ghost@nowhere.example"}).json()
        assert unknown == {"token": None, "delivery": "dev_token"}

        done = client.post("/auth/reset-password", json={"token": body["token"], "new_password": "brand-new-pass-1"})
        assert done.status_code == 200
        assert client.post("/auth/login", json={"email": "a@acme.example", "password": "correct-horse-1"}).status_code == 401
        assert client.post("/auth/login", json={"email": "a@acme.example", "password": "brand-new-pass-1"}).status_code == 200

        # Reused token is rejected.
        again = client.post("/auth/reset-password", json={"token": body["token"], "new_password": "another-pass-22"})
        assert again.status_code == 400


# ---------------------------------------------------------------
# (d) job list carries how the job was configured and why it failed
# ---------------------------------------------------------------

class TestJobFields:
    def _setup(self, client, coordinator):
        acme = _signup(client, "Acme Corp", "a@acme.example")
        node = GCONAgent(node_id="acme-node-1")
        node.org_id = acme["organization"]["org_id"]
        coordinator.register_agent(node)
        return acme["api_key"]["secret"], acme

    def test_successful_job_reports_kind_and_no_error(self, env):
        client, coordinator, _ = env
        key, _ = self._setup(client, coordinator)
        assert client.post("/jobs", json={"job_id": "ok-1", "command": "echo hello"}, headers=_bearer(key)).status_code == 200
        assert _wait_for(lambda: coordinator.jobs.get("ok-1", {}).get("status") == "completed")

        job = client.get("/jobs/ok-1", headers=_bearer(key)).json()
        assert job["kind"] == "command"
        assert job["requires"] is None and job["verify"] is None
        assert job["error"] is None
        assert "hello" in job["output"]

    def test_failed_job_reports_its_error_and_return_code(self, env):
        client, coordinator, _ = env
        key, _ = self._setup(client, coordinator)
        client.post(
            "/jobs", json={"job_id": "bad-1", "command": "echo boom-details >&2; exit 3"},
            headers=_bearer(key),
        )
        assert _wait_for(lambda: coordinator.jobs.get("bad-1", {}).get("status") == "failed")

        job = client.get("/jobs/bad-1", headers=_bearer(key)).json()
        assert job["status"] == "failed"
        assert job["return_code"] == 3
        assert "boom-details" in (job["error"] or "")

    def test_verify_request_is_reported_back(self, env):
        client, coordinator, _ = env
        key, _ = self._setup(client, coordinator)
        r = client.post(
            "/jobs",
            json={"job_id": "v-1", "command": "echo hi", "verify": {"replicas": 2, "tolerance": 0.05}},
            headers=_bearer(key),
        )
        assert r.status_code == 200, r.text
        job = client.get("/jobs/v-1", headers=_bearer(key)).json()
        assert job["verify"] == {"replicas": 2, "tolerance": 0.05}

    def test_running_job_has_no_error_text(self, env):
        client, coordinator, _ = env
        key, _ = self._setup(client, coordinator)
        client.post("/jobs", json={"job_id": "slow-1", "command": "sleep 3"}, headers=_bearer(key))
        assert _wait_for(lambda: coordinator.jobs.get("slow-1", {}).get("status") == "running")
        job = client.get("/jobs/slow-1", headers=_bearer(key)).json()
        assert job["status"] == "running" and job["error"] is None
        client.post("/jobs/slow-1/cancel", headers=_bearer(key))

    def test_another_org_cannot_see_the_job_or_its_error(self, env):
        client, coordinator, _ = env
        key, _ = self._setup(client, coordinator)
        globex = _signup(client, "Globex Inc", "g@globex.example")
        client.post("/jobs", json={"job_id": "bad-2", "command": "echo secret-detail >&2; exit 1"}, headers=_bearer(key))
        assert _wait_for(lambda: coordinator.jobs.get("bad-2", {}).get("status") == "failed")

        gkey = globex["api_key"]["secret"]
        assert client.get("/jobs/bad-2", headers=_bearer(gkey)).status_code == 404
        assert client.get("/jobs", headers=_bearer(gkey)).json() == []


# ---------------------------------------------------------------
# Signup / reset validation (server-side -- the frontend check is only
# a convenience) and the orphan-organization bug
# ---------------------------------------------------------------

class TestSignupValidation:
    def _post(self, client, **overrides):
        body = {"org_name": "Acme Corp", "name": "Ann", "email": "a@acme.example", "password": "correct-horse-1"}
        body.update(overrides)
        return client.post("/auth/signup", json=body)

    def test_short_password_is_rejected(self, env):
        client, _, _ = env
        r = self._post(client, password="short")
        assert r.status_code == 400
        assert "at least 8" in r.json()["detail"]

    def test_empty_password_is_rejected(self, env):
        client, _, _ = env
        assert self._post(client, password="").status_code == 400

    def test_bad_email_is_rejected(self, env):
        client, _, _ = env
        for bad in ("not-an-email", "a@b", "a b@c.d", ""):
            r = self._post(client, email=bad)
            assert r.status_code == 400, bad
            assert "valid email" in r.json()["detail"]

    def test_blank_org_and_name_are_rejected(self, env):
        client, _, _ = env
        assert "Organization name" in self._post(client, org_name="   ").json()["detail"]
        assert "name is required" in self._post(client, name="  ").json()["detail"]

    def test_rejected_signups_create_nothing(self, env):
        client, _, management = env
        before_orgs = len(management.org_registry.list_organizations())
        before_keys = len(management.api_key_manager.list_keys())
        self._post(client, password="x")
        self._post(client, email="nope")
        assert len(management.org_registry.list_organizations()) == before_orgs
        assert len(management.api_key_manager.list_keys()) == before_keys

    def test_duplicate_email_is_rejected_and_leaves_no_orphan_org(self, env):
        client, _, management = env
        assert self._post(client).status_code == 200
        orgs_after_first = len(management.org_registry.list_organizations())

        dup = self._post(client, org_name="Second Org")
        assert dup.status_code == 400
        assert "already exists" in dup.json()["detail"]
        assert len(management.org_registry.list_organizations()) == orgs_after_first

        # Case/whitespace variants are the same account.
        dup2 = self._post(client, org_name="Third Org", email="  A@ACME.example ")
        assert dup2.status_code == 400
        assert len(management.org_registry.list_organizations()) == orgs_after_first


class TestResetPasswordValidation:
    def test_weak_new_password_is_rejected_and_token_survives(self, env, monkeypatch):
        client, _, _ = env
        monkeypatch.setenv("GCON_EXPOSE_RESET_TOKEN", "1")
        _signup(client, "Acme Corp", "a@acme.example")
        token = client.post("/auth/forgot-password", json={"email": "a@acme.example"}).json()["token"]

        weak = client.post("/auth/reset-password", json={"token": token, "new_password": "abc"})
        assert weak.status_code == 400 and "at least 8" in weak.json()["detail"]
        # Password unchanged, token still usable with a valid password.
        assert client.post("/auth/login", json={"email": "a@acme.example", "password": "correct-horse-1"}).status_code == 200
        ok = client.post("/auth/reset-password", json={"token": token, "new_password": "long-enough-pass"})
        assert ok.status_code == 200
