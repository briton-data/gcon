"""
Plans as a customer experiences them over /api/v1: the plan and its limits are
visible, the request limit and the concurrent-job cap both follow the plan (and
staff changes to it, immediately), a customer can never choose their own plan,
and hitting the job cap is a clean 429, not a 500.
"""
from types import SimpleNamespace

import pytest
from tests.support.label_client import LabelClient as TestClient

from gcon.api.api_v1 import create_api_v1_app
from gcon.cluster.coordinator import GCONCoordinator
from gcon.dashboard.presentation import PresentationLayer
from gcon.management import plans
from gcon.management.management_layer import ManagementLayer
from gcon.persistence.control_plane import ControlPlane

pytestmark = pytest.mark.real_plan_defaults

OWNER = SimpleNamespace(name="Olive", role="Owner")


def build(tmp_path, monkeypatch, rate_env=None, jobs_env=None):
    for name, value in (("GCON_API_RATE_LIMIT_PER_MINUTE", rate_env), ("GCON_MAX_CONCURRENT_JOBS_PER_ORG", jobs_env)):
        if value is None:
            monkeypatch.delenv(name, raising=False)
        else:
            monkeypatch.setenv(name, str(value))
    coordinator = GCONCoordinator(control_plane=ControlPlane(path=str(tmp_path / "cp.db")))
    management = ManagementLayer(coordinator=coordinator, db_path=str(tmp_path / "mgmt.db"))
    client = TestClient(create_api_v1_app(management, PresentationLayer(coordinator)))
    return client, coordinator, management


@pytest.fixture
def env(tmp_path, monkeypatch):
    client, coordinator, management = build(tmp_path, monkeypatch)
    yield client, coordinator, management
    coordinator.shutdown()


def signup(client, org="Acme", email="a@acme.example", **extra):
    r = client.post(
        "/auth/signup",
        json={"org_name": org, "name": "Ann", "email": email, "password": "correct-horse-1", **extra},
    )
    assert r.status_code == 200, r.text
    body = r.json()
    return body["organization"]["org_id"], {"Authorization": f"Bearer {body['api_key']['secret']}"}


def submit(client, headers):
    return client.post("/jobs", json={"command": "echo hi"}, headers=headers)


class TestAccountPlan:
    def test_requires_an_api_key(self, env):
        client, _, _ = env
        assert client.get("/account/plan").status_code == 401

    def test_a_new_customer_is_on_standard_with_its_limits(self, env):
        client, _, _ = env
        _, h = signup(client)
        body = client.get("/account/plan", headers=h).json()
        standard = plans.DEFAULT_PLAN_LIMITS["Standard"]
        assert body == {
            "plan": "Standard",
            "requests_per_minute": standard["requests_per_minute"],
            "max_concurrent_jobs": standard["max_concurrent_jobs"],
            "allowed_programs": standard["allowed_programs"],
            "max_runtime_seconds": standard["max_runtime_seconds"],
            "jobs_in_flight": 0,
        }

    def test_a_customer_cannot_pick_their_plan_at_signup(self, env):
        client, _, _ = env
        org_id, h = signup(client, plan="Enterprise")
        assert client.get("/account/plan", headers=h).json()["plan"] == "Standard"

    def test_staff_changes_show_up_on_the_next_call(self, env):
        client, _, management = env
        org_id, h = signup(client)
        management.update_organization(org_id, plan="Pro")
        assert client.get("/account/plan", headers=h).json()["plan"] == "Pro"
        management.set_plan_limits("Pro", 0, 9, actor=OWNER)
        body = client.get("/account/plan", headers=h).json()
        assert body["requests_per_minute"] is None            # 0 = no limit -> null
        assert body["max_concurrent_jobs"] == 9

    def test_counts_only_the_callers_jobs_in_flight(self, env):
        client, _, _ = env
        _, acme = signup(client, "Acme", "a@acme.example")
        _, globex = signup(client, "Globex", "g@globex.example")
        for _ in range(2):
            assert submit(client, acme).status_code == 200
        assert submit(client, globex).status_code == 200
        assert client.get("/account/plan", headers=acme).json()["jobs_in_flight"] == 2
        assert client.get("/account/plan", headers=globex).json()["jobs_in_flight"] == 1


class TestRequestRateFollowsThePlan:
    def test_each_plan_gets_its_own_allowance(self, env):
        client, _, management = env
        management.set_plan_limits("Standard", 2, 50, actor=OWNER)
        management.set_plan_limits("Pro", 4, 50, actor=OWNER)
        acme_id, acme = signup(client, "Acme", "a@acme.example")
        globex_id, globex = signup(client, "Globex", "g@globex.example")
        management.update_organization(globex_id, plan="Pro")

        assert [client.get("/jobs", headers=acme).status_code for _ in range(3)] == [200, 200, 429]
        assert [client.get("/jobs", headers=globex).status_code for _ in range(5)] == [200, 200, 200, 200, 429]

    def test_the_429_names_the_plans_limit_and_has_retry_after(self, env):
        client, _, management = env
        management.set_plan_limits("Standard", 1, 50, actor=OWNER)
        _, h = signup(client)
        client.get("/jobs", headers=h)
        r = client.get("/jobs", headers=h)
        assert r.status_code == 429
        assert "at most 1 requests per minute on your plan" in r.json()["detail"]
        assert 1 <= int(r.headers["Retry-After"]) <= 60

    def test_upgrading_the_organization_lifts_the_limit_immediately(self, env):
        client, _, management = env
        management.set_plan_limits("Standard", 1, 50, actor=OWNER)
        org_id, h = signup(client)
        client.get("/jobs", headers=h)
        assert client.get("/jobs", headers=h).status_code == 429
        management.update_organization(org_id, plan="Enterprise")
        assert client.get("/jobs", headers=h).status_code == 200

    def test_raising_the_tier_lifts_it_for_everyone_on_it(self, env):
        client, _, management = env
        management.set_plan_limits("Standard", 1, 50, actor=OWNER)
        _, h = signup(client)
        client.get("/jobs", headers=h)
        assert client.get("/jobs", headers=h).status_code == 429
        management.set_plan_limits("Standard", 5, 50, actor=OWNER)
        assert client.get("/jobs", headers=h).status_code == 200

    def test_a_plan_with_no_request_limit_is_never_limited(self, env):
        client, _, management = env
        management.set_plan_limits("Standard", 0, 50, actor=OWNER)
        _, h = signup(client)
        assert all(client.get("/jobs", headers=h).status_code == 200 for _ in range(30))

    def test_the_environment_override_beats_the_plans(self, tmp_path, monkeypatch):
        client, coordinator, management = build(tmp_path, monkeypatch, rate_env=2)
        try:
            org_id, h = signup(client)
            management.update_organization(org_id, plan="Enterprise")      # plan says 12000
            assert [client.get("/jobs", headers=h).status_code for _ in range(3)] == [200, 200, 429]
        finally:
            coordinator.shutdown()

    def test_zero_in_the_environment_switches_limiting_off_whatever_the_plan(self, tmp_path, monkeypatch):
        client, coordinator, management = build(tmp_path, monkeypatch, rate_env=0)
        try:
            management.set_plan_limits("Standard", 1, 50, actor=OWNER)
            _, h = signup(client)
            assert all(client.get("/jobs", headers=h).status_code == 200 for _ in range(10))
        finally:
            coordinator.shutdown()


class TestConcurrentJobCapFollowsThePlan:
    def test_over_the_cap_is_a_clean_429_not_a_500(self, env):
        client, _, management = env
        management.set_plan_limits("Standard", 600, 2, actor=OWNER)
        _, h = signup(client)
        assert submit(client, h).status_code == 200
        assert submit(client, h).status_code == 200
        r = submit(client, h)
        assert r.status_code == 429
        assert "concurrent-job limit of 2" in r.json()["detail"]

    def test_the_cap_is_per_organization(self, env):
        client, _, management = env
        management.set_plan_limits("Standard", 600, 1, actor=OWNER)
        _, acme = signup(client, "Acme", "a@acme.example")
        _, globex = signup(client, "Globex", "g@globex.example")
        assert submit(client, acme).status_code == 200
        assert submit(client, acme).status_code == 429
        assert submit(client, globex).status_code == 200

    def test_a_higher_plan_has_a_higher_cap_and_an_upgrade_applies_at_once(self, env):
        client, _, management = env
        management.set_plan_limits("Standard", 600, 1, actor=OWNER)
        management.set_plan_limits("Pro", 600, 3, actor=OWNER)
        org_id, h = signup(client)
        assert submit(client, h).status_code == 200
        assert submit(client, h).status_code == 429
        management.update_organization(org_id, plan="Pro")
        assert submit(client, h).status_code == 200
        assert submit(client, h).status_code == 200
        assert submit(client, h).status_code == 429

    def test_a_plan_with_no_job_cap_is_not_capped(self, env):
        client, _, management = env
        management.set_plan_limits("Standard", 600, 0, actor=OWNER)
        _, h = signup(client)
        assert all(submit(client, h).status_code == 200 for _ in range(8))

    def test_the_environment_cap_beats_the_plans(self, tmp_path, monkeypatch):
        client, coordinator, management = build(tmp_path, monkeypatch, jobs_env=1)
        try:
            org_id, h = signup(client)
            management.update_organization(org_id, plan="Enterprise")      # plan says 1000
            assert submit(client, h).status_code == 200
            assert submit(client, h).status_code == 429
        finally:
            coordinator.shutdown()

    def test_every_submission_path_is_capped_not_only_the_jobs_route(self, env):
        # Workflows (and anything else) submit through the coordinator, so the
        # cap lives there; a customer cannot get around it by using them.
        from gcon.cluster.coordinator import ConcurrentJobLimitError
        _, coordinator, management = env
        management.set_plan_limits("Standard", 600, 1, actor=OWNER)
        org = management.create_organization("Acme", "Standard")
        coordinator.submit_job("wf-1", "echo hi", org_id=org["org_id"])
        with pytest.raises(ConcurrentJobLimitError):
            coordinator.submit_job("wf-2", "echo hi", org_id=org["org_id"])
        # still a RuntimeError, as it always was
        with pytest.raises(RuntimeError):
            coordinator.submit_job("wf-3", "echo hi", org_id=org["org_id"])
        # jobs with no organization are not capped (nothing to attribute them to)
        coordinator.submit_job("internal-1", "echo hi")
        coordinator.submit_job("internal-2", "echo hi")

    def test_a_failing_plan_lookup_never_blocks_submissions(self, env):
        # The cap is a fairness guard; a bug in looking it up must not stop
        # every customer from submitting. It is logged instead.
        _, coordinator, management = env
        org = management.create_organization("Acme", "Standard")

        def broken(org_id):
            raise RuntimeError("lookup broke")

        coordinator.org_job_cap_resolver = broken
        coordinator.submit_job("a-1", "echo hi", org_id=org["org_id"])
        coordinator.submit_job("a-2", "echo hi", org_id=org["org_id"])
