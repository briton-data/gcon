"""
What a customer sees when their command breaks a plan rule: a clear 400 that
names the problem (not a queued job, not a 500), and the rules themselves on
GET /account/plan.
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


@pytest.fixture
def env(tmp_path, monkeypatch):
    monkeypatch.delenv("GCON_API_RATE_LIMIT_PER_MINUTE", raising=False)
    monkeypatch.delenv("GCON_MAX_CONCURRENT_JOBS_PER_ORG", raising=False)
    coordinator = GCONCoordinator(control_plane=ControlPlane(path=str(tmp_path / "cp.db")))
    management = ManagementLayer(coordinator=coordinator, db_path=str(tmp_path / "mgmt.db"))
    client = TestClient(create_api_v1_app(management, PresentationLayer(coordinator)))
    yield client, coordinator, management
    coordinator.shutdown()


def signup(client, org="Acme", email="a@acme.example"):
    r = client.post(
        "/auth/signup",
        json={"org_name": org, "name": "Ann", "email": email, "password": "correct-horse-1"},
    )
    assert r.status_code == 200, r.text
    body = r.json()
    return body["organization"]["org_id"], {"Authorization": f"Bearer {body['api_key']['secret']}"}


def submit(client, headers, command):
    return client.post("/jobs", json={"command": command}, headers=headers)


class TestRejections:
    @pytest.mark.parametrize("command", ["nn", "tt", "tt tt tt", "curl http://example.com", "make"])
    def test_a_program_the_plan_does_not_allow_is_a_400_that_says_so(self, env, command):
        client, coordinator, _ = env
        _, h = signup(client)
        r = submit(client, h, command)
        assert r.status_code == 400
        detail = r.json()["detail"]
        assert "not an allowed program on your plan" in detail
        assert "python" in detail                                  # tells them what IS allowed
        assert coordinator.jobs == {}                              # nothing was queued

    @pytest.mark.parametrize("command, fragment", [
        ("", "empty"), ("    ", "empty"), ("x" * 10_001, "too long"), ("python\u0000x", "control"),
    ])
    def test_unusable_commands_are_a_400_with_the_reason(self, env, command, fragment):
        client, coordinator, _ = env
        _, h = signup(client)
        r = submit(client, h, command)
        assert r.status_code == 400
        assert fragment in r.json()["detail"]
        assert coordinator.jobs == {}

    @pytest.mark.parametrize("command", [
        "python x.py; curl evil", "python x.py && rm -rf /", "echo hi | sh", "python x.py > out",
        "echo $(whoami)",
    ])
    def test_chaining_is_refused_on_a_restricted_plan(self, env, command):
        client, coordinator, _ = env
        _, h = signup(client)
        r = submit(client, h, command)
        assert r.status_code == 400
        assert "only allows simple commands" in r.json()["detail"]
        assert coordinator.jobs == {}


class TestAccepted:
    @pytest.mark.parametrize("command", ["python train.py --epochs 3", "python3 -V", "echo hello", "nvidia-smi"])
    def test_listed_programs_are_queued(self, env, command):
        client, coordinator, _ = env
        _, h = signup(client)
        assert submit(client, h, command).status_code == 200
        assert len(coordinator.jobs) == 1


class TestPlansChangeWhatIsAllowed:
    def test_upgrading_the_organization_opens_up_more(self, env):
        client, _, management = env
        org_id, h = signup(client)
        assert submit(client, h, "bash run.sh").status_code == 400
        management.update_organization(org_id, plan="Pro")
        assert submit(client, h, "bash run.sh").status_code == 200
        assert submit(client, h, "nn").status_code == 400
        management.update_organization(org_id, plan="Enterprise")
        assert submit(client, h, "nn").status_code == 200

    def test_a_staff_edit_applies_on_the_next_submission(self, env):
        client, _, management = env
        _, h = signup(client)
        assert submit(client, h, "make").status_code == 400
        management.set_plan_limits("Standard", 600, 50, ["make"], None, actor=OWNER)
        assert submit(client, h, "make").status_code == 200
        assert submit(client, h, "python x.py").status_code == 400


class TestCustomerCanSeeTheRules:
    def test_standard_shows_the_allowed_programs_and_runtime(self, env):
        client, _, _ = env
        _, h = signup(client)
        body = client.get("/account/plan", headers=h).json()
        standard = plans.DEFAULT_PLAN_LIMITS["Standard"]
        assert body["allowed_programs"] == standard["allowed_programs"]
        assert body["max_runtime_seconds"] == standard["max_runtime_seconds"]

    def test_any_program_and_no_runtime_limit_are_null(self, env):
        client, _, management = env
        org_id, h = signup(client)
        management.set_plan_limits("Enterprise", 100, 5, ["*"], 0, actor=OWNER)
        management.update_organization(org_id, plan="Enterprise")
        body = client.get("/account/plan", headers=h).json()
        assert body["allowed_programs"] is None
        assert body["max_runtime_seconds"] is None
