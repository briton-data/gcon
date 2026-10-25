"""
Plans through ManagementLayer: validation on every way an organization gets a
plan, the audit trail, who may change tier limits, persistence, and the limits
that really apply to an organization (plan + environment).
"""
from types import SimpleNamespace

import pytest

from gcon.cluster.coordinator import GCONCoordinator
from gcon.management import plans
from gcon.management.management_layer import ManagementLayer
from gcon.persistence.control_plane import ControlPlane

pytestmark = pytest.mark.real_plan_defaults


def build(tmp_path, monkeypatch, name="a"):
    monkeypatch.delenv("GCON_API_RATE_LIMIT_PER_MINUTE", raising=False)
    monkeypatch.delenv("GCON_MAX_CONCURRENT_JOBS_PER_ORG", raising=False)
    coordinator = GCONCoordinator(control_plane=ControlPlane(path=str(tmp_path / f"cp_{name}.db")))
    management = ManagementLayer(coordinator=coordinator, db_path=str(tmp_path / f"m_{name}.db"))
    return coordinator, management


@pytest.fixture
def env(tmp_path, monkeypatch):
    coordinator, management = build(tmp_path, monkeypatch)
    yield coordinator, management
    coordinator.shutdown()


OWNER = SimpleNamespace(name="Olive", role="Owner")
ADMIN = SimpleNamespace(name="Adam", role="Administrator")
OPERATOR = SimpleNamespace(name="Opal", role="Operator")


class TestAssigningAPlan:
    def test_create_organization_canonicalises_and_reports_limits(self, env):
        _, management = env
        org = management.create_organization("Acme", "pro")
        assert org["plan"] == "Pro"
        assert org["limits"] == plans.DEFAULT_PLAN_LIMITS["Pro"]

    @pytest.mark.parametrize("bad", ["Free", "Gold", ""])
    def test_create_organization_rejects_unknown_plans(self, env, bad):
        _, management = env
        with pytest.raises(ValueError):
            management.create_organization("Acme", bad)
        assert management.get_organizations() == []

    def test_signup_gets_standard_and_is_never_client_chosen(self, env):
        _, management = env
        result = management.signup_customer("Acme", "Ann", "a@acme.example", "correct-horse-1")
        assert result["organization"]["plan"] == "Standard"

    def test_update_changes_the_plan_and_the_audit_log_says_so(self, env):
        _, management = env
        org = management.create_organization("Acme", "Standard")
        updated = management.update_organization(org["org_id"], name="Acme", plan="Enterprise")
        assert updated["plan"] == "Enterprise"
        assert updated["limits"] == plans.DEFAULT_PLAN_LIMITS["Enterprise"]
        entries = " | ".join(f"{e['action']} {e.get('target', '')}" for e in management.audit_logger.list_entries())
        assert "changed plan Standard -> Enterprise for organization" in entries

    def test_update_rejects_an_unknown_plan_and_changes_nothing(self, env):
        _, management = env
        org = management.create_organization("Acme", "Pro")
        with pytest.raises(ValueError):
            management.update_organization(org["org_id"], name="Renamed", plan="Gold")
        assert management.get_organization(org["org_id"])["plan"] == "Pro"
        assert management.get_organization(org["org_id"])["name"] == "Acme"

    @pytest.mark.parametrize("blank", [None, ""])
    def test_a_blank_plan_leaves_the_plan_alone(self, env, blank):
        _, management = env
        org = management.create_organization("Acme", "Pro")
        updated = management.update_organization(org["org_id"], name="Acme 2", plan=blank)
        assert updated["plan"] == "Pro" and updated["name"] == "Acme 2"

    def test_an_organization_with_a_retired_label_is_treated_as_standard(self, env):
        _, management = env
        legacy = management.org_registry.add_organization("Old Co", plan="Free")
        data = management.get_organization(legacy.org_id)
        assert data["plan"] == "Free"                                    # shown as stored
        assert data["limits"] == plans.DEFAULT_PLAN_LIMITS["Standard"]   # limited as Standard
        assert management.get_org_limits(legacy.org_id)["plan"] == "Free"
        # it can still be renamed without choosing a plan, and re-planned later
        assert management.update_organization(legacy.org_id, name="Old Co 2", plan=None)["plan"] == "Free"
        assert management.update_organization(legacy.org_id, plan="Pro")["plan"] == "Pro"


class TestEditingTheTiers:
    def test_owner_and_administrator_can_change_a_tier_and_it_is_audited(self, env):
        _, management = env
        overview = management.set_plan_limits("Pro", 100, 7, actor=OWNER)
        pro = next(p for p in overview["plans"] if p["plan"] == "Pro")
        assert (pro["requests_per_minute"], pro["max_concurrent_jobs"]) == (100, 7)
        management.set_plan_limits("pro", 0, 0, actor=ADMIN)            # 0 = no limit
        pro_now = management.get_plan_limits()["Pro"]
        assert (pro_now["requests_per_minute"], pro_now["max_concurrent_jobs"]) == (0, 0)
        entries = [e["action"] for e in management.audit_logger.list_entries()]
        assert any("changed Pro plan limits 3000/min, 200 jobs -> 100/min, 7 jobs" in a for a in entries)

    @pytest.mark.parametrize("actor", [OPERATOR, SimpleNamespace(name="Dev", role="Developer"), None])
    def test_nobody_else_can(self, env, actor):
        _, management = env
        with pytest.raises(PermissionError):
            management.set_plan_limits("Pro", 1, 1, actor=actor)
        assert management.get_plan_limits() == plans.DEFAULT_PLAN_LIMITS

    @pytest.mark.parametrize("plan, rpm, jobs", [("Gold", 1, 1), ("Pro", -1, 1), ("Pro", 1, "x"), ("Pro", 1.5, 1)])
    def test_bad_input_is_a_value_error_and_stores_nothing(self, env, plan, rpm, jobs):
        _, management = env
        with pytest.raises(ValueError):
            management.set_plan_limits(plan, rpm, jobs, actor=OWNER)
        assert management.get_plan_limits() == plans.DEFAULT_PLAN_LIMITS

    def test_tier_edits_survive_a_restart(self, tmp_path, monkeypatch):
        coordinator, management = build(tmp_path, monkeypatch, "restart")
        management.set_plan_limits("Standard", 11, 2, actor=OWNER)
        coordinator.shutdown()
        coordinator2 = GCONCoordinator(control_plane=ControlPlane(path=str(tmp_path / "cp_restart.db")))
        try:
            management2 = ManagementLayer(coordinator=coordinator2, db_path=str(tmp_path / "m_restart.db"))
            standard = management2.get_plan_limits()["Standard"]
            assert (standard["requests_per_minute"], standard["max_concurrent_jobs"]) == (11, 2)
        finally:
            coordinator2.shutdown()

    def test_without_a_control_plane_there_is_nowhere_to_store_it(self, tmp_path):
        management = ManagementLayer(coordinator=None, db_path=str(tmp_path / "m.db"))
        assert management.get_plan_limits() == plans.DEFAULT_PLAN_LIMITS      # defaults still work
        with pytest.raises(ValueError):
            management.set_plan_limits("Pro", 1, 1, actor=OWNER)


class TestWhatReallyApplies:
    def test_an_organizations_limits_follow_its_plan_and_staff_edits(self, env):
        _, management = env
        org = management.create_organization("Acme", "Pro")
        assert management.get_org_limits(org["org_id"]) == {"plan": "Pro", **plans.DEFAULT_PLAN_LIMITS["Pro"]}
        management.set_plan_limits("Pro", 5, 2, actor=OWNER)
        limits = management.get_org_limits(org["org_id"])
        assert (limits["plan"], limits["requests_per_minute"], limits["max_concurrent_jobs"]) == ("Pro", 5, 2)
        assert limits["allowed_programs"] == plans.DEFAULT_PLAN_LIMITS["Pro"]["allowed_programs"]

    def test_no_organization_or_an_unknown_one_gets_the_default_tier(self, env):
        _, management = env
        for org_id in (None, "org_missing"):
            limits = management.get_org_limits(org_id)
            assert limits["plan"] == "Standard"
            assert limits["requests_per_minute"] == 600

    def test_environment_settings_win_for_everyone(self, tmp_path, monkeypatch):
        monkeypatch.setenv("GCON_API_RATE_LIMIT_PER_MINUTE", "25")
        monkeypatch.setenv("GCON_MAX_CONCURRENT_JOBS_PER_ORG", "4")
        coordinator = GCONCoordinator(control_plane=ControlPlane(path=str(tmp_path / "cp_env.db")))
        try:
            management = ManagementLayer(coordinator=coordinator, db_path=str(tmp_path / "m_env.db"))
            org = management.create_organization("Acme", "Enterprise")
            limits = management.get_org_limits(org["org_id"])
            assert (limits["plan"], limits["requests_per_minute"], limits["max_concurrent_jobs"]) == (
                "Enterprise", 25, 4)
            overview = management.get_plans_overview()
            assert overview["env_overrides"] == {"requests_per_minute": 25, "max_concurrent_jobs": 4}
        finally:
            coordinator.shutdown()

    def test_rate_limiting_switched_off_by_the_environment_is_reported_as_no_limit(self, tmp_path, monkeypatch):
        monkeypatch.setenv("GCON_API_RATE_LIMIT_PER_MINUTE", "0")
        coordinator = GCONCoordinator(control_plane=ControlPlane(path=str(tmp_path / "cp_off.db")))
        try:
            management = ManagementLayer(coordinator=coordinator, db_path=str(tmp_path / "m_off.db"))
            org = management.create_organization("Acme", "Standard")
            assert management.get_org_limits(org["org_id"])["requests_per_minute"] == 0
        finally:
            coordinator.shutdown()

    def test_overview_counts_organizations_per_plan_and_old_labels(self, env):
        _, management = env
        management.create_organization("A", "Standard")
        management.create_organization("B", "Pro")
        management.create_organization("C", "Pro")
        management.org_registry.add_organization("Old", plan="Free")
        overview = management.get_plans_overview()
        assert {p["plan"]: p["organizations"] for p in overview["plans"]} == {
            "Standard": 1, "Pro": 2, "Enterprise": 0}
        assert overview["organizations_on_other_labels"] == 1
        assert overview["env_overrides"] == {"requests_per_minute": None, "max_concurrent_jobs": None}


class TestCustomerView:
    def test_shows_plan_limits_and_null_for_no_limit(self, env):
        _, management = env
        org = management.create_organization("Acme", "Pro")
        management.set_plan_limits("Pro", 0, 7, actor=OWNER)
        view = management.get_customer_plan(org["org_id"])
        assert view == {
            "plan": "Pro", "requests_per_minute": None, "max_concurrent_jobs": 7,
            "allowed_programs": plans.DEFAULT_PLAN_LIMITS["Pro"]["allowed_programs"],
            "max_runtime_seconds": plans.DEFAULT_PLAN_LIMITS["Pro"]["max_runtime_seconds"],
            "jobs_in_flight": 0,
            "max_workers": 25,
            "max_leased_workers": 4,
            "leased_workers_held": 0,
        }

    def test_any_program_and_no_runtime_limit_show_as_null(self, env):
        _, management = env
        org = management.create_organization("Big", "Enterprise")
        management.set_plan_limits("Enterprise", 100, 5, ["*"], 0, actor=OWNER)
        view = management.get_customer_plan(org["org_id"])
        assert view["allowed_programs"] is None and view["max_runtime_seconds"] is None

    def test_requires_an_organization(self, env):
        _, management = env
        with pytest.raises(ValueError):
            management.get_customer_plan(None)
