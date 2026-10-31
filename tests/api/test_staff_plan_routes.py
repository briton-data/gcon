"""
The internal staff dashboard's side of plans: GET/PUT /management/plans, the
organization routes (plan validated, limits shown) and the dashboard page
itself carrying the Plans card. Built through the real WebServer.
"""
import pytest
from fastapi.testclient import TestClient

from gcon.cluster.coordinator import GCONCoordinator
from gcon.dashboard.presentation import PresentationLayer
from gcon.dashboard.web_server import WebServer
from gcon.management import plans
from gcon.persistence import ControlPlane

pytestmark = pytest.mark.real_plan_defaults

OWNER_EMAIL = "owner@example.com"
OWNER_PASSWORD = "owner-pw-123"


@pytest.fixture
def stack(tmp_path, monkeypatch):
    monkeypatch.setenv("GCON_DB_PATH", str(tmp_path / "gcon.db"))
    monkeypatch.setenv("GCON_OWNER_PASSWORD", OWNER_PASSWORD)
    monkeypatch.delenv("GCON_API_RATE_LIMIT_PER_MINUTE", raising=False)
    monkeypatch.delenv("GCON_MAX_CONCURRENT_JOBS_PER_ORG", raising=False)
    coordinator = GCONCoordinator(control_plane=ControlPlane(path=str(tmp_path / "cp.db")))
    server = WebServer(PresentationLayer(coordinator))
    yield server, coordinator
    coordinator.shutdown()


def login(server, email=OWNER_EMAIL, password=OWNER_PASSWORD):
    client = TestClient(server.app)
    r = client.post("/auth/login", json={"email": email, "password": password})
    assert r.status_code == 200, r.text
    return client


def staff_user(server, role):
    email = f"{role.lower()}@example.com"
    server.management.create_user(
        role, email, role, password="staff-pw-12345",
        actor=server.management.user_registry.get_user_by_email(OWNER_EMAIL),
    )
    return login(server, email, "staff-pw-12345")


class TestPlansRoutes:
    def test_need_a_staff_login(self, stack):
        server, _ = stack
        anonymous = TestClient(server.app)
        assert anonymous.get("/management/plans").status_code == 401
        assert anonymous.put("/management/plans/Pro", json={"requests_per_minute": 1, "max_concurrent_jobs": 1}).status_code == 401

    def test_get_lists_the_three_plans_with_defaults_and_org_counts(self, stack):
        server, _ = stack
        client = login(server)
        client.post("/management/organizations", json={"name": "A", "plan": "Pro"})
        body = client.get("/management/plans").json()
        assert [p["plan"] for p in body["plans"]] == ["Standard", "Pro", "Enterprise"]
        pro = body["plans"][1]
        assert pro["requests_per_minute"] == plans.DEFAULT_PLAN_LIMITS["Pro"]["requests_per_minute"]
        assert pro["max_concurrent_jobs"] == plans.DEFAULT_PLAN_LIMITS["Pro"]["max_concurrent_jobs"]
        assert pro["organizations"] == 1
        assert body["env_overrides"] == {"requests_per_minute": None, "max_concurrent_jobs": None}

    def test_owner_can_change_a_plan_and_it_is_what_customers_get(self, stack):
        server, coordinator = stack
        client = login(server)
        org = client.post("/management/organizations", json={"name": "Acme", "plan": "Pro"}).json()
        r = client.put("/management/plans/Pro", json={"requests_per_minute": 123, "max_concurrent_jobs": 4})
        assert r.status_code == 200, r.text
        assert r.json()["plans"][1]["requests_per_minute"] == 123
        listed = next(o for o in client.get("/management/organizations").json() if o["org_id"] == org["org_id"])
        assert (listed["limits"]["requests_per_minute"], listed["limits"]["max_concurrent_jobs"]) == (123, 4)
        assert server.management.get_customer_plan(org["org_id"])["max_concurrent_jobs"] == 4

    @pytest.mark.parametrize("payload", [
        {}, {"requests_per_minute": 5}, {"requests_per_minute": -1, "max_concurrent_jobs": 1},
        {"requests_per_minute": "10", "max_concurrent_jobs": 1}, {"requests_per_minute": 1.5, "max_concurrent_jobs": 1},
    ])
    def test_bad_numbers_are_a_400(self, stack, payload):
        server, _ = stack
        r = login(server).put("/management/plans/Pro", json=payload)
        assert r.status_code == 400
        assert server.management.get_plan_limits() == plans.DEFAULT_PLAN_LIMITS

    def test_an_unknown_plan_name_is_a_400(self, stack):
        server, _ = stack
        r = login(server).put("/management/plans/Gold", json={"requests_per_minute": 1, "max_concurrent_jobs": 1})
        assert r.status_code == 400

    @pytest.mark.parametrize("role", ["Operator", "Developer", "Viewer"])
    def test_only_owner_or_administrator_may_change_a_plan(self, stack, role):
        server, _ = stack
        client = staff_user(server, role)
        r = client.put("/management/plans/Pro", json={"requests_per_minute": 1, "max_concurrent_jobs": 1})
        assert r.status_code == 403
        assert server.management.get_plan_limits() == plans.DEFAULT_PLAN_LIMITS

    def test_changes_are_in_the_audit_log(self, stack):
        server, _ = stack
        login(server).put("/management/plans/Standard", json={"requests_per_minute": 9, "max_concurrent_jobs": 3})
        actions = [e["action"] for e in server.management.audit_logger.list_entries()]
        assert any("changed Standard plan limits 600/min, 50 jobs -> 9/min, 3 jobs" in a for a in actions)


class TestOrganizationRoutes:
    def test_create_defaults_to_nothing_unknown_and_rejects_retired_plans(self, stack):
        server, _ = stack
        client = login(server)
        assert client.post("/management/organizations", json={"name": "Old", "plan": "Free"}).status_code == 400
        ok = client.post("/management/organizations", json={"name": "New", "plan": "enterprise"})
        assert ok.status_code == 200
        assert ok.json()["plan"] == "Enterprise"
        assert ok.json()["limits"] == plans.DEFAULT_PLAN_LIMITS["Enterprise"]

    def test_update_changes_the_plan_and_rejects_unknown_ones(self, stack):
        server, _ = stack
        client = login(server)
        org = client.post("/management/organizations", json={"name": "Acme", "plan": "Standard"}).json()
        r = client.put(f"/management/organizations/{org['org_id']}", json={"name": "Acme", "plan": "Pro"})
        assert r.status_code == 200 and r.json()["plan"] == "Pro"
        bad = client.put(f"/management/organizations/{org['org_id']}", json={"name": "Acme", "plan": "Gold"})
        assert bad.status_code == 400
        assert client.get(f"/management/organizations/{org['org_id']}").json()["plan"] == "Pro"

    def test_editing_only_the_name_keeps_an_old_label(self, stack):
        server, _ = stack
        client = login(server)
        legacy = server.management.org_registry.add_organization("Old Co", plan="Free")
        r = client.put(f"/management/organizations/{legacy.org_id}", json={"name": "Old Co 2"})
        assert r.status_code == 200
        assert r.json()["plan"] == "Free" and r.json()["name"] == "Old Co 2"
        assert r.json()["limits"] == plans.DEFAULT_PLAN_LIMITS["Standard"]


class TestStaffCanChangeVerificationAndTheAllowance:
    def test_put_saves_both_and_get_shows_them(self, stack):
        server, _ = stack
        client = login(server)
        before = {p["plan"]: p for p in client.get("/management/plans").json()["plans"]}
        assert before["Standard"]["verification"] is False and before["Standard"]["free_allowance_cents"] == 500
        r = client.put("/management/plans/Standard", json={
            "requests_per_minute": 600, "max_concurrent_jobs": 50, "verification": True, "free_allowance_cents": 1250})
        assert r.status_code == 200, r.text
        after = {p["plan"]: p for p in client.get("/management/plans").json()["plans"]}
        assert after["Standard"]["verification"] is True and after["Standard"]["free_allowance_cents"] == 1250
        assert after["Pro"]["verification"] is True and after["Pro"]["free_allowance_cents"] == 0     # untouched

    def test_leaving_them_out_keeps_what_was_there(self, stack):
        server, _ = stack
        client = login(server)
        client.put("/management/plans/Standard", json={"requests_per_minute": 100, "max_concurrent_jobs": 5})
        std = {p["plan"]: p for p in client.get("/management/plans").json()["plans"]}["Standard"]
        assert std["verification"] is False and std["free_allowance_cents"] == 500

    def test_a_bad_allowance_is_a_400(self, stack):
        server, _ = stack
        client = login(server)
        for bad in (-1, 2.5, "5", True):
            r = client.put("/management/plans/Standard", json={"requests_per_minute": 1, "max_concurrent_jobs": 1, "free_allowance_cents": bad})
            assert r.status_code == 400, bad

    def test_the_page_has_the_two_columns(self, stack):
        server, _ = stack
        html = login(server).get("/").text
        assert "<th>Verification</th>" in html and "<th>Free monthly allowance ($)</th>" in html


class TestDashboardPage:
    def test_the_page_has_the_plans_card_and_only_the_three_plans(self, stack):
        server, _ = stack
        html = login(server).get("/").text
        assert 'id="plans-body"' in html and 'id="plans-note"' in html
        assert "<th>Limits</th>" in html
        for select_id in ("create-org-plan", "edit-org-plan"):
            block = html[html.index(f'id="{select_id}"'):]
            block = block[:block.index("</select>")]
            assert 'value="Standard"' in block and 'value="Pro"' in block and 'value="Enterprise"' in block
            assert 'value="Free"' not in block

    def test_the_script_talks_to_the_plan_routes(self, stack):
        server, _ = stack
        js = login(server).get("/static/js/dashboard.js").text
        assert '"/management/plans"' in js and "/management/plans/" in js
        assert "loadPlansPanel" in js


class TestProgramsAndRuntimeFromTheDashboard:
    def test_owner_sets_programs_and_runtime_and_the_plan_overview_shows_them(self, stack):
        server, _ = stack
        client = login(server)
        r = client.put("/management/plans/Standard", json={
            "requests_per_minute": 600, "max_concurrent_jobs": 50,
            "allowed_programs": ["python", "make"], "max_runtime_seconds": 120,
        })
        assert r.status_code == 200, r.text
        standard = r.json()["plans"][0]
        assert standard["allowed_programs"] == ["python", "make"]
        assert standard["max_runtime_seconds"] == 120
        assert server.management.get_plan_limits()["Standard"]["allowed_programs"] == ["python", "make"]

    def test_an_older_style_request_without_them_keeps_the_current_values(self, stack):
        server, _ = stack
        client = login(server)
        client.put("/management/plans/Standard", json={
            "requests_per_minute": 600, "max_concurrent_jobs": 50,
            "allowed_programs": ["make"], "max_runtime_seconds": 77})
        r = client.put("/management/plans/Standard", json={"requests_per_minute": 9, "max_concurrent_jobs": 3})
        assert r.status_code == 200
        standard = server.management.get_plan_limits()["Standard"]
        assert (standard["allowed_programs"], standard["max_runtime_seconds"]) == (["make"], 77)

    @pytest.mark.parametrize("payload", [
        {"allowed_programs": []},
        {"allowed_programs": "python"},
        {"allowed_programs": ["/usr/bin/python"]},
        {"allowed_programs": ["py thon"]},
        {"allowed_programs": [1]},
        {"max_runtime_seconds": -1},
        {"max_runtime_seconds": "60"},
        {"max_runtime_seconds": 1.5},
    ])
    def test_bad_values_are_a_400_and_change_nothing(self, stack, payload):
        server, _ = stack
        body = {"requests_per_minute": 600, "max_concurrent_jobs": 50, **payload}
        r = login(server).put("/management/plans/Standard", json=body)
        assert r.status_code == 400
        assert server.management.get_plan_limits() == plans.DEFAULT_PLAN_LIMITS

    def test_the_audit_log_records_the_programs_and_runtime_change(self, stack):
        server, _ = stack
        login(server).put("/management/plans/Standard", json={
            "requests_per_minute": 600, "max_concurrent_jobs": 50,
            "allowed_programs": ["python"], "max_runtime_seconds": 60})
        actions = " | ".join(e["action"] for e in server.management.audit_logger.list_entries())
        assert "allowed programs python, python3, nvidia-smi, echo -> python" in actions
        assert "max runtime 3600s -> 60s" in actions

    def test_the_page_and_script_have_the_new_columns(self, stack):
        server, _ = stack
        client = login(server)
        html = client.get("/").text
        assert "Max runtime (seconds)" in html and "Allowed programs" in html
        js = client.get("/static/js/dashboard.js").text
        assert "plan-programs" in js and "plan-runtime" in js
        assert "allowed_programs: programs" in js
