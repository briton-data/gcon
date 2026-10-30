"""
A job (or a verified job) that the organization has no workers to run is refused at submission, with what to do about
it. The rule: workers the organization can ever use are its own (registered, not archived, online or not) plus hosted
workers under an ACTIVE lease. Busy means wait, never refuse. Validation comes first.
"""
from types import SimpleNamespace

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
    yield client, coordinator, management
    coordinator.shutdown()


def _signup(client, management, name="Acme", email="a@acme.example"):
    r = client.post("/auth/signup", json={"org_name": name, "name": "Ann", "email": email,
                                          "password": "correct-horse-1"})
    assert r.status_code == 200, r.text
    org_id = next(o["org_id"] for o in management.get_organizations() if o["name"] == name)
    return {"Authorization": f"Bearer {r.json()['api_key']['secret']}"}, org_id


def _own_worker(coordinator, org_id, node_id="own-1"):
    coordinator.control_plane.nodes.upsert(node_id=node_id, hostname=node_id, status="offline", org_id=org_id)


def _hosted(coordinator, *names):
    for n in names:
        coordinator.control_plane.fleet_nodes.add(n, None)
        coordinator.registry.register(SimpleNamespace(
            node_id=n, org_id=None, sandboxed=True, agent_version="1.1.0", status="idle", is_connected=lambda: True))


JOB = {"command": "python -c pass"}


class TestRefusedWithWhatToDo:
    def test_no_workers_is_a_409_that_names_the_limit_and_the_way_to_request(self, env):
        client, coordinator, management = env
        acme, _ = _signup(client, management)
        r = client.post("/jobs", json=JOB, headers=acme)
        assert r.status_code == 409
        body = r.json()
        assert body["code"] == "no_workers" and body["workers_have"] == 0 and body["workers_needed"] == 1
        assert body["max_leased_workers"] == 1 and body["leased_workers_held"] == 0
        assert body["request_workers"] == "POST /workers/leases" and "POST /workers/leases" in body["detail"]

    def test_nothing_is_created_by_a_refusal(self, env):
        client, coordinator, management = env
        acme, _ = _signup(client, management)
        client.post("/jobs", json=JOB, headers=acme)
        assert coordinator.jobs == {}

    def test_a_plan_with_no_hosted_workers_is_told_to_enroll_its_own_or_upgrade(self, env, monkeypatch):
        client, coordinator, management = env
        acme, _ = _signup(client, management)
        real = management.get_org_limits
        monkeypatch.setattr(management, "get_org_limits", lambda org: {**real(org), "max_leased_workers": 0})
        body = client.post("/jobs", json=JOB, headers=acme).json()
        assert body["request_workers"] is None
        assert "does not include GCON-hosted workers" in body["detail"]

    def test_at_the_plans_limit_the_answer_says_release_enroll_or_upgrade(self, env):
        client, coordinator, management = env
        acme, org_id = _signup(client, management)
        _hosted(coordinator, "h1")
        [lease] = coordinator.worker_leases.request(org_id, 1, "ann")
        coordinator.registry.nodes["h1"]["status"] = "busy"            # still finishing a job, so the lease stays open
        coordinator.registry.nodes["h1"]["running_jobs"] = 1
        coordinator.worker_leases.release(lease["lease_id"], org_id=org_id, by="ann")      # ending: holds a place, runs nothing
        body = client.post("/jobs", json=JOB, headers=acme).json()
        assert body["code"] == "no_workers" and body["request_workers"] is None
        assert "already hold 1 of the 1" in body["detail"]


class TestWhatCountsAsAWorker:
    def test_an_active_lease_counts(self, env):
        client, coordinator, management = env
        acme, org_id = _signup(client, management)
        _hosted(coordinator, "h1")
        coordinator.worker_leases.request(org_id, 1, "ann")
        assert client.post("/jobs", json=JOB, headers=acme).status_code == 200

    def test_the_customers_own_worker_counts_even_when_it_is_offline(self, env):
        client, coordinator, management = env
        acme, org_id = _signup(client, management)
        _own_worker(coordinator, org_id)                      # briefly offline: the job waits, it is not refused
        assert client.post("/jobs", json=JOB, headers=acme).status_code == 200

    def test_an_archived_worker_does_not_count(self, env):
        client, coordinator, management = env
        acme, org_id = _signup(client, management)
        _own_worker(coordinator, org_id)
        coordinator.control_plane.nodes.archive("own-1")
        assert client.post("/jobs", json=JOB, headers=acme).status_code == 409

    def test_another_organizations_worker_does_not_count(self, env):
        client, coordinator, management = env
        acme, _ = _signup(client, management)
        _, globex_id = _signup(client, management, "Globex", "g@globex.example")
        _own_worker(coordinator, globex_id)
        _hosted(coordinator, "h1")
        coordinator.worker_leases.request(globex_id, 1, "g")
        assert client.post("/jobs", json=JOB, headers=acme).status_code == 409


class TestVerificationNeedsTwoWorkers:
    VERIFY = {**JOB, "verify": {"replicas": 2}}

    def test_one_worker_cannot_verify(self, env):
        client, coordinator, management = env
        acme, org_id = _signup(client, management)
        management.update_organization(org_id, plan="Pro")   # verification is a Pro feature
        _own_worker(coordinator, org_id)
        r = client.post("/jobs", json=self.VERIFY, headers=acme)
        assert r.status_code == 409
        body = r.json()
        assert body["code"] == "not_enough_workers_to_verify" and body["workers_have"] == 1 and body["workers_needed"] == 2
        assert "Verification needs 2 workers and you have 1" in body["detail"]

    def test_two_workers_can(self, env):
        client, coordinator, management = env
        acme, org_id = _signup(client, management)
        management.update_organization(org_id, plan="Pro")   # verification is a Pro feature
        _own_worker(coordinator, org_id, "own-1")
        _own_worker(coordinator, org_id, "own-2")
        assert client.post("/jobs", json=self.VERIFY, headers=acme).status_code == 200

    def test_the_number_asked_for_is_the_number_needed(self, env):
        client, coordinator, management = env
        acme, org_id = _signup(client, management)
        management.update_organization(org_id, plan="Pro")   # verification is a Pro feature
        _own_worker(coordinator, org_id, "own-1")
        _own_worker(coordinator, org_id, "own-2")
        r = client.post("/jobs", json={**JOB, "verify": {"replicas": 3}}, headers=acme)
        assert r.status_code == 409 and r.json()["workers_needed"] == 3


class TestVerificationIsAPlanFeature:
    VERIFY = {**JOB, "verify": {"replicas": 2}}

    def test_standard_cannot_verify_even_with_workers(self, env):
        client, coordinator, management = env
        acme, org_id = _signup(client, management)
        _own_worker(coordinator, org_id, "own-1")
        _own_worker(coordinator, org_id, "own-2")
        r = client.post("/jobs", json=self.VERIFY, headers=acme)
        assert r.status_code == 400 and "Verification is not included in your plan" in r.json()["detail"]

    def test_a_plain_job_on_standard_is_unaffected(self, env):
        client, coordinator, management = env
        acme, org_id = _signup(client, management)
        _own_worker(coordinator, org_id)
        assert client.post("/jobs", json=JOB, headers=acme).status_code == 200

    def test_the_plan_view_says_so(self, env):
        client, _coordinator, management = env
        acme, org_id = _signup(client, management)
        assert client.get("/account/plan", headers=acme).json()["verification"] is False
        management.update_organization(org_id, plan="Pro")
        assert client.get("/account/plan", headers=acme).json()["verification"] is True

    def test_staff_can_switch_it_on_for_a_plan(self, env):
        client, coordinator, management = env
        acme, org_id = _signup(client, management)
        _own_worker(coordinator, org_id, "own-1")
        _own_worker(coordinator, org_id, "own-2")
        management.set_plan_limits("Standard", 600, 50, verification=True, actor=SimpleNamespace(name="Olive", role="Owner"))
        assert client.post("/jobs", json=self.VERIFY, headers=acme).status_code == 200


class TestValidationComesFirst:
    def test_an_unusable_command_is_a_400_even_with_no_workers(self, env):
        client, _coordinator, management = env
        acme, _ = _signup(client, management)
        assert client.post("/jobs", json={"command": ""}, headers=acme).status_code == 400

    def test_a_bad_verify_spec_is_a_400_not_a_409(self, env):
        client, _coordinator, management = env
        acme, org_id = _signup(client, management)
        management.update_organization(org_id, plan="Pro")   # verification is a Pro feature
        assert client.post("/jobs", json={**JOB, "verify": {"replicas": 1}}, headers=acme).status_code == 400

    def test_a_bad_workflow_is_a_400_and_an_ordinary_one_without_workers_a_409(self, env):
        client, coordinator, management = env
        acme, org_id = _signup(client, management)
        bad = {"jobs": [{"job_id": "a", "command": "x", "depends_on": ["ghost"]}]}
        assert client.post("/workflows", json=bad, headers=acme).status_code == 400
        good = {"jobs": [{"job_id": "a", "command": "python -c pass"}]}
        assert client.post("/workflows", json=good, headers=acme).status_code == 409
        _own_worker(coordinator, org_id)
        assert client.post("/workflows", json=good, headers=acme).status_code == 200


def test_a_job_the_workflow_engine_submits_is_not_gated_again(env):
    _client, coordinator, _management = env
    coordinator.org_worker_gate_resolver = lambda org_id, replicas: {"detail": "no", "code": "no_workers"}
    coordinator.submit_job("j1", "python -c pass", org_id="acme", workflow_id="wf-1")   # admitted with its workflow
    assert "j1" in coordinator.jobs


def test_a_system_key_with_no_organization_is_never_gated(env):
    _client, _coordinator, management = env
    assert management.worker_gate(None, 5) is None
