"""
The customer owns neither the canonical job id nor how/where the job runs.

  * POST /jobs mints `job_<uuid>`; the submitter's own label is `client_reference`
    (a correlation tag -- stored, echoed, filterable, never a key).
  * Two organizations using the same label cannot collide, overwrite or probe
    each other (previously `job_id` was a global key).
  * A request that names a platform-only field (sandbox, privilege, image, node,
    organization ...) is refused rather than silently ignored.
  * Every job submitted through the API is marked as requiring a sandbox.
  * Workflows follow the same rules: minted ids, label -> id map in the response.
"""
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


def _signup(client, org, email):
    r = client.post("/auth/signup", json={"org_name": org, "name": "N", "email": email, "password": "correct-horse-1"})
    assert r.status_code == 200, r.text
    return r.json()["api_key"]["secret"]


def _h(secret):
    return {"Authorization": f"Bearer {secret}"}


@pytest.fixture
def two_orgs(env):
    client, coordinator = env
    return client, coordinator, _signup(client, "Acme", "a@acme.example"), _signup(client, "Globex", "g@globex.example")


class TestCanonicalIds:
    def test_the_job_id_is_minted_by_gcon_not_chosen_by_the_caller(self, env):
        client, coordinator = env
        key = _signup(client, "Acme", "a@acme.example")
        r = client.post("/jobs", json={"job_id": "my-own-id", "command": "echo hi"}, headers=_h(key))
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["job_id"].startswith("job_") and body["job_id"] != "my-own-id"
        assert body["client_reference"] == "my-own-id"           # deprecated job_id is only a label
        assert "my-own-id" not in coordinator.jobs and body["job_id"] in coordinator.jobs

    def test_client_reference_wins_over_the_deprecated_job_id(self, env):
        client, _ = env
        key = _signup(client, "Acme", "a@acme.example")
        r = client.post("/jobs", json={"job_id": "old", "client_reference": "new", "command": "echo"}, headers=_h(key))
        assert r.json()["client_reference"] == "new"

    def test_the_label_is_returned_on_the_job_and_searchable_within_the_org(self, env):
        client, _ = env
        key = _signup(client, "Acme", "a@acme.example")
        a = client.post("/jobs", json={"client_reference": "batch-7", "command": "sleep 5"}, headers=_h(key)).json()["job_id"]
        b = client.post("/jobs", json={"client_reference": "batch-7", "command": "sleep 6"}, headers=_h(key)).json()["job_id"]
        client.post("/jobs", json={"client_reference": "other", "command": "sleep 7"}, headers=_h(key))
        assert a != b                                            # a label is not unique, an id is
        found = client.get("/jobs", params={"client_reference": "batch-7"}, headers=_h(key)).json()
        assert {j["job_id"] for j in found} == {a, b}
        assert client.get(f"/jobs/{a}", headers=_h(key)).json()["client_reference"] == "batch-7"

    @pytest.mark.parametrize("bad", ["x" * 129, "line\nbreak", "tab\there"])
    def test_a_malformed_label_is_refused(self, env, bad):
        client, _ = env
        key = _signup(client, "Acme", "a@acme.example")
        assert client.post("/jobs", json={"client_reference": bad, "command": "echo"}, headers=_h(key)).status_code == 400


class TestNoCrossTenantCollision:
    def test_two_orgs_using_the_same_label_get_two_independent_jobs(self, two_orgs):
        client, coordinator, acme, globex = two_orgs
        a = client.post("/jobs", json={"client_reference": "job-1", "command": "sleep 5"}, headers=_h(acme))
        g = client.post("/jobs", json={"client_reference": "job-1", "command": "sleep 5"}, headers=_h(globex))
        assert a.status_code == g.status_code == 200
        assert a.json()["job_id"] != g.json()["job_id"]
        assert coordinator.jobs[a.json()["job_id"]]["org_id"] != coordinator.jobs[g.json()["job_id"]]["org_id"]

    def test_a_caller_cannot_probe_for_or_reach_another_orgs_job_by_guessing(self, two_orgs):
        client, _, acme, globex = two_orgs
        a = client.post("/jobs", json={"client_reference": "secret-job", "command": "sleep 5"}, headers=_h(acme)).json()["job_id"]
        # Globex re-submitting Acme's label creates ITS OWN job; the label tells it nothing.
        g = client.post("/jobs", json={"job_id": "secret-job", "command": "echo mine"}, headers=_h(globex))
        assert g.status_code == 200 and g.json()["job_id"] != a
        assert client.get(f"/jobs/{a}", headers=_h(globex)).status_code == 404
        assert client.get("/jobs", params={"client_reference": "secret-job"}, headers=_h(globex)).json() != \
            client.get("/jobs", params={"client_reference": "secret-job"}, headers=_h(acme)).json()


class TestPlatformOnlyFieldsAreRefused:
    @pytest.mark.parametrize("field,value", [
        ("sandbox", "trusted"), ("sandbox_policy", "trusted"), ("trusted", True), ("privileged", True),
        ("execution_backend", "subprocess"), ("image", "evil:latest"), ("node_id", "node-1"),
        ("org_id", "someone-else"), ("sandbox_required", False), ("Sandbox", "none"),
    ])
    def test_naming_one_is_a_422_not_a_silent_no_op(self, env, field, value):
        client, coordinator = env
        key = _signup(client, "Acme", "a@acme.example")
        r = client.post("/jobs", json={"command": "echo hi", field: value}, headers=_h(key))
        assert r.status_code == 422, r.text
        assert len(coordinator.jobs) == 0

    def test_every_api_job_requires_a_sandbox(self, env):
        client, coordinator = env
        key = _signup(client, "Acme", "a@acme.example")
        job_id = client.post("/jobs", json={"command": "sleep 5"}, headers=_h(key)).json()["job_id"]
        assert coordinator.jobs[job_id]["require_sandbox"] is True


class TestWorkflows:
    def test_ids_are_minted_and_the_label_map_is_returned(self, env):
        client, coordinator = env
        key = _signup(client, "Acme", "a@acme.example")
        r = client.post("/workflows", json={
            "workflow_id": "nightly", "name": "Nightly",
            "jobs": [
                {"job_id": "extract", "command": "sleep 5"},
                {"job_id": "load", "command": "sleep 5", "depends_on": ["extract"]},
            ],
        }, headers=_h(key))
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["workflow_id"].startswith("wf_") and body["client_reference"] == "nightly"
        assert set(body["jobs"]) == {"extract", "load"}
        assert all(v.startswith("job_") for v in body["jobs"].values())
        assert "extract" not in coordinator.jobs
        assert coordinator.jobs[body["jobs"]["extract"]]["require_sandbox"] is True

    def test_two_orgs_can_use_the_same_workflow_and_job_labels(self, two_orgs):
        client, _, acme, globex = two_orgs
        payload = {"workflow_id": "wf", "jobs": [{"job_id": "a", "command": "sleep 5"}]}
        r1 = client.post("/workflows", json=payload, headers=_h(acme))
        r2 = client.post("/workflows", json=payload, headers=_h(globex))
        assert r1.status_code == r2.status_code == 200
        assert r1.json()["workflow_id"] != r2.json()["workflow_id"]
        assert r1.json()["jobs"]["a"] != r2.json()["jobs"]["a"]

    def test_a_dependency_on_an_unknown_label_is_a_400_and_a_duplicate_label_too(self, env):
        client, _ = env
        key = _signup(client, "Acme", "a@acme.example")
        bad_dep = {"jobs": [{"job_id": "a", "command": "x", "depends_on": ["ghost"]}]}
        dup = {"jobs": [{"job_id": "a", "command": "x"}, {"job_id": "a", "command": "y"}]}
        assert client.post("/workflows", json=bad_dep, headers=_h(key)).status_code == 400
        assert client.post("/workflows", json=dup, headers=_h(key)).status_code == 400
