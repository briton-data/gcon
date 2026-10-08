"""
A key with no organization must never see, cancel or change customer data.

`org_id=None` used to mean "no filter" on every query, so a staff-owned key, or
a key whose owner no longer exists, listed every tenant's jobs (stdout
included) and could cancel them. Customer routes now refuse such keys (403) and
a key can no longer be minted for an owner that does not exist.
"""
import pytest
from tests.support.label_client import LabelClient as TestClient

from gcon.api.api_v1 import create_api_v1_app
from gcon.cluster.coordinator import GCONCoordinator
from gcon.dashboard.presentation import PresentationLayer
from gcon.management.management_layer import ManagementLayer
from gcon.persistence.control_plane import ControlPlane

SCOPES = ["View monitoring", "Submit workflows"]


@pytest.fixture
def env(tmp_path):
    coordinator = GCONCoordinator(control_plane=ControlPlane(path=str(tmp_path / "cp.db")))
    management = ManagementLayer(coordinator=coordinator, db_path=str(tmp_path / "mgmt.db"))
    client = TestClient(create_api_v1_app(management, PresentationLayer(coordinator)))
    yield client, coordinator, management
    coordinator.shutdown()


def _h(secret):
    return {"X-API-Key": secret}


@pytest.fixture
def world(env):
    client, coordinator, management = env
    acme = management.signup_customer("Acme", "Ann", "ann@acme.example", "correct-horse-1")["api_key"]["secret"]
    job_id = client.post("/jobs", json={"client_reference": "acme-secret", "command": "sleep 30"}, headers=_h(acme)).json()["job_id"]
    staff = management.create_user("Staff", "staff@gcon.example", role="Owner", organization_id=None)
    staff_key = management.create_api_key("staff-key", staff["user_id"], scopes=SCOPES)["secret"]
    return client, coordinator, management, acme, staff_key, job_id


CUSTOMER_ROUTES = [
    ("get", "/jobs"), ("get", "/workflows"), ("get", "/receipts"), ("get", "/nodes"),
    ("get", "/telemetry/events"), ("get", "/artifacts"), ("get", "/auth/api-keys"),
]


class TestOrgLessKeysAreRefused:
    @pytest.mark.parametrize("method,path", CUSTOMER_ROUTES)
    def test_listing_routes_return_403(self, world, method, path):
        client, _, _, _, staff_key, _ = world
        assert getattr(client, method)(path, headers=_h(staff_key)).status_code == 403

    def test_a_staff_key_cannot_read_cancel_retry_or_submit(self, world):
        client, coordinator, _, _, staff_key, job_id = world
        assert client.get(f"/jobs/{job_id}", headers=_h(staff_key)).status_code == 403
        assert client.post(f"/jobs/{job_id}/cancel", headers=_h(staff_key)).status_code == 403
        assert client.post(f"/jobs/{job_id}/retry", headers=_h(staff_key)).status_code == 403
        assert client.post("/jobs", json={"command": "echo x"}, headers=_h(staff_key)).status_code == 403
        assert client.post("/jobs/clear", json={"job_ids": [job_id]}, headers=_h(staff_key)).status_code == 403
        assert coordinator.jobs[job_id]["status"] != "cancelled"

    def test_the_customer_still_has_full_access_to_their_own_data(self, world):
        client, _, _, acme, _, job_id = world
        assert client.get(f"/jobs/{job_id}", headers=_h(acme)).status_code == 200
        assert [j["job_id"] for j in client.get("/jobs", headers=_h(acme)).json()] == [job_id]

    def test_cluster_wide_and_identity_routes_still_work_for_a_staff_key(self, world):
        client, _, _, _, staff_key, _ = world
        for path in ("/cluster", "/health", "/metrics", "/whoami"):
            assert client.get(path, headers=_h(staff_key)).status_code == 200, path


class TestOwnerlessKeys:
    def test_a_key_cannot_be_minted_for_a_user_that_does_not_exist(self, env):
        _, _, management = env
        with pytest.raises(ValueError, match="unknown user"):
            management.create_api_key("ghost", "no-such-user", scopes=SCOPES)

    def test_a_key_whose_owner_was_deleted_is_refused_on_customer_routes(self, world):
        client, _, management, _, _, job_id = world
        user = management.create_user("Temp", "temp@gcon.example", role="Owner", organization_id=None)
        key = management.create_api_key("temp", user["user_id"], scopes=SCOPES)["secret"]
        management.delete_user(user["user_id"])
        assert client.get("/jobs", headers=_h(key)).status_code == 403
        assert client.post(f"/jobs/{job_id}/cancel", headers=_h(key)).status_code == 403
