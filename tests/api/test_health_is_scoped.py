"""
GET /health for a customer key describes that customer's own slice only.

/cluster and /metrics were already scoped per organization. /health was not: it returned the platform-wide counts and
the internal per-branch detail (queue, storage, node registry, reasons) to any key with "View monitoring", so a
customer could read other customers' activity and GCON's own infrastructure from it.
"""
import pytest
from tests.support.label_client import LabelClient as TestClient

from gcon.api.api_v1 import create_api_v1_app
from gcon.cluster.coordinator import GCONCoordinator
from gcon.dashboard.presentation import PresentationLayer
from gcon.management.management_layer import ManagementLayer
from gcon.persistence.control_plane import ControlPlane

SCOPES = ["View monitoring"]


def _h(key):
    return {"X-API-Key": key}


@pytest.fixture
def world(tmp_path):
    coordinator = GCONCoordinator(control_plane=ControlPlane(path=str(tmp_path / "cp.db")))
    management = ManagementLayer(coordinator=coordinator, db_path=str(tmp_path / "mgmt.db"))
    coordinator.org_worker_gate_resolver = None
    coordinator.pause_scheduler()
    keys = {}
    for name in ("acme", "globex"):
        org = management.create_organization(f"{name} Inc")
        user = management.create_user(f"{name} user", f"u@{name}.example", role="Owner",
                                      organization_id=org["org_id"])
        keys[name] = (org["org_id"], management.create_api_key(f"{name}-key", user["user_id"], scopes=SCOPES)["secret"])
    staff = management.create_user("Staff", "staff@gcon.example", role="Owner", organization_id=None)
    keys["staff"] = (None, management.create_api_key("staff-key", staff["user_id"], scopes=SCOPES)["secret"])

    # globex has done work (3 failed jobs) that acme must not be able to see
    for i in range(3):
        coordinator.submit_job(f"globex-{i}", "echo hi", org_id=keys["globex"][0])
        coordinator.jobs[f"globex-{i}"]["status"] = "failed"
    client = TestClient(create_api_v1_app(management, PresentationLayer(coordinator)))
    yield client, keys
    coordinator.shutdown()


class TestCustomerHealthIsTheirOwn:
    def test_a_customer_does_not_see_other_customers_activity(self, world):
        client, keys = world
        body = client.get("/health", headers=_h(keys["acme"][1])).json()
        assert body["metrics"]["failed_jobs"] == 0          # globex's three failures are not acme's
        assert body["metrics"]["total_nodes"] == 0

    def test_a_customer_does_not_see_internal_platform_detail(self, world):
        client, keys = world
        body = client.get("/health", headers=_h(keys["acme"][1])).json()
        for internal in ("checks", "reasons", "reason", "score", "services", "last_issue"):
            assert internal not in body, internal

    def test_a_customer_still_gets_the_overall_state(self, world):
        client, keys = world
        body = client.get("/health", headers=_h(keys["acme"][1])).json()
        assert body["state"] and body["computed_at"]

    def test_a_customer_sees_their_own_numbers(self, world):
        client, keys = world
        body = client.get("/health", headers=_h(keys["globex"][1])).json()
        assert body["metrics"]["failed_jobs"] == 3


class TestStaffHealthIsUnchanged:
    def test_a_staff_key_still_gets_the_full_platform_view(self, world):
        client, keys = world
        body = client.get("/health", headers=_h(keys["staff"][1])).json()
        assert "checks" in body and "reasons" in body
        assert body["metrics"]["failed_jobs"] == 3
