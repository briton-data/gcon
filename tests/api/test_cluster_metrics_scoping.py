"""/cluster and /metrics gave every customer the platform-wide counts (other
customers' jobs, all workers, uptime, disk). A customer now gets only its own
organization's workers and jobs; a staff key (no organization) still sees all."""
import pytest
from tests.support.label_client import LabelClient as TestClient

from gcon.api.api_v1 import create_api_v1_app
from gcon.cluster.coordinator import GCONCoordinator
from gcon.dashboard.presentation import PresentationLayer
from gcon.management.management_layer import ManagementLayer
from gcon.persistence.control_plane import ControlPlane

SCOPES = ["View monitoring", "Submit workflows"]


def _h(secret):
    return {"X-API-Key": secret}


@pytest.fixture
def world(tmp_path):
    coordinator = GCONCoordinator(control_plane=ControlPlane(path=str(tmp_path / "cp.db")))
    management = ManagementLayer(coordinator=coordinator, db_path=str(tmp_path / "mgmt.db"))
    client = TestClient(create_api_v1_app(management, PresentationLayer(coordinator)))
    acme = management.signup_customer("Acme", "Ann", "ann@acme.example", "correct-horse-1")["api_key"]["secret"]
    beta = management.signup_customer("Beta", "Bob", "bob@beta.example", "correct-horse-2")["api_key"]["secret"]
    staff = management.create_user("Staff", "staff@gcon.example", role="Owner", organization_id=None)
    staff_key = management.create_api_key("staff-key", staff["user_id"], scopes=SCOPES)["secret"]
    # Jobs stay pending (no worker) -- enough to tell the organizations apart.
    for i in range(3):
        assert client.post("/jobs", json={"command": "echo a"}, headers=_h(acme)).status_code in (200, 201, 202)
    assert client.post("/jobs", json={"command": "echo b"}, headers=_h(beta)).status_code in (200, 201, 202)
    yield client, acme, beta, staff_key
    coordinator.shutdown()


def test_metrics_for_a_customer_count_only_its_own_jobs(world):
    client, acme, beta, staff_key = world
    assert client.get("/metrics", headers=_h(acme)).json()["queued_jobs"] == 3
    assert client.get("/metrics", headers=_h(beta)).json()["queued_jobs"] == 1
    assert client.get("/metrics", headers=_h(staff_key)).json()["queued_jobs"] == 4


def test_metrics_for_a_customer_leave_out_the_platform_fields(world):
    client, acme, _, staff_key = world
    own = client.get("/metrics", headers=_h(acme)).json()
    full = client.get("/metrics", headers=_h(staff_key)).json()
    for platform_field in ("uptime_seconds", "event_count", "node_summary", "disk_remaining_pct",
                           "artifact_count", "coordinator_online", "scheduler_running"):
        assert platform_field in full and platform_field not in own


def test_cluster_for_a_customer_counts_only_its_own_workers_and_jobs(world):
    client, acme, _, staff_key = world
    own = client.get("/cluster", headers=_h(acme)).json()
    assert own["total_nodes"] == 0 and own["registered_nodes"] == [] and own["running_jobs"] == 0
    assert set(own) >= {"total_nodes", "idle_nodes", "registered_node_count",
                        "running_jobs", "completed_jobs", "failed_jobs"}
    assert client.get("/cluster", headers=_h(staff_key)).status_code == 200
