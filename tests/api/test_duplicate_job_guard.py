"""
The same command is not run twice by accident: a second POST /jobs with a command that matches one of the
organization's pending, running or completed jobs is refused with 409 code=duplicate_job, unless the caller sends
confirm_rerun=true. A failed or cancelled job does not block. Other organizations never interfere.
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
    coordinator.org_worker_gate_resolver = None
    coordinator.pause_scheduler()
    client = TestClient(create_api_v1_app(management, PresentationLayer(coordinator)))
    client.guard_duplicates = True
    yield client, coordinator
    coordinator.shutdown()


def _signup(client, org, email):
    r = client.post("/auth/signup", json={"org_name": org, "name": "Ann", "email": email, "password": "correct-horse-1"})
    assert r.status_code == 200, r.text
    return {"Authorization": f"Bearer {r.json()['api_key']['secret']}"}


def test_same_command_is_refused_until_the_rerun_is_confirmed(env):
    client, _ = env
    h = _signup(client, "Acme", "a@acme.example")
    first = client.post("/jobs", json={"command": "python train.py"}, headers=h)
    assert first.status_code == 200, first.text

    again = client.post("/jobs", json={"command": "python   train.py"}, headers=h)   # extra spaces do not dodge it
    assert again.status_code == 409
    body = again.json()
    assert body["code"] == "duplicate_job" and body["existing_job_id"] == first.json()["job_id"]

    confirmed = client.post("/jobs", json={"command": "python train.py", "confirm_rerun": True}, headers=h)
    assert confirmed.status_code == 200
    assert confirmed.json()["job_id"] != first.json()["job_id"]


def test_a_different_command_is_not_a_duplicate(env):
    client, _ = env
    h = _signup(client, "Acme", "a@acme.example")
    assert client.post("/jobs", json={"command": "python train.py"}, headers=h).status_code == 200
    assert client.post("/jobs", json={"command": "python train.py --epochs 2"}, headers=h).status_code == 200


def test_a_failed_job_does_not_block_a_resubmit(env):
    client, coordinator = env
    h = _signup(client, "Acme", "a@acme.example")
    job_id = client.post("/jobs", json={"command": "python train.py"}, headers=h).json()["job_id"]
    coordinator.control_plane.jobs.set_status(job_id, "failed", completed=True)
    assert client.post("/jobs", json={"command": "python train.py"}, headers=h).status_code == 200


def test_other_organizations_do_not_interfere(env):
    client, _ = env
    a = _signup(client, "Acme", "a@acme.example")
    b = _signup(client, "Beta", "b@beta.example")
    assert client.post("/jobs", json={"command": "python train.py"}, headers=a).status_code == 200
    assert client.post("/jobs", json={"command": "python train.py"}, headers=b).status_code == 200


def test_replaying_an_idempotency_key_returns_the_first_job_not_a_duplicate_error(env):
    client, _ = env
    h = {**_signup(client, "Acme", "a@acme.example"), "Idempotency-Key": "k-1"}
    first = client.post("/jobs", json={"command": "python train.py"}, headers=h)
    again = client.post("/jobs", json={"command": "python train.py"}, headers=h)
    assert again.status_code == 200 and again.json()["job_id"] == first.json()["job_id"]
