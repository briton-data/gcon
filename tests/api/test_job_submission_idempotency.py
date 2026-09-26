"""
POST /jobs' Idempotency-Key header -- durable, per-org, survives
restart (backed by job_submission_idempotency_keys, not an in-memory
dict). A client retrying an ambiguous-outcome submission (dropped
response, etc.) with the same key gets back the job the *original*
request created, even if the retry's body names a different job_id --
that's the whole point: job_id uniqueness alone doesn't protect a
client that generates a fresh job_id per attempt, which is a very
ordinary thing to do.
"""
import time

import pytest
from fastapi.testclient import TestClient

from gcon.api.api_v1 import create_api_v1_app
from gcon.cluster.coordinator import GCONCoordinator
from gcon.dashboard.presentation import PresentationLayer
from gcon.execution.agent import GCONAgent
from gcon.management.management_layer import ManagementLayer
from gcon.persistence.control_plane import ControlPlane


@pytest.fixture
def env(tmp_path):
    db = str(tmp_path / "cp.db")
    coordinator = GCONCoordinator(control_plane=ControlPlane(path=db))
    management = ManagementLayer(coordinator=coordinator, db_path=str(tmp_path / "mgmt.db"))
    client = TestClient(create_api_v1_app(management, PresentationLayer(coordinator)))
    yield client, coordinator, db
    coordinator.shutdown()


def _signup(client, org_name="Acme", email="a@acme.example"):
    r = client.post(
        "/auth/signup",
        json={"org_name": org_name, "name": "Ann", "email": email, "password": "correct-horse-1"},
    )
    assert r.status_code == 200, r.text
    return r.json()


def _h(secret, idempotency_key=None):
    headers = {"Authorization": f"Bearer {secret}"}
    if idempotency_key:
        headers["Idempotency-Key"] = idempotency_key
    return headers


def _wait(pred, timeout=6.0):
    end = time.time() + timeout
    while time.time() < end:
        if pred():
            return True
        time.sleep(0.05)
    return False


def test_replayed_key_returns_the_original_job_even_with_a_different_job_id(env):
    client, coordinator, _ = env
    acme = _signup(client)
    key = acme["api_key"]["secret"]

    r1 = client.post(
        "/jobs", json={"job_id": "job-attempt-1", "command": "echo hi"},
        headers=_h(key, idempotency_key="idem-key-a"),
    )
    assert r1.status_code == 200, r1.text
    assert r1.json()["job_id"] == "job-attempt-1"
    assert "Idempotent-Replayed" not in r1.headers

    # Retry: client generated a fresh job_id (an entirely ordinary
    # thing for a client to do on retry), same idempotency key. Must
    # get back job-attempt-1, not create job-attempt-2.
    r2 = client.post(
        "/jobs", json={"job_id": "job-attempt-2", "command": "echo hi"},
        headers=_h(key, idempotency_key="idem-key-a"),
    )
    assert r2.status_code == 200, r2.text
    assert r2.json()["job_id"] == "job-attempt-1"
    assert r2.headers["Idempotent-Replayed"] == "true"

    assert "job-attempt-2" not in coordinator.jobs
    assert "job-attempt-1" in coordinator.jobs


def test_different_keys_create_separate_jobs(env):
    client, coordinator, _ = env
    acme = _signup(client)
    key = acme["api_key"]["secret"]

    r1 = client.post(
        "/jobs", json={"job_id": "job-b1", "command": "echo hi"},
        headers=_h(key, idempotency_key="idem-key-b1"),
    )
    r2 = client.post(
        "/jobs", json={"job_id": "job-b2", "command": "echo hi"},
        headers=_h(key, idempotency_key="idem-key-b2"),
    )
    assert r1.json()["job_id"] == "job-b1"
    assert r2.json()["job_id"] == "job-b2"
    assert "job-b1" in coordinator.jobs
    assert "job-b2" in coordinator.jobs


def test_no_idempotency_key_behaves_exactly_as_before(env):
    client, coordinator, _ = env
    acme = _signup(client)
    key = acme["api_key"]["secret"]

    r = client.post("/jobs", json={"job_id": "job-no-key", "command": "echo hi"}, headers=_h(key))
    assert r.status_code == 200
    assert "Idempotent-Replayed" not in r.headers
    assert "job-no-key" in coordinator.jobs

    # Same job_id again, still no idempotency key -- the pre-existing
    # in-memory job_id-uniqueness check still applies unchanged (this
    # feature is additive, not a replacement for it).
    r2 = client.post("/jobs", json={"job_id": "job-no-key", "command": "echo hi"}, headers=_h(key))
    assert r2.status_code == 400


def test_different_orgs_can_use_the_same_idempotency_key(env):
    client, coordinator, _ = env
    acme = _signup(client, org_name="Acme", email="a@acme.example")
    globex = _signup(client, org_name="Globex", email="g@globex.example")

    r1 = client.post(
        "/jobs", json={"job_id": "job-acme-1", "command": "echo hi"},
        headers=_h(acme["api_key"]["secret"], idempotency_key="shared-key"),
    )
    r2 = client.post(
        "/jobs", json={"job_id": "job-globex-1", "command": "echo hi"},
        headers=_h(globex["api_key"]["secret"], idempotency_key="shared-key"),
    )
    assert r1.json()["job_id"] == "job-acme-1"
    assert r2.json()["job_id"] == "job-globex-1"


def test_idempotency_key_survives_a_coordinator_restart(env):
    client, coordinator, db_path = env
    acme = _signup(client)
    key = acme["api_key"]["secret"]

    r1 = client.post(
        "/jobs", json={"job_id": "job-restart-idem", "command": "echo hi"},
        headers=_h(key, idempotency_key="idem-key-restart"),
    )
    assert r1.json()["job_id"] == "job-restart-idem"

    # Not an in-memory dict -- reading it back straight from a fresh
    # ControlPlane pointed at the same db file (no coordinator in the
    # loop at all here) is the actual claim under test: this survives
    # a process restart, per the original requirement ("do not solve
    # idempotency with arbitrary in-memory dictionaries").
    fresh_cp = ControlPlane(path=db_path)
    org_id = acme["organization"]["org_id"]
    assert fresh_cp.idempotency_keys.get_job_id(org_id, "idem-key-restart") == "job-restart-idem"
    fresh_cp.close()
