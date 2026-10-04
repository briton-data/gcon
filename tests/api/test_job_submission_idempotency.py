"""
POST /jobs' Idempotency-Key header -- durable, per-org, survives restart
(backed by job_submission_idempotency_keys, not an in-memory dict). A client
retrying an ambiguous-outcome submission (dropped response, etc.) with the same
key gets back the job the *original* request created.

GCON mints the canonical job_id, so a retry can never collide by id: the key is
the ONLY thing that ties a retry to the original. That makes the key's
behaviour under concurrency, and when it is re-used for a different request,
part of what is tested here.
"""
import threading
import time

import pytest
from tests.support.label_client import LabelClient as TestClient

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


def _post(client, key, idem=None, **body):
    body.setdefault("command", "echo hi")
    return client.post("/jobs", json=body, headers=_h(key, idempotency_key=idem))


def test_replayed_key_returns_the_original_job(env):
    client, coordinator, _ = env
    key = _signup(client)["api_key"]["secret"]

    r1 = _post(client, key, "idem-key-a", client_reference="attempt-1")
    assert r1.status_code == 200, r1.text
    original = r1.json()["job_id"]
    assert original.startswith("job_") and original != "attempt-1"
    assert "Idempotent-Replayed" not in r1.headers

    # A retry with a different label is still the same request (the label is a
    # correlation tag, not part of what is being asked of GCON).
    r2 = _post(client, key, "idem-key-a", client_reference="attempt-2")
    assert r2.status_code == 200, r2.text
    assert r2.json()["job_id"] == original
    assert r2.json()["client_reference"] == "attempt-1"
    assert r2.headers["Idempotent-Replayed"] == "true"
    assert sum(1 for j in coordinator.jobs.values() if j.get("client_reference")) == 1


def test_different_keys_create_separate_jobs(env):
    client, coordinator, _ = env
    key = _signup(client)["api_key"]["secret"]
    a = _post(client, key, "idem-key-b1").json()["job_id"]
    b = _post(client, key, "idem-key-b2").json()["job_id"]
    assert a != b and a in coordinator.jobs and b in coordinator.jobs


def test_no_idempotency_key_always_creates_a_new_job(env):
    client, coordinator, _ = env
    key = _signup(client)["api_key"]["secret"]
    a = _post(client, key, client_reference="same-label")
    b = _post(client, key, client_reference="same-label")
    assert a.status_code == b.status_code == 200
    assert a.json()["job_id"] != b.json()["job_id"]
    assert "Idempotent-Replayed" not in a.headers


def test_a_key_reused_for_a_different_request_is_refused(env):
    client, coordinator, _ = env
    key = _signup(client)["api_key"]["secret"]
    first = _post(client, key, "idem-key-c", command="echo one")
    assert first.status_code == 200
    again = _post(client, key, "idem-key-c", command="echo two")
    assert again.status_code == 422
    assert len([j for j in coordinator.jobs.values()]) == 1


def test_concurrent_requests_with_one_key_create_one_job(env):
    client, coordinator, _ = env
    key = _signup(client)["api_key"]["secret"]
    results = []

    def go():
        results.append(_post(client, key, "idem-race", command="sleep 1"))

    threads = [threading.Thread(target=go) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert all(r.status_code == 200 for r in results), [r.text for r in results]
    assert len({r.json()["job_id"] for r in results}) == 1
    assert len(coordinator.jobs) == 1


def test_a_rejected_submission_does_not_burn_the_key(env):
    client, coordinator, _ = env
    key = _signup(client)["api_key"]["secret"]
    bad = _post(client, key, "idem-key-d", kind="nonsense")
    assert bad.status_code == 400
    # Nothing was recorded, so the same key works once the request is fixed.
    good = _post(client, key, "idem-key-d", kind="command")
    assert good.status_code == 200 and "Idempotent-Replayed" not in good.headers


def test_different_orgs_can_use_the_same_idempotency_key(env):
    client, coordinator, _ = env
    acme = _signup(client, org_name="Acme", email="a@acme.example")
    globex = _signup(client, org_name="Globex", email="g@globex.example")
    r1 = _post(client, acme["api_key"]["secret"], "shared-key")
    r2 = _post(client, globex["api_key"]["secret"], "shared-key")
    assert r1.status_code == r2.status_code == 200
    assert r1.json()["job_id"] != r2.json()["job_id"]
    assert "Idempotent-Replayed" not in r2.headers


def test_idempotency_key_survives_a_coordinator_restart(env):
    client, coordinator, db_path = env
    acme = _signup(client)
    key = acme["api_key"]["secret"]
    job_id = _post(client, key, "idem-key-restart", client_reference="x").json()["job_id"]

    # Read straight from a fresh ControlPlane on the same file: this is the
    # claim under test -- the mapping is durable, not an in-memory dict.
    fresh_cp = ControlPlane(path=db_path)
    org_id = acme["organization"]["org_id"]
    assert fresh_cp.idempotency_keys.get_job_id(org_id, "idem-key-restart") == job_id
    assert fresh_cp.idempotency_keys.get(org_id, "idem-key-restart")["request_hash"]
    fresh_cp.close()


def test_over_long_or_unprintable_keys_are_refused(env):
    client, _, _ = env
    key = _signup(client)["api_key"]["secret"]
    assert _post(client, key, "k" * 300).status_code == 400
