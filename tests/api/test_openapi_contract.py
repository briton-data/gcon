"""
The OpenAPI spec is what a generated client is built from, so it must describe
the API as it really behaves: every route has a response schema, authentication
is declared, and the declared schemas match what the routes actually return.
"""
import time

import pytest
from tests.support.label_client import LabelClient as TestClient

from gcon.api import api_v1
from gcon.api.api_v1 import create_api_v1_app
from gcon.cluster.coordinator import GCONCoordinator
from gcon.dashboard.presentation import PresentationLayer
from gcon.execution.agent import GCONAgent
from gcon.management.management_layer import ManagementLayer
from gcon.persistence.control_plane import ControlPlane
from gcon.transport import tls

PUBLIC = {
    ("post", "/auth/signup"), ("post", "/auth/login"), ("post", "/auth/forgot-password"),
    ("post", "/auth/reset-password"), ("get", "/enroll/ca"),
}


@pytest.fixture
def env(tmp_path, monkeypatch):
    monkeypatch.setenv("GCON_TLS_CERT_DIR", str(tmp_path / "tls"))
    tls.ensure_ca(str(tmp_path / "tls"))
    coordinator = GCONCoordinator(control_plane=ControlPlane(path=str(tmp_path / "cp.db")))
    management = ManagementLayer(coordinator=coordinator, db_path=str(tmp_path / "mgmt.db"))
    app = create_api_v1_app(management, PresentationLayer(coordinator))
    yield TestClient(app), app, coordinator
    coordinator.shutdown()


def _h(secret):
    return {"Authorization": f"Bearer {secret}"}


def _wait(pred, timeout=8.0):
    end = time.time() + timeout
    while time.time() < end:
        if pred():
            return True
        time.sleep(0.05)
    return False


def test_every_route_documents_a_json_response_schema(env):
    _, app, _ = env
    missing = []
    for path, operations in app.openapi()["paths"].items():
        for method, op in operations.items():
            schema = op["responses"].get("200", {}).get("content", {}).get("application/json", {}).get("schema")
            if not schema:
                missing.append(f"{method.upper()} {path}")
    assert missing == []


def test_both_api_key_headers_are_declared_as_security_schemes(env):
    _, app, _ = env
    spec = app.openapi()
    schemes = spec["components"]["securitySchemes"]
    assert schemes["BearerAuth"] == {
        "type": "http", "scheme": "bearer",
        "description": schemes["BearerAuth"]["description"],
    }
    assert schemes["ApiKeyAuth"]["type"] == "apiKey"
    assert schemes["ApiKeyAuth"]["in"] == "header"
    assert schemes["ApiKeyAuth"]["name"] == "X-API-Key"
    # two alternatives (either header works), not one requirement needing both
    assert spec["security"] == [{"BearerAuth": []}, {"ApiKeyAuth": []}]


def test_only_the_routes_that_need_no_key_opt_out_of_security(env):
    _, app, _ = env
    opted_out = {
        (method, path)
        for path, ops in app.openapi()["paths"].items()
        for method, op in ops.items()
        if op.get("security") == []
    }
    assert opted_out == PUBLIC


def test_routes_that_need_a_key_really_reject_a_missing_one(env):
    client, app, _ = env
    for path, ops in app.openapi()["paths"].items():
        for method in ops:
            if (method, path) in PUBLIC or "{" in path or method != "get":
                continue
            assert client.get(path).status_code == 401, path


def test_the_key_headers_are_not_also_listed_as_per_route_parameters(env):
    _, app, _ = env
    for path, ops in app.openapi()["paths"].items():
        for method, op in ops.items():
            names = {p["name"].lower() for p in op.get("parameters", []) if p["in"] == "header"}
            assert not names & {"authorization", "x-api-key"}, f"{method} {path}"


def test_real_responses_match_the_declared_models(env):
    client, _, coordinator = env
    signup = client.post("/auth/signup", json={
        "org_name": "Acme", "name": "Ann", "email": "a@acme.example", "password": "correct-horse-1"})
    assert signup.status_code == 200, signup.text
    key = signup.json()["api_key"]["secret"]
    h = _h(key)

    node = GCONAgent(node_id="acme-node")
    node.org_id = signup.json()["organization"]["org_id"]
    coordinator.register_agent(node)
    client.post("/jobs", json={"client_reference": "ok", "command": "echo hi"}, headers=h)
    client.post("/jobs", json={"client_reference": "bad",
                               "command": 'python -c "import sys; sys.exit(3)"'}, headers=h)
    ok_id, bad_id = client.ids["ok"], client.ids["bad"]
    assert _wait(lambda: coordinator.jobs[ok_id]["status"] == "completed")
    assert _wait(lambda: coordinator.jobs[bad_id]["status"] == "failed")
    client.post("/workflows", json={"name": "w", "jobs": [{"job_id": "a", "command": "echo a"}]}, headers=h)
    assert _wait(lambda: client.get("/receipts", headers=h).json())
    receipt_id = client.get("/receipts", headers=h).json()[0]["receipt_id"]

    created = client.post("/auth/api-keys", json={"name": "second"}, headers=h)

    def one(model, response):
        assert response.status_code == 200, response.text
        model.model_validate(response.json())

    def many(model, response, at_least_one=True):
        assert response.status_code == 200, response.text
        rows = response.json()
        assert rows or not at_least_one
        for row in rows:
            model.model_validate(row)

    one(api_v1.WhoamiOut, client.get("/whoami", headers=h))
    one(api_v1.SharedPoolOut, client.get("/org/shared-pool", headers=h))
    one(api_v1.EnrollCaOut, client.get("/enroll/ca"))
    many(api_v1.ApiKeyOut, client.get("/auth/api-keys", headers=h))
    one(api_v1.ApiKeyOut, created)
    one(api_v1.ApiKeyOut, client.delete(f"/auth/api-keys/{created.json()['key_id']}", headers=h))
    one(api_v1.MetricsOut, client.get("/metrics", headers=h))
    many(api_v1.EnrollmentAttemptOut, client.get("/nodes/acme-node/enrollment-history", headers=h), False)
    many(api_v1.JobAttemptOut, client.get(f"/jobs/{ok_id}/attempts", headers=h), False)
    one(api_v1.JobRetryOut, client.post(f"/jobs/{bad_id}/retry", headers=h))
    many(api_v1.WorkflowOut, client.get("/workflows", headers=h))
    one(api_v1.ReceiptDetailOut, client.get(f"/receipts/{receipt_id}", headers=h))
    many(api_v1.TelemetryEventOut, client.get("/telemetry/events", headers=h))

    one(api_v1.ChangePasswordOut, client.post("/auth/change-password", headers=h, json={
        "current_password": "correct-horse-1", "new_password": "correct-horse-2"}))


def test_staff_metrics_with_the_extra_platform_fields_still_validate():
    api_v1.MetricsOut.model_validate({
        "avg_cpu": 0, "avg_memory": 0, "queued_jobs": 0, "running_jobs": 0,
        "completed_jobs": 0, "failed_jobs": 0, "uptime_seconds": 5, "node_summary": {}})


def test_reset_and_delete_confirmations_have_models():
    api_v1.ResetPasswordOut.model_validate({"reset": True})
    api_v1.DeleteAccountOut.model_validate({"deleted": True})
