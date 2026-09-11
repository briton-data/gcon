"""
Regression test for a real bug found while live-testing the Python
SDK's error handling: PolicyRejectionError (raised by
GCONCoordinator.submit_job when PolicyEngine.check_submission()
rejects a job -- see policy_engine.py/coordinator.py) is a
RuntimeError subclass, not a ValueError, so api_v1.py's
`except ValueError` around POST /jobs never caught it. The result was
an unhandled exception propagating all the way to a raw 500 Internal
Server Error with a full server stack trace, instead of the clean 400
+ readable detail message every other rejection reason on this route
already gets.

Goes through the real FastAPI app + a real org/key/policy file (not
mocks), since the point is confirming the actual HTTP response shape
a real SDK/API caller sees, not just that the exception class matches
in isolation.
"""
import json

import pytest
from fastapi.testclient import TestClient

from gcon.api.api_v1 import create_api_v1_app
from gcon.cluster.coordinator import GCONCoordinator
from gcon.dashboard.presentation import PresentationLayer
from gcon.management.management_layer import ManagementLayer


@pytest.fixture
def client_under_strict_policy(tmp_path, monkeypatch):
    policy_file = tmp_path / "policy.json"
    policy_file.write_text(json.dumps({"require_org_id": True}))
    monkeypatch.setenv("GCON_POLICY_FILE", str(policy_file))

    coordinator = GCONCoordinator()
    management = ManagementLayer(coordinator=coordinator, db_path=str(tmp_path / "mgmt.db"))

    # A user/key with NO organization -- exactly what require_org_id
    # rejects.
    user = management.create_user(
        "No Org User", "noorg@example.com", role="Owner", organization_id=None,
    )
    key = management.create_api_key(
        "noorg-key", owner_user_id=user["user_id"], scopes=["Submit workflows"],
    )

    presentation = PresentationLayer(coordinator)
    app = create_api_v1_app(management, presentation)
    client = TestClient(app, raise_server_exceptions=False)  # see real HTTP response, not a raised exception

    yield client, key["secret"]
    coordinator.shutdown()


class TestPolicyRejectionReturnsCleanHTTPError:
    def test_policy_rejected_submission_returns_400_not_500(self, client_under_strict_policy):
        client, api_key = client_under_strict_policy

        response = client.post(
            "/jobs", json={"job_id": "policy-rejected-job", "command": "echo hi"},
            headers={"X-API-Key": api_key},
        )

        assert response.status_code == 400, (
            f"expected a clean 400 for a policy-rejected submission, got "
            f"{response.status_code} -- PolicyRejectionError is escaping "
            f"as an unhandled exception again"
        )
        assert "org_id" in response.json()["detail"]
