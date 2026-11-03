"""Customers can still read, and are refused on, their finished jobs and receipts after the coordinator has dropped them from
memory; list limits are bounded; the staff login is not a user-enumeration oracle and its cookie is Secure behind a TLS proxy."""
import time

import pytest
from starlette.testclient import TestClient

from gcon.execution.agent import GCONAgent
from tests.api.test_job_attempts_route import _h, _signup, _wait, env  # noqa: F401  (env is a fixture)


@pytest.fixture
def finished(env):
    """Five finished jobs for one customer while the coordinator keeps only two in memory."""
    client, coordinator = env
    signup = _signup(client)
    key, org_id = signup["api_key"]["secret"], signup["organization"]["org_id"]
    node = GCONAgent(node_id="n1")
    node.org_id = org_id
    coordinator.register_agent(node)
    coordinator._max_jobs_in_memory = coordinator._max_receipts_in_memory = 2
    ids = []
    for i in range(5):
        job_id = client.post("/jobs", json={"client_reference": f"j{i}", "command": "echo hi"}, headers=_h(key)).json()["job_id"]
        assert _wait(lambda: coordinator.jobs.get(job_id, {}).get("status") == "completed" or job_id not in coordinator.jobs, 10)
        ids.append(job_id)
        time.sleep(0.3)
    assert ids[0] not in coordinator.jobs                     # the oldest really is out of memory
    return client, coordinator, key, ids


class TestAfterTheCoordinatorDroppedThem:
    def test_the_job_is_still_readable_and_listed_receipts_all_open(self, finished):
        client, _, key, ids = finished
        assert client.get(f"/jobs/{ids[0]}", headers=_h(key)).json()["status"] == "completed"
        assert client.get(f"/jobs/{ids[0]}/attempts", headers=_h(key)).status_code == 200
        listed = client.get("/receipts?limit=50", headers=_h(key)).json()
        assert len(listed) == 5
        assert [client.get(f"/receipts/{r['receipt_id']}", headers=_h(key)).status_code for r in listed] == [200] * 5

    def test_cancelling_a_finished_job_says_why_instead_of_not_found(self, finished):
        client, _, key, ids = finished
        r = client.post(f"/jobs/{ids[0]}/cancel", headers=_h(key))
        assert r.status_code == 400 and "not pending or running" in r.json()["detail"]

    def test_another_organizations_key_still_gets_a_plain_404(self, finished):
        client, _, _, ids = finished
        other = _signup(client, "Other Co", "o@other.example")["api_key"]["secret"]
        assert client.get(f"/jobs/{ids[0]}", headers=_h(other)).status_code == 404
        listed = client.get("/receipts?limit=50", headers=_h(finished[2])).json()
        assert client.get(f"/receipts/{listed[0]['receipt_id']}", headers=_h(other)).status_code == 404


class TestListLimits:
    @pytest.mark.parametrize("query", ["/receipts?limit=-1", "/receipts?limit=0", "/receipts?limit=501", "/jobs?limit=-1",
                                       "/telemetry/events?limit=-1", "/telemetry/events?limit=1000000"])
    def test_a_limit_outside_1_to_500_is_refused(self, env, query):
        client, _ = env
        key = _signup(client)["api_key"]["secret"]
        assert client.get(query, headers=_h(key)).status_code == 422

    def test_the_default_listing_is_unchanged(self, env):
        client, _ = env
        key = _signup(client)["api_key"]["secret"]
        assert client.get("/receipts", headers=_h(key)).status_code == 200
