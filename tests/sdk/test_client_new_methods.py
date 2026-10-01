"""
#9 SDK parity: get_receipt(id), retry_job, clear_jobs, telemetry
events, node enrollment history, and status/limit filters on
list_jobs/list_receipts. The backend routes for most of these already
existed (see gcon.api.api_v1) and already have their own route-level
tests under tests/api/ -- this file is specifically about the SDK
*client* class itself (gcon_sdk.client.GconClient): does it build the
right URL/method/params, send the real Bearer header, and parse a real
response correctly.

Runs GconClient (real `requests` library) against a real, live
uvicorn server in a background thread -- not FastAPI's TestClient
(which is httpx-based and GconClient can't be pointed at it) and not a
mocked session -- so this is exercising the client exactly as a real
caller would, over a real TCP socket on localhost.
"""
import asyncio
import socket
import sys
import threading
import time

import pytest
import uvicorn

from gcon.api.api_v1 import create_api_v1_app
from gcon.cluster.coordinator import GCONCoordinator
from gcon.dashboard.presentation import PresentationLayer
from gcon.execution.agent import GCONAgent
from gcon.management.management_layer import ManagementLayer
from gcon.persistence.control_plane import ControlPlane

from fastapi import FastAPI

from gcon_sdk import GconAPIError, GconClient


def _free_tcp_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_until(predicate, timeout=10.0, interval=0.05):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()


class LiveServer:
    """Mounts api_v1 at /api/v1 under a plain FastAPI app -- the same
    wiring web_server.py itself uses -- and serves it with a real
    uvicorn.Server on a real port, in a background thread."""

    def __init__(self, tmp_path):
        db = str(tmp_path / "cp.db")
        self.coordinator = GCONCoordinator(control_plane=ControlPlane(path=db))
        self.management = ManagementLayer(coordinator=self.coordinator, db_path=str(tmp_path / "mgmt.db"))
        api_v1_app = create_api_v1_app(self.management, PresentationLayer(self.coordinator))
        app = FastAPI()
        app.mount("/api/v1", api_v1_app)

        self.port = _free_tcp_port()
        self.base_url = f"http://127.0.0.1:{self.port}"
        config = uvicorn.Config(app, host="127.0.0.1", port=self.port, log_level="warning")
        self.server = uvicorn.Server(config)

        def _run():
            # Windows-only: a fresh asyncio event loop started inside a
            # background (non-main) thread can hang under the default
            # ProactorEventLoop policy -- uvicorn.Server.run() calls
            # asyncio.run(), which creates a new loop in whatever thread
            # it's called from, and on Windows that combination has a
            # known history of silently never servicing the first
            # request (matches exactly what a real Windows run of this
            # file showed: server.started reported True, but every
            # request timed out with no response at all). The selector
            # policy doesn't have this issue. No effect on Linux/macOS,
            # where this whole block is skipped.
            if sys.platform == "win32":
                asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
            self.server.run()

        self.thread = threading.Thread(target=_run, daemon=True)
        self.thread.start()
        assert _wait_until(lambda: self.server.started, timeout=10), "uvicorn should actually come up"

    def shutdown(self):
        self.server.should_exit = True
        self.thread.join(timeout=10)
        self.coordinator.shutdown()


@pytest.fixture
def live_server(tmp_path):
    srv = LiveServer(tmp_path)
    yield srv
    srv.shutdown()


def _signup_and_client(live_server, org_name="Acme", email="a@acme.example"):
    import requests
    r = requests.post(
        f"{live_server.base_url}/api/v1/auth/signup",
        json={"org_name": org_name, "name": "Ann", "email": email, "password": "correct-horse-1"},
        timeout=5,
    )
    assert r.status_code == 200, r.text
    result = r.json()
    secret = result["api_key"]["secret"]
    org_id = result["organization"]["org_id"]
    client = GconClient(api_key=secret, base_url=live_server.base_url)
    return client, org_id


def test_get_receipt_returns_full_detail_including_assurance(live_server):
    client, org_id = _signup_and_client(live_server)
    node = GCONAgent(node_id="sdk-node-1")
    node.org_id = org_id
    live_server.coordinator.register_agent(node)

    client.submit_job("sdk-job-1", "echo hi")
    assert _wait_until(lambda: live_server.coordinator.jobs["sdk-job-1"]["status"] == "completed")
    assert _wait_until(lambda: "sdk-job-1" in live_server.coordinator.receipts)

    receipt_id = live_server.coordinator.receipts["sdk-job-1"]["receipt_id"]
    detail = client.get_receipt(receipt_id)
    assert detail["job_id"] == "sdk-job-1"
    # This is #7's field, reached here for the first time via the SDK
    # itself rather than a direct coordinator call.
    assert detail["assurance"]["level"] == "verified"
    assert detail["assurance"]["assured"] is True


def test_get_receipt_unknown_id_raises_gcon_api_error(live_server):
    client, _org_id = _signup_and_client(live_server)
    with pytest.raises(GconAPIError) as exc_info:
        client.get_receipt("does-not-exist")
    assert exc_info.value.status_code == 404


def test_retry_job_resubmits_a_failed_job(live_server):
    client, org_id = _signup_and_client(live_server)
    node = GCONAgent(node_id="sdk-node-2")
    node.org_id = org_id
    live_server.coordinator.register_agent(node)

    client.submit_job("sdk-job-2", "exit 1")
    assert _wait_until(lambda: live_server.coordinator.jobs["sdk-job-2"]["status"] == "failed")

    result = client.retry_job("sdk-job-2")
    assert result is not None
    assert _wait_until(
        lambda: live_server.coordinator.jobs["sdk-job-2"].get("attempt_number", 1) >= 2
        or live_server.coordinator.jobs["sdk-job-2"]["status"] in ("pending", "running", "completed"),
        timeout=10,
    )


def test_clear_jobs_reports_cleared_and_skipped(live_server):
    client, org_id = _signup_and_client(live_server)
    node = GCONAgent(node_id="sdk-node-3")
    node.org_id = org_id
    live_server.coordinator.register_agent(node)

    client.submit_job("sdk-job-3", "exit 1")
    assert _wait_until(lambda: live_server.coordinator.jobs["sdk-job-3"]["status"] == "failed")

    result = client.clear_jobs(["sdk-job-3", "does-not-exist"])
    assert "sdk-job-3" in result["cleared"]
    skipped_ids = {s["job_id"] for s in result["skipped"]}
    assert "does-not-exist" in skipped_ids


def test_get_node_enrollment_history_returns_a_list(live_server):
    client, org_id = _signup_and_client(live_server)
    node = GCONAgent(node_id="sdk-node-4")
    node.org_id = org_id
    live_server.coordinator.register_agent(node)

    history = client.get_node_enrollment_history("sdk-node-4")
    assert isinstance(history, list)


def test_get_telemetry_events_returns_a_list_and_accepts_filters(live_server):
    client, org_id = _signup_and_client(live_server)
    node = GCONAgent(node_id="sdk-node-5")
    node.org_id = org_id
    live_server.coordinator.register_agent(node)
    client.submit_job("sdk-job-5", "echo hi")
    assert _wait_until(lambda: live_server.coordinator.jobs["sdk-job-5"]["status"] == "completed")

    events = client.get_telemetry_events()
    assert isinstance(events, list)
    scoped = client.get_telemetry_events(job_id="sdk-job-5", limit=10)
    assert isinstance(scoped, list)


def test_list_jobs_status_filter_matches_the_http_route(live_server):
    client, org_id = _signup_and_client(live_server)
    node = GCONAgent(node_id="sdk-node-6")
    node.org_id = org_id
    live_server.coordinator.register_agent(node)

    client.submit_job("sdk-ok", "echo hi")
    client.submit_job("sdk-bad", "exit 1")
    assert _wait_until(lambda: live_server.coordinator.jobs["sdk-ok"]["status"] == "completed")
    assert _wait_until(lambda: live_server.coordinator.jobs["sdk-bad"]["status"] == "failed")

    failed_only = client.list_jobs(status="failed")
    ids = {j["job_id"] for j in failed_only}
    assert ids == {"sdk-bad"}

    capped = client.list_jobs(limit=1)
    assert len(capped) == 1


def test_list_receipts_verified_filter_matches_the_http_route(live_server):
    """The verified=/limit= path reads control_plane.receipts's own
    `verified` column (see get_receipts_page's docstring), which is
    only committed once health_check_loop's real 3s tick runs its
    verification pass -- not instantly at receipt creation (that's
    also true of the plain, unfiltered GET /receipts, which reports
    "verified" from the same cache) -- so this polls for it to settle
    rather than asserting immediately after the receipt appears."""
    client, org_id = _signup_and_client(live_server)
    node = GCONAgent(node_id="sdk-node-7")
    node.org_id = org_id
    live_server.coordinator.register_agent(node)
    client.submit_job("sdk-job-7", "echo hi")
    assert _wait_until(lambda: "sdk-job-7" in live_server.coordinator.receipts)

    assert _wait_until(
        lambda: any(r["job_id"] == "sdk-job-7" for r in client.list_receipts(verified=True)),
        timeout=15,
    )
    unverified = client.list_receipts(verified=False)
    assert all(r["job_id"] != "sdk-job-7" for r in unverified)
