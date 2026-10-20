"""
A job runner thread that dies unexpectedly must not strand its job as "running" (while the worker shows idle), and a
receipt that cannot be checked because the signing key is gone must say so instead of looking forged.
"""
import time
from datetime import datetime, UTC
from types import SimpleNamespace

import pytest

from gcon.cluster.coordinator import GCONCoordinator
from gcon.execution.agent import GCONAgent
from gcon.execution.verifier import ExecutionVerifier
from gcon.persistence.control_plane import ControlPlane


class Node(GCONAgent):
    sandboxed = True

    def execute_job(self, job_id, job_script, timeout=None, usage_report_path=None, stage_report_path=None):
        return {"job_id": job_id, "status": "success", "return_code": 0, "runtime_seconds": 1.0, "stdout": "ok\n",
                "stderr": "", "usage": None, "stages": [], "timestamp": datetime.now(UTC).isoformat(),
                "metrics": {"job_id": job_id, "gpu_name": "x", "gpu_memory_total": 0, "gpu_memory_used": 0,
                            "cpu_percent": 0, "memory_percent": 0, "runtime_seconds": 1.0,
                            "timestamp": datetime.now(UTC).isoformat()}}


@pytest.fixture
def co(tmp_path, monkeypatch):
    monkeypatch.setenv("GCON_HMAC_KEY_PATH", str(tmp_path / "keys" / "hmac.key"))
    coordinator = GCONCoordinator(control_plane=ControlPlane(path=str(tmp_path / "cp.db")))
    node = Node("n1"); node.org_id = "acme"
    coordinator.register_agent(node)
    coordinator.test_node = node
    yield coordinator
    coordinator.shutdown()


def wait_for(predicate, seconds=15):
    deadline = time.time() + seconds
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(0.05)
    return False


class TestRunnerGuard:
    def test_a_healthy_job_is_unaffected(self, co):
        co.submit_job("ok", "echo hi", org_id="acme")
        assert wait_for(lambda: co.jobs["ok"]["status"] == "completed")

    def test_an_unexpected_exception_fails_the_job_instead_of_leaving_it_running(self, co):
        co.receive_resource_report = lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom while recording resources"))
        co.submit_job("stuck", "echo hi", org_id="acme")
        assert wait_for(lambda: co.jobs["stuck"]["status"] != "running" and co.jobs["stuck"]["status"] != "pending")
        job = co.jobs["stuck"]
        assert job["status"] == "failed" and "boom while recording resources" in job["result"]["message"]
        assert job["completed_at"]
        assert co.test_node.status == "idle" and co.registry.nodes["n1"]["status"] == "idle"
        # and the worker is usable again
        del co.receive_resource_report
        co.submit_job("after", "echo hi", org_id="acme")
        assert wait_for(lambda: co.jobs["after"]["status"] == "completed")

    def test_the_failure_is_persisted_not_just_in_memory(self, co):
        co.receive_resource_report = lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom"))
        co.submit_job("stuck", "echo hi", org_id="acme")
        assert wait_for(lambda: co.jobs["stuck"]["status"] == "failed")
        assert wait_for(lambda: (co.control_plane.jobs.get("stuck") or {}).get("status") == "failed")

    def test_a_job_that_already_finished_is_not_overwritten(self, co):
        calls = []

        def advance(job_id, job, success):
            calls.append(success)
            if success:
                raise RuntimeError("late failure after the job completed")
        co._advance_workflow = advance
        co.submit_job("done", "echo hi", org_id="acme")
        assert wait_for(lambda: calls)
        time.sleep(0.3)
        assert co.jobs["done"]["status"] == "completed"           # the guard saw a final state and left it alone
        assert calls == [True]                                    # and did not also record a failure

    def test_a_replicated_group_frees_every_worker(self, co):
        a, b = SimpleNamespace(node_id="a", status="busy", heartbeat=lambda: {"timestamp": "t"}), \
               SimpleNamespace(node_id="b", status="busy", heartbeat=lambda: {"timestamp": "t"})
        co.registry.heartbeat = lambda *args, **kw: None
        co.jobs["rep"] = {"status": "running", "attempt_number": 2, "command": "x"}
        co._fail_stranded_job("rep", 2, [a, b], RuntimeError("group failed"))
        assert (a.status, b.status) == ("idle", "idle") and co.jobs["rep"]["status"] == "failed"

    def test_a_stale_attempt_is_not_touched(self, co):
        co.jobs["moved-on"] = {"status": "running", "attempt_number": 3, "command": "x"}
        co._fail_stranded_job("moved-on", 2, [], RuntimeError("old attempt died"))
        assert co.jobs["moved-on"]["status"] == "running"        # attempt 3 is current; attempt 2's crash is not its business


RESULT = {"status": "success", "return_code": 0, "runtime_seconds": 1.0, "stdout": "ok\n", "stderr": "",
          "metrics": {"job_id": "j", "gpu_name": "x", "gpu_memory_total": 0, "gpu_memory_used": 0, "cpu_percent": 0,
                      "memory_percent": 0, "runtime_seconds": 1.0, "timestamp": "2026-01-01T00:00:00+00:00"}}


class TestLostKeyIsNotAForgery:
    def _receipt(self, tmp_path, monkeypatch, name):
        monkeypatch.setenv("GCON_HMAC_KEY_PATH", str(tmp_path / name / "hmac.key"))
        verifier = ExecutionVerifier()
        return verifier, verifier.create_receipt("j", "n1", RESULT, "in", "out")

    def test_a_receipt_signed_with_a_key_that_is_gone_says_so(self, tmp_path, monkeypatch):
        _, receipt = self._receipt(tmp_path, monkeypatch, "old")
        monkeypatch.setenv("GCON_HMAC_KEY_PATH", str(tmp_path / "new" / "hmac.key"))    # the file was lost; a new key is made
        valid, message = ExecutionVerifier().validate_proof(receipt["proof"])
        assert not valid and "no longer has" in message and "lost or replaced" in message

    def test_a_tampered_receipt_is_still_just_an_invalid_signature(self, tmp_path, monkeypatch):
        verifier, receipt = self._receipt(tmp_path, monkeypatch, "same")
        receipt["proof"]["job_id"] = "somebody-else"
        assert verifier.validate_proof(receipt["proof"]) == (False, "Invalid signature")

    def test_a_genuine_receipt_still_verifies(self, tmp_path, monkeypatch):
        verifier, receipt = self._receipt(tmp_path, monkeypatch, "fine")
        assert verifier.validate_proof(receipt["proof"])[0] is True
