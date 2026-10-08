"""
One customer's malformed submission must never stop dispatch for everyone.

  SL1  `requires` with a non-numeric value used to raise inside the scheduler's
       matching (float("abc")), killing the scheduler thread for all tenants.
  D9   the policy engine raised TypeError comparing a string to a ceiling.
  D10  a malformed or missing policy.json silently turned every limit off.
"""
import json
import logging
import time

import pytest

from gcon.cluster.coordinator import GCONCoordinator
from gcon.execution.agent import GCONAgent
from gcon.execution.policy_engine import PolicyEngine
from gcon.persistence.control_plane import ControlPlane


@pytest.fixture
def coord(tmp_path):
    c = GCONCoordinator(control_plane=ControlPlane(path=str(tmp_path / "cp.db")))
    c.register_agent(GCONAgent(node_id="n1"))
    yield c
    c.shutdown()


BAD_REQUIRES = [
    {"min_vram_gb": "abc"}, {"min_vram_gb": [1]}, {"min_vram_gb": {"x": 1}}, {"min_vram_gb": -1},
    {"min_vram_gb": True}, {"min_vram_gb": float("nan")}, {"min_cpu_cores": "many"},
    {"gpu": "yes"}, ["gpu"], "gpu", 5,
]


@pytest.mark.parametrize("requires", BAD_REQUIRES, ids=repr)
def test_a_malformed_requires_is_refused_at_submission(coord, requires):
    with pytest.raises(ValueError, match="requires"):
        coord.submit_job("bad", "echo hi", kind="resourced", requires=requires)
    assert "bad" not in coord.jobs


def test_a_refused_job_does_not_disturb_the_scheduler_or_other_jobs(coord):
    with pytest.raises(ValueError):
        coord.submit_job("bad", "echo hi", kind="resourced", requires={"min_vram_gb": "abc"})
    coord.submit_job("good", "echo ok")
    deadline = time.time() + 15
    while time.time() < deadline and coord.jobs["good"]["status"] != "completed":
        time.sleep(0.05)
    assert coord.scheduler_thread.is_alive()
    assert coord.jobs["good"]["status"] == "completed"


@pytest.mark.parametrize("requires", [{"min_vram_gb": "abc"}, {"min_cpu_cores": [1]}, ["gpu"], "gpu", 5])
def test_the_scheduler_itself_never_raises_on_a_bad_requires(coord, requires):
    info = {"node": coord.registry.get_node("n1")}
    assert coord.scheduler._satisfies(info, requires) is False


def test_valid_requires_still_work(coord):
    coord.submit_job("ok", "sleep 5", kind="resourced", requires={"gpu": False, "min_vram_gb": 0, "min_cpu_cores": 1})
    assert "ok" in coord.jobs


class TestPolicyEngine:
    def test_non_numeric_values_are_rejected_not_a_TypeError(self, tmp_path):
        f = tmp_path / "policy.json"
        f.write_text(json.dumps({"max_replicas": 3, "max_requires": {"min_vram_gb": 80}}))
        engine = PolicyEngine(str(f))
        assert engine.check_submission(verify={"replicas": "lots"}) [0] is False
        allowed, reason = engine.check_submission(requires={"min_vram_gb": "huge"})
        assert allowed is False and "number" in reason

    def test_a_malformed_policy_file_refuses_to_load(self, tmp_path):
        f = tmp_path / "policy.json"
        f.write_text("{ not json")
        with pytest.raises(ValueError, match="not valid JSON"):
            PolicyEngine(str(f))
        f.write_text("[1, 2]")
        with pytest.raises(ValueError, match="JSON object"):
            PolicyEngine(str(f))

    def test_a_missing_policy_file_uses_defaults_but_says_so(self, tmp_path, caplog):
        with caplog.at_level(logging.WARNING):
            engine = PolicyEngine(str(tmp_path / "absent.json"))
        assert engine.policy["max_replicas"] is None
        assert "NOT enforced" in caplog.text

    def test_a_policy_file_the_operator_pointed_at_must_exist(self, tmp_path, monkeypatch):
        monkeypatch.setenv("GCON_POLICY_FILE", str(tmp_path / "typo.json"))
        with pytest.raises(ValueError, match="does not exist"):
            GCONCoordinator()
