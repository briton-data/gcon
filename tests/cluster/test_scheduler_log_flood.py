"""A job nobody can take is retried ~10x/second. It must not write a log line
per retry (it used to write two, ~20 lines/s forever), while a job that is
really dispatched is still logged."""
import logging
import time

from gcon.cluster.coordinator import GCONCoordinator
from gcon.execution.agent import GCONAgent


def _wait_for(predicate, timeout=15.0):
    end = time.time() + timeout
    while time.time() < end:
        if predicate():
            return True
        time.sleep(0.05)
    return False


def test_undispatchable_job_does_not_flood_the_log(caplog):
    coordinator = GCONCoordinator()
    try:
        agent = GCONAgent("globex-node")
        agent.org_id = "globex"
        coordinator.register_agent(agent)
        coordinator.submit_job("J", "echo hi", org_id="acme")
        time.sleep(0.3)
        with caplog.at_level(logging.DEBUG, logger="gcon.coordinator"):
            caplog.clear()
            time.sleep(2)
        queue_lines = [r for r in caplog.records if "[QUEUE]" in r.getMessage()]
        assert queue_lines == []
        assert coordinator.jobs["J"]["status"] == "pending"
    finally:
        coordinator.shutdown()


def test_a_real_dispatch_is_still_logged(caplog):
    coordinator = GCONCoordinator()
    try:
        coordinator.register_agent(GCONAgent("n1"))
        with caplog.at_level(logging.INFO, logger="gcon.coordinator"):
            coordinator.submit_job("J", "echo hi")
            assert _wait_for(lambda: coordinator.jobs["J"]["status"] == "completed")
        assert any("[QUEUE] Dispatching J" in r.getMessage() for r in caplog.records)
    finally:
        coordinator.shutdown()
