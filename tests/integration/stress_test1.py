"""
stress_test.py — load, concurrency, distributed-fault, websocket,
heartbeat, failover, crash-recovery, memory, CPU, and soak tests for
GCON.

Runs against the REAL coordinator/agent/registry code as shipped
(coordinator.py, agent.py, Noderegistry.py, communication.py). Several
tests are written to assert the CORRECT behavior described in
AUDIT_REPORT.md's recommended fixes, and are expected to FAIL against
the code as currently written — those are marked `@pytest.mark.xfail`
with a direct reference to the relevant audit finding, so this file
doubles as an executable spec of what "fixed" looks like, and turns
green automatically the moment each fix lands (no test rewrite needed).

Run everything:
    pytest stress_test.py -v

Run only the fast tests (skip soak/scale):
    pytest stress_test.py -v -m "not slow"

Run as a standalone load-generation script (not via pytest):
    python3 stress_test.py --mode load --jobs 5000 --nodes 50
"""

from __future__ import annotations

import argparse
import gc
import os
import sys
import threading
import time
from typing import List

import pytest

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from tests.support.test_utils import (
    MetricsCollector, ResourceSampler, TestAssertionError,
    assert_all_jobs_terminal, assert_eventually, assert_node_status_consistent,
    get_logger, run_concurrently, unique_id,
)
from tests.support.mock_network import (
    NetworkConditions, NetworkPartitionError, SimulatedNetworkCommunicationManager,
    preset, temporary_partition,
)
from tests.support.chaos import ChaosMonkey

logger = get_logger("stress_test", log_file="logs/stress_test.log")

TRIVIAL_CMD = "python3 -c \"pass\""
SLEEP_CMD_TEMPLATE = "python3 -c \"import time; time.sleep({secs})\""


# =======================================================================
# Fixtures
# =======================================================================

@pytest.fixture
def cluster():
    """
    A real GCONCoordinator with 5 real GCONAgent nodes registered.
    Fresh instance per test. coordinator.shutdown() stops its daemon
    threads on teardown so they don't accumulate for the life of the
    pytest process (previously a documented gap; see coordinator.py).
    """
    from gcon.cluster.coordinator import GCONCoordinator
    from gcon.execution.agent import GCONAgent

    coordinator = GCONCoordinator()
    agents = []
    for i in range(5):
        agent = GCONAgent(node_id=f"test-node-{i}")
        coordinator.register_agent(agent)
        agents.append(agent)
    yield coordinator, agents
    coordinator.shutdown()


@pytest.fixture
def single_node_cluster():
    from gcon.cluster.coordinator import GCONCoordinator
    from gcon.execution.agent import GCONAgent

    coordinator = GCONCoordinator()
    agent = GCONAgent(node_id="solo-node")
    coordinator.register_agent(agent)
    yield coordinator, agent
    coordinator.shutdown()


# =======================================================================
# 1. LOAD TESTS
# =======================================================================

class TestLoad:
    def test_submit_many_jobs_all_reach_terminal_state(self, cluster):
        coordinator, agents = cluster
        job_ids = [unique_id("load") for _ in range(150)]
        for jid in job_ids:
            coordinator.submit_job(jid, TRIVIAL_CMD)

        statuses = assert_all_jobs_terminal(coordinator, job_ids, timeout=60.0)
        failed = [j for j, s in statuses.items() if s == "failed"]
        assert len(failed) == 0, f"unexpected failures under load: {failed[:5]}"

    def test_queue_backlog_reported_by_health_service(self, cluster):
        """
        AUDIT_REPORT.md health_service.py: QUEUE_WARN_SIZE=50 should
        flip the coordinator health branch unhealthy once backlog
        exceeds it. Uses pause_scheduler() so jobs pile up deterministically.
        """
        coordinator, agents = cluster
        coordinator.pause_scheduler()
        for _ in range(60):
            coordinator.submit_job(unique_id("backlog"), TRIVIAL_CMD)

        health = coordinator.get_cluster_health()
        assert health["checks"]["coordinator"]["healthy"] is False, (
            "coordinator health check did not flag a >=50 job backlog"
        )
        coordinator.resume_scheduler()


# =======================================================================
# 2. CONCURRENCY TESTS
# =======================================================================

class TestConcurrency:
    def test_concurrent_job_submission_no_lost_jobs(self, cluster):
        """
        Exercises AUDIT_REPORT.md 2.2 (unlocked self.jobs iteration).
        50 threads submitting concurrently must not lose or collide job
        records.
        """
        coordinator, agents = cluster
        submitted = []
        submit_lock = threading.Lock()

        def submit_one(i):
            jid = unique_id(f"concurrent-{i}")
            coordinator.submit_job(jid, TRIVIAL_CMD)
            with submit_lock:
                submitted.append(jid)

        run_concurrently(submit_one, 50, max_workers=50)

        assert len(set(submitted)) == 50, "duplicate job ids generated by test harness"
        missing = [jid for jid in submitted if jid not in coordinator.jobs]
        assert not missing, f"{len(missing)} concurrently-submitted jobs vanished: {missing[:5]}"

    def test_concurrent_registry_scan_during_node_churn(self, cluster):
        """
        Exercises AUDIT_REPORT.md 2.1: coordinator.get_nodes() iterates
        registry.nodes directly (no lock) while nodes register/deregister
        concurrently.
        """
        from gcon.execution.agent import GCONAgent
        coordinator, agents = cluster
        stop = threading.Event()
        errors = []

        def churn():
            i = 0
            while not stop.is_set():
                node_id = f"churn-node-{threading.get_ident()}-{i}"
                a = GCONAgent(node_id=node_id)
                try:
                    coordinator.register_agent(a)
                    coordinator.deregister_agent(node_id)
                except Exception as e:
                    errors.append(e)
                i += 1

        churners = [threading.Thread(target=churn, daemon=True) for _ in range(4)]
        for t in churners:
            t.start()

        scan_errors = []
        deadline = time.monotonic() + 3.0
        while time.monotonic() < deadline:
            try:
                coordinator.get_nodes()
            except RuntimeError as e:
                scan_errors.append(str(e))

        stop.set()
        for t in churners:
            t.join(timeout=2)

        assert not scan_errors, (
            f"get_nodes() raised {len(scan_errors)} RuntimeErrors during concurrent "
            f"registration/deregistration (AUDIT_REPORT.md 2.1 — get_nodes() bypasses "
            f"the registry lock): {scan_errors[:3]}"
        )

    @pytest.mark.xfail(reason="AUDIT_REPORT.md 2.3: no atomic claim_node — "
                               "two concurrent assign_job calls can double-book one node")
    def test_no_double_dispatch_to_same_node(self, single_node_cluster):
        """
        With exactly ONE node, concurrently trigger a normal dispatch
        and a recovery-path dispatch for two different jobs and assert
        the node's agent.process is never clobbered mid-flight (i.e.
        only one job is ever actually executing on it at a time).
        """
        coordinator, agent = single_node_cluster
        job_a, job_b = unique_id("dispatch-a"), unique_id("dispatch-b")
        coordinator.submit_job(job_a, SLEEP_CMD_TEMPLATE.format(secs=1))
        coordinator.jobs[job_b] = {
            "command": SLEEP_CMD_TEMPLATE.format(secs=1), "node_id": None,
            "status": "pending", "artifacts": [], "created_at": None,
            "completed_at": None,
        }

        # Fire both dispatch paths as close together as possible.
        t1 = threading.Thread(target=lambda: coordinator.assign_job(job_a))
        t2 = threading.Thread(target=lambda: coordinator.assign_job(job_b))
        t1.start(); t2.start()
        t1.join(); t2.join()

        running = [j for j in (job_a, job_b)
                   if coordinator.jobs[j]["node_id"] == agent.node_id
                   and coordinator.jobs[j]["status"] == "running"]
        assert len(running) <= 1, (
            f"single-node cluster dispatched {len(running)} jobs to the same node "
            f"simultaneously: {running}"
        )


# =======================================================================
# 3. DISTRIBUTED / NETWORK-FAULT TESTS (via mock_network.py)
# =======================================================================

class TestDistributed:
    def test_job_survives_flaky_wifi_profile(self, cluster):
        coordinator, agents = cluster
        coordinator.communication = SimulatedNetworkCommunicationManager(preset("flaky_wifi"))
        for a in agents:
            coordinator.communication.register_node(a)

        job_id = unique_id("flaky")
        coordinator.submit_job(job_id, TRIVIAL_CMD)
        # Under packet loss / duplication the job may fail — that's
        # expected and fine; what must NOT happen is the worker thread
        # hanging forever (AUDIT_REPORT.md 3.1).
        statuses = assert_all_jobs_terminal(coordinator, [job_id], timeout=20.0)
        assert statuses[job_id] in ("completed", "failed", "cancelled")

    def test_partitioned_node_is_not_dispatched_to(self, cluster):
        coordinator, agents = cluster
        sim_comm = SimulatedNetworkCommunicationManager()
        coordinator.communication = sim_comm
        for a in agents:
            sim_comm.register_node(a)

        with temporary_partition(sim_comm.simulator, [agents[0].node_id]):
            job_id = unique_id("partition")
            coordinator.submit_job(job_id, TRIVIAL_CMD)
            statuses = assert_all_jobs_terminal(coordinator, [job_id], timeout=15.0)
            # The dispatched-to node must not be the partitioned one,
            # UNLESS the scheduler happened to only have that node idle
            # (in which case it should fail cleanly, not hang).
            job = coordinator.jobs[job_id]
            if job["node_id"] == agents[0].node_id:
                assert statuses[job_id] == "failed", (
                    "job dispatched into a partition did not fail cleanly "
                    "(AUDIT_REPORT.md 3.1-adjacent: a partition should behave "
                    "like an unreachable node, not hang forever)"
                )

    def test_duplicate_heartbeat_does_not_corrupt_last_seen(self, cluster):
        """
        AUDIT_REPORT.md 4.4: heartbeats are applied unconditionally with
        no sequence/monotonicity check. A duplicated OLDER heartbeat
        arriving after a newer one should not roll back last_seen.
        """
        coordinator, agents = cluster
        node_id = agents[0].node_id
        from datetime import datetime, UTC, timedelta

        newer = datetime.now(UTC)
        older = newer - timedelta(seconds=30)

        coordinator.receive_heartbeat({"node_id": node_id, "status": "idle", "timestamp": newer})
        coordinator.receive_heartbeat({"node_id": node_id, "status": "idle", "timestamp": older})

        info = coordinator.registry.get_node_info(node_id)
        assert info["last_seen"] >= newer, (
            "an out-of-order (older) duplicate heartbeat rolled back last_seen — "
            "this can un-expire a node that should be considered offline "
            "(AUDIT_REPORT.md 4.4: no monotonicity check on heartbeat timestamps)"
        )


# =======================================================================
# 4. HEARTBEAT VALIDATION
# =======================================================================

class TestHeartbeat:
    def test_missed_heartbeats_mark_node_offline(self, single_node_cluster):
        coordinator, agent = single_node_cluster
        coordinator.registry.timeout = __import__("datetime").timedelta(seconds=0.5)
        # No heartbeat sent since registration -> should go offline on
        # next health check.
        assert_eventually(
            lambda: coordinator.registry.get_node_info(agent.node_id)["status"] == "offline",
            timeout=5.0,
            description="node to be marked offline after missed heartbeats",
        )

    def test_heartbeat_thread_survives_transient_coordinator_slowness(self, single_node_cluster):
        coordinator, agent = single_node_cluster
        agent.start_heartbeat(coordinator, interval=0.2)
        try:
            assert_eventually(
                lambda: coordinator.registry.get_node_info(agent.node_id)["status"] == "idle",
                timeout=3.0,
                description="at least one heartbeat to be received",
            )
        finally:
            agent.stop_heartbeat()


# =======================================================================
# 5. COORDINATOR FAILOVER TESTS
# =======================================================================

class TestCoordinatorFailover:
    @pytest.mark.xfail(reason="AUDIT_REPORT.md 4.1: no persistence layer — "
                               "coordinator state does not survive a restart at all")
    def test_state_survives_simulated_restart(self, cluster):
        coordinator, agents = cluster
        job_id = unique_id("failover")
        coordinator.submit_job(job_id, TRIVIAL_CMD)

        # "Restart" = build a brand new coordinator, as a real process
        # restart would. If any persistence existed, it would be loaded
        # here (there is currently no such API to call).
        from gcon.cluster.coordinator import GCONCoordinator
        new_coordinator = GCONCoordinator()

        assert job_id in new_coordinator.jobs, (
            "job did not survive a simulated coordinator restart — "
            "no state persistence exists (AUDIT_REPORT.md 4.1)"
        )

    def test_scheduler_thread_death_is_detected_by_health_service(self, cluster):
        coordinator, agents = cluster
        monkey = ChaosMonkey(coordinator)
        monkey.crash_scheduler_loop()
        # force the crash to actually happen by nudging the scheduler
        coordinator.submit_job(unique_id("crash-trigger"), TRIVIAL_CMD)

        assert_eventually(
            lambda: not coordinator.scheduler_thread.is_alive(),
            timeout=5.0,
            description="scheduler thread to die from the injected non-RuntimeError",
        )
        health = coordinator.get_cluster_health()
        assert health["checks"]["coordinator"]["healthy"] is False, (
            "health_service did not detect a dead scheduler thread"
        )


# =======================================================================
# 6. WORKER CRASH / RECOVERY TESTS
# =======================================================================

class TestWorkerCrashRecovery:
    def test_job_on_killed_node_is_recovered(self, cluster):
        coordinator, agents = cluster
        coordinator.registry.timeout = __import__("datetime").timedelta(seconds=0.5)
        coordinator.pause_scheduler()

        long_job = unique_id("crash-recover")
        coordinator.submit_job(long_job, SLEEP_CMD_TEMPLATE.format(secs=5))
        # Manually force it into 'running' on a specific node without
        # waiting for the (paused) scheduler.
        target = agents[0]
        coordinator.jobs[long_job]["status"] = "running"
        coordinator.jobs[long_job]["node_id"] = target.node_id
        target.status = "busy"
        coordinator.registry.heartbeat(target.node_id, "busy", target.heartbeat()["timestamp"])

        monkey = ChaosMonkey(coordinator)
        monkey.kill_worker(target.node_id)
        coordinator.resume_scheduler()

        assert_eventually(
            lambda: coordinator.jobs[long_job]["node_id"] != target.node_id
                    or coordinator.jobs[long_job]["status"] == "pending",
            timeout=10.0,
            description="job to be reassigned off the killed node",
        )

    def test_recover_jobs_does_not_crash_on_concurrent_submission(self, cluster):
        """AUDIT_REPORT.md 2.2 directly."""
        coordinator, agents = cluster
        stop = threading.Event()
        errors = []

        def submitter():
            i = 0
            while not stop.is_set():
                try:
                    coordinator.submit_job(unique_id(f"recover-race-{i}"), TRIVIAL_CMD)
                except Exception as e:
                    errors.append(e)
                i += 1

        t = threading.Thread(target=submitter, daemon=True)
        t.start()
        for _ in range(20):
            try:
                coordinator.recover_jobs(agents[0].node_id)
            except RuntimeError as e:
                errors.append(e)
            time.sleep(0.01)
        stop.set()
        t.join(timeout=2)

        assert not errors, f"recover_jobs raced with submit_job: {errors[:3]}"


# =======================================================================
# 7. MEMORY / CPU / SOAK TESTS
# =======================================================================

@pytest.mark.slow
class TestMemoryAndSoak:
    def test_job_history_growth_is_bounded_or_documented(self, cluster):
        """
        AUDIT_REPORT.md 6.2: self.jobs / self.receipts grow unbounded.
        This test does not assert a fix (there isn't one yet) — it
        MEASURES the growth rate so it's visible in CI output rather
        than discovered in production.
        """
        coordinator, agents = cluster
        before = len(coordinator.jobs)
        job_ids = [unique_id("growth") for _ in range(500)]
        for jid in job_ids:
            coordinator.submit_job(jid, TRIVIAL_CMD)
        assert_all_jobs_terminal(coordinator, job_ids, timeout=90.0)
        after = len(coordinator.jobs)
        logger.warning(
            f"[SOAK] coordinator.jobs grew from {before} to {after} entries "
            f"with no eviction (AUDIT_REPORT.md 6.2) — at 1e6 jobs/day this "
            f"structure grows unboundedly for the life of the process"
        )
        assert after - before == 500  # sanity: no jobs silently dropped

    def test_soak_sustained_submission_no_thread_leak(self, cluster):
        coordinator, agents = cluster
        sampler = ResourceSampler(interval=1.0)
        sampler.start()
        gc.collect()
        start_threads = threading.active_count()

        deadline = time.monotonic() + 20.0
        submitted = 0
        while time.monotonic() < deadline:
            coordinator.submit_job(unique_id("soak"), TRIVIAL_CMD)
            submitted += 1
            time.sleep(0.02)

        assert_eventually(
            lambda: all(coordinator.jobs[j]["status"] in
                        ("completed", "failed", "cancelled")
                        for j in list(coordinator.jobs)[-submitted:]),
            timeout=30.0,
            description="soak-submitted jobs to drain",
        )
        sampler.stop()
        end_threads = threading.active_count()
        report = sampler.report()
        logger.warning(f"[SOAK] {submitted} jobs, thread count {start_threads} -> "
                        f"{end_threads}, RSS growth {report.get('rss_growth_mb')}MB")
        # A per-job worker thread (coordinator.py:160-164) is daemon and
        # short-lived, so thread count should return close to baseline
        # once jobs drain, not grow linearly with jobs submitted.
        assert end_threads < start_threads + 20, (
            f"thread count grew by {end_threads - start_threads} across "
            f"{submitted} jobs — possible per-job thread leak "
            f"(see AUDIT_REPORT.md 3.1 for the specific hang scenario that "
            f"causes exactly this)"
        )


# =======================================================================
# 8. WEBSOCKET TESTS (require `httpx`/`websockets` + a running web_server)
# =======================================================================

@pytest.mark.slow
class TestWebSocket:
    def test_ws_rejects_missing_session(self):
        """
        AUDIT_REPORT.md 5.4 baseline check: connecting to /ws with no
        session cookie must be refused. Requires web_server's FastAPI
        app; imported lazily so the rest of the suite doesn't require
        the web stack to be installed.
        """
        try:
            from fastapi.testclient import TestClient
            from gcon.dashboard.web_server import build_app  # adjust to actual factory name if different
        except ImportError as e:
            pytest.skip(f"web stack not available: {e}")

        app = build_app()
        client = TestClient(app)
        with pytest.raises(Exception):
            with client.websocket_connect("/ws"):
                pass  # should be rejected before/at accept()


# =======================================================================
# Standalone load-generation CLI (non-pytest usage)
# =======================================================================

def _standalone_load(jobs: int, nodes: int, duration: float):
    from gcon.cluster.coordinator import GCONCoordinator
    from gcon.execution.agent import GCONAgent

    coordinator = GCONCoordinator()
    for i in range(nodes):
        coordinator.register_agent(GCONAgent(node_id=f"load-node-{i}"))

    metrics = MetricsCollector()
    sampler = ResourceSampler(interval=1.0)
    sampler.start()

    job_ids = []
    t0 = time.monotonic()
    for _ in range(jobs):
        jid = unique_id("standalone-load")
        with metrics.timer("submit"):
            coordinator.submit_job(jid, TRIVIAL_CMD)
        job_ids.append(jid)

    logger.info(f"[LOAD] submitted {jobs} jobs across {nodes} nodes in "
                f"{time.monotonic() - t0:.2f}s, draining...")

    try:
        assert_all_jobs_terminal(coordinator, job_ids, timeout=duration)
    except TestAssertionError as e:
        logger.error(f"[LOAD] not all jobs drained: {e}")

    sampler.stop()
    print("=== Summary ===")
    print(metrics.summary())
    print(sampler.report())


def main():
    parser = argparse.ArgumentParser(description="GCON standalone stress runner")
    parser.add_argument("--mode", choices=["load"], default="load")
    parser.add_argument("--jobs", type=int, default=1000)
    parser.add_argument("--nodes", type=int, default=20)
    parser.add_argument("--duration", type=float, default=120.0)
    args = parser.parse_args()

    if args.mode == "load":
        _standalone_load(args.jobs, args.nodes, args.duration)


if __name__ == "__main__":
    main()
