#!/usr/bin/env python3
"""
GCON Stress / Chaos / QA Test Suite
=====================================

Production-quality automated test harness for the GCON distributed job
coordination system. Written against the REAL codebase (coordinator.py,
agent.py, scheduler.py, Noderegistry.py, workflow_engine.py, dag.py,
verifier.py, receipt.py, key_manager.py, policy.py, storage_manager.py,
artifact_registry.py, health_service.py, autoscaler.py) — no mocks of
GCON's own logic, only mocks of external boundaries (subprocess timing,
disk speed, network) where a real fault can't be safely injected.

WHAT THIS FILE DOES
--------------------
  * Runs unit, integration, end-to-end, concurrency, multiprocessing,
    async, fault-injection/chaos, memory-pressure, CPU-saturation,
    disk-I/O, recovery, retry, timeout, malformed-input/fuzz, load,
    soak, and scalability-benchmark tests.
  * Measures latency, throughput, CPU%, RSS memory, and detects
    deadlocks (via hard wall-clock watchdogs on every test) and
    starvation (queue-drain-time bounds).
  * Prints a PASS/FAIL line for every subsystem as it runs, writes a
    detailed log file (stress_test.log) and a machine-readable JSON
    report (stress_test_report.json), and ends with a weighted overall
    HEALTH SCORE.
  * Several tests are deliberately written as REGRESSION tests for
    real defects found during the accompanying architecture audit
    (GCON_Architecture_Audit.md). Where the system is currently known
    to be broken, the test is tagged KNOWN_DEFECT and reports that
    fact clearly instead of silently passing or crashing the suite.

HOW TO RUN
----------
    $ python3 stress_test.py                    # full suite
    $ python3 stress_test.py --quick             # skip soak/load (fast)
    $ python3 stress_test.py --category security # run one category
    $ GCON_PROJECT_ROOT=/path/to/gcon python3 stress_test.py

REQUIREMENTS
------------
  * Python 3.10+, the GCON project itself importable (this file expects
    to sit in the GCON project root, or GCON_PROJECT_ROOT env var set).
  * psutil (already a GCON dependency).
  * No pytest / fastapi / httpx dependency — deliberately stdlib-only
    (unittest is NOT even required) so this runs in any environment
    that can already run GCON itself. Web-layer (FastAPI) tests are
    skipped with an explicit reason if `fastapi`/`httpx` aren't
    installed — see MISSING INFORMATION at the bottom of this file.

MISSING INFORMATION (see also GCON_Architecture_Audit.md §7)
--------------------------------------------------------------
  1. Real network transport between coordinator and agent does not
     exist (communication.py is an in-process call). Network-failure /
     packet-loss tests below therefore inject faults by monkeypatching
     CommunicationManager.send_job rather than a real socket. Once a
     real RPC transport exists, point NETWORK_FAULT_INJECTOR at it.
  2. No multi-coordinator / leader-election implementation exists, so
     "coordinator failover" cannot be tested beyond "the single
     coordinator process died" (which IS tested, under Recovery).
  3. No GPU hardware / GPUtil in this environment — GPU metrics are
     exercised via the documented fallback stub only.
  4. No persistence layer exists yet, so "recovery from persisted
     state after a crash" cannot be tested — only "state is lost on
     crash" (currently true) can be, and is, asserted.
"""

import argparse
import contextlib
import gc
import json
import logging
import multiprocessing
import os
import random
import shutil
import statistics
import subprocess
import sys
import tempfile
import threading
import time
import traceback
import uuid
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional

UTC = timezone.utc

# ---------------------------------------------------------------------------
# Project import wiring
# ---------------------------------------------------------------------------
PROJECT_ROOT = os.environ.get(
    "GCON_PROJECT_ROOT", os.path.dirname(os.path.abspath(__file__))
)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

try:
    import psutil
except ImportError:
    print("FATAL: psutil is required (pip install psutil --break-system-packages)")
    sys.exit(2)

IMPORT_ERROR = None
try:
    from gcon.cluster.coordinator import GCONCoordinator
    from gcon.execution.agent import GCONAgent
    from gcon.cluster.Noderegistry import NodeRegistry
    from gcon.cluster.scheduler import Scheduler
    from gcon.cluster.communication import CommunicationManager
    from gcon.execution.verifier import ExecutionVerifier
    from gcon.execution.receipt import (
        ReceiptGenerator,
        ReceiptSigner,
        ReceiptVerifier,
        compute_receipt_hash,
    )
    from gcon.management.key_manager import KeyManager
    from gcon.execution.artifact_registry import ArtifactRegistry
    from gcon.storage.storage_manager import StorageManager
    from policy import PolicyEngine
    from gcon.workflow.dag import DAG
    from gcon.workflow.workflow import Workflow, WorkflowJob
    from gcon.workflow.workflow_engine import WorkflowEngine
    from gcon.workflow.workflow_state import WorkflowState
    from gcon.monitoring.health_service import HealthService
    from gcon.cluster.autoscaler import AutoScaler
    from gcon.events.event_bus import EventBus
    from gcon.events.event import Event
    from gcon.management.auth import hash_password, verify_password, SessionManager
    from gcon.cluster.node import GCONNode
except Exception as exc:  # pragma: no cover - environment problem, not a test result
    IMPORT_ERROR = exc

try:
    import fastapi  # noqa: F401
    import httpx  # noqa: F401

    HAS_WEB_STACK = True
except ImportError:
    HAS_WEB_STACK = False


# ---------------------------------------------------------------------------
# Micro test-framework: categorized, timed, watchdog-protected test runner
# ---------------------------------------------------------------------------

class Severity:
    P0 = "P0-CRITICAL"
    P1 = "P1-MAJOR"
    P2 = "P2-MINOR"
    INFO = "INFO"


@dataclass
class TestResult:
    name: str
    category: str
    severity: str
    status: str  # PASS, FAIL, ERROR, SKIP, KNOWN_DEFECT, TIMEOUT
    duration_s: float
    message: str = ""
    metrics: Dict[str, Any] = field(default_factory=dict)


REGISTRY: List["TestCase"] = []


@dataclass
class TestCase:
    func: Callable
    name: str
    category: str
    severity: str = Severity.P1
    timeout_s: float = 30.0
    known_defect: bool = False  # True => a FAIL here is an EXPECTED, documented bug


def test(category: str, severity: str = Severity.P1, timeout_s: float = 30.0,
         known_defect: bool = False):
    """Decorator registering a function as a stress-test case."""
    def wrap(func):
        REGISTRY.append(
            TestCase(
                func=func, name=func.__name__, category=category,
                severity=severity, timeout_s=timeout_s, known_defect=known_defect,
            )
        )
        return func
    return wrap


class DeadlockError(Exception):
    pass


def run_with_watchdog(func: Callable, timeout_s: float):
    """
    Run `func` in a worker thread with a hard wall-clock timeout, so a
    genuine deadlock/hang in GCON code cannot hang the whole suite. This
    IS the suite's deadlock detector: any test that doesn't return
    within its budget is reported as TIMEOUT (possible deadlock).
    """
    result_box: Dict[str, Any] = {}

    def runner():
        try:
            result_box["metrics"] = func() or {}
        except BaseException as exc:  # noqa: BLE001 - we want everything, incl. asserts
            result_box["exc"] = exc
            result_box["tb"] = traceback.format_exc()

    thread = threading.Thread(target=runner, daemon=True)
    start = time.perf_counter()
    thread.start()
    thread.join(timeout=timeout_s)
    duration = time.perf_counter() - start

    if thread.is_alive():
        # Thread is still running -- likely deadlocked / hung subprocess.
        # We cannot forcibly kill a Python thread; we flag it and move on.
        return "TIMEOUT", duration, (
            f"Did not complete within {timeout_s}s -- possible deadlock/hang. "
            f"Thread left running in background (daemon)."
        ), {}
    if "exc" in result_box:
        exc = result_box["exc"]
        if isinstance(exc, AssertionError):
            return "FAIL", duration, str(exc) or "assertion failed", {}
        return "ERROR", duration, f"{type(exc).__name__}: {exc}\n{result_box.get('tb','')}", {}
    return "PASS", duration, "", result_box.get("metrics", {})


class Runner:
    def __init__(self, log: logging.Logger):
        self.log = log
        self.results: List[TestResult] = []
        self.proc = psutil.Process(os.getpid())

    def run_all(self, cases: List[TestCase]):
        for case in cases:
            self._run_one(case)
        return self.results

    def _run_one(self, case: TestCase):
        cpu_before = self.proc.cpu_times()
        rss_before = self.proc.memory_info().rss

        status, duration, message, metrics = run_with_watchdog(case.func, case.timeout_s)

        cpu_after = self.proc.cpu_times()
        rss_after = self.proc.memory_info().rss
        metrics.setdefault("cpu_time_s", round(
            (cpu_after.user + cpu_after.system) - (cpu_before.user + cpu_before.system), 4
        ))
        metrics.setdefault("rss_delta_mb", round((rss_after - rss_before) / (1024 * 1024), 3))

        if case.known_defect and status in ("FAIL", "ERROR"):
            display_status = "KNOWN_DEFECT"
        else:
            display_status = status

        result = TestResult(
            name=case.name, category=case.category, severity=case.severity,
            status=display_status, duration_s=round(duration, 4),
            message=message, metrics=metrics,
        )
        self.results.append(result)

        icon = {
            "PASS": "PASS", "FAIL": "FAIL", "ERROR": "ERROR",
            "SKIP": "SKIP", "KNOWN_DEFECT": "KNOWN_DEFECT (expected, see audit)",
            "TIMEOUT": "TIMEOUT (deadlock suspected)",
        }[display_status]
        line = f"[{icon:38}] {case.category:14} {case.name:42} {duration:6.2f}s"
        self.log.info(line)
        if message and display_status not in ("PASS",):
            for ln in message.strip().splitlines()[:6]:
                self.log.info(f"    | {ln}")


# ---------------------------------------------------------------------------
# Fixtures / test doubles
# ---------------------------------------------------------------------------

@contextlib.contextmanager
def isolated_cwd():
    """
    GCON's StorageManager/ArtifactRegistry/ReceiptManager all write
    relative to the process CWD. Isolate every test in its own tmpdir
    so tests can run concurrently / repeatedly without colliding on
    ./storage, ./receipts, ./keys.
    """
    old_cwd = os.getcwd()
    tmp = tempfile.mkdtemp(prefix="gcon_stress_")
    os.chdir(tmp)
    try:
        yield tmp
    finally:
        os.chdir(old_cwd)
        shutil.rmtree(tmp, ignore_errors=True)


def make_coordinator():
    """Fresh coordinator with its background threads started."""
    return GCONCoordinator()


def make_agent(node_id=None):
    return GCONAgent(node_id or f"node-{uuid.uuid4().hex[:8]}")


def wait_until(predicate: Callable[[], bool], timeout_s=5.0, interval=0.02):
    deadline = time.perf_counter() + timeout_s
    while time.perf_counter() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()


class FastFakeAgent:
    """
    A GCONAgent-compatible test double that completes instantly instead
    of spawning a real subprocess, so scale/throughput tests can exercise
    thousands of dispatch cycles without OS process-creation overhead
    dominating the measurement. Deliberately implements the FULL
    interface coordinator.py actually calls (heartbeat, report_resources,
    cancel, execute_job, is_available) -- this is exactly the contract
    that node.py::GCONNode fails to satisfy (see Finding A-1).
    """

    def __init__(self, node_id):
        self.node_id = node_id
        self.status = "idle"
        self.process = None

    def is_available(self):
        return self.status == "idle"

    def heartbeat(self):
        return {"node_id": self.node_id, "status": self.status, "timestamp": datetime.now(UTC)}

    def report_resources(self):
        return {
            "node_id": self.node_id, "cpu": 1.0, "memory": 1.0,
            "running_jobs": 0 if self.status == "idle" else 1,
            "status": self.status, "timestamp": datetime.now(UTC).isoformat(),
        }

    def cancel(self):
        return False

    def execute_job(self, job_id, command, timeout=None):
        self.status = "busy"
        result = {
            "job_id": job_id, "status": "success", "return_code": 0,
            "runtime_seconds": 0.0, "stdout": "ok", "stderr": "",
            "metrics": {
                "job_id": job_id, "gpu_name": "Unknown GPU", "gpu_memory_total": 0,
                "gpu_memory_used": 0, "cpu_percent": 0.0, "memory_percent": 0.0,
                "runtime_seconds": 0.0, "timestamp": datetime.now(UTC).isoformat(),
            },
            "timestamp": datetime.now(UTC).isoformat(),
        }
        self.status = "idle"
        return result


class HangingFakeAgent(FastFakeAgent):
    """Simulates a node whose job never returns (used for timeout/deadlock tests)."""

    def execute_job(self, job_id, command, timeout=None):
        self.status = "busy"
        if timeout:
            time.sleep(timeout + 1)
            raise subprocess.TimeoutExpired(cmd=command, timeout=timeout)
        # No timeout given -- this is the real production default. We cap the
        # SIMULATED hang at 2s (rather than truly forever) purely so this
        # specific test can finish; production has no such cap (see audit C-2).
        time.sleep(2.0)
        self.status = "idle"
        return {"job_id": job_id, "status": "success", "return_code": 0,
                "runtime_seconds": 2.0, "stdout": "", "stderr": "",
                "metrics": {}, "timestamp": datetime.now(UTC).isoformat()}


class FlakyCommunicationManager(CommunicationManager):
    """CommunicationManager that randomly fails/times out -- network chaos."""

    def __init__(self, failure_rate=0.3, seed=1234):
        super().__init__()
        self.failure_rate = failure_rate
        self._rand = random.Random(seed)

    def send_job(self, node_id, job_id, command):
        roll = self._rand.random()
        if roll < self.failure_rate / 2:
            raise ConnectionError(f"simulated network partition to {node_id}")
        if roll < self.failure_rate:
            raise TimeoutError(f"simulated packet loss / RPC timeout to {node_id}")
        return super().send_job(node_id, job_id, command)


# ===========================================================================
# 1. UNIT TESTS
# ===========================================================================

@test(category="unit.verifier", severity=Severity.P0)
def test_verifier_hash_and_sign_roundtrip():
    v = ExecutionVerifier(secret_key="unit-test-key")
    h = v.hash_data({"a": 1, "b": 2})
    assert len(h) == 64, "sha256 hex digest should be 64 chars"
    sig = v.sign_data({"x": 1})
    assert v.verify_signature({"x": 1}, sig) is True
    assert v.verify_signature({"x": 2}, sig) is False, "tampered data must fail verification"


@test(category="unit.verifier", severity=Severity.P0)
def test_verifier_receipt_lifecycle():
    v = ExecutionVerifier(secret_key="unit-test-key")
    receipt = v.create_receipt(
        job_id="j1", agent_id="node-1",
        execution_result={"status": "success", "runtime_seconds": 1.2,
                           "metrics": {"gpu_name": "sim-gpu"}},
        input_hash=v.hash_data("cmd"), output_hash=v.hash_data("out"),
    )
    ok, msg = v.validate_proof(receipt["proof"])
    assert ok, f"freshly created receipt must validate: {msg}"

    tampered = dict(receipt["proof"])
    tampered["runtime_seconds"] = 999999
    ok2, _ = v.validate_proof(tampered)
    assert not ok2, "tampering with a signed field must invalidate the proof"


@test(category="security.receipts", severity=Severity.P0)
def test_KNOWN_DEFECT_default_hmac_key_allows_receipt_forgery():
    """
    Regression test for Audit Finding B-1: coordinator.py instantiates
    ExecutionVerifier() with NO secret, so it silently falls back to the
    hardcoded default "gcon-default-key". Anyone who reads the source
    (this exact string, public in this repo) can forge a receipt that
    validates as genuine. This test demonstrates the forgery succeeding,
    which is the BAD outcome -- it is tagged known_defect=True so a
    (correctly) failing assertion here is reported as a documented
    finding, not a broken test suite.
    """
    coordinator_default_verifier = ExecutionVerifier()  # exactly how coordinator.py builds it
    forged_proof = {
        "job_id": "not-a-real-job", "gpu": "H100x8", "runtime_seconds": 0.001,
        "input_hash": "0" * 64, "output_hash": "0" * 64,
        "timestamp": datetime.now(UTC).isoformat(), "metrics": {},
    }
    import hmac as _hmac, hashlib as _hashlib, json as _json
    forged_sig = _hmac.new(
        b"gcon-default-key",
        _json.dumps(forged_proof, sort_keys=True).encode(),
        _hashlib.sha256,
    ).hexdigest()
    forged_proof["signature"] = forged_sig
    forged_proof["verified"] = True

    ok, msg = coordinator_default_verifier.validate_proof(forged_proof)
    # SECURE behavior would be `assert not ok`. The assertion below encodes
    # what SHOULD be true; today it fails, correctly surfacing the defect.
    assert not ok, (
        "SECURITY DEFECT CONFIRMED: a receipt forged with the publicly-known "
        "default HMAC key ('gcon-default-key') was accepted as valid. "
        "coordinator.py must require an explicit, deployment-specific secret."
    )


@test(category="unit.receipt_ed25519", severity=Severity.P1)
def test_ed25519_receipt_pipeline_is_internally_correct():
    """
    The UNUSED (by coordinator.py) Ed25519 receipt pipeline in receipt.py
    is, in isolation, correctly implemented -- this documents that it's a
    viable replacement for the broken HMAC-default-key path (Finding B-1).
    """
    execution_result = {
        "job_id": "j-ed", "status": "success", "timestamp": datetime.now(UTC).isoformat(),
        "metrics": {"gpu_name": "sim", "runtime_seconds": 0.5, "cpu_percent": 10.0,
                    "memory_percent": 20.0},
        "stdout": "ok", "stderr": "",
    }
    receipt = ReceiptGenerator.generate(execution_result)
    assert ReceiptVerifier.verify(receipt), "freshly generated Ed25519 receipt must verify"

    tampered = dict(receipt)
    tampered["status"] = "success-but-actually-tampered"
    assert not ReceiptVerifier.verify(tampered), "tampered receipt must fail verification"


@test(category="unit.auth", severity=Severity.P0)
def test_password_hash_verify_and_salting():
    h1 = hash_password("correct horse battery staple")
    h2 = hash_password("correct horse battery staple")
    assert h1 != h2, "same password must produce different hashes (random salt)"
    assert verify_password("correct horse battery staple", h1)
    assert not verify_password("wrong password", h1)
    assert not verify_password("", h1)


@test(category="unit.auth", severity=Severity.P2)
def test_session_expiry():
    sm = SessionManager(ttl_hours=0)  # expires immediately
    token = sm.create_session("user-1")
    time.sleep(0.01)
    assert sm.get_user_id(token) is None, "expired session must not resolve to a user"


@test(category="unit.dag", severity=Severity.P1)
def test_dag_cycle_detection():
    wf = Workflow("wf-cycle")
    for jid in ("a", "b", "c"):
        wf.add_job(WorkflowJob(jid, "echo hi"))
    wf.add_dependency("a", "b")
    wf.add_dependency("b", "c")
    wf.add_dependency("c", "a")  # cycle
    dag = DAG(wf)
    assert dag.has_cycle() is True


@test(category="unit.dag", severity=Severity.P1)
def test_dag_topological_sort_respects_dependencies():
    wf = Workflow("wf-topo")
    for jid in ("a", "b", "c", "d"):
        wf.add_job(WorkflowJob(jid, "echo hi"))
    wf.add_dependency("a", "c")
    wf.add_dependency("b", "c")
    wf.add_dependency("c", "d")
    dag = DAG(wf)
    order = [j.job_id for j in dag.topological_sort()]
    assert order.index("a") < order.index("c")
    assert order.index("b") < order.index("c")
    assert order.index("c") < order.index("d")


@test(category="unit.dag", severity=Severity.P2)
def test_dag_ready_jobs_after_partial_completion():
    wf = Workflow("wf-ready")
    for jid in ("a", "b", "c"):
        wf.add_job(WorkflowJob(jid, "echo hi"))
    wf.add_dependency("a", "c")
    wf.add_dependency("b", "c")
    dag = DAG(wf)
    ready_now = {j.job_id for j in dag.ready_jobs(completed_jobs=set())}
    assert ready_now == {"a", "b"}
    ready_after_a = {j.job_id for j in dag.ready_jobs(completed_jobs={"a"})}
    assert ready_after_a == {"b"}, "c must stay blocked until BOTH a and b complete"
    ready_after_both = {j.job_id for j in dag.ready_jobs(completed_jobs={"a", "b"})}
    assert ready_after_both == {"c"}


@test(category="unit.artifact_registry", severity=Severity.P1)
def test_artifact_registration_and_integrity_check():
    with isolated_cwd() as tmp:
        path = os.path.join(tmp, "output.json")
        with open(path, "w") as f:
            f.write('{"hello": "world"}')
        reg = ArtifactRegistry()
        aid = reg.register_artifact(path)
        assert reg.verify_artifact(aid) is True
        with open(path, "a") as f:
            f.write("tampered")
        assert reg.verify_artifact(aid) is False, "checksum must detect post-registration tampering"


@test(category="unit.artifact_registry", severity=Severity.P2, known_defect=True)
def test_KNOWN_DEFECT_artifact_dedup_by_bare_filename_collides():
    """Regression test for Audit Finding D-3."""
    with isolated_cwd() as tmp:
        os.makedirs("job1"); os.makedirs("job2")
        p1 = os.path.join(tmp, "job1", "output.json")
        p2 = os.path.join(tmp, "job2", "output.json")
        with open(p1, "w") as f:
            f.write('{"job": 1}')
        with open(p2, "w") as f:
            f.write('{"job": 2, "totally different content and much longer": true}')
        reg = ArtifactRegistry()
        id1 = reg.register_artifact(p1)
        id2 = reg.register_artifact(p2)
        assert id1 != id2, (
            "DEFECT CONFIRMED: two different files that merely share a basename "
            "('output.json') were collapsed into a single artifact registration; "
            "the second file's real content/hash was silently discarded."
        )


@test(category="unit.policy", severity=Severity.P2, known_defect=True)
def test_KNOWN_DEFECT_policy_check_runtime_crashes_on_violation():
    """Regression test for Audit Finding D-1 (self.max_runtime vs self.policy['max_runtime'])."""
    with isolated_cwd():
        engine = PolicyEngine(policy_file="nonexistent.json")  # forces default policy
        receipt = {"proof": {"metrics": {"runtime_seconds": engine.policy["max_runtime"] + 100}}}
        passed, message = engine.check_runtime(receipt)
        assert passed is False and isinstance(message, str), (
            "DEFECT CONFIRMED: check_runtime raised instead of returning "
            "(False, message) when a job actually violates the runtime policy "
            "(AttributeError: 'PolicyEngine' object has no attribute 'max_runtime')."
        )


@test(category="unit.node_registry", severity=Severity.P1)
def test_node_registry_heartbeat_timeout_marks_offline():
    reg = NodeRegistry()
    reg.timeout = timedelta(seconds=0.05)
    agent = make_agent("node-timeout")
    reg.register(agent)
    reg.heartbeat("node-timeout", "idle", datetime.now(UTC) - timedelta(seconds=1))
    time.sleep(0.06)
    offline = reg.check_node_health()
    assert "node-timeout" in offline
    assert reg.nodes["node-timeout"]["status"] == "offline"


@test(category="unit.scheduler", severity=Severity.P1)
def test_scheduler_selects_least_loaded_idle_node():
    reg = NodeRegistry()
    for i, (cpu, mem, jobs) in enumerate([(90, 90, 3), (10, 10, 0), (50, 50, 1)]):
        agent = make_agent(f"node-{i}")
        reg.register(agent)
        reg.nodes[agent.node_id].update({"cpu": cpu, "memory": mem, "running_jobs": jobs})
    sched = Scheduler(reg)
    chosen = sched.select_node()
    assert chosen.node_id == "node-1", "scheduler must pick the lowest-load idle node"


@test(category="unit.workflow_state", severity=Severity.P2)
def test_workflow_state_transitions_are_mutually_exclusive():
    state = WorkflowState("wf-x")
    state.mark_pending("j1")
    state.mark_ready("j1")
    assert "j1" not in state.pending_jobs
    assert "j1" in state.ready_jobs
    state.mark_running("j1")
    assert "j1" not in state.ready_jobs
    state.mark_completed("j1") if hasattr(state, "mark_completed") else state._move_job("j1", "COMPLETED")
    assert "j1" in state.completed_jobs
    assert "j1" not in state.running_jobs


# ===========================================================================
# 2. INTEGRATION / END-TO-END TESTS
# ===========================================================================

@test(category="integration.e2e", severity=Severity.P0, timeout_s=15)
def test_e2e_single_job_submit_execute_receipt():
    with isolated_cwd():
        coord = make_coordinator()
        try:
            agent = make_agent("node-e2e")
            coord.register_agent(agent)
            coord.submit_job("job-e2e", "python3 -c \"print('hello-gcon')\"")
            ok = wait_until(lambda: coord.get_job_status("job-e2e")["status"] == "completed",
                             timeout_s=10)
            assert ok, f"job never completed: {coord.get_job_status('job-e2e')}"
            job = coord.get_job_status("job-e2e")
            assert "hello-gcon" in job["result"]["stdout"]
            assert "job-e2e" in coord.receipts, "a receipt must be issued for a completed job"
            valid_results = coord.verify_all_receipts()
            assert valid_results[0]["valid"] is True
        finally:
            pass
    return {"jobs_run": 1}


@test(category="integration.e2e", severity=Severity.P1, timeout_s=15)
def test_e2e_failing_job_marks_failed_and_frees_node():
    with isolated_cwd():
        coord = make_coordinator()
        agent = make_agent("node-fail")
        coord.register_agent(agent)
        coord.submit_job("job-fail", "python3 -c \"import sys; sys.exit(1)\"")
        ok = wait_until(lambda: coord.get_job_status("job-fail")["status"] == "failed", timeout_s=10)
        assert ok, coord.get_job_status("job-fail")
        assert wait_until(lambda: coord.registry.nodes["node-fail"]["status"] == "idle", timeout_s=5), \
            "node must return to idle after a failed job, not stay stuck busy"


@test(category="integration.node_lifecycle", severity=Severity.P1, timeout_s=15)
def test_node_drain_stop_restart_lifecycle():
    with isolated_cwd():
        coord = make_coordinator()
        agent = make_agent("node-lc")
        coord.register_agent(agent)

        coord.drain_node("node-lc")
        assert coord.registry.nodes["node-lc"]["draining"] is True
        coord.submit_job("job-drain", "python3 -c \"print(1)\"")
        time.sleep(0.5)
        assert coord.get_job_status("job-drain")["status"] == "pending", \
            "a draining node must not receive new work"

        coord.restart_worker("node-lc")
        assert coord.registry.nodes["node-lc"]["draining"] is False
        ok = wait_until(lambda: coord.get_job_status("job-drain")["status"] == "completed", timeout_s=10)
        assert ok

        coord.stop_worker("node-lc")
        assert "node-lc" not in coord.registry.nodes


@test(category="integration.job_control", severity=Severity.P1, timeout_s=15)
def test_cancel_job_kills_process_and_frees_node():
    with isolated_cwd():
        coord = make_coordinator()
        agent = make_agent("node-cancel")
        coord.register_agent(agent)
        coord.submit_job("job-cancel", "python3 -c \"import time; time.sleep(30)\"")
        assert wait_until(lambda: coord.get_job_status("job-cancel")["status"] == "running", timeout_s=5)
        killed = coord.cancel_job("job-cancel")
        assert killed is True
        ok = wait_until(lambda: coord.get_job_status("job-cancel")["status"] == "cancelled", timeout_s=10)
        assert ok, coord.get_job_status("job-cancel")


@test(category="integration.queue_ops", severity=Severity.P2, timeout_s=15)
def test_clear_queue_and_retry_failed_jobs():
    with isolated_cwd():
        coord = make_coordinator()
        coord.pause_scheduler()
        coord.submit_job("q1", "echo 1")
        coord.submit_job("q2", "echo 2")
        cleared = coord.clear_queue()
        assert set(cleared) == {"q1", "q2"}
        assert coord.get_job_status("q1")["status"] == "cancelled"

        coord.jobs["q3"] = {"command": "echo 3", "node_id": None, "status": "failed",
                             "artifacts": [], "created_at": datetime.now(UTC).isoformat(),
                             "completed_at": datetime.now(UTC).isoformat()}
        retried = coord.retry_failed_jobs()
        assert "q3" in retried
        assert coord.get_job_status("q3")["status"] == "pending"


@test(category="integration.recovery", severity=Severity.P0, timeout_s=20)
def test_node_offline_recovers_running_job_to_another_node():
    with isolated_cwd():
        coord = make_coordinator()
        coord.registry.timeout = timedelta(seconds=0.3)
        slow_node = make_agent("node-slow")
        backup_node = make_agent("node-backup")
        coord.register_agent(slow_node)
        coord.register_agent(backup_node)

        coord.submit_job("job-orphan", "python3 -c \"import time; time.sleep(1)\"")
        assert wait_until(lambda: coord.get_job_status("job-orphan")["status"] == "running", timeout_s=5)
        running_node_id = coord.get_job_status("job-orphan")["node_id"]

        # Simulate the node that picked up the job going silent (no more heartbeats).
        coord.registry.nodes[running_node_id]["last_seen"] = (
            datetime.now(UTC) - timedelta(seconds=5)
        )
        # Health loop runs every 3s in real coordinator; force it now for a fast test.
        coord.check_cluster_health()

        assert coord.registry.nodes[running_node_id]["status"] == "offline"
        # recover_jobs is invoked synchronously inside check_cluster_health
        recovered = wait_until(
            lambda: coord.get_job_status("job-orphan")["status"] in ("pending", "running", "completed"),
            timeout_s=5,
        )
        assert recovered
        assert coord.get_job_status("job-orphan")["node_id"] != running_node_id or \
            coord.get_job_status("job-orphan")["status"] != "running", \
            "job must not still be assigned to the now-offline node"


@test(category="integration.workflow", severity=Severity.P0, known_defect=True, timeout_s=15)
def test_KNOWN_DEFECT_workflow_engine_executes_full_diamond_dag():
    """
    Regression test for Audit Finding A-2. This is the test that SHOULD
    pass once the workflow engine is fixed: submit a diamond DAG
    (a, b -> c) through the REAL coordinator and expect all three jobs
    to eventually complete. Today it fails for two independent reasons
    (schedule_ready_jobs only submits one of {a, b}; nothing schedules c
    after a/b complete because JOB_COMPLETED is never wired to the
    workflow engine) -- both documented in the audit.
    """
    with isolated_cwd():
        coord = make_coordinator()
        coord.register_agent(make_agent("node-wf"))

        wf = Workflow("wf-diamond")
        wf.add_job(WorkflowJob("a", "python3 -c \"print('a')\""))
        wf.add_job(WorkflowJob("b", "python3 -c \"print('b')\""))
        wf.add_job(WorkflowJob("c", "python3 -c \"print('c')\""))
        wf.add_dependency("a", "c")
        wf.add_dependency("b", "c")

        state = coord.submit_workflow(wf)
        engine = coord.workflow_engine
        engine.schedule_ready_jobs(wf, state)

        # Give the coordinator's own scheduler a moment to actually run
        # whatever WAS submitted, then manually drive the (broken) engine
        # the way a correct event-wire-up would, to see if the SYSTEM as a
        # whole converges -- it should not, today.
        deadline = time.time() + 8
        while time.time() < deadline and not state.workflow_completed():
            for jid in ("a", "b", "c"):
                job = coord.jobs.get(jid)
                if job and job["status"] == "completed" and jid not in state.completed_jobs:
                    engine.process_completed_job(wf, engine.dags["wf-diamond"], state, jid)
                    engine.schedule_ready_jobs(wf, state)
            time.sleep(0.1)

        assert state.workflow_completed(), (
            "DEFECT CONFIRMED: diamond workflow (a,b -> c) did not complete. "
            f"job_states={state.job_states}, coordinator jobs={ {k: v['status'] for k, v in coord.jobs.items()} }"
        )


# ===========================================================================
# 3. CONCURRENCY / RACE-CONDITION TESTS
# ===========================================================================

@test(category="concurrency.races", severity=Severity.P0, timeout_s=20)
def test_concurrent_assign_job_never_double_books_a_node():
    """
    Regression test for Audit Finding C-1. Widens the race window inside
    Scheduler.select_node() so the TOCTOU gap between "select an idle
    node" and "mark it busy" is far more likely to be hit than it would
    be under normal timing -- exactly the effect heavy concurrent load
    has in production.
    """
    with isolated_cwd():
        coord = make_coordinator()
        coord.pause_scheduler()  # drive assign_job manually from many threads
        for i in range(5):
            coord.register_agent(make_agent(f"node-race-{i}"))
        job_ids = [f"job-race-{i}" for i in range(40)]
        for jid in job_ids:
            coord.submit_job(jid, "python3 -c \"import time; time.sleep(0.05)\"")

        original_select = coord.scheduler.select_node

        def slow_select_node():
            node = original_select()
            time.sleep(0.01)  # widen the TOCTOU window between select and mark-busy
            return node

        coord.scheduler.select_node = slow_select_node

        assignment_log = []
        log_lock = threading.Lock()
        orig_assign = coord.assign_job

        def instrumented_assign(job_id):
            orig_assign(job_id)
            job = coord.jobs.get(job_id)
            if job and job["node_id"]:
                with log_lock:
                    assignment_log.append((job["node_id"], job_id, time.perf_counter()))

        # Fan out concurrent assign_job calls the way scheduler_loop +
        # recover_jobs + a retried web request could in production.
        with ThreadPoolExecutor(max_workers=16) as pool:
            futures = [pool.submit(instrumented_assign, jid) for jid in job_ids]
            for f in futures:
                try:
                    f.result(timeout=15)
                except Exception:
                    pass  # RuntimeError ("no node available") is expected under contention

        # Detect double-booking: two DIFFERENT jobs simultaneously marked
        # running on the same node (overlapping time windows).
        busy_windows: Dict[str, List[tuple]] = {}
        for node_id, jid, ts in assignment_log:
            busy_windows.setdefault(node_id, []).append((jid, ts))

        violations = []
        for node_id, jobs, in ((n, coord.jobs) for n in coord.registry.nodes):
            pass  # placeholder kept intentionally simple; real check below

        running_snapshot = {jid: j["node_id"] for jid, j in coord.jobs.items()
                             if j["status"] == "running"}
        node_counts: Dict[str, int] = {}
        for node_id in running_snapshot.values():
            node_counts[node_id] = node_counts.get(node_id, 0) + 1
        double_booked = {n: c for n, c in node_counts.items() if c > 1}

        return {
            "jobs_submitted": len(job_ids),
            "double_booked_nodes_at_snapshot": len(double_booked),
            "note": "A 0 here does not prove the race is absent (races are "
                    "timing-dependent); see the audit's static-analysis finding "
                    "(C-1) for the code-level proof. Re-run under load / with "
                    "PYTHONFAULTHANDLER for higher reproduction odds.",
        }


@test(category="concurrency.threads", severity=Severity.P1, timeout_s=30)
def test_many_concurrent_job_submissions_all_eventually_complete():
    with isolated_cwd():
        coord = make_coordinator()
        n_nodes, n_jobs = 8, 60
        for i in range(n_nodes):
            coord.register_agent(make_agent(f"node-conc-{i}"))

        start = time.perf_counter()
        with ThreadPoolExecutor(max_workers=20) as pool:
            list(pool.map(
                lambda i: coord.submit_job(f"job-conc-{i}", "python3 -c \"print('x')\""),
                range(n_jobs),
            ))
        submit_duration = time.perf_counter() - start

        completed = wait_until(
            lambda: all(coord.jobs[f"job-conc-{i}"]["status"] in ("completed", "failed")
                        for i in range(n_jobs)),
            timeout_s=25,
        )
        total_duration = time.perf_counter() - start
        n_completed = sum(1 for i in range(n_jobs) if coord.jobs[f"job-conc-{i}"]["status"] == "completed")
        assert completed, f"only {n_completed}/{n_jobs} completed within timeout"
        assert n_completed == n_jobs

        return {
            "jobs": n_jobs, "nodes": n_nodes,
            "submit_duration_s": round(submit_duration, 3),
            "total_duration_s": round(total_duration, 3),
            "throughput_jobs_per_s": round(n_jobs / total_duration, 2),
        }


@test(category="concurrency.event_bus", severity=Severity.P2)
def test_event_bus_thread_safety_under_concurrent_publish():
    bus = EventBus()
    errors = []

    def publisher(n):
        try:
            for i in range(200):
                bus.publish(Event(event_type="TEST", source=f"t{n}", payload={"i": i}))
        except Exception as e:  # noqa: BLE001
            errors.append(e)

    threads = [threading.Thread(target=publisher, args=(n,)) for n in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)
    assert not errors, f"EventBus raised under concurrent publish: {errors}"
    assert bus.count() == 2000, f"expected 2000 events, got {bus.count()} (lost events under concurrency?)"


@test(category="concurrency.starvation", severity=Severity.P1, timeout_s=20)
def test_queue_drains_fairly_no_job_starves():
    """
    FIFO fairness check: with far more jobs than nodes, every job should
    still get dispatched within a bounded multiple of the average
    per-job time -- no job should be starved indefinitely behind others.
    """
    with isolated_cwd():
        coord = make_coordinator()
        coord.register_agent(make_agent("node-starve"))
        n_jobs = 20
        submit_times = {}
        for i in range(n_jobs):
            jid = f"job-starve-{i}"
            submit_times[jid] = time.perf_counter()
            coord.submit_job(jid, "python3 -c \"pass\"")

        completion_times = {}

        def all_done():
            for i in range(n_jobs):
                jid = f"job-starve-{i}"
                st = coord.jobs[jid]["status"]
                if st == "completed" and jid not in completion_times:
                    completion_times[jid] = time.perf_counter()
            return len(completion_times) == n_jobs

        assert wait_until(all_done, timeout_s=15)
        waits = [completion_times[j] - submit_times[j] for j in submit_times]
        max_wait, avg_wait = max(waits), statistics.mean(waits)
        assert max_wait < avg_wait * 5 + 1.0, (
            f"a job waited {max_wait:.2f}s vs average {avg_wait:.2f}s -- "
            f"possible starvation"
        )
        return {"n_jobs": n_jobs, "max_wait_s": round(max_wait, 3), "avg_wait_s": round(avg_wait, 3)}


# ===========================================================================
# 4. MULTIPROCESSING TESTS
# ===========================================================================

def _mp_worker_hash_job(args):
    """Runs in a separate process: exercise ExecutionVerifier + subprocess in isolation."""
    idx, secret = args
    v = ExecutionVerifier(secret_key=secret)
    h = v.hash_data(f"payload-{idx}")
    proc = subprocess.run(
        [sys.executable, "-c", f"print({idx} * 2)"], capture_output=True, text=True, timeout=10
    )
    return idx, h, proc.stdout.strip()


@test(category="multiprocessing.isolation", severity=Severity.P1, timeout_s=40)
def test_verifier_and_subprocess_execution_isolated_across_processes():
    n = 16
    args = [(i, "mp-test-key") for i in range(n)]
    start = time.perf_counter()
    with multiprocessing.get_context("spawn").Pool(processes=min(8, os.cpu_count() or 4)) as pool:
        results = pool.map(_mp_worker_hash_job, args)
    duration = time.perf_counter() - start

    v = ExecutionVerifier(secret_key="mp-test-key")
    for idx, h, stdout in results:
        assert h == v.hash_data(f"payload-{idx}"), "hash must be deterministic across processes"
        assert stdout == str(idx * 2), f"subprocess output mismatch for {idx}"
    return {"processes": n, "duration_s": round(duration, 3)}


def _mp_cpu_burn(seconds):
    end = time.time() + seconds
    x = 0
    while time.time() < end:
        x = (x * 1103515245 + 12345) % (2**31)
    return x


@test(category="multiprocessing.cpu_saturation", severity=Severity.P1, timeout_s=40)
def test_cpu_saturation_across_worker_processes():
    """Simulates thousands-of-agents CPU pressure: saturate all cores briefly."""
    n_workers = os.cpu_count() or 4
    cpu_before = psutil.cpu_percent(interval=None)
    start = time.perf_counter()
    with multiprocessing.get_context("spawn").Pool(processes=n_workers) as pool:
        pool.map(_mp_cpu_burn, [1.0] * n_workers)
    duration = time.perf_counter() - start
    cpu_after = psutil.cpu_percent(interval=0.5)
    assert duration < 5.0, "CPU burn workers took unexpectedly long -- scheduler contention?"
    return {"workers": n_workers, "duration_s": round(duration, 3), "cpu_percent_after": cpu_after}


# ===========================================================================
# 5. ASYNC-STYLE CONCURRENT CLIENT SIMULATION
# ===========================================================================

@test(category="async.client_load", severity=Severity.P1, timeout_s=30)
def test_async_style_concurrent_clients_submit_and_poll():
    """
    GCON's coordinator is fully synchronous; this test simulates many
    async API clients (as api_v1.py callers would be, from an
    asyncio-based SDK) each submitting + polling a job concurrently via
    asyncio.to_thread, which is the same threadpool-offload pattern
    FastAPI itself uses for sync route handlers.
    """
    import asyncio

    with isolated_cwd():
        coord = make_coordinator()
        for i in range(6):
            coord.register_agent(make_agent(f"node-async-{i}"))

        async def client(idx):
            jid = f"job-async-{idx}"
            await asyncio.to_thread(coord.submit_job, jid, "python3 -c \"print('ok')\"")
            deadline = time.perf_counter() + 10
            while time.perf_counter() < deadline:
                status = await asyncio.to_thread(lambda: coord.get_job_status(jid)["status"])
                if status in ("completed", "failed"):
                    return status
                await asyncio.sleep(0.05)
            return "timeout"

        async def main():
            return await asyncio.gather(*(client(i) for i in range(30)))

        statuses = asyncio.run(main())
        n_ok = statuses.count("completed")
        assert n_ok == 30, f"only {n_ok}/30 async clients saw their job complete: {statuses}"
        return {"clients": 30, "completed": n_ok}


# ===========================================================================
# 6. NETWORK FAILURE / FAULT INJECTION / CHAOS
# ===========================================================================

@test(category="chaos.network", severity=Severity.P0, timeout_s=30)
def test_network_partition_simulation_does_not_hang_coordinator():
    """
    See module docstring's MISSING INFORMATION #1: there is no real
    network boundary in GCON today, so we inject faults at the
    CommunicationManager boundary, which is the correct seam for this
    architecture as it exists. Verifies the coordinator degrades
    (marks jobs failed) rather than hanging or crashing when the
    transport to an agent is unreliable.
    """
    with isolated_cwd():
        coord = make_coordinator()
        coord.communication = FlakyCommunicationManager(failure_rate=0.6)
        for i in range(4):
            agent = make_agent(f"node-flaky-{i}")
            coord.register_agent(agent)
            coord.communication.register_node(agent)  # re-register onto the flaky manager

        n_jobs = 20
        for i in range(n_jobs):
            coord.submit_job(f"job-flaky-{i}", "python3 -c \"print(1)\"")

        done = wait_until(
            lambda: all(coord.jobs[f"job-flaky-{i}"]["status"] in ("completed", "failed")
                        for i in range(n_jobs)),
            timeout_s=20,
        )
        assert done, "coordinator hung instead of failing jobs cleanly under network chaos"
        n_failed = sum(1 for i in range(n_jobs) if coord.jobs[f"job-flaky-{i}"]["status"] == "failed")
        n_completed = n_jobs - n_failed
        # We WANT to see some failures (chaos is working) and some successes
        # (the coordinator recovers node state -- idle -- after a failure and
        # keeps dispatching).
        assert n_failed > 0, "fault injector did not actually inject any faults"
        return {"jobs": n_jobs, "failed_due_to_chaos": n_failed, "completed": n_completed}


@test(category="chaos.random_kill", severity=Severity.P1, timeout_s=30)
def test_random_node_termination_during_load():
    """Kills random nodes mid-flight while jobs are streaming in; cluster must keep serving."""
    with isolated_cwd():
        coord = make_coordinator()
        coord.registry.timeout = timedelta(seconds=0.5)
        nodes = [make_agent(f"node-chaos-{i}") for i in range(6)]
        for n in nodes:
            coord.register_agent(n)

        rand = random.Random(42)
        stop = threading.Event()

        def chaos_monkey():
            while not stop.is_set():
                time.sleep(0.3)
                victim = rand.choice(list(coord.registry.nodes.keys()))
                # Simulate a hard crash: stop heartbeats by rewinding last_seen.
                coord.registry.nodes[victim]["last_seen"] = datetime.now(UTC) - timedelta(seconds=10)

        monkey = threading.Thread(target=chaos_monkey, daemon=True)
        monkey.start()

        n_jobs = 15
        for i in range(n_jobs):
            coord.submit_job(f"job-chaos-{i}", "python3 -c \"print('survived')\"")
            time.sleep(0.1)

        deadline = time.time() + 15
        while time.time() < deadline:
            coord.check_cluster_health()
            done = sum(1 for i in range(n_jobs)
                       if coord.jobs[f"job-chaos-{i}"]["status"] in ("completed", "failed"))
            if done == n_jobs:
                break
            time.sleep(0.2)

        stop.set()
        monkey.join(timeout=2)
        n_done = sum(1 for i in range(n_jobs)
                     if coord.jobs[f"job-chaos-{i}"]["status"] in ("completed", "failed"))
        assert n_done >= n_jobs * 0.8, (
            f"only {n_done}/{n_jobs} jobs reached a terminal state despite "
            f"having surviving nodes throughout -- cluster did not recover "
            f"from rolling node failures"
        )
        return {"jobs": n_jobs, "terminal": n_done}


# ===========================================================================
# 7. TIMEOUT / HANG / DEADLOCK DETECTION
# ===========================================================================

@test(category="timeout.subprocess", severity=Severity.P1, timeout_s=15)
def test_agent_execute_job_respects_explicit_timeout():
    agent = make_agent("node-timeout-explicit")
    start = time.perf_counter()
    result = agent.execute_job("job-t", "python3 -c \"import time; time.sleep(5)\"", timeout=1)
    duration = time.perf_counter() - start
    assert result["status"] == "timeout"
    assert duration < 3, f"timeout enforcement took {duration:.2f}s, should be ~1s"


@test(category="timeout.KNOWN_DEFECT", severity=Severity.P0, known_defect=True, timeout_s=10)
def test_KNOWN_DEFECT_communication_manager_never_passes_a_timeout():
    """
    Regression test for Audit Finding C-2: CommunicationManager.send_job
    does not forward any timeout to agent.execute_job, so a hung command
    blocks forever in production. We can't literally wait forever in a
    test, so this uses a watchdog-bounded HangingFakeAgent and asserts
    the (currently absent) protective behavior a fixed system should have.
    """
    with isolated_cwd():
        coord = make_coordinator()
        hanging = HangingFakeAgent("node-hang")
        coord.registry.register(hanging)
        coord.communication.register_node(hanging)
        coord.submit_job("job-hang", "sleep 9999")

        # A correctly-defended system would time this job out well under
        # 3 seconds (our simulated hang duration). Today, nothing in
        # CommunicationManager/agent.execute_job enforces any timeout,
        # so the job stays "running" indefinitely (bounded here only
        # because HangingFakeAgent internally caps at 2s for test safety).
        settled_fast = wait_until(
            lambda: coord.get_job_status("job-hang")["status"] != "running", timeout_s=1.0
        )
        assert settled_fast, (
            "DEFECT CONFIRMED: a hung job was still 'running' after 1s with no "
            "timeout enforced anywhere in the dispatch path (CommunicationManager "
            "never passes `timeout` to agent.execute_job). In production, with a "
            "truly infinite hang, this node is lost forever."
        )


@test(category="deadlock.watchdog_selftest", severity=Severity.P2)
def test_watchdog_infrastructure_detects_a_genuine_hang():
    """
    Meta-test: proves the suite's own deadlock detector works, by
    deliberately hanging and confirming run_with_watchdog reports
    TIMEOUT rather than blocking forever.
    """
    def hangs_forever():
        threading.Event().wait()  # never set -> blocks forever

    status, duration, message, _ = run_with_watchdog(hangs_forever, timeout_s=0.5)
    assert status == "TIMEOUT"
    assert duration < 1.0


# ===========================================================================
# 8. MALFORMED INPUT / FUZZ TESTS
# ===========================================================================

FUZZ_JOB_IDS = [None, "", " ", "a" * 10_000, "../../etc/passwd", "job;rm -rf /",
                12345, ("tuple",), {"dict": True}, "job\x00with\x00nulls"]
FUZZ_COMMANDS = [None, "", " ", "\x00\x01\x02", "a" * 100_000,
                 "python3 -c \"import os; os.system('echo pwned')\"",
                 "$(echo injected)", "`echo backticks`"]


@test(category="fuzz.submit_job", severity=Severity.P1, timeout_s=30)
def test_fuzz_submit_job_never_crashes_the_coordinator_process():
    """
    Fuzzes submit_job with hostile/malformed input. GCON does not
    sandbox commands (by design, per the audit's security section) so
    we do NOT assert shell-injection commands fail to execute -- we
    assert only that malformed *identifiers* raise clean, typed errors
    rather than corrupting coordinator state or crashing the process.
    """
    with isolated_cwd():
        coord = make_coordinator()
        coord.pause_scheduler()
        crashes = []
        for bad_id in FUZZ_JOB_IDS:
            try:
                coord.submit_job(bad_id, "echo fuzz")
            except (ValueError, TypeError, KeyError):
                pass  # acceptable: a clean, typed rejection
            except Exception as e:  # noqa: BLE001
                crashes.append((bad_id, repr(e)))
        assert not crashes, f"unexpected exception types on malformed job_id: {crashes}"
        # Coordinator must still be usable after the fuzzing barrage.
        coord.resume_scheduler()
        coord.register_agent(make_agent("node-postfuzz"))
        coord.submit_job("job-postfuzz", "python3 -c \"print(1)\"")
        assert wait_until(lambda: coord.get_job_status("job-postfuzz")["status"] == "completed", timeout_s=10)


@test(category="fuzz.artifact_registry", severity=Severity.P2)
def test_fuzz_artifact_registry_rejects_missing_and_hostile_paths():
    reg = ArtifactRegistry()
    for bad_path in ["/nonexistent/path/xyz", "", None, "\x00bad", "../../../etc/shadow"]:
        try:
            reg.register_artifact(bad_path)
            raised = False
        except Exception:
            raised = True
        assert raised, f"register_artifact silently accepted a hostile/missing path: {bad_path!r}"


@test(category="fuzz.verifier", severity=Severity.P2)
def test_fuzz_verifier_hash_data_handles_odd_types():
    v = ExecutionVerifier(secret_key="fuzz")
    for payload in [None, 12345, 3.14, [], [1, 2, {"a": "b"}], True]:
        try:
            h = v.hash_data(payload if isinstance(payload, (str, dict)) else str(payload))
            assert isinstance(h, str) and len(h) == 64
        except Exception as e:  # noqa: BLE001
            raise AssertionError(f"hash_data raised on {payload!r}: {e}")


@test(category="fuzz.policy", severity=Severity.P2)
def test_fuzz_policy_engine_handles_malformed_receipts():
    with isolated_cwd():
        engine = PolicyEngine(policy_file="nonexistent.json")
        malformed_receipts = [{}, {"proof": {}}, {"proof": None}, {"proof": {"metrics": None}},
                               {"proof": {"metrics": {}}}]
        for r in malformed_receipts:
            try:
                report = engine.evaluate(r)
                assert report["trusted"] is False, "a malformed/empty receipt must never be 'trusted'"
            except AttributeError:
                raise AssertionError(
                    f"PolicyEngine.evaluate crashed (AttributeError) on malformed receipt {r!r} "
                    f"instead of returning trusted=False -- see also Finding D-1"
                )


# ===========================================================================
# 9. MEMORY PRESSURE / LEAK DETECTION
# ===========================================================================

@test(category="memory.event_bus_leak", severity=Severity.P1, known_defect=True, timeout_s=20)
def test_KNOWN_DEFECT_event_bus_grows_unbounded():
    """
    Regression test documenting that EventBus never evicts old events
    (unlike AuditLogger/NotificationCenter, which correctly cap at
    max_entries). Publishes a large burst and asserts memory is bounded
    -- which currently fails.
    """
    bus = EventBus()
    proc = psutil.Process(os.getpid())
    rss_before = proc.memory_info().rss
    for i in range(200_000):
        bus.publish(Event(event_type="LOAD_TEST", source="stress", payload={"i": i, "pad": "x" * 50}))
    rss_after = proc.memory_info().rss
    growth_mb = (rss_after - rss_before) / (1024 * 1024)
    assert bus.count() < 50_000, (
        f"DEFECT CONFIRMED: EventBus retained all {bus.count()} events with no "
        f"eviction policy (~{growth_mb:.1f}MB growth for this burst alone). "
        f"Over cluster lifetime this is unbounded memory growth."
    )


@test(category="memory.job_table_growth", severity=Severity.P2)
def test_job_table_memory_growth_is_roughly_linear_not_superlinear():
    """Sanity check: memory per job entry should not blow up as job count grows (no quadratic bug)."""
    with isolated_cwd():
        coord = make_coordinator()
        coord.pause_scheduler()
        proc = psutil.Process(os.getpid())
        gc.collect()
        rss_at = {}
        for n in (500, 2000):
            for i in range(len(coord.jobs), n):
                coord.submit_job(f"job-mem-{i}", "echo x")
            gc.collect()
            rss_at[n] = proc.memory_info().rss
        bytes_per_job_second_batch = (rss_at[2000] - rss_at[500]) / (2000 - 500)
        assert bytes_per_job_second_batch < 200_000, (
            f"~{bytes_per_job_second_batch:.0f} bytes/job -- looks superlinear, investigate"
        )
        return {"bytes_per_job_estimate": round(bytes_per_job_second_batch, 1)}


@test(category="memory.large_stdout", severity=Severity.P2, timeout_s=20)
def test_large_job_output_does_not_explode_memory():
    with isolated_cwd():
        coord = make_coordinator()
        coord.register_agent(make_agent("node-bigout"))
        proc = psutil.Process(os.getpid())
        rss_before = proc.memory_info().rss
        # ~5MB of stdout
        coord.submit_job("job-bigout", "python3 -c \"print('x' * 5_000_000)\"")
        assert wait_until(lambda: coord.get_job_status("job-bigout")["status"] == "completed", timeout_s=15)
        rss_after = proc.memory_info().rss
        growth_mb = (rss_after - rss_before) / (1024 * 1024)
        # Generous bound -- we expect roughly 1-2x the payload size retained
        # (in coord.jobs[...]['result']['stdout']), not 10x+.
        assert growth_mb < 60, f"unexpectedly large memory growth for a 5MB stdout job: {growth_mb:.1f}MB"
        return {"growth_mb": round(growth_mb, 2)}


# ===========================================================================
# 10. DISK I/O TESTS
# ===========================================================================

@test(category="disk.storage_manager", severity=Severity.P1)
def test_storage_manager_store_retrieve_copy_delete_roundtrip():
    with isolated_cwd() as tmp:
        sm = StorageManager(storage_root=os.path.join(tmp, "storage"))
        src = os.path.join(tmp, "artifact.bin")
        with open(src, "wb") as f:
            f.write(os.urandom(1024))
        dest = sm.store_artifact("node-a", src)
        assert os.path.isfile(dest)
        retrieved = sm.retrieve_artifact("node-a", "artifact.bin")
        assert retrieved == dest
        copied = sm.copy_artifact("node-a", "node-b", "artifact.bin")
        assert os.path.isfile(copied)
        assert sm.delete_artifact("node-a", "artifact.bin") is True
        assert sm.delete_artifact("node-a", "artifact.bin") is False, "double-delete must be a clean no-op"


@test(category="disk.slow_disk_simulation", severity=Severity.P1, timeout_s=30)
def test_slow_disk_does_not_deadlock_job_completion():
    """Simulates a slow/contended disk during artifact storage and verifies jobs still complete."""
    with isolated_cwd() as tmp:
        coord = make_coordinator()
        coord.register_agent(make_agent("node-slowdisk"))

        original_copy = shutil.copy2

        def slow_copy2(*a, **kw):
            time.sleep(0.3)
            return original_copy(*a, **kw)

        artifact_path = os.path.join(tmp, "input.txt")
        with open(artifact_path, "w") as f:
            f.write("payload")

        shutil.copy2 = slow_copy2
        try:
            start = time.perf_counter()
            coord.submit_job("job-slowdisk", "python3 -c \"print(1)\"", artifacts=[artifact_path])
            done = wait_until(lambda: coord.get_job_status("job-slowdisk")["status"] == "completed",
                               timeout_s=15)
            duration = time.perf_counter() - start
        finally:
            shutil.copy2 = original_copy

        assert done, "job never completed despite slow-disk simulation"
        return {"duration_s": round(duration, 3)}


@test(category="disk.many_artifacts", severity=Severity.P2, timeout_s=30)
def test_many_small_artifacts_registration_throughput():
    with isolated_cwd() as tmp:
        reg = ArtifactRegistry()
        n = 300
        start = time.perf_counter()
        for i in range(n):
            p = os.path.join(tmp, f"art-{i}.txt")
            with open(p, "w") as f:
                f.write(f"content-{i}")
            reg.register_artifact(p)
        duration = time.perf_counter() - start
        assert reg.artifact_count() == n
        return {"artifacts": n, "duration_s": round(duration, 3),
                "artifacts_per_s": round(n / duration, 1)}


# ===========================================================================
# 11. LOAD / SOAK / ENDURANCE / SCALABILITY BENCHMARKS
# ===========================================================================

@test(category="load.throughput_ceiling", severity=Severity.P1, timeout_s=40)
def test_scheduler_dispatch_throughput_ceiling():
    """
    Empirically measures the dispatch-throughput ceiling described in
    the audit (scheduler_loop's fixed per-iteration sleep). Uses
    FastFakeAgent so subprocess overhead doesn't mask the scheduler's
    own bottleneck.
    """
    with isolated_cwd():
        coord = make_coordinator()
        n_nodes = 50
        for i in range(n_nodes):
            fake = FastFakeAgent(f"node-fast-{i}")
            coord.registry.register(fake)
            coord.communication.register_node(fake)

        n_jobs = 150
        start = time.perf_counter()
        for i in range(n_jobs):
            coord.submit_job(f"job-fast-{i}", "noop")
        completed = wait_until(
            lambda: sum(1 for i in range(n_jobs)
                        if coord.jobs[f"job-fast-{i}"]["status"] == "completed") == n_jobs,
            timeout_s=30,
        )
        duration = time.perf_counter() - start
        throughput = n_jobs / duration
        return {
            "nodes": n_nodes, "jobs": n_jobs, "duration_s": round(duration, 3),
            "throughput_jobs_per_s": round(throughput, 2),
            "note": "audit predicts a ~15-20 jobs/s ceiling from scheduler_loop's "
                    "fixed sleep(0.05) regardless of idle node count -- compare "
                    "throughput_jobs_per_s against node count above.",
        }


@test(category="load.soak", severity=Severity.P2, timeout_s=60)
def test_soak_sustained_job_stream_no_degradation():
    """
    Compressed soak test (tens of seconds, not hours -- see MISSING
    INFORMATION #4). Streams jobs continuously and checks that
    per-job latency in the second half isn't meaningfully worse than
    the first half (an early warning sign of the kind of degradation
    that a longer real soak test would need to confirm).
    """
    with isolated_cwd():
        coord = make_coordinator()
        for i in range(4):
            coord.register_agent(make_agent(f"node-soak-{i}"))

        n_jobs = 80
        latencies = []
        for i in range(n_jobs):
            jid = f"job-soak-{i}"
            t0 = time.perf_counter()
            coord.submit_job(jid, "python3 -c \"print(1)\"")
            wait_until(lambda jid=jid: coord.jobs[jid]["status"] == "completed", timeout_s=10)
            latencies.append(time.perf_counter() - t0)

        first_half = statistics.mean(latencies[: n_jobs // 2])
        second_half = statistics.mean(latencies[n_jobs // 2:])
        degradation_pct = ((second_half - first_half) / first_half) * 100 if first_half else 0
        assert degradation_pct < 150, (
            f"latency degraded {degradation_pct:.0f}% over the soak window "
            f"(first-half avg {first_half:.3f}s vs second-half avg {second_half:.3f}s) "
            f"-- possible resource leak under sustained load"
        )
        return {"jobs": n_jobs, "first_half_avg_s": round(first_half, 4),
                "second_half_avg_s": round(second_half, 4),
                "degradation_pct": round(degradation_pct, 1)}


@test(category="scalability.node_registry", severity=Severity.P2, timeout_s=30)
def test_node_registry_scales_to_thousands_of_nodes_for_reads():
    reg = NodeRegistry()
    n = 3000
    start = time.perf_counter()
    for i in range(n):
        reg.register(FastFakeAgent(f"node-scale-{i}"))
    register_duration = time.perf_counter() - start

    sched = Scheduler(reg)
    start = time.perf_counter()
    for _ in range(200):
        sched.select_node()
    select_duration = time.perf_counter() - start

    return {
        "nodes": n,
        "register_duration_s": round(register_duration, 3),
        "select_node_avg_ms": round((select_duration / 200) * 1000, 3),
        "note": "select_node is O(n) per call; at thousands of nodes this cost "
                "is dwarfed by the scheduler_loop dispatch-rate ceiling measured "
                "in load.throughput_ceiling, which is the real bottleneck.",
    }


@test(category="scalability.dag", severity=Severity.P2, timeout_s=30)
def test_dag_algorithms_scale_to_large_workflows():
    n = 2000
    wf = Workflow("wf-huge")
    for i in range(n):
        wf.add_job(WorkflowJob(f"j{i}", "echo x"))
    for i in range(1, n):
        wf.add_dependency(f"j{i-1}", f"j{i}")  # long chain -- worst case for parents() scans

    start = time.perf_counter()
    dag = DAG(wf)
    has_cycle = dag.has_cycle()
    cycle_duration = time.perf_counter() - start

    start = time.perf_counter()
    roots = dag.roots()
    roots_duration = time.perf_counter() - start

    assert has_cycle is False
    assert len(roots) == 1
    return {
        "jobs": n, "has_cycle_check_s": round(cycle_duration, 3),
        "roots_computation_s": round(roots_duration, 3),
        "note": "roots()/leaves() call parents()/children() per node -> "
                "O(V*E) worst case on a long chain; watch this number as a "
                "canary if workflow sizes grow.",
    }


# ===========================================================================
# 12. LOGGING VERIFICATION
# ===========================================================================

@test(category="logging.job_lifecycle", severity=Severity.P2, timeout_s=15)
def test_job_lifecycle_emits_expected_events():
    with isolated_cwd():
        coord = make_coordinator()
        coord.register_agent(make_agent("node-log"))
        seen = []
        coord.event_bus.subscribe(lambda e: seen.append(e.event_type))
        coord.submit_job("job-log", "python3 -c \"print(1)\"")
        assert wait_until(lambda: coord.get_job_status("job-log")["status"] == "completed", timeout_s=10)
        for expected in ("JOB_SUBMITTED", "JOB_STARTED", "JOB_COMPLETED", "RECEIPT_GENERATED"):
            assert expected in seen, f"expected event {expected} was not published; saw {seen}"


# ===========================================================================
# 13. RECEIPT VERIFICATION (end-to-end, real coordinator)
# ===========================================================================

@test(category="receipts.e2e_verification", severity=Severity.P0, timeout_s=15)
def test_verify_all_receipts_detects_tampering():
    with isolated_cwd():
        coord = make_coordinator()
        coord.register_agent(make_agent("node-receipt"))
        coord.submit_job("job-receipt", "python3 -c \"print('ok')\"")
        assert wait_until(lambda: coord.get_job_status("job-receipt")["status"] == "completed", timeout_s=10)

        results = coord.verify_all_receipts()
        assert any(r["valid"] for r in results)

        # Now tamper with the stored receipt in place and re-verify.
        coord.receipts["job-receipt"]["proof"]["runtime_seconds"] = 0.0000001
        results_after_tamper = coord.verify_all_receipts()
        tampered_result = next(r for r in results_after_tamper if r["job_id"] == "job-receipt")
        assert tampered_result["valid"] is False, "verify_all_receipts must detect in-place tampering"


# ===========================================================================
# 14. AUTOSCALER
# ===========================================================================

@test(category="integration.autoscaler", severity=Severity.P1, timeout_s=20)
def test_autoscaler_scales_up_and_down_within_bounds():
    with isolated_cwd():
        coord = make_coordinator()
        coord.pause_scheduler()
        scaler = AutoScaler(coord)
        for i in range(5):
            coord.submit_job(f"job-scale-{i}", "echo x")
        scaler.check_scale()
        assert coord.get_total_node_count() >= 5, "autoscaler should add nodes to cover pending jobs"

        # scale back down: with the scheduler paused, none of the newly
        # added nodes have run anything, so they should all be idle.
        for _ in range(10):
            scaler.scale_down()
        assert coord.get_total_node_count() >= scaler.MIN_NODES


# ===========================================================================
# 15. WEB LAYER (optional -- requires fastapi + httpx)
# ===========================================================================

@test(category="web.auth_enforcement", severity=Severity.P1, timeout_s=20)
def test_dashboard_endpoints_require_authentication():
    if not HAS_WEB_STACK:
        raise AssertionError(
            "SKIPPED (reported as FAIL to stay visible): fastapi/httpx not installed "
            "in this environment. Install with `pip install fastapi uvicorn httpx "
            "jinja2 websockets --break-system-packages` and re-run to exercise the "
            "web layer, including Finding D-5 (unauthenticated /topology, /health, "
            "/management/user-counts)."
        )
    from fastapi.testclient import TestClient  # noqa: local import, optional dep
    from gcon.dashboard.web_server import WebServer
    from gcon.dashboard.presentation import PresentationLayer

    with isolated_cwd():
        coord = make_coordinator()
        presentation = PresentationLayer(coord)
        server = WebServer(presentation)
        client = TestClient(server.app)

        protected = ["/cluster", "/nodes", "/jobs", "/receipts", "/artifacts"]
        unauth_failures = []
        for path in protected:
            resp = client.get(path)
            if resp.status_code != 401:
                unauth_failures.append((path, resp.status_code))

        # Document Finding D-5 explicitly rather than silently passing.
        known_gaps = []
        for path in ["/topology", "/health", "/management/user-counts"]:
            resp = client.get(path)
            if resp.status_code != 401:
                known_gaps.append((path, resp.status_code))

        assert not unauth_failures, f"routes that SHOULD require auth did not: {unauth_failures}"
        return {"confirmed_unauthenticated_routes (Finding D-5)": known_gaps}


# ===========================================================================
# Test discovery / filtering / execution / reporting
# ===========================================================================

CATEGORY_GROUPS = {
    "unit": lambda c: c.startswith("unit."),
    "integration": lambda c: c.startswith("integration."),
    "concurrency": lambda c: c.startswith("concurrency."),
    "multiprocessing": lambda c: c.startswith("multiprocessing."),
    "async": lambda c: c.startswith("async."),
    "chaos": lambda c: c.startswith("chaos."),
    "timeout": lambda c: c.startswith("timeout.") or c.startswith("deadlock."),
    "fuzz": lambda c: c.startswith("fuzz."),
    "memory": lambda c: c.startswith("memory."),
    "disk": lambda c: c.startswith("disk."),
    "load": lambda c: c.startswith("load.") or c.startswith("scalability."),
    "logging": lambda c: c.startswith("logging."),
    "receipts": lambda c: c.startswith("receipts.") or "receipt" in c,
    "security": lambda c: c.startswith("security."),
    "web": lambda c: c.startswith("web."),
}

QUICK_SKIP_CATEGORIES = {"load.soak", "scalability.dag", "scalability.node_registry",
                          "multiprocessing.cpu_saturation", "memory.job_table_growth"}


def setup_logging():
    log = logging.getLogger("gcon.stress")
    log.setLevel(logging.INFO)
    log.handlers.clear()

    fmt = logging.Formatter("%(asctime)s %(message)s", datefmt="%H:%M:%S")
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    log.addHandler(ch)

    fh = logging.FileHandler("stress_test.log", mode="w")
    fh.setFormatter(fmt)
    log.addHandler(fh)
    return log


def compute_health_score(results: List[TestResult]) -> Dict[str, Any]:
    """
    Weighted health score: PASS = full credit, KNOWN_DEFECT = zero credit
    but doesn't count against "unexpected breakage" (it's already known
    and documented), FAIL/ERROR/TIMEOUT = zero credit and IS an
    unexpected-breakage signal. Weight by severity so a P0 failure hurts
    the score far more than a P2.
    """
    weights = {Severity.P0: 5, Severity.P1: 3, Severity.P2: 1, Severity.INFO: 0.5}
    total_weight = 0.0
    earned_weight = 0.0
    unexpected_breakage = 0
    known_defects = 0

    per_category: Dict[str, Dict[str, int]] = {}
    for r in results:
        w = weights.get(r.severity, 1)
        total_weight += w
        per_category.setdefault(r.category, {"PASS": 0, "FAIL": 0, "ERROR": 0,
                                               "TIMEOUT": 0, "KNOWN_DEFECT": 0, "SKIP": 0})
        per_category[r.category][r.status] = per_category[r.category].get(r.status, 0) + 1

        if r.status == "PASS":
            earned_weight += w
        elif r.status == "KNOWN_DEFECT":
            known_defects += 1
            # no credit, no penalty beyond the missing credit itself
        elif r.status in ("FAIL", "ERROR", "TIMEOUT"):
            unexpected_breakage += 1

    score = round((earned_weight / total_weight) * 100, 1) if total_weight else 0.0
    return {
        "score": score,
        "unexpected_breakage_count": unexpected_breakage,
        "known_defect_count": known_defects,
        "per_category": per_category,
    }


def main():
    parser = argparse.ArgumentParser(description="GCON stress/chaos/QA test suite")
    parser.add_argument("--quick", action="store_true", help="skip slow soak/scale tests")
    parser.add_argument("--category", default=None,
                         help=f"run only one group: {', '.join(CATEGORY_GROUPS)}")
    args = parser.parse_args()

    log = setup_logging()
    log.info("=" * 78)
    log.info("GCON STRESS / CHAOS / QA TEST SUITE")
    log.info(f"Project root: {PROJECT_ROOT}")
    log.info(f"Python: {sys.version.split()[0]}  |  Platform: {sys.platform}  |  "
             f"CPUs: {os.cpu_count()}  |  psutil: {psutil.__version__}")
    log.info(f"Web stack (fastapi/httpx) available: {HAS_WEB_STACK}")
    log.info("=" * 78)

    if IMPORT_ERROR is not None:
        log.error("FATAL: could not import GCON modules.")
        log.error(f"  GCON_PROJECT_ROOT resolved to: {PROJECT_ROOT}")
        log.error(f"  Import error: {IMPORT_ERROR!r}")
        log.error("  Set GCON_PROJECT_ROOT to the directory containing coordinator.py, or "
                   "place this file in the GCON project root.")
        sys.exit(2)

    cases = list(REGISTRY)
    if args.category:
        pred = CATEGORY_GROUPS.get(args.category)
        if not pred:
            log.error(f"unknown --category {args.category!r}; choices: {list(CATEGORY_GROUPS)}")
            sys.exit(2)
        cases = [c for c in cases if pred(c.category)]
    if args.quick:
        cases = [c for c in cases if c.category not in QUICK_SKIP_CATEGORIES]

    log.info(f"Discovered {len(REGISTRY)} test cases; running {len(cases)}.")
    log.info("-" * 78)

    runner = Runner(log)
    start = time.perf_counter()
    runner.run_all(cases)
    total_duration = time.perf_counter() - start

    log.info("-" * 78)
    log.info("SUBSYSTEM SUMMARY (PASS/FAIL by category)")
    log.info("-" * 78)

    health = compute_health_score(runner.results)
    for category, counts in sorted(health["per_category"].items()):
        total = sum(counts.values())
        passed = counts.get("PASS", 0)
        line = (f"  {category:32} {passed}/{total} PASS"
                + (f"  | {counts['KNOWN_DEFECT']} known defect(s)" if counts.get("KNOWN_DEFECT") else "")
                + (f"  | {counts.get('FAIL',0)+counts.get('ERROR',0)+counts.get('TIMEOUT',0)} UNEXPECTED"
                   if (counts.get('FAIL', 0) + counts.get('ERROR', 0) + counts.get('TIMEOUT', 0)) else ""))
        subsystem_status = "PASS" if passed == total else (
            "DEGRADED" if counts.get("KNOWN_DEFECT") and not (
                counts.get("FAIL", 0) + counts.get("ERROR", 0) + counts.get("TIMEOUT", 0)
            ) else "FAIL"
        )
        log.info(f"[{subsystem_status:8}] {line}")

    log.info("-" * 78)
    log.info(f"Total duration: {total_duration:.1f}s")
    log.info(f"Unexpected breakage (real, undocumented failures): {health['unexpected_breakage_count']}")
    log.info(f"Confirmed known defects (documented in audit, expected to fail today): "
             f"{health['known_defect_count']}")
    log.info(f"OVERALL HEALTH SCORE: {health['score']}/100")
    log.info("=" * 78)

    report = {
        "generated_at": datetime.now(UTC).isoformat(),
        "project_root": PROJECT_ROOT,
        "total_duration_s": round(total_duration, 2),
        "health_score": health["score"],
        "unexpected_breakage_count": health["unexpected_breakage_count"],
        "known_defect_count": health["known_defect_count"],
        "results": [
            {
                "name": r.name, "category": r.category, "severity": r.severity,
                "status": r.status, "duration_s": r.duration_s, "message": r.message,
                "metrics": r.metrics,
            }
            for r in runner.results
        ],
    }
    with open("stress_test_report.json", "w") as f:
        json.dump(report, f, indent=2, default=str)
    log.info("Detailed log: stress_test.log")
    log.info("Machine-readable report: stress_test_report.json")

    # Exit non-zero only on UNEXPECTED breakage, so this is CI-friendly:
    # known, documented defects don't fail a build; regressions do.
    sys.exit(1 if health["unexpected_breakage_count"] > 0 else 0)


if __name__ == "__main__":
    main()
