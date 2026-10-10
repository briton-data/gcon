"""
Real multi-process fault injection.

tests/support/chaos.py is explicit about its own limitation (see its
module docstring): GCON currently has no test harness that runs a
worker as a genuinely separate OS process, so "killing a worker" there
means calling agent.cancel()/stop_heartbeat() from inside the SAME
test process -- an approximation of a crash, not one. Similarly,
tests/transport/test_restart_recovery_late_completion.py says outright
that it chose precise, deterministic direct-injection over
"orchestrating two full live coordinator processes."

This file is that missing piece: a real coordinator (in-process object
+ a real GrpcTransport bound to a real TCP socket -- the same
already-real transport every tests/transport/ test uses) and real
*separate* worker OS processes, started with subprocess.Popen running
the actual scripts/run_worker.py entry point a real deployment would
use, killed with a real, uncatchable Popen.kill() (SIGKILL on POSIX,
TerminateProcess on Windows) -- not a
graceful stop.

Sequenced after #1-#4 on purpose (per the task's own ordering): this
is only meaningful once durable job_attempts, retry caps, restart
recovery, and staleness fencing actually exist to prove real recovery
against, rather than proving the old, already-known-broken behavior.
"""
from __future__ import annotations

import json
import os
import signal
import shutil
import socket
import subprocess
import sys
import time
from pathlib import Path

import pytest
import requests

from gcon.cluster.communication import CommunicationManager
from gcon.cluster.coordinator import GCONCoordinator
from gcon.persistence.control_plane import ControlPlane
from gcon.transport import tls
from gcon.transport.config import TransportConfig
from gcon.transport.grpc_transport import GrpcTransport
from gcon.transport.remote_node import RemoteNodeProxy

REPO_ROOT = Path(__file__).resolve().parents[2]


def _free_tcp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("localhost", 0))
        return s.getsockname()[1]


def _wait_until(predicate, timeout=30.0, interval=0.1):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()


class RealCoordinator:
    """
    Same construction as tests/transport/*'s coordinator_over_grpc
    fixture (real GrpcTransport, real socket) -- not reinvented here,
    just wrapped in a small class so this file can spawn real worker
    *processes* against it rather than the in-process `_start_agent`
    test client those files use.
    """

    def __init__(self, tmp_path):
        self.cert_dir = tmp_path / "certs"
        self.cert_dir.mkdir()
        self.control_plane = ControlPlane(path=str(tmp_path / "control_plane.db"))
        self.control_plane.settings.set("grpc_port", str(_free_tcp_port()))
        self.control_plane.settings.set("tls_cert_dir", str(self.cert_dir))
        # Fast heartbeat + a low miss threshold so a real SIGKILL is
        # detected in a few seconds, not the 15s production default
        # (5s interval * 3 miss threshold) -- see coordinator.py's
        # __init__ for how node_timeout_seconds is derived from these.
        self.control_plane.settings.set("heartbeat_interval_seconds", "1")
        self.control_plane.settings.set("heartbeat_miss_threshold", "2")
        config = TransportConfig.load(self.control_plane)
        self.address = f"localhost:{config.grpc_port}"

        self.coordinator = GCONCoordinator(transport=None, control_plane=self.control_plane)

        def on_node_registered(node_id, capabilities, org_id=None, address=None):
            proxy = RemoteNodeProxy(node_id, self.transport, org_id=org_id, address=address)
            self.coordinator.register_agent(proxy)

        def on_heartbeat(node_id, payload):
            import datetime
            self.coordinator.receive_heartbeat({
                "node_id": node_id, "status": payload["status"],
                "timestamp": datetime.datetime.now(datetime.UTC),
            })

        def on_node_disconnected(node_id):
            self.coordinator.on_node_disconnected(node_id)

        self.transport = GrpcTransport(
            control_plane=self.control_plane, config=config,
            on_heartbeat=on_heartbeat, on_node_registered=on_node_registered,
            on_node_disconnected=on_node_disconnected,
        )
        self.coordinator.communication = CommunicationManager(transport=self.transport)
        self.transport.start()  # real grpc.Server bound to a real socket; also ensure_ca()s the cert_dir

    def issue_worker_cert(self, node_id):
        """Pre-provision node_id's cert against the CA transport.start()
        already generated, exactly like tests/transport's _start_agent
        does -- so a real `run_worker.py` subprocess for this node_id
        can connect without going through self-enrollment."""
        tls.issue_agent_cert(str(self.cert_dir), node_id)

    def spawn_real_worker_process(self, node_id, log_path):
        """A genuinely separate OS process -- the actual entry point
        scripts/run_worker.py, not an in-process test double."""
        self.issue_worker_cert(node_id)
        log_file = open(log_path, "w")
        proc = subprocess.Popen(
            [
                sys.executable, "scripts/run_worker.py",
                "--node-id", node_id,
                "--coordinator", self.address,
                "--cert-dir", str(self.cert_dir),
                "--log-level", "INFO",
            ],
            cwd=str(REPO_ROOT),
            stdout=log_file, stderr=subprocess.STDOUT,
        )
        return proc

    def shutdown(self):
        self.transport.shutdown(grace_period=3)
        self.coordinator.shutdown()
        self.control_plane.close()


@pytest.fixture
def real_coordinator(tmp_path):
    rc = RealCoordinator(tmp_path)
    yield rc
    rc.shutdown()


def _kill_process_tree(proc):
    """Best-effort cleanup at teardown -- SIGKILL is a one-way trip so
    a test that already killed its own target process must not try to
    kill it (or wait on it) a second time."""
    if proc.poll() is None:
        proc.kill()
        proc.wait(timeout=5)


@pytest.mark.slow
def test_sigkilling_a_real_worker_process_mid_job_still_completes_via_the_survivor(
    real_coordinator, tmp_path,
):
    """
    Two real `run_worker.py` OS processes. Submit a job that runs long
    enough to still be "running" when we act. Identify which real
    process actually got it, Popen.kill() that process (an
    uncatchable, ungraceful crash -- not proc.terminate()/SIGTERM,
    which AgentDaemon already handles cleanly and isn't the scenario
    #8 is about). Assert the coordinator's real heartbeat-timeout
    detection (NodeRegistry.check_node_health(), real 1s-interval
    heartbeats from the surviving process, real health_check_loop
    tick) notices the dead node, recover_jobs() actually reassigns the
    job, and it completes for real on the survivor -- with a genuine,
    durable (#1) job_attempts row for each dispatch, proving the
    attempt history survived an actual process death, not a
    synthetic in-memory mutation.
    """
    worker_a = real_coordinator.spawn_real_worker_process("worker-mp-a", tmp_path / "worker-a.log")
    worker_b = real_coordinator.spawn_real_worker_process("worker-mp-b", tmp_path / "worker-b.log")
    procs = {"worker-mp-a": worker_a, "worker-mp-b": worker_b}

    try:
        assert _wait_until(
            lambda: len(real_coordinator.coordinator.registry.list_nodes()) == 2, timeout=20
        ), "both real worker processes should register over real mTLS"

        job_id = "mp-fault-job-1"
        # Long enough to still be "running" by the time we've
        # identified + killed the process; short enough the whole
        # test doesn't drag.
        real_coordinator.coordinator.submit_job(job_id, "sleep 4 && echo done")

        assert _wait_until(
            lambda: real_coordinator.coordinator.jobs[job_id]["status"] == "running", timeout=10
        ), "job should have been dispatched to one of the two real workers"

        dispatched_node = real_coordinator.coordinator.jobs[job_id]["node_id"]
        assert dispatched_node in procs
        victim_proc = procs[dispatched_node]
        survivor_node_id = "worker-mp-b" if dispatched_node == "worker-mp-a" else "worker-mp-a"

        # The real, uncatchable kill -- this is the actual thing #8
        # asked for: a genuine OS process death, not
        # agent.cancel()/stop_heartbeat() called from this same
        # process the way chaos.py's kill_worker() does. Popen.kill()
        # rather than os.kill(pid, signal.SIGKILL): SIGKILL doesn't
        # exist in the `signal` module on Windows at all (only on
        # POSIX) -- Popen.kill() is the actual cross-platform way to
        # get the same uncatchable-crash property (SIGKILL on POSIX,
        # TerminateProcess on Windows; neither can be caught or
        # cleanly handled by the target process).
        victim_proc.kill()
        assert _wait_until(lambda: victim_proc.poll() is not None, timeout=5), \
            "the OS should have actually reaped the killed process"

        # Real detection: an offline node stays *listed* (list_nodes()
        # returns every known node id regardless of status -- checked
        # directly against real registry.py rather than assumed), so
        # the real signal to check is its status flipping to
        # "offline", either via the gRPC stream actually dropping
        # (on_node_disconnected) or via health_check_loop's real 3s
        # timeout tick -- whichever fires first against real elapsed
        # wall-clock time, not a test hook.
        assert _wait_until(
            lambda: real_coordinator.coordinator.registry.get_node_info(dispatched_node)["status"]
            == "offline",
            timeout=20,
        ), "coordinator should have marked the real dead process's node offline"

        # Real recovery: recover_jobs() requeues, scheduler_loop
        # redispatches to the one remaining real live process, and
        # that process's own real subprocess actually runs the
        # command and reports back over its own real gRPC stream.
        assert _wait_until(
            lambda: real_coordinator.coordinator.jobs[job_id]["status"] == "completed", timeout=30
        ), "job should complete via the surviving real worker process after real recovery"

        final_job = real_coordinator.coordinator.jobs[job_id]
        assert final_job["node_id"] == survivor_node_id
        assert final_job.get("attempt_number", 1) >= 2, \
            "a real reassignment must show as a second dispatch attempt"

        assert job_id in real_coordinator.coordinator.receipts
        receipt = real_coordinator.coordinator.receipts[job_id]
        is_valid, _ = real_coordinator.coordinator.verifier.validate_proof(receipt["proof"])
        assert is_valid, "the completed job's receipt must carry a genuinely valid signature"

        # #1's durable job_attempts history, over a REAL crash -- this
        # is the thing test_restart_recovery_late_completion.py's own
        # docstring says it deliberately did NOT test, in favor of a
        # precise direct-injection approximation.
        attempts = real_coordinator.coordinator.get_job_attempts(job_id)
        assert len(attempts) >= 2, (
            "expected at least the original dispatch to the killed node plus the "
            f"real reassignment to be durably recorded, got: {attempts}"
        )
        attempted_nodes = {a["node_id"] for a in attempts}
        assert dispatched_node in attempted_nodes
        assert survivor_node_id in attempted_nodes

    finally:
        _kill_process_tree(worker_a)
        _kill_process_tree(worker_b)


@pytest.mark.slow
@pytest.mark.skipif(
    sys.platform == "win32",
    reason=(
        "os.kill(pid, signal.SIGTERM) sent from another process is not a real, "
        "catchable signal delivery on Windows -- CPython's Windows implementation "
        "calls TerminateProcess() directly using the signal number as the exit "
        "code, so the target process's own signal handler never runs at all (this "
        "is exactly why a Windows run of this test observed returncode 15, not a "
        "clean 0: the process was hard-killed, identically to the SIGKILL test "
        "above, not gracefully stopped). There is no POSIX-equivalent way to "
        "exercise run_worker.py's real SIGTERM handler from another process on "
        "Windows without adding new SIGBREAK/CTRL_BREAK_EVENT handling to "
        "run_worker.py itself, which is a real source change, not a test one -- "
        "flagged, not attempted here without being asked."
    ),
)
def test_sigterm_is_a_graceful_stop_not_a_recovery_trigger(real_coordinator, tmp_path):
    """
    Contrast case for the test above: SIGTERM is the signal run_worker.
    py's own docstring says it handles deliberately (calls
    AgentDaemon.stop() with the signal as the reason) -- a real,
    separate OS process asked to stop cleanly should be able to exit
    without the coordinator treating it as an unexpected crash needing
    job recovery, as long as it wasn't mid-job. This is a real process
    exercising a real signal handler, not a mocked shutdown call.

    POSIX only -- see the skipif above for why Windows can't exercise
    this same real-signal-handler path from another process.
    """
    worker = real_coordinator.spawn_real_worker_process("worker-mp-term", tmp_path / "worker-term.log")
    try:
        assert _wait_until(
            lambda: len(real_coordinator.coordinator.registry.list_nodes()) == 1, timeout=20
        )
        os.kill(worker.pid, signal.SIGTERM)
        assert _wait_until(lambda: worker.poll() is not None, timeout=10), \
            "a real SIGTERM should let run_worker.py's own signal handler exit cleanly"
        assert worker.returncode == 0, (
            f"a clean SIGTERM shutdown should exit 0, got {worker.returncode} "
            f"(see {(tmp_path / 'worker-term.log')})"
        )
    finally:
        _kill_process_tree(worker)


# ---------------------------------------------------------------------
# Coordinator-process kill + restart, across a genuine OS process
# boundary -- both the coordinator AND the worker are real,
# independently-started subprocess.Popen processes here (scripts/
# run_coordinator.py and scripts/run_worker.py, the actual production
# entry points), driven entirely over real HTTP + real mTLS, exactly
# how a real deployment would be. This is deliberately the scenario
# tests/transport/test_restart_recovery_late_completion.py's own
# docstring says it chose NOT to build ("precise and deterministic...
# rather than orchestrating two full live coordinator processes") --
# that file still stands (it's a faster, more precise regression
# guard for the fencing logic itself), this is the thing layered on
# top of it: proof the same logic actually survives a real crash and
# a real second process picking up the same on-disk state.
# ---------------------------------------------------------------------

OWNER_EMAIL = "chaos-test-owner@example.com"
OWNER_PASSWORD = "ChaosTest2026!"


def _wait_http_up(url, timeout=20):
    return _wait_until(lambda: _http_ok(url), timeout=timeout, interval=0.25)


def _http_ok(url):
    try:
        requests.get(url, timeout=1)
        return True
    except requests.exceptions.RequestException:
        return False


class RealClusterProcesses:
    """Both the coordinator and its one worker as real, independent
    OS processes talking real mTLS + real HTTP -- see module docstring
    above for why this exists alongside RealCoordinator."""

    def __init__(self, tmp_path):
        self.tmp_path = tmp_path
        self.cert_dir = tmp_path / "certs"
        self.data_dir = tmp_path / "data"
        self.cert_dir.mkdir()
        self.grpc_port = _free_tcp_port()
        self.dashboard_port = _free_tcp_port()
        self.base_url = f"http://127.0.0.1:{self.dashboard_port}"
        self.coordinator_proc = None
        self.worker_proc = None

        subprocess.run(
            [
                sys.executable, "scripts/generate_dev_certs.py",
                "--cert-dir", str(self.cert_dir), "--node", "worker-restart-1",
            ],
            cwd=str(REPO_ROOT), check=True, capture_output=True,
        )

    def _coordinator_env(self):
        env = os.environ.copy()
        env.update({
            "GCON_TLS_CERT_DIR": str(self.cert_dir),
            "GCON_GRPC_HOST": "0.0.0.0",
            "GCON_GRPC_PORT": str(self.grpc_port),
            "GCON_DASHBOARD_HOST": "127.0.0.1",
            "GCON_DASHBOARD_PORT": str(self.dashboard_port),
            "GCON_OWNER_EMAIL": OWNER_EMAIL,
            "GCON_OWNER_PASSWORD": OWNER_PASSWORD,
        })
        return env

    def start_coordinator(self, log_name):
        log_file = open(self.tmp_path / log_name, "w")
        self.coordinator_proc = subprocess.Popen(
            [
                sys.executable, "scripts/run_coordinator.py",
                "--data-dir", str(self.data_dir),
                "--log-level", "INFO",
            ],
            cwd=str(REPO_ROOT), env=self._coordinator_env(),
            stdout=log_file, stderr=subprocess.STDOUT,
        )
        assert _wait_http_up(f"{self.base_url}/login", timeout=20), (
            f"coordinator dashboard/API should come up as a real HTTP server "
            f"(see {self.tmp_path / log_name})"
        )
        return self.coordinator_proc

    def start_worker(self, log_name="worker.log", sandboxed=False):
        log_file = open(self.tmp_path / log_name, "w")
        env = os.environ.copy()
        if sandboxed:
            # Jobs from the public API only run on a worker that has verified
            # its sandbox (run_worker's startup probe), so this one needs Docker.
            env["GCON_EXECUTION_BACKEND"] = "docker"
        self.worker_proc = subprocess.Popen(
            [
                sys.executable, "scripts/run_worker.py",
                "--node-id", "worker-restart-1",
                "--coordinator", f"127.0.0.1:{self.grpc_port}",
                "--cert-dir", str(self.cert_dir),
                "--log-level", "INFO",
            ],
            cwd=str(REPO_ROOT), env=env, stdout=log_file, stderr=subprocess.STDOUT,
        )
        return self.worker_proc

    def put_worker_in_shared_pool(self, node_id, timeout=30):
        """
        The public API only places a customer's job on that customer's own
        workers or on a worker the operator has put in the shared pool. This
        test's worker belongs to no organization, so the operator (the staff
        Owner) flags it -- which only works once the real worker process has
        registered, so this retries until it has.
        """
        session = requests.Session()
        r = session.post(f"{self.base_url}/auth/login",
                          json={"email": OWNER_EMAIL, "password": OWNER_PASSWORD}, timeout=5)
        assert r.status_code == 200, f"real login should succeed: {r.status_code} {r.text}"
        deadline = time.monotonic() + timeout
        last = None
        while time.monotonic() < deadline:
            r = session.put(f"{self.base_url}/management/nodes/{node_id}/shared-pool",
                             json={"enabled": True}, timeout=5)
            if r.status_code == 200:
                return
            last = f"{r.status_code} {r.text}"
            time.sleep(0.5)
        raise AssertionError(
            f"the real worker process should have registered and joined the shared pool; last answer: {last}")

    def signup_customer_and_get_api_key(self):
        # Customer routes refuse a key that belongs to no organization, so the
        # job is submitted the way a real customer would: with a signup key.
        r = requests.post(f"{self.base_url}/api/v1/auth/signup", json={
            "org_name": "Chaos Test Org", "name": "Chaos Customer",
            "email": "customer@chaos.example", "password": "correct-horse-1",
        }, timeout=10)
        assert r.status_code == 200, f"customer signup should succeed: {r.status_code} {r.text}"
        return r.json()["api_key"]["secret"]

    def shutdown(self):
        for proc in (self.worker_proc, self.coordinator_proc):
            if proc is not None:
                _kill_process_tree(proc)


@pytest.fixture
def real_cluster(tmp_path):
    cluster = RealClusterProcesses(tmp_path)
    yield cluster
    cluster.shutdown()


def _docker_daemon_available():
    if shutil.which("docker") is None:
        return False
    try:
        return subprocess.run(["docker", "info"], capture_output=True, timeout=20).returncode == 0
    except (OSError, subprocess.TimeoutExpired):
        return False


@pytest.mark.slow
@pytest.mark.skipif(
    not _docker_daemon_available(),
    reason="submits through the public API, whose jobs only run on a worker that has "
           "verified a Docker sandbox -- needs a reachable Docker daemon",
)
def test_sigkilling_the_real_coordinator_process_and_restarting_resumes_the_job(real_cluster):
    """
    Real coordinator process #1 + real worker process, over real HTTP
    + real mTLS. Submit a job through the real public API. While it's
    genuinely still running (the real worker's own real subprocess
    hasn't finished yet), Popen.kill() the real coordinator
    process -- an actual crash, mid-job, of the whole process, not a
    graceful shutdown and not an in-memory double-__init__ the way
    existing restart-recovery unit tests simulate it. Then start a
    genuinely NEW `run_coordinator.py` OS process (new PID) pointed at
    the same on-disk --data-dir, and confirm restart recovery (#3)
    actually redispatches the job for real and it completes, visible
    entirely through the real HTTP API of the new process -- proving
    the durable state (#1's job_attempts, control_plane.jobs) really
    survived the process boundary, not just survived within one
    Python process's memory.
    """
    api_headers = None

    real_cluster.start_coordinator("coordinator-1.log")
    real_cluster.start_worker(sandboxed=True)
    # The real worker process registers with the real coordinator, then the
    # operator puts it in the shared pool so a customer's job can run on it.
    real_cluster.put_worker_in_shared_pool("worker-restart-1")
    secret = real_cluster.signup_customer_and_get_api_key()
    api_headers = {"Authorization": f"Bearer {secret}"}

    r = requests.post(f"{real_cluster.base_url}/api/v1/jobs", headers=api_headers,
                       json={"client_reference": "mp-restart-job-1", "command": "sleep 6 && echo done"}, timeout=5)
    assert r.status_code == 200, r.text
    job_id = r.json()["job_id"]            # minted by GCON

    def _job_status():
        r = requests.get(f"{real_cluster.base_url}/api/v1/jobs/{job_id}",
                          headers=api_headers, timeout=2)
        return r.json().get("status") if r.status_code == 200 else None

    assert _wait_until(lambda: _job_status() == "running", timeout=10), \
        "job should be genuinely running on the real worker process before we crash the coordinator"

    # The real, uncatchable kill of the whole coordinator OS process --
    # gRPC server, dashboard/API server, everything -- mid-job.
    # Popen.kill(), not os.kill(pid, signal.SIGKILL): SIGKILL isn't in
    # the `signal` module on Windows.
    old_pid = real_cluster.coordinator_proc.pid
    real_cluster.coordinator_proc.kill()
    assert _wait_until(lambda: real_cluster.coordinator_proc.poll() is not None, timeout=5), \
        "the OS should have actually reaped the killed coordinator process"
    assert not _http_ok(f"{real_cluster.base_url}/login"), \
        "the real HTTP server should genuinely be gone, not just unresponsive for a moment"

    # A genuinely new OS process (new PID), pointed at the exact same
    # on-disk --data-dir the dead one was using.
    real_cluster.start_coordinator("coordinator-2.log")
    assert real_cluster.coordinator_proc.pid != old_pid

    # AgentDaemon's own real reconnect-with-backoff (agent_daemon.py)
    # has to notice the stream broke and re-register with the new
    # process on its own; restore_from_persistence() + restart
    # recovery has to actually requeue+redispatch the job that was
    # "running" in the durable DB when the old process died.
    assert _wait_until(lambda: _job_status() == "completed", timeout=40), (
        f"job should complete via real restart recovery against the new coordinator "
        f"process (see {real_cluster.tmp_path / 'coordinator-2.log'} and "
        f"{real_cluster.tmp_path / 'worker.log'})"
    )

    r = requests.get(f"{real_cluster.base_url}/api/v1/jobs/{job_id}/attempts", headers=api_headers, timeout=5)
    assert r.status_code == 200, r.text
    attempts = r.json()
    assert len(attempts) >= 2, (
        "restart recovery should have durably recorded a real second dispatch attempt, "
        f"got: {attempts}"
    )
