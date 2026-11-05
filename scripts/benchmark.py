"""
benchmark.py — throughput, latency, CPU/RAM, and scalability
measurements for GCON.

Produces a JSON report (benchmark_report.json by default) intended to
be run before/after any fix from AUDIT_REPORT.md to quantify the
before/after impact — e.g. run once against today's polling
scheduler_loop (AUDIT_REPORT.md 2.4) and again after switching it to a
condition-variable design, and diff the two reports' throughput and
lock-contention numbers.

Run directly:
    python3 benchmark.py --nodes 20 --jobs 500 --out benchmark_report.json

What it covers, and what it does not, is printed as a matrix at the end of every run (see COVERAGE below). This file
measures the platform IN-PROCESS: scheduler, durable control plane, workflows, the dashboard's worker/health views
as the fleet grows, and the real gRPC fleet transport. The public API and the staff dashboard under HTTP and
websocket load are measured by benchmarks/benchmark.py.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import tempfile
import time
from datetime import datetime, UTC
from typing import Dict, List

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from tests.support.test_utils import (
    MetricsCollector, ResourceSampler, assert_all_jobs_terminal,
    get_logger, run_concurrently, unique_id,
)

logger = get_logger("benchmark", log_file="logs/benchmark.log")


# What the benchmarks measure, so a platform area cannot quietly go unmeasured. "benchmark" is the key in the report
# (or the other file); "not covered" areas say why, and are printed with the rest.
COVERAGE = [
    ("Scheduler, dispatch, execution, receipts (in memory)", "job_throughput, dispatch_latency, concurrent_submission, scalability_curve, resource_usage"),
    ("Durable control plane (SQLite): jobs, attempts, receipts as production writes them", "durable_throughput"),
    ("Workflows (DAG fan-out and join)", "workflows"),
    ("Staff dashboard cost as the fleet grows: worker overview, health, node summary, the live update every viewer gets", "observability_cost"),
    ("gRPC fleet transport with real TLS: enrollment, registration, control-stream heartbeats", "transport"),
    ("Public API: every /api/v1 route, auth, billing, leases, Idempotency-Key, tenants under load", "benchmarks/benchmark.py"),
    ("Staff dashboard under load: every read-only page and /management route, live websocket viewers, worker view consistency", "benchmarks/benchmark.py"),
    ("NOT COVERED: webhook and callback_url delivery", "needs a public https endpoint to receive it; the safety checks refuse a local one on purpose"),
    ("NOT COVERED: GCON-hosted workers running real sandboxed jobs in Docker", "needs Docker and the worker image; leases are exercised, execution in the sandbox is not"),
    ("NOT COVERED: several coordinators (HA lease, failover)", "needs two coordinators over one shared database"),
]


def print_coverage():
    print("\nCOVERAGE (what these benchmarks measure; the rest of the platform is listed as NOT COVERED, with why)")
    for area, how in COVERAGE:
        print(f"  {area}\n      -> {how}")


def _build_cluster(node_count: int, durable_dir: str = None):
    """
    Build a real GCONCoordinator with `node_count` real GCONAgent
    "nodes" registered against it, exactly as the production entry
    points (run_job.py, examples/*.py) do. No mocking of coordinator
    internals — this measures the actual code path.
    """
    from gcon.cluster.coordinator import GCONCoordinator
    from gcon.execution.agent import GCONAgent

    # The coordinator refuses to run ANY job on an unsandboxed worker unless told the workers are the operator's own
    # (GCON_SANDBOX_POLICY=trusted), and these in-process agents are unsandboxed. Without this nothing is dispatched
    # and every benchmark below waits until it times out. The policy is read when the coordinator is built.
    os.environ.setdefault("GCON_SANDBOX_POLICY", "trusted")

    if durable_dir:
        # What production runs: a coordinator over a real (SQLite) control plane, not the bare in-memory one.
        from gcon.persistence.control_plane import ControlPlane
        coordinator = GCONCoordinator(control_plane=ControlPlane(path=os.path.join(durable_dir, "control_plane.db")))
    else:
        coordinator = GCONCoordinator()
    agents = []
    for i in range(node_count):
        agent = GCONAgent(node_id=f"bench-node-{i:04d}")
        coordinator.register_agent(agent)
        agents.append(agent)
    return coordinator, agents


class BenchmarkSuite:
    def __init__(self, node_count: int = 10):
        self.node_count = node_count
        self.results: Dict[str, Dict] = {}

    # -----------------------------------------------------------------
    # Throughput: how many trivial jobs/sec can the coordinator dispatch
    # and complete, end-to-end, through the real scheduler_loop?
    # -----------------------------------------------------------------

    def bench_job_throughput(self, job_count: int = 200,
                              command: str = "python3 -c \"pass\"") -> Dict:
        coordinator, agents = _build_cluster(self.node_count)
        metrics = MetricsCollector()
        job_ids = [unique_id("bench-throughput") for _ in range(job_count)]

        t0 = time.monotonic()
        with metrics.timer("submit_all"):
            for jid in job_ids:
                coordinator.submit_job(jid, command)

        with metrics.timer("drain_all"):
            statuses = assert_all_jobs_terminal(
                coordinator, job_ids, timeout=max(30.0, job_count * 0.5)
            )
        wall = time.monotonic() - t0

        completed = sum(1 for s in statuses.values() if s == "completed")
        failed = sum(1 for s in statuses.values() if s == "failed")

        result = {
            "job_count": job_count,
            "node_count": self.node_count,
            "wall_seconds": round(wall, 3),
            "jobs_per_second": round(job_count / wall, 3) if wall > 0 else 0.0,
            "completed": completed,
            "failed": failed,
            **metrics.summary(),
        }
        self.results["job_throughput"] = result
        logger.info(f"[BENCH] job_throughput: {result['jobs_per_second']} jobs/sec "
                    f"({completed}/{job_count} completed)")
        return result

    # -----------------------------------------------------------------
    # Latency: single-job dispatch latency distribution under a quiet
    # (uncontended) cluster — isolates per-job overhead from queueing.
    # -----------------------------------------------------------------

    def bench_dispatch_latency(self, sample_count: int = 100) -> Dict:
        coordinator, agents = _build_cluster(self.node_count)
        metrics = MetricsCollector()

        for _ in range(sample_count):
            jid = unique_id("bench-latency")
            with metrics.timer("dispatch_to_running"):
                coordinator.submit_job(jid, "python3 -c \"pass\"")
                from tests.support.test_utils import assert_eventually
                assert_eventually(
                    lambda: coordinator.jobs[jid]["status"] in
                            ("running", "completed", "failed"),
                    timeout=10.0,
                    description=f"job {jid} to leave 'pending'",
                )
            assert_all_jobs_terminal(coordinator, [jid], timeout=10.0)

        result = {"sample_count": sample_count, "node_count": self.node_count,
                  **metrics.summary()}
        self.results["dispatch_latency"] = result
        p50 = result["latency_seconds"]["median"] if result["latency_seconds"] else None
        logger.info(f"[BENCH] dispatch_latency: p50={p50}s over {sample_count} samples")
        return result

    # -----------------------------------------------------------------
    # Concurrency: N submitter threads hammering submit_job()
    # simultaneously — this is the scenario most likely to trigger
    # AUDIT_REPORT.md 2.2 (unlocked self.jobs iteration).
    # -----------------------------------------------------------------

    def bench_concurrent_submission(self, submitters: int = 50,
                                     jobs_per_submitter: int = 20) -> Dict:
        coordinator, agents = _build_cluster(self.node_count)
        metrics = MetricsCollector()
        all_job_ids: List[str] = []
        lock_errors = []

        def submit_batch(i: int):
            ids = []
            for _ in range(jobs_per_submitter):
                jid = unique_id(f"bench-concurrent-{i}")
                try:
                    with metrics.timer("submit"):
                        coordinator.submit_job(jid, "python3 -c \"pass\"")
                    ids.append(jid)
                except Exception as e:
                    lock_errors.append(f"{type(e).__name__}: {e}")
                    metrics.incr("submit_errors")
            return ids

        t0 = time.monotonic()
        batches = run_concurrently(submit_batch, submitters, max_workers=submitters)
        wall = time.monotonic() - t0

        for batch in batches:
            if isinstance(batch, list):
                all_job_ids.extend(batch)

        result = {
            "submitters": submitters,
            "jobs_per_submitter": jobs_per_submitter,
            "total_submitted": len(all_job_ids),
            "submit_errors": len(lock_errors),
            "error_samples": lock_errors[:10],
            "wall_seconds": round(wall, 3),
            "submits_per_second": round(len(all_job_ids) / wall, 3) if wall > 0 else 0.0,
            **metrics.summary(),
        }
        self.results["concurrent_submission"] = result
        logger.info(f"[BENCH] concurrent_submission: {result['submits_per_second']} "
                    f"submits/sec, {result['submit_errors']} errors")
        return result

    # -----------------------------------------------------------------
    # Scalability: throughput as node_count increases — quantifies the
    # linear-scan bottleneck in scheduler.select_node (AUDIT_REPORT.md 6.8)
    # and the single global registry lock (6.5).
    # -----------------------------------------------------------------

    def bench_scalability_curve(self, node_counts: List[int] = (5, 20, 50, 100),
                                 jobs_per_run: int = 100) -> Dict:
        curve = []
        for n in node_counts:
            self.node_count = n
            r = self.bench_job_throughput(job_count=jobs_per_run)
            curve.append({"node_count": n, "jobs_per_second": r["jobs_per_second"]})
            logger.info(f"[BENCH] scalability_curve: nodes={n} -> {r['jobs_per_second']} jobs/sec")
        result = {"curve": curve}
        self.results["scalability_curve"] = result
        return result

    # -----------------------------------------------------------------
    # CPU / RAM under sustained load (feeds soak-test-style analysis)
    # -----------------------------------------------------------------

    def bench_resource_usage(self, duration_seconds: float = 30.0,
                              submit_interval: float = 0.05) -> Dict:
        coordinator, agents = _build_cluster(self.node_count)
        sampler = ResourceSampler(interval=1.0)
        sampler.start()

        deadline = time.monotonic() + duration_seconds
        submitted = 0
        while time.monotonic() < deadline:
            jid = unique_id("bench-resource")
            try:
                coordinator.submit_job(jid, "python3 -c \"pass\"")
                submitted += 1
            except Exception as e:
                logger.warning(f"[BENCH] submit failed during resource test: {e}")
            time.sleep(submit_interval)

        sampler.stop()
        result = {
            "duration_seconds": duration_seconds,
            "jobs_submitted": submitted,
            **sampler.report(),
        }
        self.results["resource_usage"] = result
        logger.info(f"[BENCH] resource_usage: RSS growth={result.get('rss_growth_mb')}MB "
                     f"over {duration_seconds}s / {submitted} jobs "
                     "(unbounded self.jobs/self.receipts growth — AUDIT_REPORT.md 6.2 — "
                     "should show up here as roughly-linear RSS growth with job count)")
        return result

    # -----------------------------------------------------------------
    # Durable throughput: the same job flow over a real control plane
    # (SQLite), which is what production runs. The bare coordinator above
    # never writes a row, so it overstates what a deployment can do.
    # -----------------------------------------------------------------

    def bench_durable_throughput(self, job_count: int = 200,
                                  command: str = "python3 -c \"pass\"") -> Dict:
        tmp = tempfile.mkdtemp(prefix="gcon-bench-durable-")
        coordinator = None
        try:
            coordinator, agents = _build_cluster(self.node_count, durable_dir=tmp)
            job_ids = [unique_id("bench-durable") for _ in range(job_count)]
            t0 = time.monotonic()
            for jid in job_ids:
                coordinator.submit_job(jid, command)
            statuses = assert_all_jobs_terminal(coordinator, job_ids, timeout=max(60.0, job_count * 1.0))
            wall = time.monotonic() - t0
            completed = sum(1 for st in statuses.values() if st == "completed")
            result = {"job_count": job_count, "node_count": self.node_count, "wall_seconds": round(wall, 3),
                      "jobs_per_second": round(job_count / wall, 3) if wall > 0 else 0.0,
                      "completed": completed, "failed": sum(1 for st in statuses.values() if st == "failed")}
            in_memory = self.results.get("job_throughput", {}).get("jobs_per_second")
            if in_memory:
                result["vs_in_memory"] = round(result["jobs_per_second"] / in_memory, 3)
            self.results["durable_throughput"] = result
            logger.info(f"[BENCH] durable_throughput: {result['jobs_per_second']} jobs/sec ({completed}/{job_count} completed)")
            return result
        finally:
            if coordinator is not None:
                coordinator.shutdown()
            shutil.rmtree(tmp, ignore_errors=True)

    # -----------------------------------------------------------------
    # Workflows: DAGs (root -> fan-out -> join) through the real engine
    # -----------------------------------------------------------------

    def bench_workflows(self, workflow_count: int = 10, fanout: int = 4,
                         command: str = "python3 -c \"pass\"") -> Dict:
        from gcon.workflow.workflow import Workflow, WorkflowJob
        tmp = tempfile.mkdtemp(prefix="gcon-bench-wf-")
        coordinator = None
        try:
            coordinator, agents = _build_cluster(self.node_count, durable_dir=tmp)
            ids = []
            t0 = time.monotonic()
            for _ in range(workflow_count):
                wid = unique_id("bench-wf")
                wf = Workflow(wid)
                wf.add_job(WorkflowJob(f"{wid}-root", command))
                wf.add_job(WorkflowJob(f"{wid}-join", command))
                for k in range(fanout):
                    wf.add_job(WorkflowJob(f"{wid}-f{k}", command))
                    wf.add_dependency(f"{wid}-root", f"{wid}-f{k}")
                    wf.add_dependency(f"{wid}-f{k}", f"{wid}-join")
                coordinator.submit_workflow(wf)
                ids.append(wid)
            submit_s = time.monotonic() - t0
            deadline = time.monotonic() + max(60.0, workflow_count * 5.0)
            while time.monotonic() < deadline:
                done = [coordinator.workflow_engine.states[w].status for w in ids]
                if all(st in ("COMPLETED", "FAILED", "CANCELLED") for st in done):
                    break
                time.sleep(0.1)
            wall = time.monotonic() - t0
            final = [coordinator.workflow_engine.states[w].status for w in ids]
            jobs_each = fanout + 2
            result = {"workflows": workflow_count, "jobs_per_workflow": jobs_each, "node_count": self.node_count,
                      "submit_seconds": round(submit_s, 3), "wall_seconds": round(wall, 3),
                      "workflows_per_second": round(workflow_count / wall, 3) if wall > 0 else 0.0,
                      "jobs_per_second": round(workflow_count * jobs_each / wall, 3) if wall > 0 else 0.0,
                      "completed": final.count("COMPLETED"), "failed": final.count("FAILED"),
                      "unfinished": sum(1 for st in final if st not in ("COMPLETED", "FAILED", "CANCELLED"))}
            self.results["workflows"] = result
            logger.info(f"[BENCH] workflows: {result['workflows_per_second']} workflows/sec "
                        f"({result['completed']}/{workflow_count} completed)")
            return result
        finally:
            if coordinator is not None:
                coordinator.shutdown()
            shutil.rmtree(tmp, ignore_errors=True)

    # -----------------------------------------------------------------
    # Observability cost: what the staff dashboard asks of the coordinator,
    # as the fleet grows. Every open dashboard is sent a full update every
    # 2 seconds, built separately for each viewer, so if one update takes
    # longer than that the dashboard falls behind.
    # -----------------------------------------------------------------

    def bench_observability_cost(self, fleet_sizes=(100, 1000, 5000), reps: int = 5) -> Dict:
        from types import SimpleNamespace
        from gcon.cluster.coordinator import GCONCoordinator
        from gcon.dashboard.presentation import PresentationLayer
        from gcon.persistence.control_plane import ControlPlane
        from gcon.dashboard import web_server as web_server_module

        def timed(fn, n):
            samples = []
            for _ in range(n):
                t = time.perf_counter()
                fn()
                samples.append((time.perf_counter() - t) * 1000.0)
            samples.sort()
            return {"mean_ms": round(sum(samples) / len(samples), 2),
                    "p95_ms": round(samples[min(len(samples) - 1, int(0.95 * len(samples)))], 2),
                    "max_ms": round(samples[-1], 2)}

        out = {}
        old_cwd, old_db, old_pw = os.getcwd(), os.environ.get("GCON_DB_PATH"), os.environ.get("GCON_OWNER_PASSWORD")
        for size in fleet_sizes:
            tmp = tempfile.mkdtemp(prefix="gcon-bench-obs-")
            coordinator = None
            try:
                os.environ["GCON_DB_PATH"] = os.path.join(tmp, "gcon.db")
                os.environ["GCON_OWNER_PASSWORD"] = "bench-owner-pw-1"
                os.chdir(_REPO_ROOT)                     # WebServer reads templates/ and static/ relative to here
                coordinator = GCONCoordinator(control_plane=ControlPlane(path=os.path.join(tmp, "control_plane.db")))
                coordinator.pause_scheduler()
                # a realistic mix: ~10% GCON-hosted, the rest customer-hosted across 50 organizations,
                # ~5% offline, ~30% busy, a handful quarantined
                for i in range(size):
                    node_id = f"obs-{i:05d}"
                    fleet = i % 10 == 0
                    if fleet:
                        coordinator.control_plane.fleet_nodes.add(node_id, None)
                    status = "offline" if i % 20 == 3 else ("busy" if i % 10 in (2, 4, 6) else "idle")
                    node = SimpleNamespace(node_id=node_id, org_id=None if fleet else f"org{i % 50:02d}", sandboxed=True,
                                           agent_version="1.1.0", status=status, is_connected=lambda s=status: s != "offline")
                    coordinator.registry.register(node)
                    coordinator.registry.nodes[node_id]["status"] = status
                    if status == "busy":
                        coordinator.registry.nodes[node_id]["running_jobs"] = 1
                    if i % 500 == 7:
                        coordinator.registry.set_quarantined(node_id, True, reason="benchmark: simulated verification failures")
                presentation = PresentationLayer(coordinator)
                server = web_server_module.WebServer(presentation)
                n = reps if size <= 1000 else max(2, reps // 2)
                row = {
                    "worker_overview": timed(coordinator.get_worker_overview, n),
                    "node_summary": timed(presentation.get_node_summary, n),
                    "nodes_list": timed(presentation.get_nodes, n),
                    "cluster_health": timed(coordinator.get_cluster_health, n),
                    "live_update_per_viewer": timed(server._build_live_payload, n),
                }
                row["live_update_keeps_up_with_2s_push"] = row["live_update_per_viewer"]["p95_ms"] < 2000.0
                row["live_update_kb"] = round(len(json.dumps(server._build_live_payload(), default=str)) / 1024.0, 1)
                out[str(size)] = row
                logger.info(f"[BENCH] observability_cost {size} workers: live update "
                            f"{row['live_update_per_viewer']['mean_ms']} ms mean / {row['live_update_per_viewer']['p95_ms']} ms p95, "
                            f"{row['live_update_kb']} KB, worker_overview {row['worker_overview']['mean_ms']} ms")
            finally:
                if coordinator is not None:
                    coordinator.shutdown()
                os.chdir(old_cwd)
                for key, old in (("GCON_DB_PATH", old_db), ("GCON_OWNER_PASSWORD", old_pw)):
                    if old is None:
                        os.environ.pop(key, None)
                    else:
                        os.environ[key] = old
                shutil.rmtree(tmp, ignore_errors=True)
        self.results["observability_cost"] = out
        return out

    # -----------------------------------------------------------------
    # Transport: the real gRPC fleet path with real TLS -- enrollment
    # (certificate signing), registration, and heartbeats over Control
    # streams, which is how every real worker talks to the coordinator.
    # -----------------------------------------------------------------

    def bench_transport(self, workers: int = 20, heartbeats: int = 20, enrollments: int = 40, parallel: int = 8) -> Dict:
        import queue
        import socket
        import threading
        import grpc
        from gcon.persistence.control_plane import ControlPlane
        from gcon.transport import tls
        from gcon.transport.config import TransportConfig
        from gcon.transport.grpc_transport import GrpcTransport
        from gcon.transport.proto import gcon_transport_pb2 as pb
        from gcon.transport.proto import gcon_transport_pb2_grpc as pb_grpc

        def pct(samples, q):
            samples = sorted(samples)
            return round(samples[min(len(samples) - 1, int(q * len(samples)))] * 1000.0, 2) if samples else None

        env = {"GCON_ENROLL_TOKEN": "bench-token", "GCON_ENROLL_INSECURE": "1",
               # the flood throttle would (correctly) refuse a benchmark that enrolls this fast from one address
               "GCON_ENROLL_RATE_PER_SOURCE": "0", "GCON_ENROLL_RATE_OVERALL": "0"}
        old = {k: os.environ.get(k) for k in env}
        os.environ.update(env)
        tmp = tempfile.mkdtemp(prefix="gcon-bench-transport-")
        cert_dir = os.path.join(tmp, "certs")
        os.makedirs(cert_dir)
        control_plane = ControlPlane(path=os.path.join(tmp, "control_plane.db"))
        transport, channels = None, []
        try:
            with socket.socket() as sk:
                sk.bind(("localhost", 0))
                port = sk.getsockname()[1]
            control_plane.settings.set("grpc_port", str(port))
            control_plane.settings.set("tls_cert_dir", cert_dir)
            control_plane.settings.set("heartbeat_interval_seconds", "1")
            transport = GrpcTransport(control_plane=control_plane, config=TransportConfig.load(control_plane))
            transport.start()

            # -- enrollment: a CSR is signed for each worker (no certificate is needed to call it)
            enroll_channel = grpc.insecure_channel(f"localhost:{port + 1}")
            channels.append(enroll_channel)
            enroll_stub = pb_grpc.AgentControlStub(enroll_channel)
            csrs = [(f"enroll-{i:04d}",) + (tls.generate_agent_csr(f"enroll-{i:04d}")[1],) for i in range(enrollments)]
            enroll_lat, refused = [], []

            def enroll(item):
                node_id, csr = item
                t = time.perf_counter()
                resp = enroll_stub.Enroll(pb.EnrollRequest(node_id=node_id, enroll_token="bench-token", csr_pem=csr), timeout=30)
                enroll_lat.append(time.perf_counter() - t)
                if not resp.accepted:
                    refused.append(resp.reason)
            t0 = time.perf_counter()
            run_concurrently(lambda i: enroll(csrs[i]), len(csrs), max_workers=parallel)
            enroll_wall = time.perf_counter() - t0

            # -- registration + control streams
            stubs = []
            for i in range(workers):
                node_id = f"tr-{i:04d}"
                tls.issue_agent_cert(cert_dir, node_id)
                ch = grpc.secure_channel(f"localhost:{port}", tls.load_agent_channel_credentials(cert_dir, node_id),
                                         options=[("grpc.ssl_target_name_override", "localhost")])
                channels.append(ch)
                stubs.append((node_id, pb_grpc.AgentControlStub(ch)))
            tokens, reg_lat = {}, []

            def register(i):
                node_id, stub = stubs[i]
                t = time.perf_counter()
                resp = stub.Register(pb.RegisterRequest(node_id=node_id, hostname=node_id), timeout=30)
                reg_lat.append(time.perf_counter() - t)
                tokens[node_id] = resp.session_token if resp.accepted else None
            run_concurrently(register, workers, max_workers=parallel)
            registered = sum(1 for v in tokens.values() if v)

            outboxes = {}

            def open_stream(i):
                node_id, stub = stubs[i]
                q = queue.Queue()
                outboxes[node_id] = q

                def gen():
                    while True:
                        item = q.get()
                        if item is None:
                            return
                        yield item
                responses = stub.Control(gen())

                def drain(stream=responses):
                    try:
                        for _ in stream:
                            pass
                    except grpc.RpcError:
                        pass                        # the stream is cancelled when the benchmark closes its channels
                threading.Thread(target=drain, daemon=True).start()
            for i in range(workers):
                if tokens.get(stubs[i][0]):
                    open_stream(i)
            t1 = time.perf_counter()
            for node_id, q in outboxes.items():
                for seq in range(1, heartbeats + 1):
                    q.put(pb.AgentEnvelope(node_id=node_id, session_token=tokens[node_id],
                                           heartbeat=pb.Heartbeat(sequence=seq, status="idle")))
            deadline = time.perf_counter() + 60
            while time.perf_counter() < deadline:
                if all(control_plane.heartbeats.last_sequence(n) >= heartbeats for n in outboxes):
                    break
                time.sleep(0.05)
            hb_wall = time.perf_counter() - t1
            ingested = sum(min(control_plane.heartbeats.last_sequence(n), heartbeats) for n in outboxes)
            for q in outboxes.values():
                q.put(None)
            result = {
                "enroll": {"count": enrollments, "parallel": parallel, "refused": len(refused),
                           "per_second": round(enrollments / enroll_wall, 2) if enroll_wall else None,
                           "p50_ms": pct(enroll_lat, 0.5), "p95_ms": pct(enroll_lat, 0.95)},
                "register": {"workers": workers, "registered": registered, "p50_ms": pct(reg_lat, 0.5), "p95_ms": pct(reg_lat, 0.95)},
                "heartbeats": {"streams": len(outboxes), "sent": len(outboxes) * heartbeats, "ingested": ingested,
                               "wall_seconds": round(hb_wall, 3),
                               "per_second": round(ingested / hb_wall, 1) if hb_wall else None,
                               "note": "every heartbeat is a durable database row; a worker sends one about every 2s, "
                                       "so the sustained rate a fleet of N workers needs is N/2 per second"},
            }
            self.results["transport"] = result
            logger.info(f"[BENCH] transport: enroll {result['enroll']['per_second']}/s, register p95 {result['register']['p95_ms']} ms, "
                        f"heartbeats {result['heartbeats']['per_second']}/s")
            return result
        finally:
            for c in channels:
                try:
                    c.close()
                except Exception:
                    pass
            if transport is not None:
                transport.shutdown(grace_period=1)
            control_plane.close()
            for key, value in old.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value
            shutil.rmtree(tmp, ignore_errors=True)

    def full_report(self) -> Dict:
        return {
            "generated_at": datetime.now(UTC).isoformat(),
            "node_count_final": self.node_count,
            "coverage": [{"area": a, "measured_by": h} for a, h in COVERAGE],
            "results": self.results,
        }


def main():
    parser = argparse.ArgumentParser(description="GCON benchmark suite")
    parser.add_argument("--nodes", type=int, default=10)
    parser.add_argument("--jobs", type=int, default=200)
    parser.add_argument("--submitters", type=int, default=50)
    parser.add_argument("--resource-duration", type=float, default=15.0)
    parser.add_argument("--skip-scalability", action="store_true",
                         help="scalability curve is the slowest benchmark; skip for a quick run")
    parser.add_argument("--skip-durable", action="store_true", help="skip the durable (SQLite) throughput benchmark")
    parser.add_argument("--skip-workflows", action="store_true", help="skip the workflow benchmark")
    parser.add_argument("--skip-observability", action="store_true", help="skip the dashboard cost vs fleet size benchmark")
    parser.add_argument("--skip-transport", action="store_true", help="skip the gRPC transport benchmark")
    parser.add_argument("--fleet-sizes", default="100,1000,5000",
                         help="worker counts for the observability cost benchmark (default 100,1000,5000)")
    parser.add_argument("--workflows", type=int, default=10, help="workflows submitted by the workflow benchmark")
    parser.add_argument("--transport-workers", type=int, default=20, help="workers connected in the transport benchmark")
    parser.add_argument("--out", default="benchmark_report.json")
    args = parser.parse_args()

    suite = BenchmarkSuite(node_count=args.nodes)
    suite.bench_job_throughput(job_count=args.jobs)
    suite.bench_dispatch_latency(sample_count=min(50, args.jobs))
    suite.bench_concurrent_submission(submitters=args.submitters, jobs_per_submitter=5)
    suite.bench_resource_usage(duration_seconds=args.resource_duration)
    if not args.skip_scalability:
        suite.bench_scalability_curve()
    if not args.skip_durable:
        suite.bench_durable_throughput(job_count=args.jobs)
    if not args.skip_workflows:
        suite.bench_workflows(workflow_count=args.workflows)
    if not args.skip_observability:
        suite.bench_observability_cost(fleet_sizes=tuple(int(x) for x in args.fleet_sizes.split(",") if x.strip()))
    if not args.skip_transport:
        suite.bench_transport(workers=args.transport_workers)

    report = suite.full_report()
    with open(args.out, "w") as f:
        json.dump(report, f, indent=2, default=str)
    logger.info(f"[BENCH] report written to {args.out}")
    print(json.dumps(report, indent=2, default=str))
    print_coverage()
    # Fail loudly if something the benchmark was meant to measure did not work, instead of printing a clean report.
    r = suite.results
    problems = []
    for name in ("job_throughput", "durable_throughput", "workflows"):
        res = r.get(name)
        if res and res.get("failed"):
            problems.append(f"{name}: {res['failed']} job(s) failed")
    if r.get("workflows", {}).get("unfinished"):
        problems.append(f"workflows: {r['workflows']['unfinished']} did not finish")
    tr = r.get("transport")
    if tr:
        if tr["enroll"]["refused"]:
            problems.append(f"transport: {tr['enroll']['refused']} enrollments refused")
        if tr["register"]["registered"] != tr["register"]["workers"]:
            problems.append("transport: not every worker registered")
        if tr["heartbeats"]["ingested"] != tr["heartbeats"]["sent"]:
            problems.append(f"transport: {tr['heartbeats']['sent'] - tr['heartbeats']['ingested']} heartbeats were not ingested in time")
    for size, row in (r.get("observability_cost") or {}).items():
        if not row["live_update_keeps_up_with_2s_push"]:
            problems.append(f"observability: at {size} workers one dashboard update takes longer than the 2s it is pushed every")
    if problems:
        print("\nPROBLEMS: " + "; ".join(problems), file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
