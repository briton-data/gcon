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
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, UTC
from typing import Dict, List

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from test_utils import (
    MetricsCollector, ResourceSampler, assert_all_jobs_terminal,
    get_logger, run_concurrently, unique_id,
)

logger = get_logger("benchmark", log_file="logs/benchmark.log")


def _build_cluster(node_count: int):
    """
    Build a real GCONCoordinator with `node_count` real GCONAgent
    "nodes" registered against it, exactly as the production entry
    points (run_job.py, examples/*.py) do. No mocking of coordinator
    internals — this measures the actual code path.
    """
    from gcon.cluster.coordinator import GCONCoordinator
    from gcon.execution.agent import GCONAgent

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
                from test_utils import assert_eventually
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

    def full_report(self) -> Dict:
        return {
            "generated_at": datetime.now(UTC).isoformat(),
            "node_count_final": self.node_count,
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
    parser.add_argument("--out", default="benchmark_report.json")
    args = parser.parse_args()

    suite = BenchmarkSuite(node_count=args.nodes)
    suite.bench_job_throughput(job_count=args.jobs)
    suite.bench_dispatch_latency(sample_count=min(50, args.jobs))
    suite.bench_concurrent_submission(submitters=args.submitters, jobs_per_submitter=5)
    suite.bench_resource_usage(duration_seconds=args.resource_duration)
    if not args.skip_scalability:
        suite.bench_scalability_curve()

    report = suite.full_report()
    with open(args.out, "w") as f:
        json.dump(report, f, indent=2, default=str)
    logger.info(f"[BENCH] report written to {args.out}")
    print(json.dumps(report, indent=2, default=str))


if __name__ == "__main__":
    main()
