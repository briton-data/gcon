"""
Both benchmarks still run, and still cover what they say they cover.

scripts/benchmark.py was found stuck on every run (the coordinator's sandbox policy had since started refusing to run
jobs on the benchmark's unsandboxed agents, so nothing was ever dispatched) and nobody noticed, because nothing ran
it. These run each in a tiny configuration. They take under a minute, so they are marked slow like the repo's other
long tests: run them with `pytest tests/test_benchmarks_still_run.py`.
"""
import json
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent

pytestmark = pytest.mark.slow


def _run(args, cwd, timeout):
    return subprocess.run([sys.executable, *args], cwd=cwd, capture_output=True, text=True, timeout=timeout)


def test_the_traffic_benchmark_calls_every_route_and_sweeps_the_dashboard(tmp_path):
    proc = _run([str(ROOT / "benchmarks" / "benchmark.py"), "--smoke", "--nodes", "5", "--tenants", "2",
                 "--out", str(tmp_path / "smoke.json")], tmp_path, 240)
    out = proc.stdout + proc.stderr
    assert proc.returncode == 0, out[-3000:]
    assert "nothing called: none" in out                       # every /api/v1 route was called
    assert "5xx/failed: none" in out
    assert "dashboard worker view: adds up to what was registered" in out
    assert "Idempotency-Key replays the same job" in out and "Idempotency-Key replays the same workflow" in out
    assert "DELETE /workers/leases/{id}" in out                # the hosted-worker lease lifecycle ran end to end


def test_the_in_process_benchmark_runs_every_benchmark_and_states_what_it_does_not_cover(tmp_path):
    report = tmp_path / "report.json"
    proc = _run([str(ROOT / "scripts" / "benchmark.py"), "--nodes", "3", "--jobs", "10", "--submitters", "3",
                 "--resource-duration", "1", "--skip-scalability", "--workflows", "2", "--fleet-sizes", "50",
                 "--transport-workers", "4", "--out", str(report)], tmp_path, 280)
    out = proc.stdout + proc.stderr
    assert proc.returncode == 0, out[-3000:]
    results = json.loads(report.read_text())["results"]
    for name in ("job_throughput", "durable_throughput", "workflows", "observability_cost", "transport"):
        assert name in results, f"{name} did not run"
    assert results["job_throughput"]["completed"] == 10        # jobs are actually dispatched and finish
    assert results["workflows"]["completed"] == 2
    assert results["transport"]["heartbeats"]["ingested"] == results["transport"]["heartbeats"]["sent"]
    assert "NOT COVERED: webhook and callback_url delivery" in out
    assert "Traceback" not in out
