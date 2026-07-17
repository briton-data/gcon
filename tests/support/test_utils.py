"""
test_utils.py — reusable assertions, logging, and metrics-collection
helpers shared by stress_test.py, chaos.py, and benchmark.py.

Everything here is written against the REAL GCON modules (coordinator.py,
agent.py, Noderegistry.py, etc.) as they exist today — an in-process
simulation with no real network boundary (see AUDIT_REPORT.md section 4).
Where that matters for a given helper, it is called out explicitly so
these utilities keep working once a real transport layer replaces
CommunicationManager.
"""

from __future__ import annotations

import json
import logging
import os
import statistics
import sys
import threading
import time
import traceback
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, UTC
from typing import Any, Callable, Dict, List, Optional

# ---------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------

def get_logger(name: str, log_file: Optional[str] = None,
               level: int = logging.INFO) -> logging.Logger:
    """
    Build a logger that writes structured lines to stdout AND (optionally)
    to a file, so a soak test running for hours has a durable record even
    if the terminal scrolls off / the SSH session drops.
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.propagate = False

    if logger.handlers:
        return logger  # already configured (avoid duplicate handlers)

    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    stream = logging.StreamHandler(sys.stdout)
    stream.setFormatter(fmt)
    logger.addHandler(stream)

    if log_file:
        os.makedirs(os.path.dirname(log_file) or ".", exist_ok=True)
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(fmt)
        logger.addHandler(file_handler)

    return logger


# ---------------------------------------------------------------------
# Assertions (raise AssertionError with rich, structured context —
# a bare "assert x == y" tells you nothing when a test fails at 3am
# in a soak-test log with 40,000 preceding lines)
# ---------------------------------------------------------------------

class TestAssertionError(AssertionError):
    def __init__(self, message: str, context: Optional[Dict[str, Any]] = None):
        self.context = context or {}
        full = message
        if self.context:
            full += " | context=" + json.dumps(self.context, default=str)
        super().__init__(full)


def assert_eventually(
    condition: Callable[[], bool],
    timeout: float = 5.0,
    interval: float = 0.05,
    description: str = "condition",
) -> None:
    """
    Poll `condition` until it returns truthy or `timeout` elapses.
    Use this instead of a fixed `time.sleep(N)` anywhere a background
    thread (scheduler_loop, health_check_loop, a heartbeat thread) is
    expected to eventually converge state — GCON's coordinator is
    fully asynchronous internally (see AUDIT_REPORT.md 2.4), so a
    fixed sleep is either flaky (too short) or wastes wall-clock time
    (too long) depending on machine load.
    """
    deadline = time.monotonic() + timeout
    last_exc = None
    while time.monotonic() < deadline:
        try:
            if condition():
                return
        except Exception as e:  # condition itself may touch racy state
            last_exc = e
        time.sleep(interval)
    extra = f" (last exception while polling: {last_exc!r})" if last_exc else ""
    raise TestAssertionError(
        f"Timed out after {timeout}s waiting for: {description}{extra}"
    )


def assert_no_duplicate_dispatch(coordinator, job_id: str) -> None:
    """
    Guards against AUDIT_REPORT.md 2.3 (unfenced node-claim race):
    a job must never be associated with more than one node over its
    lifetime. Call after a job reaches a terminal state.
    """
    job = coordinator.jobs.get(job_id)
    if job is None:
        raise TestAssertionError(f"job '{job_id}' vanished from coordinator.jobs")
    # There is currently no history of node reassignment kept on the job
    # dict itself (another audit gap) so this checks the one signal that
    # IS available: a job stuck 'running' with no owner is itself a bug.
    if job["status"] == "running" and job["node_id"] is None:
        raise TestAssertionError(
            "job is 'running' with no assigned node_id — orphaned by a "
            "race between claim and release",
            context={"job_id": job_id, "job": job},
        )


def assert_node_status_consistent(coordinator, node_id: str) -> None:
    """
    Cross-checks the three independent copies of node status called out
    in AUDIT_REPORT.md 8.2: the live node/agent object, the registry
    entry, and (indirectly) whether the coordinator would dispatch to it.
    """
    info = coordinator.registry.get_node_info(node_id)
    live_status = info["node"].status
    registry_status = info["status"]
    if live_status != registry_status:
        raise TestAssertionError(
            "node status diverged between live object and registry",
            context={
                "node_id": node_id,
                "live_status": live_status,
                "registry_status": registry_status,
            },
        )


def assert_all_jobs_terminal(coordinator, job_ids: List[str],
                              timeout: float = 30.0) -> Dict[str, str]:
    """
    Wait for every job in `job_ids` to reach a terminal status
    (completed/failed/cancelled) and return the final status map.
    Raises with the list of still-pending/running jobs on timeout —
    this is the single most useful check for catching AUDIT_REPORT.md
    3.1 (unbounded job timeout leaking a node forever): a hung job
    will show up here as "still running" past the deadline.
    """
    terminal = {"completed", "failed", "cancelled"}
    deadline = time.monotonic() + timeout
    statuses: Dict[str, str] = {}

    while time.monotonic() < deadline:
        statuses = {
            jid: coordinator.jobs[jid]["status"]
            for jid in job_ids if jid in coordinator.jobs
        }
        if all(statuses.get(jid) in terminal for jid in job_ids):
            return statuses
        time.sleep(0.1)

    stuck = {jid: statuses.get(jid, "MISSING") for jid in job_ids
             if statuses.get(jid) not in terminal}
    raise TestAssertionError(
        f"{len(stuck)}/{len(job_ids)} jobs did not reach a terminal "
        f"state within {timeout}s",
        context={"stuck_jobs": stuck},
    )


# ---------------------------------------------------------------------
# Metrics collection
# ---------------------------------------------------------------------

@dataclass
class LatencySample:
    label: str
    seconds: float
    timestamp: str = field(default_factory=lambda: datetime.now(UTC).isoformat())
    ok: bool = True
    error: Optional[str] = None


class MetricsCollector:
    """
    Thread-safe collector for latency samples, counters, and gauges,
    shared across many concurrent worker threads during load/stress
    tests. Deliberately independent of GCON's own MetricsCollector
    (metrics.py) — this one measures the *test harness's* view of the
    system (end-to-end, from the caller's perspective), which is the
    number that actually matters for SLOs, versus internal counters
    the system reports about itself.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._samples: List[LatencySample] = []
        self._counters: Dict[str, int] = {}
        self._gauges: Dict[str, float] = {}
        self._start = time.monotonic()

    def record_latency(self, label: str, seconds: float,
                        ok: bool = True, error: Optional[str] = None) -> None:
        with self._lock:
            self._samples.append(LatencySample(label, seconds, ok=ok, error=error))

    @contextmanager
    def timer(self, label: str):
        t0 = time.monotonic()
        ok, error = True, None
        try:
            yield
        except Exception as e:
            ok, error = False, f"{type(e).__name__}: {e}"
            raise
        finally:
            self.record_latency(label, time.monotonic() - t0, ok=ok, error=error)

    def incr(self, name: str, amount: int = 1) -> None:
        with self._lock:
            self._counters[name] = self._counters.get(name, 0) + amount

    def set_gauge(self, name: str, value: float) -> None:
        with self._lock:
            self._gauges[name] = value

    def summary(self, label_filter: Optional[str] = None) -> Dict[str, Any]:
        with self._lock:
            samples = [s for s in self._samples
                       if label_filter is None or s.label == label_filter]
            counters = dict(self._counters)
            gauges = dict(self._gauges)

        latencies = [s.seconds for s in samples]
        errors = [s for s in samples if not s.ok]
        wall = time.monotonic() - self._start

        out = {
            "sample_count": len(samples),
            "error_count": len(errors),
            "error_rate": (len(errors) / len(samples)) if samples else 0.0,
            "wall_seconds": round(wall, 3),
            "throughput_per_sec": round(len(samples) / wall, 3) if wall > 0 else 0.0,
            "counters": counters,
            "gauges": gauges,
        }

        if latencies:
            sorted_lat = sorted(latencies)
            out["latency_seconds"] = {
                "min": round(sorted_lat[0], 6),
                "max": round(sorted_lat[-1], 6),
                "mean": round(statistics.mean(sorted_lat), 6),
                "median": round(statistics.median(sorted_lat), 6),
                "p95": round(_percentile(sorted_lat, 0.95), 6),
                "p99": round(_percentile(sorted_lat, 0.99), 6),
                "stdev": round(statistics.pstdev(sorted_lat), 6) if len(sorted_lat) > 1 else 0.0,
            }
        else:
            out["latency_seconds"] = None

        if errors:
            error_kinds: Dict[str, int] = {}
            for e in errors:
                error_kinds[e.error or "unknown"] = error_kinds.get(e.error or "unknown", 0) + 1
            out["error_breakdown"] = error_kinds

        return out

    def dump_json(self, path: str) -> None:
        with open(path, "w") as f:
            json.dump(self.summary(), f, indent=2, default=str)


def _percentile(sorted_values: List[float], pct: float) -> float:
    if not sorted_values:
        return 0.0
    k = (len(sorted_values) - 1) * pct
    f, c = int(k), min(int(k) + 1, len(sorted_values) - 1)
    if f == c:
        return sorted_values[f]
    return sorted_values[f] + (sorted_values[c] - sorted_values[f]) * (k - f)


# ---------------------------------------------------------------------
# Process/memory sampling (for memory & CPU tests / soak tests)
# ---------------------------------------------------------------------

class ResourceSampler:
    """
    Samples the CURRENT PROCESS's RSS and CPU usage over time. Because
    GCON's coordinator and every simulated node currently live in the
    *same* OS process (AUDIT_REPORT.md section 4), this is the correct
    place to detect the memory-growth issues flagged in section 6.2/6.3
    (unbounded self.jobs / self.receipts / event_bus._events) — any
    leak in those structures shows up directly as RSS growth here.
    """

    def __init__(self, interval: float = 1.0):
        import psutil
        self._psutil = psutil
        self._process = psutil.Process(os.getpid())
        self.interval = interval
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self.samples: List[Dict[str, float]] = []
        self._lock = threading.Lock()

    def start(self) -> None:
        self._running = True
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._running = False
        if self._thread:
            self._thread.join(timeout=self.interval * 2)

    def _loop(self) -> None:
        self._process.cpu_percent(interval=None)  # prime, per monitor.py's own caveat
        while self._running:
            with self._lock:
                self.samples.append({
                    "t": time.monotonic(),
                    "rss_mb": self._process.memory_info().rss / (1024 * 1024),
                    "cpu_percent": self._process.cpu_percent(interval=None),
                    "num_threads": self._process.num_threads(),
                    "num_fds": _safe_num_fds(self._process),
                })
            time.sleep(self.interval)

    def report(self) -> Dict[str, Any]:
        with self._lock:
            samples = list(self.samples)
        if not samples:
            return {"samples": 0}
        rss = [s["rss_mb"] for s in samples]
        threads = [s["num_threads"] for s in samples]
        return {
            "samples": len(samples),
            "rss_mb_start": round(rss[0], 2),
            "rss_mb_end": round(rss[-1], 2),
            "rss_mb_peak": round(max(rss), 2),
            "rss_growth_mb": round(rss[-1] - rss[0], 2),
            "thread_count_start": threads[0],
            "thread_count_end": threads[-1],
            "thread_growth": threads[-1] - threads[0],
        }


def _safe_num_fds(process) -> int:
    try:
        return process.num_fds()
    except (AttributeError, Exception):
        return -1  # not supported on this platform (e.g. Windows)


# ---------------------------------------------------------------------
# Misc helpers
# ---------------------------------------------------------------------

def run_concurrently(fn: Callable[[int], Any], count: int,
                      max_workers: int = 50) -> List[Any]:
    """
    Run `fn(i)` for i in range(count) across a bounded thread pool and
    return results in submission order, capturing exceptions instead of
    letting one failed worker abort the whole batch.
    """
    from concurrent.futures import ThreadPoolExecutor

    results: List[Any] = [None] * count
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(fn, i): i for i in range(count)}
        for future in futures:
            idx = futures[future]
            try:
                results[idx] = future.result()
            except Exception as e:
                results[idx] = {"error": f"{type(e).__name__}: {e}",
                                 "traceback": traceback.format_exc()}
    return results


def unique_id(prefix: str) -> str:
    return f"{prefix}-{int(time.time() * 1_000_000)}-{threading.get_ident()}"
