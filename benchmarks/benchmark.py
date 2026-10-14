#!/usr/bin/env python3
"""
benchmark.py -- load tool for the GCON coordinator + public REST API.

Not a unit/functional test and not scripts/benchmark.py (that one calls the
coordinator in-process, one job at a time). This one is a TRAFFIC SIMULATOR:

    * a real server process: the real coordinator, scheduler, SQLite control
      plane and FastAPI app (/api/v1) served by uvicorn on localhost;
    * N simulated nodes inside it: real GCONAgent objects registered with the
      coordinator, sending real heartbeats and taking real dispatches. Only the
      subprocess is replaced by `sleep(--job-ms)`, so the scheduler, locks,
      database and receipt path are the production code paths;
    * bots outside it, talking real HTTP with API keys: submitters that push
      jobs at a target rate, pollers, cancellers, readers and writers that hit
      every /api/v1 route, so the API is measured under contention from the
      scheduler, and the scheduler under contention from the API.

It reports throughput, queue/execution/end-to-end latency, per-endpoint API
latency, node utilisation and fairness, SQLite query cost, and LOCK CONTENTION
(every coordinator/db/registry/transport/telemetry lock is wrapped to time how
long threads wait for it and how long it is held).

    python benchmarks/benchmark.py --nodes 200 --jobs 5000
    python benchmarks/benchmark.py --nodes 200 --jobs 5000 --rate 400 --job-ms 250
    python benchmarks/benchmark.py --nodes 20 --jobs 500 --out before.json
    python benchmarks/benchmark.py --compare before.json after.json
    python benchmarks/benchmark.py --smoke        # one call to every route, then exit

Run it on a quiet machine. The bots share one Python process (the GIL); the
report says when the generator itself, not the server, was the bottleneck.
"""
from __future__ import annotations

import argparse
import http.client
import itertools
import json
import os
import random
import subprocess
import sys
import tempfile
import threading
import time
from datetime import datetime

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
perf = time.perf_counter

TERMINAL = {"completed", "failed", "cancelled", "canceled", "timeout", "timed_out", "rejected"}


# =============================================================================
# shared helpers
# =============================================================================

def pct(sorted_xs, p):
    if not sorted_xs:
        return 0.0
    return sorted_xs[min(len(sorted_xs) - 1, int(len(sorted_xs) * p))]


class ThreadDist:
    """Latency/size distribution that never makes threads fight over it: each
    thread appends to its own record, merged when asked."""
    CAP = 60000

    def __init__(self):
        self._tl = threading.local()
        self._all = []
        self._reg = threading.Lock()

    def _mine(self):
        d = getattr(self._tl, "d", None)
        if d is None:
            d = self._tl.d = {"n": 0, "total": 0.0, "max": 0.0, "s": []}
            with self._reg:
                self._all.append(d)
        return d

    def add(self, x):
        d = self._mine()
        d["n"] += 1
        d["total"] += x
        if x > d["max"]:
            d["max"] = x
        s = d["s"]
        if len(s) < self.CAP:
            s.append(x)
        else:
            j = random.randrange(d["n"])
            if j < self.CAP:
                s[j] = x

    def summary(self):
        n = sum(d["n"] for d in self._all)
        total = sum(d["total"] for d in self._all)
        samples = sorted(itertools.chain.from_iterable(d["s"] for d in self._all))
        return {
            "n": n, "total": total, "mean": (total / n if n else 0.0),
            "p50": pct(samples, 0.50), "p95": pct(samples, 0.95), "p99": pct(samples, 0.99),
            "max": max((d["max"] for d in self._all), default=0.0),
        }


def dist_summary(xs):
    xs = sorted(xs)
    n = len(xs)
    return {"n": n, "mean": (sum(xs) / n if n else 0.0), "p50": pct(xs, 0.50), "p95": pct(xs, 0.95),
            "p99": pct(xs, 0.99), "max": (xs[-1] if xs else 0.0)}


def table(headers, rows):
    rows = [[str(c) for c in r] for r in rows]
    widths = [max(len(h), *(len(r[i]) for r in rows)) if rows else len(h) for i, h in enumerate(headers)]
    line = lambda cells: "  ".join(c.ljust(w) if i == 0 else c.rjust(w) for i, (c, w) in enumerate(zip(cells, widths)))
    out = [line(headers), "  ".join("-" * w for w in widths)]
    out += [line(r) for r in rows]
    return "\n".join(out)


def ms(x):
    return f"{x * 1000:.1f}"


def parse_iso(s):
    try:
        return datetime.fromisoformat(s).timestamp()
    except Exception:
        return None


# =============================================================================
# SERVER SIDE  (python benchmark.py --serve ...): coordinator + nodes + API
# =============================================================================

class TimedLock:
    """Drop-in for threading.Lock/RLock that records how long threads WAIT to
    get it (contention) and how long it is HELD (the critical section length)."""
    WAIT_CAP = 50000

    def __init__(self, name, inner, group=None):
        self.name = name
        self.group = group or name
        self._inner = inner
        self._tl = threading.local()
        self.acq = 0
        self.contended = 0
        self.wait_total = 0.0
        self.wait_max = 0.0
        self.hold_total = 0.0
        self.hold_max = 0.0
        self.waits = []

    def acquire(self, blocking=True, timeout=-1):
        tl = self._tl
        depth = getattr(tl, "d", 0)
        if depth:                                   # re-entrant: cannot contend with itself
            self._inner.acquire()
            tl.d = depth + 1
            return True
        if self._inner.acquire(False):
            w, cont = 0.0, False
        else:
            if not blocking:
                return False
            t0 = perf()
            if not self._inner.acquire(True, timeout):
                return False
            w, cont = perf() - t0, True
        # we hold the inner lock now, so the stats below are race-free
        self.acq += 1
        if cont:
            self.contended += 1
            self.wait_total += w
            if w > self.wait_max:
                self.wait_max = w
            if len(self.waits) < self.WAIT_CAP:
                self.waits.append(w)
        tl.d = 1
        tl.t = perf()
        return True

    def release(self):
        tl = self._tl
        d = tl.d
        if d == 1:
            h = perf() - tl.t
            self.hold_total += h
            if h > self.hold_max:
                self.hold_max = h
            tl.d = 0
        else:
            tl.d = d - 1
        self._inner.release()

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, *exc):
        self.release()

    def locked(self):
        return self._inner.locked() if hasattr(self._inner, "locked") else bool(getattr(self._tl, "d", 0))

    def __getattr__(self, item):
        return getattr(self._inner, item)


def lock_report(locks):
    groups = {}
    for lk in locks:
        g = groups.setdefault(lk.group, {"acq": 0, "contended": 0, "wait_total": 0.0, "wait_max": 0.0,
                                         "hold_total": 0.0, "hold_max": 0.0, "waits": []})
        g["acq"] += lk.acq
        g["contended"] += lk.contended
        g["wait_total"] += lk.wait_total
        g["wait_max"] = max(g["wait_max"], lk.wait_max)
        g["hold_total"] += lk.hold_total
        g["hold_max"] = max(g["hold_max"], lk.hold_max)
        g["waits"].extend(lk.waits)
    out = {}
    for name, g in groups.items():
        w = sorted(g["waits"])
        out[name] = {
            "acquisitions": g["acq"], "contended": g["contended"],
            "contended_pct": (100.0 * g["contended"] / g["acq"] if g["acq"] else 0.0),
            "wait_total_s": g["wait_total"], "wait_mean_ms": (1000 * g["wait_total"] / g["contended"] if g["contended"] else 0.0),
            "wait_p99_ms": 1000 * pct(w, 0.99), "wait_max_ms": 1000 * g["wait_max"],
            "hold_total_s": g["hold_total"], "hold_mean_ms": (1000 * g["hold_total"] / g["acq"] if g["acq"] else 0.0),
            "hold_max_ms": 1000 * g["hold_max"],
        }
    return out


def serve(args):
    """Child process: build the real stack, add stub-execution nodes, serve /api/v1."""
    import signal
    import uvicorn

    sys.path.insert(0, os.path.join(REPO_ROOT, "src"))
    os.makedirs(args.workdir, exist_ok=True)
    os.chdir(args.workdir)
    # The coordinator evicts finished jobs from memory above GCON_MAX_JOBS_IN_MEMORY (default 5000), which would
    # make a big run lose records (same for receipts). Lift both for the measurement and say so in the report.
    os.environ.setdefault("GCON_MAX_JOBS_IN_MEMORY", str(args.max_jobs_in_memory))
    os.environ.setdefault("GCON_MAX_RECEIPTS_IN_MEMORY", str(args.max_jobs_in_memory))

    from gcon.cluster.coordinator import GCONCoordinator
    from gcon.dashboard.presentation import PresentationLayer
    from gcon.execution.agent import ExecutionMetrics, GCONAgent
    from gcon.management.management_layer import ManagementLayer
    from gcon.persistence.control_plane import ControlPlane
    from gcon.api import api_v1

    from gcon.transport import tls
    os.makedirs(os.path.join(args.workdir, "keys", "grpc"), exist_ok=True)
    tls.ensure_ca(os.path.join(args.workdir, "keys", "grpc"))      # so GET /enroll/ca has a real CA to serve
    cp = ControlPlane(path=os.path.join(args.workdir, "cp.db"))
    if args.sync != "full":
        cp.db._conn.execute(f"PRAGMA synchronous={args.sync.upper()}")
    co = GCONCoordinator(control_plane=cp)
    mgmt = ManagementLayer(coordinator=co, db_path=os.path.join(args.workdir, "mgmt.db"))
    pres = PresentationLayer(co)

    rng = random.Random(args.seed)
    job_times = {}            # job_id -> (node_id, start_wall, end_wall)

    class BenchNode(GCONAgent):
        """A real GCONAgent whose 'execution' is a sleep: everything around the
        subprocess (dispatch, heartbeat, result handling, receipts) is real."""

        # Customer jobs must run on a sandboxed worker; a real one proves it with a
        # signed attestation, which the coordinator only checks for remote (gRPC) nodes.
        sandboxed = True

        def __init__(self, node_id):
            super().__init__(node_id)
            self.jobs_done = 0
            self.busy_s = 0.0
            self._rng = random.Random(f"{args.seed}:{node_id}")

        def execute_job(self, job_id, job_script, timeout=None, usage_report_path=None, stage_report_path=None):
            self.status = "busy"
            t0 = time.time()
            dur = max(0.0, args.job_ms / 1000.0 * self._rng.uniform(1 - args.job_jitter, 1 + args.job_jitter))
            time.sleep(dur)
            t1 = time.time()
            failed = self._rng.random() < args.fail_pct / 100.0
            self.jobs_done += 1
            self.busy_s += t1 - t0
            job_times[job_id] = (self.node_id, t0, t1)
            self.status = "idle"
            metrics = ExecutionMetrics(job_id=job_id, gpu_name="bench", gpu_memory_total=0, gpu_memory_used=0,
                                       cpu_percent=0.0, memory_percent=0.0, runtime_seconds=t1 - t0,
                                       timestamp=datetime.utcnow().isoformat()).to_dict()
            return {"job_id": job_id, "status": "failed" if failed else "success", "return_code": 1 if failed else 0,
                    "runtime_seconds": t1 - t0, "stdout": "" if failed else "bench\n",
                    "stderr": "simulated failure" if failed else "", "metrics": metrics, "usage": None,
                    "stages": [], "timestamp": datetime.utcnow().isoformat() + "Z"}

    # ---- tenants and their API keys (real signup path, plan chosen by --plan)
    tenants = []
    for i in range(args.tenants):
        r = mgmt.signup_customer(f"Bench Org {i}", f"Bench {i}", f"bench{i}-{os.getpid()}@bench.test", "bench-password-1", plan=args.plan)
        tenants.append({"org_id": r["organization"]["org_id"], "key": r["api_key"]["secret"], "nodes": []})

    # ---- nodes: owned by tenants round-robin, heartbeating like real workers
    nodes = []
    t_nodes = time.time()
    for i in range(args.nodes):
        node = BenchNode(f"bench-{i:04d}")
        node.org_id = tenants[i % args.tenants]["org_id"]
        co.register_agent(node)
        node.start_heartbeat(co, interval=args.hb_interval)
        tenants[i % args.tenants]["nodes"].append(node.node_id)
        nodes.append(node)
    node_setup_s = time.time() - t_nodes

    # ---- instrumentation: lock wait/hold, hot method timings, SQL cost
    locks = []

    def wrap_lock(path, group=None):
        obj = None
        try:
            parts = path.split(".")
            obj = {"coordinator": co, "control_plane": cp, "management": mgmt}[parts[0]]
            for p in parts[1:-1]:
                obj = getattr(obj, p)
            inner = getattr(obj, parts[-1])
            tl = TimedLock(path, inner, group)
            setattr(obj, parts[-1], tl)
            locks.append(tl)
            return True
        except Exception:
            return False

    wanted = ["coordinator.jobs_lock", "coordinator.receipts_lock", "coordinator.replica_receipts_lock",
              "coordinator._quarantine_lock", "coordinator.registry._lock", "coordinator.communication._lock",
              "coordinator.event_bus._lock", "coordinator.telemetry._lock", "control_plane.db._lock",
              "management.db._lock"]
    instrumented = [] if args.no_instrument else [p for p in wanted if wrap_lock(p)]
    for i in range(0 if args.no_instrument else len(api_v1._IDEMPOTENCY_LOCKS)):
        tl = TimedLock(f"api.idempotency[{i}]", api_v1._IDEMPOTENCY_LOCKS[i], group="api.idempotency (64 stripes)")
        api_v1._IDEMPOTENCY_LOCKS[i] = tl
        locks.append(tl)
    if not args.no_instrument:
        instrumented.append("api.idempotency (64 stripes)")

    methods = {}

    def wrap_method(obj, name, label):
        if not hasattr(obj, name):
            return
        orig = getattr(obj, name)
        d = methods.setdefault(label, ThreadDist())

        def timed(*a, **k):
            t = perf()
            try:
                return orig(*a, **k)
            finally:
                d.add(perf() - t)
        setattr(obj, name, timed)

    if not args.no_instrument:
        for name in ("submit_job", "assign_job", "receive_heartbeat", "_commit_receipt_verification"):
            wrap_method(co, name, f"coordinator.{name}")
        for name in ("execute", "executemany", "query", "query_one"):
            wrap_method(cp.db, name, f"sqlite.{name}")

    # ---- the app, plus bench-only control routes (localhost, no auth)
    app = api_v1.create_api_v1_app(mgmt, pres)
    routes = sorted({f"{m} {r.path}" for r in app.routes if hasattr(r, "methods") for m in r.methods
                     if m not in ("HEAD", "OPTIONS") and not r.path.startswith(("/docs", "/redoc", "/openapi"))})

    samples = {"t": [], "cpu": [], "rss_mb": [], "threads": [], "busy_nodes": []}
    stop_sampler = threading.Event()
    try:
        import psutil
        proc = psutil.Process()
        proc.cpu_percent()
    except Exception:
        psutil = None
        proc = None

    def sampler():
        while not stop_sampler.wait(0.5):
            samples["t"].append(time.time())
            samples["busy_nodes"].append(sum(1 for n in nodes if n.status == "busy"))
            if proc is not None:
                samples["cpu"].append(proc.cpu_percent())
                samples["rss_mb"].append(proc.memory_info().rss / 1e6)
                samples["threads"].append(proc.num_threads())

    def snapshot_jobs():
        for _ in range(5):
            try:
                return list(co.jobs.items())
            except RuntimeError:           # dict changed size during copy; try again
                time.sleep(0.001)
        return []

    @app.get("/__bench/progress", include_in_schema=False)
    def bench_progress():
        by = {}
        for _jid, j in snapshot_jobs():
            s = j.get("status", "?")
            by[s] = by.get(s, 0) + 1
        terminal = sum(v for k, v in by.items() if k in TERMINAL)
        return {"total": sum(by.values()), "by_status": by, "terminal": terminal,
                "nonterminal": sum(by.values()) - terminal, "executed": len(job_times)}

    @app.get("/__bench/scheduler", include_in_schema=False)
    def bench_scheduler():
        st = co.scheduler_stats
        try:
            data = st.snapshot() if hasattr(st, "snapshot") else {k: str(v)[:300] for k, v in vars(st).items()}
        except Exception as e:
            data = {"error": repr(e)}
        return {"stats": data, "queue": co.job_queue.qsize(), "available_nodes": len(co.registry.available_nodes()),
                "nodes": len(co.registry.nodes), "paused": co.scheduler_paused,
                "scheduler_alive": getattr(getattr(co, "scheduler_thread", None), "is_alive", lambda: None)()}

    @app.get("/__bench/info", include_in_schema=False)
    def bench_info():
        return {"routes": routes, "tenants": tenants, "locks_instrumented": instrumented, "node_setup_s": node_setup_s,
                "pid": os.getpid()}

    @app.get("/__bench/stats", include_in_schema=False)
    def bench_stats():
        jobs = snapshot_jobs()
        q_wait, exec_t, e2e = [], [], []
        for jid, j in jobs:
            jt = job_times.get(jid)
            sub, done = parse_iso(j.get("created_at") or ""), parse_iso(j.get("completed_at") or "")
            if jt and sub is not None:
                q_wait.append(max(0.0, jt[1] - sub))
                exec_t.append(jt[2] - jt[1])
                if done is not None:
                    e2e.append(max(0.0, done - sub))
        by = {}
        for _jid, j in jobs:
            by[j.get("status", "?")] = by.get(j.get("status", "?"), 0) + 1
        span = (max((v[2] for v in job_times.values()), default=0) - min((v[1] for v in job_times.values()), default=0)) or 1e-9
        per_node = [{"node": n.node_id, "jobs": n.jobs_done, "busy_s": n.busy_s} for n in nodes]
        receipts = None
        try:
            receipts = len(co.receipts)
        except Exception:
            pass
        return {
            "queue_wait_s": q_wait, "exec_s": exec_t, "e2e_s": e2e, "by_status": by, "receipts": receipts,
            "first_start": min((v[1] for v in job_times.values()), default=None),
            "last_end": max((v[2] for v in job_times.values()), default=None), "exec_span_s": span,
            "nodes": per_node, "locks": lock_report(locks),
            "methods": {k: v.summary() for k, v in methods.items()},
            "samples": samples,
            "inflight_after": getattr(co, "count_inflight_jobs", lambda *_: None)(tenants[0]["org_id"]) if tenants else None,
        }

    @app.on_event("startup")
    async def _tune():
        import anyio.to_thread
        anyio.to_thread.current_default_thread_limiter().total_tokens = args.api_threads

    threading.Thread(target=sampler, daemon=True).start()
    cfg = uvicorn.Config(app, host="127.0.0.1", port=args.port, log_level="warning", backlog=4096,
                         timeout_keep_alive=30, access_log=False)
    server = uvicorn.Server(cfg)
    try:
        signal.signal(signal.SIGTERM, lambda *_: setattr(server, "should_exit", True))
    except Exception:
        pass
    open(args.ready_file, "w").write("ready")
    server.run()
    stop_sampler.set()


# =============================================================================
# CLIENT SIDE: bots, phases, report
# =============================================================================

_ID_SEGMENT = {"jobs": "{id}", "nodes": "{id}", "receipts": "{id}", "invoices": "{id}", "api-keys": "{id}"}


def templ(method, path):
    """'GET /jobs/job_abc/attempts' -> 'GET /jobs/{id}/attempts' so every id lands in one row."""
    path = path.split("?", 1)[0]
    parts = path.split("/")
    for i in range(1, len(parts) - 1):
        if parts[i] in _ID_SEGMENT and parts[i + 1] and parts[i + 1] not in ("clear",):
            parts[i + 1] = "{id}"
    return f"{method} {'/'.join(parts)}"


class Api:
    """One keep-alive HTTP connection per thread; every call is timed and recorded."""

    def __init__(self, port):
        self.port = port
        self.tl = threading.local()
        self.lat = {}                  # label -> ThreadDist
        self.status = {}               # label -> {code: n}   (per-thread merged at the end)
        self._st = []
        self._reg = threading.Lock()
        self.dist_lock = threading.Lock()

    def _conn(self):
        c = getattr(self.tl, "c", None)
        if c is None:
            c = self.tl.c = http.client.HTTPConnection("127.0.0.1", self.port, timeout=60)
        return c

    def _stat(self):
        s = getattr(self.tl, "s", None)
        if s is None:
            s = self.tl.s = {}
            with self._reg:
                self._st.append(s)
        return s

    def _dist(self, label):
        d = self.lat.get(label)
        if d is None:
            with self.dist_lock:
                d = self.lat.setdefault(label, ThreadDist())
        return d

    def call(self, method, path, key=None, body=None, label=None, headers=None):
        label = label or templ(method, path)
        h = {"Content-Type": "application/json"}
        if key:
            h["Authorization"] = f"Bearer {key}"
        if headers:
            h.update(headers)
        data = json.dumps(body) if body is not None else None
        t0 = perf()
        code, raw = 0, b""
        try:
            c = self._conn()
            c.request(method, path, body=data, headers=h)
            r = c.getresponse()
            raw = r.read()
            code = r.status
        except Exception:
            try:
                self.tl.c.close()
            except Exception:
                pass
            self.tl.c = None
        dt = perf() - t0
        self._dist(label).add(dt)
        st = self._stat().setdefault(label, {})
        st[code] = st.get(code, 0) + 1
        return code, raw, dt

    def json(self, *a, **k):
        code, raw, dt = self.call(*a, **k)
        try:
            return code, json.loads(raw.decode() or "null")
        except Exception:
            return code, None

    def merged_status(self):
        out = {}
        for s in self._st:
            for label, codes in s.items():
                o = out.setdefault(label, {})
                for c, n in codes.items():
                    o[c] = o.get(c, 0) + n
        return out


class Load:
    def __init__(self, args, api, tenants, routes):
        self.args, self.api, self.tenants = args, api, tenants
        self.routes = set(routes)
        self.accepted = []                       # (job_id, tenant_idx)
        self.accepted_lock = threading.Lock()
        self.seq = itertools.count()
        self.seq_lock = threading.Lock()
        self.send_lag = ThreadDist()
        self.stop = threading.Event()
        self.submit_done = threading.Event()
        self.t0 = None
        self.cancel_q = []
        self.cancel_lock = threading.Lock()
        self.rejected = 0
        self.submitted_n = 0
        self.run_id = f"{os.getpid()}-{int(time.time())}"
        self.bot_errors = []
        self.receipt_ids = []
        self.key_of = lambda ti: tenants[ti]["key"]

    def guard(self, fn, *a):
        """Run a bot; a crash is recorded and reported instead of silently shrinking the load."""
        def run():
            try:
                fn(*a)
            except Exception as e:
                import traceback
                self.bot_errors.append(f"{fn.__name__}: {e!r}")
                traceback.print_exc()
        return run

    def has(self, route):
        return route in self.routes

    # ---- bots ----------------------------------------------------------------
    def submitter(self, bot_id):
        a = self.args
        rnd = random.Random(a.seed + bot_id)
        while not self.stop.is_set():
            with self.seq_lock:
                i = next(self.seq)
            if i >= a.jobs:
                break
            if a.rate > 0:
                due = self.t0 + i / a.rate
                now = perf()
                if due > now:
                    time.sleep(due - now)
                    self.send_lag.add(0.0)
                else:
                    self.send_lag.add(now - due)       # the generator itself is behind schedule
            ti = i % len(self.tenants)
            code, doc = self.api.json("POST", "/jobs", self.key_of(ti), {"command": "echo bench", "client_reference": f"b{i}"},
                                          label="POST /jobs", headers={"Idempotency-Key": f"bench-{self.run_id}-{i}"})
            if code in (200, 201, 202) and isinstance(doc, dict) and doc.get("job_id"):
                with self.accepted_lock:
                    self.accepted.append((doc["job_id"], ti))
                if a.cancel_pct and rnd.random() < a.cancel_pct / 100.0:
                    with self.cancel_lock:
                        self.cancel_q.append((doc["job_id"], ti, perf() + rnd.uniform(0.0, 0.3)))
            else:
                self.rejected += 1
            self.submitted_n += 1

    def canceller(self):
        while not self.stop.is_set():
            item = None
            with self.cancel_lock:
                if self.cancel_q and self.cancel_q[0][2] <= perf():
                    item = self.cancel_q.pop(0)
            if item is None:
                time.sleep(0.02)
                continue
            self.api.call("POST", f"/jobs/{item[0]}/cancel", self.key_of(item[1]), label="POST /jobs/{id}/cancel")

    def poller(self, bot_id):
        """A customer waiting for a result: polls its own job until it is terminal."""
        rnd = random.Random(self.args.seed + 1000 + bot_id)
        mine = {}
        while not self.stop.is_set():
            with self.accepted_lock:
                n = len(self.accepted)
            if n == 0:
                time.sleep(0.05)
                continue
            while len(mine) < 8:
                with self.accepted_lock:
                    jid, ti = self.accepted[rnd.randrange(len(self.accepted))]
                if rnd.random() * 100 < self.args.poll_pct * 4:
                    mine[jid] = ti
                else:
                    break
            for jid, ti in list(mine.items()):
                code, doc = self.api.json("GET", f"/jobs/{jid}", self.key_of(ti), label="GET /jobs/{id}")
                if code in (404,) or (isinstance(doc, dict) and doc.get("status") in TERMINAL):
                    mine.pop(jid, None)
            time.sleep(0.25)

    def reader(self, bot_id, per_bot_rps):
        rnd = random.Random(self.args.seed + 2000 + bot_id)
        plain = [("GET /cluster", 3), ("GET /health", 3), ("GET /metrics", 2), ("GET /nodes", 3), ("GET /jobs", 5),
                 ("GET /account/plan", 2), ("GET /billing/usage", 1), ("GET /billing/pricing", 1), ("GET /billing/invoices", 1),
                 ("GET /org/shared-pool", 1), ("GET /receipts", 3), ("GET /artifacts", 1), ("GET /workflows", 1),
                 ("GET /whoami", 2), ("GET /auth/api-keys", 1), ("GET /telemetry/events", 1), ("GET /enroll/ca", 1)]
        plain = [(r, w) for r, w in plain if self.has(r)]
        by_id = [("GET /jobs/{id}", 4), ("GET /jobs/{id}/attempts", 1), ("GET /receipts/{id}", 0)]
        node_ids = [n for t in self.tenants for n in t["nodes"]]
        choices, weights = zip(*(plain + [("GET /nodes/{id}", 1), ("GET /nodes/{id}/enrollment-history", 1),
                                          ("GET /jobs/{id}", 3), ("GET /jobs/{id}/attempts", 1),
                                          ("GET /receipts/{id}", 2), ("POST /workflows", 1), ("KEYS", 1)]))
        interval = 1.0 / per_bot_rps if per_bot_rps > 0 else 0
        nxt = perf()
        while not self.stop.is_set():
            if interval:
                nxt += interval
                d = nxt - perf()
                if d > 0:
                    time.sleep(d)
            ti = rnd.randrange(len(self.tenants))
            key = self.key_of(ti)
            ep = rnd.choices(choices, weights)[0]
            if ep == "GET /nodes/{id}" and node_ids:
                nid = rnd.choice(self.tenants[ti]["nodes"] or node_ids)
                self.api.call("GET", f"/nodes/{nid}", key, label=ep)
            elif ep == "GET /nodes/{id}/enrollment-history" and node_ids:
                nid = rnd.choice(self.tenants[ti]["nodes"] or node_ids)
                self.api.call("GET", f"/nodes/{nid}/enrollment-history", key, label=ep)
            elif ep in ("GET /jobs/{id}", "GET /jobs/{id}/attempts"):
                mine = None
                with self.accepted_lock:
                    if self.accepted:
                        mine = self.accepted[rnd.randrange(len(self.accepted))]
                if mine:
                    suffix = "" if ep == "GET /jobs/{id}" else "/attempts"
                    self.api.call("GET", f"/jobs/{mine[0]}{suffix}", self.key_of(mine[1]), label=ep)
            elif ep == "GET /receipts":
                code, doc = self.api.json("GET", "/receipts", key, label=ep)
                if code == 200 and isinstance(doc, list):
                    for r in doc[:20]:
                        if isinstance(r, dict) and r.get("receipt_id") and len(self.receipt_ids) < 2000:
                            self.receipt_ids.append((r["receipt_id"], ti))
            elif ep == "GET /receipts/{id}":
                if self.receipt_ids:
                    rid, rti = self.receipt_ids[rnd.randrange(len(self.receipt_ids))]
                    self.api.call("GET", f"/receipts/{rid}", self.key_of(rti), label=ep)
            elif ep == "POST /workflows":
                n = rnd.randrange(10 ** 9)
                self.api.call("POST", "/workflows", key, {"name": f"wf{n}", "jobs": [
                    {"job_id": "a", "command": "echo a"}, {"job_id": "b", "command": "echo b", "depends_on": ["a"]}]},
                    label=ep)
            elif ep == "KEYS":
                code, doc = self.api.json("POST", "/auth/api-keys", key, {"name": f"bench-{rnd.randrange(10**9)}"}, label="POST /auth/api-keys")
                if code in (200, 201) and isinstance(doc, dict) and doc.get("key_id"):
                    self.api.call("DELETE", f"/auth/api-keys/{doc['key_id']}", key, label="DELETE /auth/api-keys/{id}")
            else:
                path = ep.split(" ", 1)[1]
                self.api.call("GET", path, key if ep != "GET /enroll/ca" else None, label=ep)

    def login_bot(self, per_bot_rps):
        """Password verification is deliberately slow (CPU heavy); include a trickle of it."""
        interval = 1.0 / per_bot_rps
        nxt = perf()
        i = 0
        while not self.stop.is_set():
            nxt += interval
            d = nxt - perf()
            if d > 0:
                time.sleep(d)
            self.api.call("POST", "/auth/login", None, {"email": self.args.login_email, "password": "bench-password-1"}, label="POST /auth/login")
            i += 1


def smoke(api, load, routes, tenants):
    """One call to every route (so coverage is real, not assumed). Returns {route: status}."""
    key = tenants[0]["key"]
    seen = {}
    ok, doc = api.json("POST", "/jobs", key, {"command": "echo smoke", "client_reference": "smoke-1"}, label="POST /jobs")
    jid = doc.get("job_id") if isinstance(doc, dict) else None
    code, wf = api.json("POST", "/workflows", key, {"name": "smoke", "jobs": [{"job_id": "a", "command": "echo a"}]}, label="POST /workflows")
    kcode, k = api.json("POST", "/auth/api-keys", key, {"name": "smoke"}, label="POST /auth/api-keys")
    rcode, rec = api.json("GET", "/receipts", key, label="GET /receipts")
    receipt_id = rec[0].get("receipt_id") if isinstance(rec, list) and rec else "rcpt_bench_missing"
    inv_code, invs = api.json("GET", "/billing/invoices", key, label="GET /billing/invoices")
    inv_id = invs[0].get("invoice_id") if isinstance(invs, list) and invs else "inv_bench_missing"      # none issued yet: still hits the route (404)
    nid = tenants[0]["nodes"][0] if tenants[0]["nodes"] else None
    plan = [("GET", "/cluster"), ("GET", "/health"), ("GET", "/metrics"), ("GET", "/nodes"), ("GET", "/jobs"), ("GET", "/workflows"),
            ("GET", "/artifacts"), ("GET", "/telemetry/events"), ("GET", "/account/plan"), ("GET", "/billing/pricing"),
            ("GET", "/billing/usage"), ("GET", "/org/shared-pool"), ("GET", "/whoami"), ("GET", "/auth/api-keys")]
    if nid:
        plan += [("GET", f"/nodes/{nid}"), ("GET", f"/nodes/{nid}/enrollment-history")]
    if jid:
        plan += [("GET", f"/jobs/{jid}"), ("GET", f"/jobs/{jid}/attempts"), ("POST", f"/jobs/{jid}/cancel"), ("POST", f"/jobs/{jid}/retry")]
    if receipt_id:
        plan.append(("GET", f"/receipts/{receipt_id}"))
    if inv_id:
        plan.append(("GET", f"/billing/invoices/{inv_id}"))
    seen["POST /jobs"], seen["POST /workflows"], seen["POST /auth/api-keys"] = ok, code, kcode
    seen["GET /receipts"], seen["GET /billing/invoices"] = rcode, inv_code
    for method, path in plan:
        c, _, _ = api.call(method, path, key, {} if method == "POST" else None)
        seen[f"{method} {path}"] = c
    seen["GET /enroll/ca"] = api.call("GET", "/enroll/ca")[0]
    if kcode in (200, 201) and isinstance(k, dict) and k.get("key_id"):
        seen["DELETE /auth/api-keys/{id}"] = api.call("DELETE", f"/auth/api-keys/{k['key_id']}", key)[0]
    # account lifecycle on a throwaway tenant: signup -> login -> change password -> delete account
    email = f"smoke-{int(time.time())}@bench.test"
    pw = "smoke-password-1"
    c, s = api.json("POST", "/auth/signup", None, {"org_name": "Smoke Org", "name": "Smoke", "email": email, "password": pw})
    seen["POST /auth/signup"] = c
    if c == 200 and isinstance(s, dict):
        c2, l = api.json("POST", "/auth/login", None, {"email": email, "password": pw})
        seen["POST /auth/login"] = c2
        sk = (l or s).get("api_key", {}).get("secret") if isinstance((l or s), dict) else None
        if sk:
            c3, _, _ = api.call("POST", "/auth/change-password", sk, {"current_password": pw, "new_password": pw + "x"})
            seen["POST /auth/change-password"] = c3
            c4, _, _ = api.call("POST", "/auth/delete-account", sk, {"password": pw + "x", "org_name": "Smoke Org"})
            seen["POST /auth/delete-account"] = c4
    return seen


def run_client(args, port, info):
    tenants = info["tenants"]
    routes = info["routes"]
    api = Api(port)
    load = Load(args, api, tenants, routes)
    load.args.login_email = f"bench0-{info['pid']}@bench.test"

    print(f"[bench] smoke sweep over {len(routes)} routes ...", flush=True)
    swept = smoke(api, load, routes, tenants)
    bad_smoke = {r: c for r, c in swept.items() if c >= 500 or c == 0}
    if args.smoke:
        return {"smoke": swept, "routes": routes, "bad": bad_smoke}, load, api

    def progress():
        code, doc = api.json("GET", "/__bench/progress", label="(bench) progress")
        return doc or {"total": 0, "terminal": 0, "by_status": {}}

    base = progress()
    base_executed = base["executed"]
    threads = []
    load.t0 = perf() + 0.2
    wall0 = time.time()
    for i in range(args.submit_bots):
        threads.append(threading.Thread(target=load.guard(load.submitter, i), daemon=True))
    for i in range(args.pollers):
        threads.append(threading.Thread(target=load.guard(load.poller, i), daemon=True))
    if args.cancel_pct:
        threads.append(threading.Thread(target=load.guard(load.canceller), daemon=True))
    per_bot = args.read_rps / max(1, args.reader_bots) if args.read_rps else 0
    for i in range(args.reader_bots):
        threads.append(threading.Thread(target=load.guard(load.reader, i, per_bot), daemon=True))
    if args.login_rps:
        threads.append(threading.Thread(target=load.guard(load.login_bot, args.login_rps), daemon=True))
    for t in threads:
        t.start()

    series = []                                      # (t_rel, accepted, finished-by-nodes, running)
    cpu_me = []
    try:
        import psutil
        me = psutil.Process()
        me.cpu_percent()
    except Exception:
        me = None
    print(f"[bench] load running: {args.jobs} jobs @ {'max' if not args.rate else args.rate} /s, "
          f"{args.nodes} nodes, {args.submit_bots} submit bots, {args.reader_bots} readers ...", flush=True)
    submit_end = None
    deadline = None
    last_print = 0
    while True:
        time.sleep(0.5)
        now = time.time() - wall0
        prog = progress()
        with load.accepted_lock:
            acc = len(load.accepted)
        series.append((now, acc, prog["executed"] - base_executed, prog["by_status"].get("running", 0)))
        cpu_me.append(me.cpu_percent() if me else 0.0)
        submitters_alive = any(t.is_alive() for t in threads[:args.submit_bots])
        if (load.submitted_n >= args.jobs or not submitters_alive) and submit_end is None:
            submit_end = now
            deadline = now + args.drain_timeout
        if now - last_print >= 2:
            last_print = now
            print(f"[bench] t={now:6.1f}s  submitted={load.submitted_n:6d}  accepted={acc:6d}  finished={prog['executed'] - base_executed:6d}  "
                  f"running={prog['by_status'].get('running', 0):4d}", flush=True)
        if submit_end is not None:
            if prog["nonterminal"] == 0:
                break
            if now > deadline:
                print("[bench] drain timeout reached; reporting what finished", flush=True)
                break
        if now > args.max_seconds:
            print("[bench] --max-seconds reached", flush=True)
            break
    load.stop.set()
    for t in threads:
        t.join(timeout=5)
    wall = time.time() - wall0
    print("[bench] final sweep over every route (receipts exist now) ...", flush=True)
    final = smoke(api, load, routes, tenants)
    swept.update(final)
    bad = {r: c for r, c in final.items() if c >= 500 or c == 0}
    bad_smoke.update(bad)
    return {"series": series, "wall": wall, "submit_end": submit_end, "swept": swept, "bad_smoke": bad_smoke,
            "client_cpu": cpu_me}, load, api


def build_report(args, run, load, api, stats, info):
    series = run["series"]
    wall = run["wall"]
    accepted = len(load.accepted)
    submit_end = run["submit_end"] or wall
    comp = [(t, d) for t, _, d, _ in series]
    done_total = comp[-1][1] if comp else 0

    def rate_between(i0, i1):
        if i1 <= i0:
            return 0.0
        return (comp[i1][1] - comp[i0][1]) / max(1e-9, comp[i1][0] - comp[i0][0])

    peak_done = max((rate_between(max(0, i - 2), i) for i in range(1, len(comp))), default=0.0)
    acc_series = [(t, a) for t, a, _, _ in series]
    peak_acc = max(((acc_series[i][1] - acc_series[max(0, i - 2)][1]) / max(1e-9, acc_series[i][0] - acc_series[max(0, i - 2)][0])
                    for i in range(1, len(acc_series))), default=0.0)
    first_done_t = next((t for t, d in comp if d > 0), None)
    active = max(1e-9, (comp[-1][0] - first_done_t)) if first_done_t is not None and comp else wall

    # throughput while jobs were still being submitted (the steady-state number), apart from the drain tail
    in_window = [(t, d) for t, d in comp if t <= submit_end]
    sustained = ((in_window[-1][1] - in_window[0][1]) / max(1e-9, in_window[-1][0] - in_window[0][0])) if len(in_window) > 1 else 0.0

    endpoints = {}
    sts = api.merged_status()
    for label, d in api.lat.items():
        if label.startswith("(bench)"):
            continue
        s = d.summary()
        codes = sts.get(label, {})
        total = sum(codes.values()) or 1
        endpoints[label] = {**s, "rps": s["n"] / wall, "status": {str(k): v for k, v in sorted(codes.items())},
                            "err5xx_pct": 100.0 * sum(v for k, v in codes.items() if k >= 500) / total,
                            "conn_fail_pct": 100.0 * codes.get(0, 0) / total}

    nodes = stats["nodes"]
    busy = [n["busy_s"] for n in nodes]
    jobs_per = [n["jobs"] for n in nodes]
    window = max(1e-9, stats["exec_span_s"])
    util = [100.0 * b / window for b in busy]
    mean = sum(jobs_per) / len(jobs_per) if jobs_per else 0
    var = (sum((x - mean) ** 2 for x in jobs_per) / len(jobs_per)) ** 0.5 if jobs_per else 0
    smp = stats["samples"]
    srv = {"cpu_mean_pct": (sum(smp["cpu"]) / len(smp["cpu"]) if smp["cpu"] else None), "cpu_peak_pct": max(smp["cpu"], default=None),
           "rss_peak_mb": max(smp["rss_mb"], default=None), "threads_peak": max(smp["threads"], default=None),
           "busy_nodes_peak": max(smp["busy_nodes"], default=0)}

    by = stats["by_status"]
    terminal_n = sum(v for k, v in by.items() if k in TERMINAL)
    stuck = sum(v for k, v in by.items() if k not in TERMINAL)
    lag = load.send_lag.summary()
    notes = []
    ccpu = run.get("client_cpu") or []
    client_cpu = sum(ccpu) / len(ccpu) if ccpu else None
    if args.rate and lag["p95"] > 0.1:
        acc_rate = accepted / max(1e-9, submit_end)
        if client_cpu is not None and client_cpu >= 85:
            notes.append(f"the load generator was CPU-bound ({client_cpu:.0f}% of a core): the offered rate was not sustained "
                         f"({acc_rate:.0f}/s vs {args.rate:.0f}/s target). Add --submit-bots or run the bots on a second machine")
        else:
            notes.append(f"the SERVER could not keep up with the offered rate: it accepted {acc_rate:.0f} jobs/s against a target of "
                         f"{args.rate:.0f}/s (bots spent their time waiting for responses; generator CPU {client_cpu:.0f}% )"
                         if client_cpu is not None else
                         f"the server accepted {acc_rate:.0f} jobs/s against a target of {args.rate:.0f}/s")
    if stuck:
        notes.append(f"{stuck} accepted jobs were still not terminal when the run ended (drain timeout or lost jobs)")
    if run["bad_smoke"]:
        notes.append(f"smoke sweep got 5xx/connection failures on: {sorted(run['bad_smoke'])}")
    five = {k: v["err5xx_pct"] for k, v in endpoints.items() if v["err5xx_pct"] > 0}
    if five:
        notes.append(f"5xx responses on: {five}")
    if load.bot_errors:
        notes.append(f"{len(load.bot_errors)} bot threads crashed, so the offered load was lower than configured: {load.bot_errors[:3]}")
    if load.rejected:
        notes.append(f"{load.rejected} job submissions were not accepted (see POST /jobs status counts)")
    if stats.get("receipts") is not None and stats["receipts"] < by.get("completed", 0):
        notes.append(f"receipts ({stats['receipts']}) < completed jobs ({by.get('completed', 0)})")
    uncovered = [r for r in info["routes"] if r not in {f"{k.split(' ')[0]} {k.split(' ', 1)[1]}" for k in endpoints}
                 and r not in {k for k in run["swept"]}]

    def norm(r):
        import re
        return re.sub(r"\{[^}]+\}", "{id}", r)
    hit = {norm(k) for k in endpoints} | {norm(k) for k in run["swept"]}
    uncovered = [r for r in info["routes"] if norm(r) not in hit and r not in
                 ("POST /auth/forgot-password", "POST /auth/reset-password", "POST /jobs/clear")]
    return {
        "config": {k: v for k, v in vars(args).items() if k not in ("serve", "ready_file", "workdir", "compare", "login_email")},
        "env": {"cpus": os.cpu_count(), "python": sys.version.split()[0], "platform": sys.platform},
        "throughput": {"wall_s": wall, "jobs_submitted": load.submitted_n, "jobs_accepted": accepted, "jobs_rejected": load.rejected,
                       "accept_per_s_mean": accepted / max(1e-9, submit_end), "accept_per_s_peak": peak_acc,
                       "complete_per_s_mean": done_total / active, "complete_per_s_peak": peak_done, "complete_per_s_during_submit": sustained,
                       "drain_s_after_last_submit": max(0.0, wall - submit_end), "job_status": by, "max_jobs_in_memory": args.max_jobs_in_memory},
        "job_latency": {"queue_wait": dist_summary(stats["queue_wait_s"]), "execution": dist_summary(stats["exec_s"]),
                        "end_to_end": dist_summary(stats["e2e_s"])},
        "generator": {"send_lag": lag, "submit_bots": args.submit_bots, "reader_bots": args.reader_bots, "cpu_mean_pct": client_cpu},
        "endpoints": endpoints,
        "nodes": {"count": len(nodes), "jobs_per_node": {"min": min(jobs_per, default=0), "max": max(jobs_per, default=0), "mean": mean, "stdev": var},
                  "utilisation_pct": {"mean": (sum(util) / len(util) if util else 0), "min": min(util, default=0), "max": max(util, default=0)}},
        "server": srv,
        "locks": stats["locks"], "methods": stats["methods"], "locks_instrumented": info["locks_instrumented"],
        "integrity": {"stuck_jobs": stuck, "receipts": stats.get("receipts"), "completed": by.get("completed", 0)},
        "coverage": {"routes": len(info["routes"]), "uncovered": uncovered, "smoke": run["swept"]},
        "series": [(round(t, 2), a, d, r) for t, a, d, r in series],
        "notes": notes,
    }


def spark(values, width=60):
    if not values:
        return ""
    step = max(1, len(values) // width)
    v = [sum(values[i:i + step]) / len(values[i:i + step]) for i in range(0, len(values), step)]
    hi = max(v) or 1
    bars = " .:-=+*#%@"
    return "".join(bars[min(len(bars) - 1, int(x / hi * (len(bars) - 1)))] for x in v)


def print_report(rep):
    c, t = rep["config"], rep["throughput"]
    print("\n" + "=" * 100)
    print(f"GCON LOAD BENCHMARK  {c['nodes']} nodes | {c['jobs']} jobs | rate {'max' if not c['rate'] else str(c['rate']) + '/s'} | "
          f"job {c['job_ms']} ms | {c['tenants']} tenants | plan {c['plan']} | sync {c['sync']} | api threads {c['api_threads']}")
    print("=" * 100)
    print(f"\nTHROUGHPUT   wall {t['wall_s']:.1f}s | accepted {t['jobs_accepted']}/{t['jobs_submitted']} | status {t['job_status']}")
    print(f"  accepted/s  mean {t['accept_per_s_mean']:.1f}   peak {t['accept_per_s_peak']:.1f}")
    print(f"  finished/s  mean {t['complete_per_s_mean']:.1f}   peak {t['complete_per_s_peak']:.1f}   while submitting {t['complete_per_s_during_submit']:.1f}   "
          f"drain after last submit {t['drain_s_after_last_submit']:.1f}s   (finished = a node ran it to the end)")
    done = [d for _, _, d, _ in rep["series"]]
    acc = [a for _, a, _, _ in rep["series"]]
    print(f"  accepted  {spark(acc)}")
    print(f"  finished  {spark(done)}")

    jl = rep["job_latency"]
    print("\nJOB LATENCY (ms)   n      mean    p50     p95     p99     max")
    for k, label in (("queue_wait", "queue wait (submit->start)"), ("execution", "execution"), ("end_to_end", "end-to-end (submit->completed)")):
        d = jl[k]
        print(f"  {label:32s} {d['n']:5d} {ms(d['mean']):>7} {ms(d['p50']):>7} {ms(d['p95']):>7} {ms(d['p99']):>7} {ms(d['max']):>7}")

    print("\nAPI LATENCY (ms) per endpoint, as a client sees it (includes queueing for a server thread)")
    rows = []
    for label, e in sorted(rep["endpoints"].items(), key=lambda kv: -kv[1]["n"]):
        rows.append([label, e["n"], f"{e['rps']:.1f}", ms(e["mean"]), ms(e["p50"]), ms(e["p95"]), ms(e["p99"]), ms(e["max"]),
                     f"{e['err5xx_pct']:.1f}%", " ".join(f"{k}:{v}" for k, v in e["status"].items())])
    print(table(["endpoint", "n", "rps", "mean", "p50", "p95", "p99", "max", "5xx", "status codes"], rows))

    n, s = rep["nodes"], rep["server"]
    print(f"\nNODES   {n['count']} nodes | jobs/node min {n['jobs_per_node']['min']} max {n['jobs_per_node']['max']} "
          f"mean {n['jobs_per_node']['mean']:.1f} stdev {n['jobs_per_node']['stdev']:.1f} | utilisation mean {n['utilisation_pct']['mean']:.0f}% "
          f"(min {n['utilisation_pct']['min']:.0f}% max {n['utilisation_pct']['max']:.0f}%) | peak busy {s['busy_nodes_peak']}")
    if s["cpu_mean_pct"] is not None:
        print(f"SERVER  cpu mean {s['cpu_mean_pct']:.0f}% peak {s['cpu_peak_pct']:.0f}% (100% = one core) | rss peak {s['rss_peak_mb']:.0f} MB | threads peak {s['threads_peak']}")

    print("\nLOCK CONTENTION  (wait = time a thread blocked to get the lock; hold = time the lock was held)")
    rows = []
    for name, l in sorted(rep["locks"].items(), key=lambda kv: -kv[1]["wait_total_s"]):
        rows.append([name, l["acquisitions"], f"{l['contended_pct']:.1f}%", f"{l['wait_total_s']:.2f}", f"{l['wait_mean_ms']:.2f}",
                     f"{l['wait_p99_ms']:.1f}", f"{l['wait_max_ms']:.1f}", f"{l['hold_total_s']:.2f}", f"{l['hold_mean_ms']:.3f}", f"{l['hold_max_ms']:.1f}"])
    print(table(["lock", "acquires", "contended", "wait tot s", "wait mean ms", "wait p99 ms", "wait max ms", "hold tot s", "hold mean ms", "hold max ms"], rows))
    print("  (wait tot s summed over all threads: it can exceed wall time. contended = share of acquisitions that had to block)")

    print("\nHOT PATHS (server-side, ms)      n       mean    p50     p95     p99     max   total s")
    for label, m in sorted(rep["methods"].items(), key=lambda kv: -kv[1]["total"]):
        print(f"  {label:30s} {m['n']:7d} {ms(m['mean']):>7} {ms(m['p50']):>7} {ms(m['p95']):>7} {ms(m['p99']):>7} {ms(m['max']):>7} {m['total']:>8.2f}")

    cv = rep["coverage"]
    print(f"\nCOVERAGE  {cv['routes']} routes on /api/v1; not exercised: {cv['uncovered'] or 'none'}"
          f"  (forgot/reset-password send email and /jobs/clear deletes history, both left out on purpose)")
    ig = rep["integrity"]
    print(f"INTEGRITY stuck jobs {ig['stuck_jobs']} | completed {ig['completed']} | receipts {ig['receipts']}")
    for note in rep["notes"]:
        print("  !! " + note)
    print()


def compare(a_path, b_path):
    a, b = json.load(open(a_path)), json.load(open(b_path))

    def row(name, x, y, higher_is_better, fmt="{:.2f}"):
        if x is None or y is None:
            return [name, "-", "-", "-"]
        d = (y - x) / x * 100 if x else 0.0
        good = (d > 0) == higher_is_better
        flag = "" if abs(d) < 3 else (" better" if good else " WORSE")
        return [name, fmt.format(x), fmt.format(y), f"{d:+.1f}%{flag}"]
    rows = [
        row("accepted/s mean", a["throughput"]["accept_per_s_mean"], b["throughput"]["accept_per_s_mean"], True),
        row("completed/s mean", a["throughput"]["complete_per_s_mean"], b["throughput"]["complete_per_s_mean"], True),
        row("completed/s peak", a["throughput"]["complete_per_s_peak"], b["throughput"]["complete_per_s_peak"], True),
        row("queue wait p95 ms", 1000 * a["job_latency"]["queue_wait"]["p95"], 1000 * b["job_latency"]["queue_wait"]["p95"], False),
        row("end-to-end p95 ms", 1000 * a["job_latency"]["end_to_end"]["p95"], 1000 * b["job_latency"]["end_to_end"]["p95"], False),
        row("end-to-end p99 ms", 1000 * a["job_latency"]["end_to_end"]["p99"], 1000 * b["job_latency"]["end_to_end"]["p99"], False),
    ]
    for ep in ("POST /jobs", "GET /jobs", "GET /jobs/{id}", "GET /cluster"):
        if ep in a["endpoints"] and ep in b["endpoints"]:
            rows.append(row(f"{ep} p95 ms", 1000 * a["endpoints"][ep]["p95"], 1000 * b["endpoints"][ep]["p95"], False))
    for name in sorted(set(a["locks"]) | set(b["locks"]), key=lambda n: -(b["locks"].get(n, {}).get("wait_total_s", 0))):
        la, lb = a["locks"].get(name), b["locks"].get(name)
        if la and lb and max(la["wait_total_s"], lb["wait_total_s"]) > 0.05:
            rows.append(row(f"lock wait total s: {name}", la["wait_total_s"], lb["wait_total_s"], False))
    print(table(["metric", a_path, b_path, "change"], rows))


# =============================================================================
# orchestration
# =============================================================================

def free_port():
    import socket
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


def parser():
    p = argparse.ArgumentParser(description="GCON load benchmark (real server + simulated nodes + traffic bots over HTTP)")
    g = p.add_argument_group("scale")
    g.add_argument("--nodes", type=int, default=200, help="simulated worker nodes (default 200)")
    g.add_argument("--jobs", type=int, default=5000, help="jobs the submitter bots send (default 5000)")
    g.add_argument("--rate", type=float, default=250.0, help="target job submissions per second, open loop; 0 = as fast as the bots can (default 250)")
    g.add_argument("--job-ms", type=float, default=100.0, help="simulated execution time per job (default 100)")
    g.add_argument("--job-jitter", type=float, default=0.3, help="execution time varies by +/- this fraction (default 0.3)")
    g.add_argument("--fail-pct", type=float, default=2.0, help="percent of jobs a node reports as failed (default 2)")
    g.add_argument("--tenants", type=int, default=4, help="customer organizations; nodes and traffic are spread across them (default 4)")
    g.add_argument("--plan", default="Enterprise", choices=["Standard", "Pro", "Enterprise"], help="plan of the tenants; Enterprise keeps plan limits out of the way (default)")
    b = p.add_argument_group("bots")
    b.add_argument("--submit-bots", type=int, default=16, help="concurrent submitter threads (default 16)")
    b.add_argument("--pollers", type=int, default=4, help="threads polling their own jobs until done (default 4)")
    b.add_argument("--poll-pct", type=float, default=10.0, help="rough share of jobs customers poll (default 10)")
    b.add_argument("--reader-bots", type=int, default=8, help="threads sending the mixed read/write API traffic (default 8)")
    b.add_argument("--read-rps", type=float, default=150.0, help="total mixed API requests per second (default 150)")
    b.add_argument("--cancel-pct", type=float, default=2.0, help="percent of jobs cancelled shortly after submit (default 2)")
    b.add_argument("--login-rps", type=float, default=2.0, help="logins per second (slow password hashing: CPU load) (default 2)")
    s = p.add_argument_group("server")
    s.add_argument("--api-threads", type=int, default=40, help="threads the API may use for requests (the framework default is 40)")
    s.add_argument("--sync", default="full", choices=["full", "normal"], help="SQLite synchronous mode; 'full' is what ships (default)")
    s.add_argument("--hb-interval", type=float, default=2.0, help="seconds between each node's heartbeat (default 2)")
    r = p.add_argument_group("run control")
    r.add_argument("--drain-timeout", type=float, default=180.0, help="seconds to wait for the queue to drain after the last submit")
    r.add_argument("--max-seconds", type=float, default=1800.0, help="hard cap on the whole run")
    r.add_argument("--max-jobs-in-memory", type=int, default=1_000_000,
                   help="GCON_MAX_JOBS_IN_MEMORY for the server (production default 5000 evicts finished jobs, which loses measurements)")
    r.add_argument("--seed", type=int, default=1)
    r.add_argument("--out", default="benchmark_report.json")
    r.add_argument("--smoke", action="store_true", help="only call every route once, then exit")
    r.add_argument("--compare", nargs=2, metavar=("BEFORE", "AFTER"), help="compare two JSON reports and exit")
    r.add_argument("--keep", action="store_true", help="keep the temporary database directory")
    r.add_argument("--no-instrument", action="store_true", help="do not wrap locks/methods (lock and hot-path tables will be empty); use it to see the cost of measuring")
    h = p.add_argument_group("internal")
    h.add_argument("--serve", action="store_true", help=argparse.SUPPRESS)
    h.add_argument("--port", type=int, default=0, help=argparse.SUPPRESS)
    h.add_argument("--workdir", default="", help=argparse.SUPPRESS)
    h.add_argument("--ready-file", default="", help=argparse.SUPPRESS)
    return p


def main():
    args = parser().parse_args()
    if args.compare:
        compare(*args.compare)
        return 0
    if args.serve:
        serve(args)
        return 0

    workdir = tempfile.mkdtemp(prefix="gcon-bench-")
    port = free_port()
    ready = os.path.join(workdir, "ready")
    cmd = [sys.executable, os.path.abspath(__file__), "--serve", "--port", str(port), "--workdir", workdir, "--ready-file", ready,
           "--nodes", str(args.nodes), "--tenants", str(args.tenants), "--plan", args.plan, "--job-ms", str(args.job_ms),
           "--job-jitter", str(args.job_jitter), "--fail-pct", str(args.fail_pct), "--hb-interval", str(args.hb_interval),
           "--api-threads", str(args.api_threads), "--sync", args.sync, "--seed", str(args.seed),
           "--max-jobs-in-memory", str(args.max_jobs_in_memory)]
    if args.no_instrument:
        cmd.append("--no-instrument")
    log = open(os.path.join(workdir, "server.log"), "w")
    print(f"[bench] starting server with {args.nodes} nodes ... (workdir {workdir})", flush=True)
    proc = subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT, cwd=REPO_ROOT)
    try:
        t0 = time.time()
        while not os.path.exists(ready):
            if proc.poll() is not None:
                print(open(os.path.join(workdir, "server.log")).read()[-3000:])
                print("[bench] server exited during startup", file=sys.stderr)
                return 2
            if time.time() - t0 > 180:
                print("[bench] server did not become ready in 180s", file=sys.stderr)
                return 2
            time.sleep(0.2)
        probe = Api(port)
        for _ in range(100):
            code, info = probe.json("GET", "/__bench/info", label="(bench) info")
            if code == 200:
                break
            time.sleep(0.1)
        print(f"[bench] server ready in {time.time() - t0:.1f}s on port {port}; locks instrumented: {len(info['locks_instrumented'])}", flush=True)

        run, load, api = run_client(args, port, info)
        if args.smoke:
            rows = [[r, run["smoke"].get(r, "-")] for r in sorted(run["smoke"])]
            print(table(["route called", "status"], rows))
            print(f"\nroutes on /api/v1: {len(run['routes'])}; 5xx/failed: {run['bad'] or 'none'}")
            return 1 if run["bad"] else 0
        code, stats = api.json("GET", "/__bench/stats", label="(bench) stats")
        if code != 200 or not stats:
            print("[bench] could not fetch server stats", file=sys.stderr)
            return 2
        report = build_report(args, run, load, api, stats, info)
        print_report(report)
        with open(args.out, "w") as f:
            json.dump(report, f, indent=1)
        print(f"[bench] full report written to {args.out}")
        return 1 if report["integrity"]["stuck_jobs"] else 0
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except Exception:
            proc.kill()
        log.close()
        if not args.keep:
            import shutil
            shutil.rmtree(workdir, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
