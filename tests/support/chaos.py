"""
chaos.py — fault-injection / chaos-engineering harness for GCON.

Targets the REAL classes (GCONCoordinator, agent.GCONAgent,
NodeRegistry) as they exist today. Because GCON currently runs every
"node" in-process (AUDIT_REPORT.md section 4), "killing a worker" here
means killing its underlying OS subprocess and/or forcibly desyncing
its in-memory status — the closest approximation available until a
real multi-process/multi-machine transport exists. Each action notes
which real-world failure it approximates and which audit finding it is
designed to surface.
"""

from __future__ import annotations

import random
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, UTC
from typing import Callable, Dict, List, Optional

from .test_utils import get_logger

logger = get_logger("chaos", log_file="logs/chaos.log")


@dataclass
class ChaosEvent:
    action: str
    target: str
    timestamp: str = field(default_factory=lambda: datetime.now(UTC).isoformat())
    detail: Optional[str] = None
    succeeded: bool = True


class ChaosMonkey:
    """
    Drives randomized and directed fault injection against a running
    GCONCoordinator instance. Every action is logged to `self.history`
    so a test can assert "recovery happened within N seconds of THIS
    specific injected fault" instead of just "the system seems fine now".
    """

    def __init__(self, coordinator, seed: Optional[int] = None):
        self.coordinator = coordinator
        self.history: List[ChaosEvent] = []
        self._lock = threading.Lock()
        self._rng = random.Random(seed)
        self._running = False
        self._thread: Optional[threading.Thread] = None

    def _log_event(self, action: str, target: str, detail: str = None,
                    succeeded: bool = True) -> None:
        event = ChaosEvent(action=action, target=target, detail=detail, succeeded=succeeded)
        with self._lock:
            self.history.append(event)
        level = logger.info if succeeded else logger.warning
        level(f"[CHAOS] {action} target={target} detail={detail} ok={succeeded}")

    # -----------------------------------------------------------------
    # Node-level chaos
    # -----------------------------------------------------------------

    def kill_worker(self, node_id: str) -> None:
        """
        Approximates a hard node crash: kills any in-flight subprocess
        the node's agent owns, then force-desyncs the registry's view
        by simply *not* sending any further heartbeats for it (the
        caller is responsible for having stopped that node's heartbeat
        thread, e.g. via `agent.stop_heartbeat()`, before calling this,
        or for using `kill_random_worker` which does both).
        Exercises: AUDIT_REPORT.md 4.2/4.6 (failure detection latency)
        and 2.7 (duplicate-execution-on-recovery gap).
        """
        try:
            node = self.coordinator.registry.get_node(node_id)
        except ValueError as e:
            self._log_event("kill_worker", node_id, detail=str(e), succeeded=False)
            return

        killed_process = False
        agent = getattr(node, "agent", node)  # GCONAgent is used directly as "node" today
        if hasattr(agent, "cancel"):
            killed_process = agent.cancel()
        if hasattr(agent, "stop_heartbeat"):
            agent.stop_heartbeat()

        self._log_event("kill_worker", node_id,
                         detail=f"process_killed={killed_process}")

    def kill_random_worker(self) -> Optional[str]:
        nodes = self.coordinator.registry.list_nodes()
        if not nodes:
            self._log_event("kill_random_worker", "-", detail="no nodes registered",
                             succeeded=False)
            return None
        target = self._rng.choice(nodes)
        self.kill_worker(target)
        return target

    def kill_busy_worker(self) -> Optional[str]:
        """
        Specifically targets a node that is mid-job — the scenario most
        likely to expose AUDIT_REPORT.md 2.3/2.7 (jobs left orphaned or
        double-executed on recovery), versus killing an idle node which
        is comparatively uninteresting.
        """
        snapshot = self.coordinator.registry.snapshot()
        busy = [nid for nid, info in snapshot.items() if info["status"] == "busy"]
        if not busy:
            self._log_event("kill_busy_worker", "-", detail="no busy nodes",
                             succeeded=False)
            return None
        target = self._rng.choice(busy)
        self.kill_worker(target)
        return target

    def restart_worker(self, node) -> None:
        """
        Re-registers a previously killed node object, simulating a
        crashed node coming back online. Exercises whether the
        coordinator correctly reclaims it as idle capacity, and whether
        any job it "recovers into" collides with one already reassigned
        elsewhere (AUDIT_REPORT.md 2.7).
        """
        try:
            self.coordinator.register_agent(node)
            self._log_event("restart_worker", node.node_id)
        except ValueError as e:
            self._log_event("restart_worker", node.node_id, detail=str(e),
                             succeeded=False)

    # -----------------------------------------------------------------
    # Coordinator-level chaos
    # -----------------------------------------------------------------

    def kill_coordinator_scheduler_thread(self) -> None:
        """
        Cannot cleanly "kill" a running daemon thread from outside in
        Python — instead this simulates the failure mode described in
        AUDIT_REPORT.md 2.5 (an uncaught exception type escaping
        scheduler_loop and silently ending it) by pausing the scheduler
        AND asserting it should be detectable via health_service. This
        is the closest safe approximation; a true kill requires either
        (a) running the coordinator as a real subprocess and SIGKILLing
        it (only meaningful once section 4's real-transport work lands),
        or (b) monkeypatching scheduler_loop to raise, which is provided
        as `crash_scheduler_loop` below for direct-injection tests.
        """
        self.coordinator.pause_scheduler()
        self._log_event("kill_coordinator_scheduler_thread", "coordinator",
                         detail="scheduler paused (see docstring for caveats)")

    def crash_scheduler_loop(self) -> None:
        """
        Directly exercises AUDIT_REPORT.md 2.5: monkeypatches
        `scheduler.select_node` to raise a non-RuntimeError exception on
        its next call, which — per the audit finding — is NOT caught by
        scheduler_loop's narrow `except RuntimeError`, killing the
        thread. A correctly-fixed coordinator should survive this call
        (i.e. the scheduler thread should still be alive afterward);
        today's coordinator will not.
        """
        original = self.coordinator.scheduler.select_node

        def boom():
            self.coordinator.scheduler.select_node = original
            raise TypeError("chaos-injected non-RuntimeError exception")

        self.coordinator.scheduler.select_node = boom
        self._log_event("crash_scheduler_loop", "coordinator",
                         detail="select_node will raise TypeError on next call")

    def flood_job_queue(self, count: int = 10_000,
                         command: str = "python3 -c \"pass\"") -> None:
        """
        Submits `count` trivial jobs as fast as possible with no
        artifacts, to exercise queue-saturation behavior
        (AUDIT_REPORT.md 6.2) and lock contention on `self.jobs`
        (2.2) under sustained submission pressure.
        """
        from .test_utils import unique_id
        for _ in range(count):
            job_id = unique_id("chaos-flood")
            try:
                self.coordinator.submit_job(job_id, command)
            except Exception as e:
                self._log_event("flood_job_queue", job_id, detail=str(e),
                                 succeeded=False)
        self._log_event("flood_job_queue", "coordinator", detail=f"submitted {count} jobs")

    def corrupt_node_registry_entry(self, node_id: str) -> None:
        """
        Directly injects an inconsistent registry entry (status set to
        an unrecognized string) to exercise AUDIT_REPORT.md 8.4 — there
        is no enum/validation anywhere preventing this, so this action
        should always "succeed" against today's code, which is itself
        the finding: nothing rejects it.
        """
        try:
            info = self.coordinator.registry.get_node_info(node_id)
            with self.coordinator.registry._lock:
                info["status"] = "definitely-not-a-real-status"
            self._log_event("corrupt_node_registry_entry", node_id)
        except ValueError as e:
            self._log_event("corrupt_node_registry_entry", node_id, detail=str(e),
                             succeeded=False)

    # -----------------------------------------------------------------
    # Resource starvation
    # -----------------------------------------------------------------

    def starve_cpu(self, duration_seconds: float = 5.0, threads: int = None) -> None:
        """
        Pins CPU with busy-loop threads for `duration_seconds`, to see
        whether heartbeat threads / scheduler_loop / health_check_loop
        keep their timing guarantees under real contention (relevant to
        AUDIT_REPORT.md 4.6 — fixed, non-adaptive heartbeat timeout with
        no tolerance for legitimate transient slowness).
        """
        import os
        threads = threads or (os.cpu_count() or 4)
        stop = threading.Event()

        def burn():
            while not stop.is_set():
                pass

        workers = [threading.Thread(target=burn, daemon=True) for _ in range(threads)]
        for w in workers:
            w.start()
        self._log_event("starve_cpu", "host", detail=f"{threads} burner threads for {duration_seconds}s")
        time.sleep(duration_seconds)
        stop.set()
        for w in workers:
            w.join(timeout=1)

    def starve_memory(self, target_mb: int = 256, duration_seconds: float = 5.0) -> None:
        """
        Allocates and holds `target_mb` of memory for `duration_seconds`
        to observe coordinator behavior (GC pauses, thread scheduling
        delays) under memory pressure — relevant given the unbounded
        growth issues in AUDIT_REPORT.md 6.2/6.3: this simulates what
        the system will eventually do to itself without intervention.
        """
        chunk = bytearray(target_mb * 1024 * 1024)
        self._log_event("starve_memory", "host", detail=f"{target_mb}MB held for {duration_seconds}s")
        time.sleep(duration_seconds)
        del chunk

    # -----------------------------------------------------------------
    # Randomized continuous chaos loop
    # -----------------------------------------------------------------

    ACTIONS: Dict[str, Callable[["ChaosMonkey"], None]] = {
        "kill_random_worker": lambda self: self.kill_random_worker(),
        "kill_busy_worker": lambda self: self.kill_busy_worker(),
        "corrupt_random_registry_entry": lambda self: self._corrupt_random(),
        "starve_cpu_brief": lambda self: self.starve_cpu(duration_seconds=1.0),
    }

    def _corrupt_random(self):
        nodes = self.coordinator.registry.list_nodes()
        if nodes:
            self.corrupt_node_registry_entry(self._rng.choice(nodes))

    def start_random_chaos(self, interval_range: tuple = (2.0, 8.0),
                            actions: Optional[List[str]] = None) -> None:
        """
        Continuously fires a random action from `actions` (default: all
        registered ACTIONS) at random intervals, on a background thread,
        until `stop_random_chaos()` is called. Intended to run alongside
        a stress_test.py workload so failures are injected concurrently
        with real traffic, not in isolation.
        """
        actions = actions or list(self.ACTIONS)
        self._running = True

        def loop():
            while self._running:
                action_name = self._rng.choice(actions)
                try:
                    self.ACTIONS[action_name](self)
                except Exception as e:
                    self._log_event(action_name, "-", detail=f"raised {e!r}", succeeded=False)
                time.sleep(self._rng.uniform(*interval_range))

        self._thread = threading.Thread(target=loop, daemon=True)
        self._thread.start()
        logger.info(f"[CHAOS] random chaos loop started (actions={actions})")

    def stop_random_chaos(self) -> None:
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)
        logger.info("[CHAOS] random chaos loop stopped")

    def report(self) -> Dict:
        with self._lock:
            history = list(self.history)
        by_action: Dict[str, int] = {}
        for e in history:
            by_action[e.action] = by_action.get(e.action, 0) + 1
        return {
            "total_events": len(history),
            "by_action": by_action,
            "failures": [e.__dict__ for e in history if not e.succeeded],
        }
