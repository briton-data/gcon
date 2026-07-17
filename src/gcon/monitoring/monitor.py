import os
import psutil
from datetime import datetime, UTC


class ResourceMonitor:
    """
    Collect live resource usage for a GCON node.

    IMPORTANT: this reports metrics for the *current OS process*
    (via psutil.Process), not the whole host. The previous
    implementation used psutil.cpu_percent()/virtual_memory(), which
    are whole-machine statistics -- when multiple simulated
    GCONAgent nodes run inside a single test/dev process (as they do
    throughout this codebase's test suite), every node reported the
    exact same host-wide numbers, making the scheduler's load-based
    node selection (Scheduler.select_node, which weights cpu/memory)
    effectively noise: two "different" nodes always looked equally
    loaded regardless of which one was actually doing work.

    In a real deployment where each GCONAgent runs as its own OS
    process, psutil.Process(os.getpid()) correctly reflects that
    node's own usage. Process-level cpu_percent() also has its own
    caveat: the first call after process start (or after a long gap)
    returns 0.0/None because psutil needs two samples to compute a
    delta -- callers should not treat an initial 0.0 reading as
    "idle", only later readings.
    """

    def __init__(self, agent):
        self.agent = agent
        self._process = psutil.Process(os.getpid())
        # Prime the internal sample so the first real collect() call
        # returns a meaningful (non-zero-by-construction) delta
        # instead of always reporting 0.0 for the very first reading.
        self._process.cpu_percent(interval=None)

    def collect(self):
        return {
            "node_id": self.agent.node_id,
            "cpu": self._process.cpu_percent(interval=None),
            "memory": self._process.memory_percent(),
            "running_jobs": 1 if self.agent.status == "busy" else 0,
            "status": self.agent.status,
            "timestamp": datetime.now(UTC).isoformat()
        }